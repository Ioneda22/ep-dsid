"""CLI interativa do peer — input()/print() puros.

Única camada do sistema autorizada a usar print (§4.3). Comandos: help,
upload, search, download, list, remove, peers, playlist
(create/add/remove/show/list/delete), status e quit.

Operação por NOME legível: download, remove e peers aceitam o nome da música
(ou o nº da última busca, ou o hash), não só o hash SHA-256. A resolução
nome→hash das operações de rede continua vindo de um search anterior na mesma
sessão (resolução em dois passos da especificação); para arquivos locais
(list/status/remove) o nome vem do NameRegistry persistido no storage.

Histórico e edição de linha (setas ↑/↓) ficam disponíveis onde a stdlib
readline existe (Linux/macOS/Git Bash); em Windows nativo degrada para input
puro sem quebrar. Cores via src.peer.console (desligam fora de um terminal).
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from src.common.hashing import is_valid_sha256
from src.peer import console
from src.peer.tracker_client import TodosTrackersIndisponiveis

try:  # histórico/edição de linha onde a stdlib oferece readline
    import readline  # noqa: F401
except ImportError:  # Windows nativo: input puro, sem histórico
    pass

if TYPE_CHECKING:
    from src.common.messages import SearchResult, SearchResultEntry
    from src.peer.downloader import Downloader
    from src.peer.name_registry import NameRegistry
    from src.peer.playlist_store import PlaylistStore
    from src.peer.storage import Storage
    from src.peer.tracker_client import PeerTrackerClient

logger = logging.getLogger(__name__)

#: Apelidos aceitos → comando canônico.
_ALIASES = {
    "?": "help",
    "h": "help",
    "q": "quit",
    "exit": "quit",
    "ls": "list",
    "l": "list",
    "s": "search",
    "dl": "download",
    "get": "download",
    "rm": "remove",
    "st": "status",
    "cls": "clear",
}


def _parse_id(valor: str) -> int | None:
    """Converte um id de playlist textual para int; None se inválido."""
    try:
        return int(valor)
    except ValueError:
        return None


_AJUDA = """\
Comandos disponíveis (aceitam nome da música, não só hash):
  help / ?                          lista os comandos
  upload <caminho>                  registra um arquivo de áudio na rede
  search <nome>                     busca por nome legível (sem arg: repete a última)
  download <nome | nº | hash>       baixa um arquivo (faça search antes)
  list / ls                         lista arquivos completos neste peer
  remove <nome | hash>              remove arquivo local e notifica o tracker
  peers <nome | nº | hash>          fontes de um arquivo (faça search antes)
  playlist create <nome>            cria uma playlist
  playlist add <id> <hash>          adiciona um hash à playlist
  playlist remove <id> <hash>       remove um hash da playlist
  playlist show <id>                mostra o conteúdo de uma playlist
  playlist list                     lista suas playlists
  playlist delete <id>              apaga uma playlist
  status / st                       tracker atual, trackers, arquivos, downloads
  clear / cls                       limpa a tela do terminal
  quit / exit                       sai ordenadamente (envia PEER_LEAVE)"""


class PeerCLI:
    """Loop de comandos do peer; roda na thread principal."""

    def __init__(
        self,
        nome_peer: str,
        storage: Storage,
        tracker_client: PeerTrackerClient,
        downloader: Downloader,
        name_registry: NameRegistry,
        playlist_store: PlaylistStore,
    ) -> None:
        """Recebe as dependências por parâmetro (injeção via construtor)."""
        self.nome_peer = nome_peer
        self.storage = storage
        self.tracker_client = tracker_client
        self.downloader = downloader
        self.name_registry = name_registry
        self.playlists = playlist_store
        #: Entradas da última busca, na ordem exibida (para nome/nº → hash).
        self._ultima_busca: list[SearchResultEntry] = []
        self._ultimo_termo = ""
        #: Serializa a escrita da barra de progresso (callback vem do pool).
        self._progress_lock = threading.Lock()

    def run(self) -> None:
        """Lê e despacha comandos até quit/EOF/Ctrl+C."""
        print(
            console.titulo(f"PeerSpot — peer '{self.nome_peer}'.")
            + " Digite 'help' para ver os comandos."
        )
        while True:
            try:
                linha = input(console.prompt(f"{self.nome_peer}> ")).strip()
            except (EOFError, KeyboardInterrupt):
                print()
                return
            if not linha:
                continue
            if not self._despachar(linha):
                return

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _despachar(self, linha: str) -> bool:
        """Executa um comando; False encerra o loop (quit)."""
        comando, _, resto = linha.partition(" ")
        comando = _ALIASES.get(comando, comando)
        resto = resto.strip()
        try:
            match comando:
                case "help":
                    print(_AJUDA)
                case "upload":
                    self._cmd_upload(resto)
                case "search":
                    self._cmd_search(resto)
                case "download":
                    self._cmd_download(resto)
                case "list":
                    self._cmd_list()
                case "remove":
                    self._cmd_remove(resto)
                case "peers":
                    self._cmd_peers(resto)
                case "playlist":
                    self._cmd_playlist(resto)
                case "status":
                    self._cmd_status()
                case "clear":
                    self._cmd_clear()
                case "quit":
                    return False
                case _:
                    print(
                        console.aviso(
                            f"Comando desconhecido: '{comando}'. Veja 'help'."
                        )
                    )
        except TodosTrackersIndisponiveis:
            print(console.erro("Todos os trackers estão indisponíveis (veja o log)."))
        return True

    def _cmd_clear(self) -> None:
        """Limpa a tela do terminal (no-op se a saída não é um terminal).

        Usa o comando nativo do SO (cls/clear) em vez de sequências ANSI, para
        funcionar mesmo em consoles sem VT habilitado.
        """
        if not sys.stdout.isatty():
            return
        os.system("cls" if sys.platform == "win32" else "clear")

    # ------------------------------------------------------------------
    # Resolução nome → hash
    # ------------------------------------------------------------------

    def _resolver_busca(self, arg: str) -> SearchResultEntry | None:
        """Casa um nome/nº/hash com uma entrada da última busca."""
        if not self._ultima_busca:
            print(console.aviso("Faça 'search <nome>' antes."))
            return None
        if arg.isdigit():
            return self._entrada_por_indice(int(arg))
        exata = next((e for e in self._ultima_busca if e.hash == arg), None)
        if exata is not None:
            return exata
        return self._entrada_por_nome(arg)

    def _entrada_por_indice(self, indice: int) -> SearchResultEntry | None:
        if 1 <= indice <= len(self._ultima_busca):
            return self._ultima_busca[indice - 1]
        print(console.aviso(f"Índice fora do intervalo: {indice}."))
        return None

    def _entrada_por_nome(self, arg: str) -> SearchResultEntry | None:
        correspondencias = [
            e for e in self._ultima_busca if e.nome.casefold() == arg.casefold()
        ]
        if not correspondencias:
            print(console.aviso(f"'{arg}' não está na última busca."))
            return None
        if len(correspondencias) > 1:
            print(console.aviso(f"'{arg}' é ambíguo; use o nº ou o hash:"))
            for e in correspondencias:
                print(f"  {console.dim(e.hash)}")
            return None
        return correspondencias[0]

    def _resolver_local(self, arg: str) -> str | None:
        """Casa um nome/hash com um arquivo COMPLETO neste peer; devolve o hash."""
        locais = self.storage.list_local_files()
        if arg in locais:
            return arg
        correspondencias = [
            h for h in self.name_registry.hashes_por_nome(arg) if h in locais
        ]
        if not correspondencias:
            print(console.aviso(f"Nenhum arquivo local chamado '{arg}'."))
            return None
        if len(correspondencias) > 1:
            print(console.aviso(f"'{arg}' é ambíguo localmente; use o hash:"))
            for h in correspondencias:
                print(f"  {console.dim(h)}")
            return None
        return correspondencias[0]

    # ------------------------------------------------------------------
    # Comandos
    # ------------------------------------------------------------------

    def _cmd_upload(self, caminho: str) -> None:
        if not caminho:
            print("Uso: upload <caminho>")
            return
        origem = Path(caminho).expanduser()
        if not origem.is_file():
            print(console.erro(f"Arquivo não encontrado: {origem}"))
            return

        hash_arquivo, tamanho, n_chunks = self.storage.import_file(origem)
        resposta = self.tracker_client.register_file(
            self.nome_peer,
            hash_arquivo,
            nome=origem.stem,
            tamanho=tamanho,
            n_chunks=n_chunks,
        )
        if resposta is None:
            print(console.erro("Falha ao registrar no tracker (veja o log)."))
            return
        self.name_registry.registrar(hash_arquivo, origem.stem)
        tam = self._formatar_tamanho(tamanho)
        print(
            console.ok(
                f"Upload registrado: '{origem.stem}' — {tam} ({n_chunks} chunks)"
            )
        )
        print(f"  {console.dim(hash_arquivo)}")

    def _cmd_search(self, nome: str) -> None:
        if not nome:
            if self._ultima_busca:
                self._exibir_resultados(self._ultimo_termo, self._ultima_busca)
                return
            print("Uso: search <nome>")
            return
        resultado = self.tracker_client.search_file(nome, str(uuid.uuid4()))
        if resultado is None:
            print(console.erro("Busca falhou: tracker indisponível (veja o log)."))
            return
        if not resultado.resultados:
            resultado = self._recuperar_indice_e_rebuscar(nome, resultado)
        if not resultado.resultados:
            print(f"Nenhum resultado para '{nome}'.")
            return
        self._ultima_busca = list(resultado.resultados)
        self._ultimo_termo = nome
        self._exibir_resultados(nome, self._ultima_busca)

    def _recuperar_indice_e_rebuscar(
        self, nome: str, vazio: SearchResult
    ) -> SearchResult:
        """Busca vazia + arquivos locais: re-anuncia e refaz a busca uma vez.

        Cobre o caso em que TODOS os trackers caíram e um voltou com o índice
        vazio: o SEED_REPORT periódico só carrega hashes (não reconstrói
        nome→hash), então a busca falha mesmo com o arquivo local. O
        REGISTER_FILE leva os metadados e reconstrói o índice; a busca é
        refeita uma vez. Devolve o resultado vazio original se nada mudou.
        """
        if not self.storage.list_local_files() or not self._reregistrar_locais():
            return vazio
        novo = self.tracker_client.search_file(nome, str(uuid.uuid4()))
        return novo if novo is not None else vazio

    def _reregistrar_locais(self) -> int:
        """Reapresenta o peer e re-registra seus arquivos locais (REGISTER_FILE).

        Idempotente. Restaura o índice de um tracker que perdeu o estado sem
        tocar no protocolo — usa só PEER_HELLO e REGISTER_FILE, que já carrega
        nome/tamanho/n_chunks. Só re-registra arquivos cujo nome legível é
        conhecido (NameRegistry).

        Returns:
            Quantos arquivos foram re-registrados com sucesso.
        """
        self.tracker_client.reenviar_hello()
        registrados = 0
        for hash_arquivo in self.storage.list_local_files():
            nome = self.name_registry.nome(hash_arquivo)
            if nome is None:
                continue
            tamanho = self.storage.assembled_path(hash_arquivo).stat().st_size
            n_chunks = self.storage.get_chunk_count(hash_arquivo)
            resposta = self.tracker_client.register_file(
                self.nome_peer,
                hash_arquivo,
                nome=nome,
                tamanho=tamanho,
                n_chunks=n_chunks,
            )
            if resposta is not None:
                registrados += 1
        return registrados

    def _exibir_resultados(
        self, termo: str, resultados: list[SearchResultEntry]
    ) -> None:
        print(console.titulo(f"{len(resultados)} resultado(s) para '{termo}':"))
        for i, entrada in enumerate(resultados, start=1):
            fontes = ", ".join(p.nome_peer for p in entrada.peers) or "(nenhuma)"
            print(
                f"  {console.destaque(str(i))}. {entrada.nome}"
                f"  [{entrada.n_chunks} chunks]  fontes: {fontes}"
            )
            print(f"     {console.dim(entrada.hash)}")
        print(console.dim("Use: download <nome | nº>"))

    def _cmd_download(self, arg: str) -> None:
        if not arg:
            print("Uso: download <nome | nº | hash>")
            return
        entrada = self._resolver_busca(arg)
        if entrada is None:
            return

        print(
            f"Baixando {console.destaque(entrada.nome)} ({entrada.n_chunks} chunks)..."
        )
        caminho = self.downloader.download_file(
            entrada.hash, entrada.nome, on_progress=self._barra_progresso
        )
        if caminho is None:
            print(console.erro("Download falhou (veja o log)."))
            return
        self.name_registry.registrar(entrada.hash, entrada.nome)
        print(console.ok(f"Download concluído: {caminho}"))

    def _barra_progresso(self, recebidos: int, total: int) -> None:
        """Renderiza uma barra in-place; chamado das threads do pool (com lock)."""
        largura = 24
        preenchido = largura * recebidos // total if total else largura
        barra = "#" * preenchido + "-" * (largura - preenchido)
        pct = 100 * recebidos // total if total else 100
        with self._progress_lock:
            fim = "\n" if recebidos >= total else ""
            print(f"\r  [{barra}] {recebidos}/{total} ({pct}%)", end=fim, flush=True)

    def _cmd_list(self) -> None:
        hashes = self.storage.list_local_files()
        if not hashes:
            print("Nenhum arquivo completo neste peer.")
            return
        print(console.titulo(f"{len(hashes)} arquivo(s) completo(s):"))
        for hash_arquivo in hashes:
            nome = self.name_registry.nome(hash_arquivo) or "(sem nome)"
            tam = self._formatar_tamanho(
                self.storage.assembled_path(hash_arquivo).stat().st_size
            )
            print(f"  {nome}  {tam}")
            print(f"    {console.dim(hash_arquivo)}")

    def _cmd_remove(self, arg: str) -> None:
        if not arg:
            print("Uso: remove <nome | hash>")
            return
        hash_arquivo = self._resolver_local(arg)
        if hash_arquivo is None:
            return

        nome = self.name_registry.nome(hash_arquivo) or hash_arquivo
        self.storage.remove_file(hash_arquivo)
        self.name_registry.esquecer(hash_arquivo)
        if self.tracker_client.peer_leave_file(self.nome_peer, hash_arquivo) is None:
            print(
                console.aviso("Removido localmente, mas o tracker não foi notificado.")
            )
            return
        print(console.ok(f"Removido e tracker notificado: {nome}"))

    def _cmd_peers(self, arg: str) -> None:
        if not arg:
            print("Uso: peers <nome | nº | hash>")
            return
        entrada = self._resolver_busca(arg)
        if entrada is None:
            return

        # Refaz a busca para obter fontes frescas do tracker (não o cache).
        resultado = self.tracker_client.search_file(entrada.nome, str(uuid.uuid4()))
        atual = None
        if resultado is not None:
            atual = next(
                (e for e in resultado.resultados if e.hash == entrada.hash), None
            )
        if atual is None or not atual.peers:
            print(f"Nenhuma fonte ativa para {console.destaque(entrada.nome)}.")
            return
        print(console.titulo(f"{len(atual.peers)} fonte(s) de '{entrada.nome}':"))
        for p in atual.peers:
            print(f"  {p.nome_peer}  {p.ip}:{p.porta}")

    # ------------------------------------------------------------------
    # Playlists
    # ------------------------------------------------------------------

    def _cmd_playlist(self, resto: str) -> None:
        sub, _, args = resto.partition(" ")
        args = args.strip()
        match sub:
            case "create":
                self._playlist_create(args)
            case "add":
                self._playlist_item(args, remover=False)
            case "remove":
                self._playlist_item(args, remover=True)
            case "show":
                self._playlist_show(args)
            case "list":
                self._playlist_list()
            case "delete":
                self._playlist_delete(args)
            case _:
                print("Uso: playlist create|add|remove|show|list|delete ...")

    def _playlist_create(self, nome: str) -> None:
        if not nome:
            print("Uso: playlist create <nome>")
            return
        playlist_id = self.playlists.criar(nome)
        print(console.ok(f"Playlist '{nome}' criada com id {playlist_id}."))

    def _playlist_item(self, args: str, *, remover: bool) -> None:
        acao = "remove" if remover else "add"
        id_str, _, hash_arquivo = args.partition(" ")
        hash_arquivo = hash_arquivo.strip()
        playlist_id = _parse_id(id_str)
        if playlist_id is None or not hash_arquivo:
            print(f"Uso: playlist {acao} <id> <hash>")
            return
        if not remover and not is_valid_sha256(hash_arquivo):
            print(console.erro(f"Hash inválido: {hash_arquivo!r} (esperado 64 hex)."))
            return
        if remover:
            ok = self.playlists.remover_item(playlist_id, hash_arquivo)
            acao_ok = "Item removido"
        else:
            ok = self.playlists.adicionar_item(playlist_id, hash_arquivo)
            acao_ok = "Item adicionado"
        if not ok:
            print(console.aviso(f"Playlist {playlist_id} não existe."))
            return
        print(console.ok(f"{acao_ok} na playlist {playlist_id}."))

    def _playlist_show(self, id_str: str) -> None:
        playlist_id = _parse_id(id_str)
        if playlist_id is None:
            print("Uso: playlist show <id>")
            return
        playlist = self.playlists.obter(playlist_id)
        if playlist is None:
            print(console.aviso(f"Playlist {playlist_id} não encontrada."))
            return
        print(console.titulo(f"Playlist {playlist_id}: '{playlist['nome']}'"))
        itens: list[str] = playlist["itens"]  # type: ignore[assignment]
        if not itens:
            print("  (vazia)")
            return
        for i, hash_arquivo in enumerate(itens, start=1):
            nome = self.name_registry.nome(hash_arquivo)
            rotulo = f"{nome}  {console.dim(hash_arquivo)}" if nome else hash_arquivo
            print(f"  {i}. {rotulo}")

    def _playlist_list(self) -> None:
        playlists = self.playlists.listar()
        if not playlists:
            print("Você não tem playlists.")
            return
        print(console.titulo(f"{len(playlists)} playlist(s):"))
        for pl in playlists:
            print(f"  [{pl['id']}] {pl['nome']}")

    def _playlist_delete(self, id_str: str) -> None:
        playlist_id = _parse_id(id_str)
        if playlist_id is None:
            print("Uso: playlist delete <id>")
            return
        if not self.playlists.deletar(playlist_id):
            print(console.aviso(f"Playlist {playlist_id} não existe."))
            return
        print(console.ok(f"Playlist {playlist_id} deletada."))

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def _cmd_status(self) -> None:
        # Marcadores ASCII (não Unicode) para não quebrar o print em consoles
        # cp1252 (Windows) quando a saída não é um terminal UTF-8.
        saude = (
            console.ok("[online]")
            if self.tracker_client.health()
            else console.erro("[offline]")
        )
        print(console.titulo("Status do peer"))
        print(
            f"  Tracker atual: {self.tracker_client.tracker_id}"
            f" ({self.tracker_client.tracker_endereco}) {saude}"
        )
        print(
            "  Trackers conhecidos: "
            + ", ".join(self.tracker_client.trackers_conhecidos)
        )
        locais = self.storage.list_local_files()
        print(f"  Arquivos locais ({len(locais)}):")
        for hash_arquivo in locais:
            nome = self.name_registry.nome(hash_arquivo) or "(sem nome)"
            print(f"    {nome}  {console.dim(hash_arquivo)}")
        self._exibir_downloads()

    def _exibir_downloads(self) -> None:
        andamento = self._downloads_em_andamento()
        if not andamento:
            print("  Downloads em andamento: nenhum")
            return
        print("  Downloads em andamento:")
        for hash_arquivo, recebidos in andamento:
            nome = self.name_registry.nome(hash_arquivo) or console.dim(hash_arquivo)
            print(f"    {nome}: {recebidos} chunk(s) baixados")

    def _downloads_em_andamento(self) -> list[tuple[str, int]]:
        """Hashes com chunks no rastreador mas ainda sem arquivo montado."""
        pendentes = []
        for hash_arquivo, chunks in self.downloader.chunk_manager.downloads.items():
            if not self.storage.assembled_path(hash_arquivo).exists():
                pendentes.append((hash_arquivo, len(chunks)))
        return pendentes

    @staticmethod
    def _formatar_tamanho(n_bytes: int) -> str:
        """Formata bytes em unidade legível (B/KB/MB/GB/TB/PB)."""
        tamanho = float(n_bytes)
        for unidade in ("B", "KB", "MB", "GB", "TB"):
            if tamanho < 1024:
                casas = 0 if unidade == "B" else 1
                return f"{tamanho:.{casas}f} {unidade}"
            tamanho /= 1024
        return f"{tamanho:.1f} PB"
