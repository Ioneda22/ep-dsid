"""CLI interativa do peer — input()/print() puros.

Única camada do sistema autorizada a usar print.
Comandos: help, upload, search, download, list,
remove, peers, playlist (create/add/remove/show/list/delete),
status e quit.

O comando download <hash> depende de uma busca anterior na mesma
sessão: é a busca que associa o hash ao nome legível (resolução em dois
passos nome->hash).
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from src.peer.tracker_client import TodosTrackersIndisponiveis

if TYPE_CHECKING:
    from src.common.messages import SearchResultEntry
    from src.peer.downloader import Downloader
    from src.peer.storage import Storage
    from src.peer.tracker_client import PeerTrackerClient

logger = logging.getLogger(__name__)


def _parse_id(valor: str) -> int | None:
    """Converte um id de playlist textual para int; None se inválido."""
    try:
        return int(valor)
    except ValueError:
        return None


_AJUDA = """\
Comandos disponíveis:
  help                              lista os comandos
  upload <caminho>                  registra um arquivo de áudio na rede
  search <nome>                     busca por nome legível
  download <hash>                   baixa um arquivo (faça search antes)
  list                              lista arquivos completos neste peer
  remove <hash>                     remove arquivo local e notifica o tracker
  peers <hash>                      fontes de um hash (faça search antes)
  playlist create <nome>            cria uma playlist
  playlist add <id> <hash>          adiciona um hash à playlist
  playlist remove <id> <hash>       remove um hash da playlist
  playlist show <id>                mostra o conteúdo de uma playlist
  playlist list                     lista suas playlists
  playlist delete <id>              apaga uma playlist
  status                            tracker atual, trackers, arquivos, downloads
  quit                              sai ordenadamente (envia PEER_LEAVE)"""


class PeerCLI:
    """Loop de comandos do peer; roda na thread principal."""

    def __init__(
        self,
        nome_peer: str,
        storage: Storage,
        tracker_client: PeerTrackerClient,
        downloader: Downloader,
    ) -> None:
        """Recebe as dependências por parâmetro (injeção via construtor)."""
        self.nome_peer = nome_peer
        self.storage = storage
        self.tracker_client = tracker_client
        self.downloader = downloader
        #: hash -> entrada da última busca (nome legível p/ download).
        self._resultados_busca: dict[str, SearchResultEntry] = {}

    def run(self) -> None:
        """Lê e despacha comandos até quit/EOF/Ctrl+C."""
        print(f"PeerSpot — peer '{self.nome_peer}'. Digite 'help' para começar.")
        while True:
            try:
                linha = input(f"{self.nome_peer}> ").strip()
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
                case "quit":
                    return False
                case _:
                    print(f"Comando desconhecido: '{comando}'. Digite 'help'.")
        except TodosTrackersIndisponiveis:
            print("Todos os trackers estão indisponíveis (veja o log).")
        return True

    # ------------------------------------------------------------------
    # Comandos
    # ------------------------------------------------------------------

    def _cmd_upload(self, caminho: str) -> None:
        if not caminho:
            print("Uso: upload <caminho>")
            return
        origem = Path(caminho).expanduser()
        if not origem.is_file():
            print(f"Arquivo não encontrado: {origem}")
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
            print("Falha ao registrar no tracker (veja o log).")
            return
        print(f"Upload registrado: '{origem.stem}' ({n_chunks} chunks)")
        print(f"  hash: {hash_arquivo}")

    def _cmd_search(self, nome: str) -> None:
        if not nome:
            print("Uso: search <nome>")
            return
        resultado = self.tracker_client.search_file(nome, str(uuid.uuid4()))
        if resultado is None:
            print("Busca falhou: tracker indisponível (veja o log).")
            return
        if not resultado.resultados:
            print(f"Nenhum resultado para '{nome}'.")
            return

        print(f"{len(resultado.resultados)} resultado(s) para '{nome}':")
        for i, entrada in enumerate(resultado.resultados, start=1):
            self._resultados_busca[entrada.hash] = entrada
            fontes = ", ".join(p.nome_peer for p in entrada.peers)
            print(f"  {i}. {entrada.nome}  [{entrada.n_chunks} chunks]")
            print(f"     hash: {entrada.hash}")
            print(f"     fontes: {fontes}")
        print("Use: download <hash>")

    def _cmd_download(self, hash_arquivo: str) -> None:
        if not hash_arquivo:
            print("Uso: download <hash>")
            return
        entrada = self._resultados_busca.get(hash_arquivo)
        if entrada is None:
            print("Hash desconhecido nesta sessão — faça 'search <nome>' antes.")
            return

        print(f"Baixando '{entrada.nome}' ({entrada.n_chunks} chunks)...")
        caminho = self.downloader.download_file(hash_arquivo, entrada.nome)
        if caminho is None:
            print("Download falhou (veja o log).")
            return
        print(f"Download concluído: {caminho}")

    def _cmd_list(self) -> None:
        hashes = self.storage.list_local_files()
        if not hashes:
            print("Nenhum arquivo completo neste peer.")
            return
        print(f"{len(hashes)} arquivo(s) completo(s):")
        for hash_arquivo in hashes:
            tamanho = self.storage.assembled_path(hash_arquivo).stat().st_size
            print(f"  {hash_arquivo}  ({tamanho} bytes)")

    def _cmd_remove(self, hash_arquivo: str) -> None:
        if not hash_arquivo:
            print("Uso: remove <hash>")
            return
        if hash_arquivo not in self.storage.list_local_files():
            print(f"Hash não encontrado localmente: {hash_arquivo}")
            return

        self.storage.remove_file(hash_arquivo)
        if self.tracker_client.peer_leave_file(self.nome_peer, hash_arquivo) is None:
            print("Removido localmente, mas o tracker não foi notificado (veja o log).")
            return
        print(f"Removido e tracker notificado: {hash_arquivo}")

    def _cmd_peers(self, hash_arquivo: str) -> None:
        if not hash_arquivo:
            print("Uso: peers <hash>")
            return
        entrada = self._resultados_busca.get(hash_arquivo)
        if entrada is None:
            print("Hash desconhecido nesta sessão — faça 'search <nome>' antes.")
            return

        # Refaz a busca para obter fontes frescas do tracker (não o cache).
        resultado = self.tracker_client.search_file(entrada.nome, str(uuid.uuid4()))
        atual = None
        if resultado is not None:
            atual = next(
                (e for e in resultado.resultados if e.hash == hash_arquivo), None
            )
        if atual is None or not atual.peers:
            print(f"Nenhuma fonte ativa para {hash_arquivo}.")
            return
        print(f"{len(atual.peers)} fonte(s) de {hash_arquivo}:")
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
        playlist_id = self.tracker_client.criar_playlist(self.nome_peer, nome)
        if playlist_id is None:
            print("Falha ao criar playlist (veja o log).")
            return
        print(f"Playlist '{nome}' criada com id {playlist_id}.")

    def _playlist_item(self, args: str, *, remover: bool) -> None:
        acao = "remove" if remover else "add"
        id_str, _, hash_arquivo = args.partition(" ")
        hash_arquivo = hash_arquivo.strip()
        playlist_id = _parse_id(id_str)
        if playlist_id is None or not hash_arquivo:
            print(f"Uso: playlist {acao} <id> <hash>")
            return
        if remover:
            resposta = self.tracker_client.remover_item_playlist(
                playlist_id, hash_arquivo
            )
            sucesso = "Item removido" if resposta else None
        else:
            resposta = self.tracker_client.adicionar_item_playlist(
                playlist_id, hash_arquivo
            )
            sucesso = "Item adicionado" if resposta else None
        if sucesso is None:
            print("Operação falhou — a playlist existe? (veja o log).")
            return
        print(f"{sucesso} na playlist {playlist_id}.")

    def _playlist_show(self, id_str: str) -> None:
        playlist_id = _parse_id(id_str)
        if playlist_id is None:
            print("Uso: playlist show <id>")
            return
        playlist = self.tracker_client.obter_playlist(playlist_id)
        if playlist is None:
            print(f"Playlist {playlist_id} não encontrada.")
            return
        print(
            f"Playlist {playlist_id}: '{playlist['nome']}' (dono: {playlist['dono']})"
        )
        itens = playlist["itens"]
        if not itens:
            print("  (vazia)")
            return
        for i, hash_arquivo in enumerate(itens, start=1):
            print(f"  {i}. {hash_arquivo}")

    def _playlist_list(self) -> None:
        playlists = self.tracker_client.listar_playlists(self.nome_peer)
        if playlists is None:
            print("Falha ao listar playlists (veja o log).")
            return
        if not playlists:
            print("Você não tem playlists.")
            return
        print(f"{len(playlists)} playlist(s):")
        for pl in playlists:
            print(f"  [{pl['id']}] {pl['nome']}")

    def _playlist_delete(self, id_str: str) -> None:
        playlist_id = _parse_id(id_str)
        if playlist_id is None:
            print("Uso: playlist delete <id>")
            return
        if self.tracker_client.deletar_playlist(playlist_id) is None:
            print("Falha ao deletar playlist (veja o log).")
            return
        print(f"Playlist {playlist_id} deletada.")

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def _cmd_status(self) -> None:
        print(f"Tracker atual: {self.tracker_client.tracker_id}")
        print("Trackers conhecidos:")
        for tid in self.tracker_client.trackers_conhecidos:
            print(f"  - {tid}")
        locais = self.storage.list_local_files()
        print(f"Arquivos locais ({len(locais)}):")
        for hash_arquivo in locais:
            print(f"  {hash_arquivo}")
        andamento = self._downloads_em_andamento()
        if not andamento:
            print("Downloads em andamento: nenhum")
            return
        print("Downloads em andamento:")
        for hash_arquivo, recebidos in andamento:
            print(f"  {hash_arquivo}: {recebidos} chunk(s) baixados")

    def _downloads_em_andamento(self) -> list[tuple[str, int]]:
        """Hashes com chunks no rastreador mas ainda sem arquivo montado."""
        pendentes = []
        for hash_arquivo, chunks in self.downloader.chunk_manager.downloads.items():
            if not self.storage.assembled_path(hash_arquivo).exists():
                pendentes.append((hash_arquivo, len(chunks)))
        return pendentes
