"""CLI interativa do peer — ``input()``/``print()`` puros (§7.2).

Única camada do sistema autorizada a usar ``print`` (§4.3/§14.6).
Comandos da Fase 3: ``help``, ``upload``, ``search``, ``download``,
``list``, ``remove``, ``quit``. Os demais (``peers``, ``playlist``,
``status``) são stubs até as Fases 5/6.

O comando ``download <hash>`` depende de uma busca anterior na mesma
sessão: é a busca que associa o hash ao nome legível (resolução em dois
passos do main.tex §"Mecanismo de Resolução de Nomes").
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

_AJUDA = """\
Comandos disponíveis:
  help                      lista os comandos
  upload <caminho>          registra um arquivo de áudio na rede
  search <nome>             busca por nome legível
  download <hash>           baixa um arquivo (faça search antes)
  list                      lista arquivos completos neste peer
  remove <hash>             remove arquivo local e notifica o tracker
  peers <hash>              [Fase 6] fontes de um hash
  playlist ...              [Fase 6] playlists
  status                    [Fase 6] estado do peer
  quit                      sai ordenadamente (envia PEER_LEAVE)"""


class PeerCLI:
    """Loop de comandos do peer; roda na thread principal (§7.1)."""

    def __init__(
        self,
        nome_peer: str,
        storage: Storage,
        tracker_client: PeerTrackerClient,
        downloader: Downloader,
    ) -> None:
        """Recebe as dependências por parâmetro (§14.4)."""
        self.nome_peer = nome_peer
        self.storage = storage
        self.tracker_client = tracker_client
        self.downloader = downloader
        #: hash -> entrada da última busca (nome legível p/ download).
        self._resultados_busca: dict[str, SearchResultEntry] = {}

    def run(self) -> None:
        """Lê e despacha comandos até ``quit``/EOF/Ctrl+C."""
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
        """Executa um comando; ``False`` encerra o loop (``quit``)."""
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
                case "peers" | "playlist" | "status":
                    print(f"'{comando}' ainda não implementado (Fase 6).")
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
