"""Orquestrador de download PARALELO entre múltiplas fontes.

Fluxo:

1. SEARCH_FILE no tracker (busca por nome — o tracker resolve
   nome → hash → peers). O SEARCH_RESULT traz o n_chunks de cada hash.
2. CHUNK_LIST_REQUEST a cada fonte EM PARALELO → mapa
   chunk_index → [fontes que têm].
3. Plano de download: para cada chunk faltante, escolhe como fonte primária a
   que tem MENOS chunks já atribuídos (balanceamento rarest-first simplificado),
   guardando as demais como fallback.
4. ThreadPoolExecutor com download_pool_size workers baixa os chunks;
   cada worker tenta a primária e, em falha, as outras fontes que têm o chunk.
5. Timeout por CHUNK_REQUEST = chunk_request_timeout_seconds (no cliente).
6. Chunk sem nenhuma fonte que sirva → o download falha (sem retransmissão).
7. Ao completar: assemble_file valida o SHA-256 e re-registra via
   REGISTER_FILE.

Retomada: chunks já no disco (storage.has_chunk) são reconciliados a partir
do disco antes do pool — o download baixa só o que falta, mesmo após reinício.
"""

from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

from src.common.errors import InvalidHashError
from src.peer.tracker_client import TodosTrackersIndisponiveis

if TYPE_CHECKING:
    from src.common.messages import SearchResultEntry, SearchResultPeer
    from src.peer.chunk_manager import ChunkManager
    from src.peer.storage import Storage
    from src.peer.tcp_client import PeerTCPClient
    from src.peer.tracker_client import PeerTrackerClient

logger = logging.getLogger(__name__)

#: Endereço de uma fonte: (ip, porta).
Fonte = tuple[str, int]
#: Mapa fonte → chunks anunciados.
ChunksPorFonte = dict[Fonte, list[int]]
#: Plano de download: chunk_index -> fontes ordenadas (primária primeiro).
PlanoDownload = dict[int, list[Fonte]]


class Downloader:
    """Coordena busca, download paralelo entre fontes, retomada e re-registro."""

    def __init__(
        self,
        nome_peer: str,
        tracker_client: PeerTrackerClient,
        tcp_client: PeerTCPClient,
        storage: Storage,
        chunk_manager: ChunkManager,
        download_pool_size: int = 8,
    ) -> None:
        """Recebe todas as dependências por parâmetro.

        Args:
            nome_peer: Nome deste peer (excluído das fontes; re-registro).
            tracker_client: Cliente REST do tracker (com fallback).
            tcp_client: Cliente TCP peer↔peer (thread-safe).
            storage: Armazenamento local de chunks/arquivos.
            chunk_manager: Rastreador de progresso.
            download_pool_size: Nº de workers do pool de download (padrão 8).
        """
        self.nome_peer = nome_peer
        self.tracker_client = tracker_client
        self.tcp_client = tcp_client
        self.storage = storage
        self.chunk_manager = chunk_manager
        self.download_pool_size = download_pool_size

    def download_file(self, hash_arquivo: str, nome_musica: str) -> Path | None:
        """Baixa hash_arquivo e devolve o caminho do arquivo montado.

        Args:
            hash_arquivo: Hash SHA-256 escolhido pelo usuário.
            nome_musica: Nome legível associado (da busca anterior), usado para
                refazer o SEARCH_FILE e obter fontes frescas.

        Returns:
            Caminho do arquivo completo, ou None em falha (já logada).
        """
        montado = self.storage.assembled_path(hash_arquivo)
        if montado.exists():
            logger.info("%s já está completo localmente", hash_arquivo)
            return montado

        entrada = self._buscar_entrada(hash_arquivo, nome_musica)
        if entrada is None:
            return None

        fontes = [p for p in entrada.peers if p.nome_peer != self.nome_peer]
        if not fontes:
            logger.error("nenhuma fonte além deste peer para %s", hash_arquivo)
            return None

        chunks_por_fonte = self._consultar_chunk_lists(hash_arquivo, fontes)
        if not chunks_por_fonte:
            logger.error("nenhuma fonte respondeu CHUNK_LIST para %s", hash_arquivo)
            return None

        self._reconciliar_progresso(hash_arquivo, entrada.n_chunks)
        faltantes = self.chunk_manager.missing_chunks(hash_arquivo, entrada.n_chunks)
        plano = self._planejar(faltantes, chunks_por_fonte)
        if plano is None:
            return None  # algum chunk sem fonte que o tenha

        logger.info(
            "download paralelo de %s: %d chunk(s) faltando, %d fonte(s), pool=%d",
            hash_arquivo,
            len(faltantes),
            len(chunks_por_fonte),
            self.download_pool_size,
        )
        if not self._baixar_paralelo(hash_arquivo, plano):
            return None
        return self._finalizar(hash_arquivo, entrada.n_chunks)

    # ------------------------------------------------------------------
    # Busca e descoberta de chunks
    # ------------------------------------------------------------------

    def _buscar_entrada(
        self, hash_arquivo: str, nome_musica: str
    ) -> SearchResultEntry | None:
        """SEARCH_FILE no tracker; seleciona a entrada do hash pedido."""
        try:
            resultado = self.tracker_client.search_file(nome_musica, str(uuid.uuid4()))
        except TodosTrackersIndisponiveis:
            logger.error("busca falhou: todos os trackers indisponíveis")
            return None
        if resultado is None:
            logger.error("busca falhou no tracker (query=%r)", nome_musica)
            return None
        for entrada in resultado.resultados:
            if entrada.hash == hash_arquivo:
                return entrada
        logger.error(
            "hash %s não consta nos resultados de %r", hash_arquivo, nome_musica
        )
        return None

    def _consultar_chunk_lists(
        self, hash_arquivo: str, fontes: list[SearchResultPeer]
    ) -> ChunksPorFonte:
        """CHUNK_LIST_REQUEST a cada fonte EM PARALELO; ignora as sem chunks."""
        chunks_por_fonte: ChunksPorFonte = {}

        def consultar(fonte: SearchResultPeer) -> tuple[Fonte, list[int] | None]:
            indices = self.tcp_client.request_chunk_list(
                fonte.ip, fonte.porta, hash_arquivo
            )
            return (fonte.ip, fonte.porta), indices

        with ThreadPoolExecutor(max_workers=max(1, len(fontes))) as executor:
            for (ip, porta), indices in executor.map(consultar, fontes):
                if indices:
                    chunks_por_fonte[(ip, porta)] = indices
                    logger.info("fonte %s:%d tem %d chunk(s)", ip, porta, len(indices))
        return chunks_por_fonte

    def _reconciliar_progresso(self, hash_arquivo: str, n_chunks: int) -> None:
        """Marca como recebidos os chunks já no disco (retomada, fonte = disco)."""
        self.chunk_manager.start_download(hash_arquivo, n_chunks)
        recuperados = 0
        for i in range(n_chunks):
            if self.storage.has_chunk(hash_arquivo, i):
                self.chunk_manager.mark_received(hash_arquivo, i)
                recuperados += 1
        if recuperados:
            logger.info(
                "retomada de %s: %d/%d chunks já no disco",
                hash_arquivo,
                recuperados,
                n_chunks,
            )

    # ------------------------------------------------------------------
    # Plano e download paralelo
    # ------------------------------------------------------------------

    def _planejar(
        self, faltantes: list[int], chunks_por_fonte: ChunksPorFonte
    ) -> PlanoDownload | None:
        """Atribui cada chunk faltante à fonte menos carregada (rarest-first).

        Chunks mais raros (menos fontes) são atribuídos primeiro, para não gastar
        a única fonte de um chunk raro com um chunk que outras também têm. A fonte
        primária de cada chunk é a de menor carga atual; as demais viram fallback.

        Returns:
            O plano, ou None se algum chunk não tem nenhuma fonte que o sirva.
        """
        fontes_do_chunk = {
            chunk: [f for f, indices in chunks_por_fonte.items() if chunk in indices]
            for chunk in faltantes
        }
        atribuidos: dict[Fonte, int] = {fonte: 0 for fonte in chunks_por_fonte}
        plano: PlanoDownload = {}
        for chunk in sorted(faltantes, key=lambda c: len(fontes_do_chunk[c])):
            candidatos = fontes_do_chunk[chunk]
            if not candidatos:
                logger.error(
                    "chunk %d não está em nenhuma fonte; download impossível", chunk
                )
                return None
            primaria = min(candidatos, key=lambda f: atribuidos[f])
            atribuidos[primaria] += 1
            plano[chunk] = [primaria, *(f for f in candidatos if f != primaria)]
        return plano

    def _baixar_paralelo(self, hash_arquivo: str, plano: PlanoDownload) -> bool:
        """Baixa os chunks do plano com um pool de download_pool_size workers."""
        with ThreadPoolExecutor(max_workers=self.download_pool_size) as executor:
            futuros = {
                executor.submit(self._baixar_chunk, hash_arquivo, chunk, fontes): chunk
                for chunk, fontes in plano.items()
            }
            for futuro in futuros:
                if not futuro.result():
                    logger.error(
                        "chunk %d de %s falhou em todas as fontes; download abortado "
                        "(progresso mantido para retomada)",
                        futuros[futuro],
                        hash_arquivo,
                    )
                    return False
        return True

    def _baixar_chunk(
        self, hash_arquivo: str, chunk_index: int, fontes: list[Fonte]
    ) -> bool:
        """Baixa um chunk tentando cada fonte em ordem até uma servir."""
        for ip, porta in fontes:
            dados = self.tcp_client.download_chunk(ip, porta, hash_arquivo, chunk_index)
            if dados is None:
                continue
            self.storage.save_chunk(hash_arquivo, chunk_index, dados)
            self.chunk_manager.mark_received(hash_arquivo, chunk_index)
            logger.debug(
                "chunk %d de %s baixado de %s:%d", chunk_index, hash_arquivo, ip, porta
            )
            return True
        return False

    # ------------------------------------------------------------------
    # Finalização
    # ------------------------------------------------------------------

    def _finalizar(self, hash_arquivo: str, n_chunks: int) -> Path | None:
        """Monta + valida SHA-256; re-registra no tracker como nova fonte."""
        try:
            caminho = self.storage.assemble_file(hash_arquivo, n_chunks)
        except (FileNotFoundError, InvalidHashError):
            # Conteúdo corrompido: descarta tudo — reter chunks que
            # não batem com o hash não permite retomada.
            logger.exception("validação falhou para %s; descartando", hash_arquivo)
            self.storage.remove_file(hash_arquivo)
            self.chunk_manager.reset(hash_arquivo)
            return None

        self.chunk_manager.reset(hash_arquivo)
        try:
            re_registro = self.tracker_client.register_file(
                self.nome_peer, hash_arquivo
            )
        except TodosTrackersIndisponiveis:
            re_registro = None
        if re_registro is None:
            logger.warning(
                "re-registro de %s falhou; arquivo local mantido", hash_arquivo
            )
        logger.info("download concluído: %s -> %s", hash_arquivo, caminho)
        return caminho
