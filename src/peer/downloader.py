"""Orquestrador de download — SEQUENCIAL na Fase 3 (§9; paralelo na Fase 5).

Fluxo (§7.4 do CLAUDE.md):

1. ``SEARCH_FILE`` no tracker (busca por nome — o tracker resolve
   nome → hash → peers, main.tex "Mecanismo de Resolução de Nomes").
   O ``SEARCH_RESULT`` traz o ``n_chunks`` de cada hash (Listing 7.2).
2. ``CHUNK_LIST_REQUEST`` a cada peer fonte.
3. Baixa chunk por chunk, em ordem; qualquer fonte que tenha o chunk
   serve; em falha tenta a próxima fonte. Se todas falharem, retenta o
   chunk após um pequeno intervalo, até ``max_tentativas``.
4. Monta o arquivo, valida o SHA-256 e re-registra com ``REGISTER_FILE``.

Retomada: em falha de download os chunks já gravados são MANTIDOS no
disco. Uma nova tentativa reconcilia o progresso a partir do disco
(``storage.has_chunk``) — não da memória — e baixa apenas o que falta,
mesmo após reinício do processo.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from src.common.errors import InvalidHashError
from src.common.messages import SearchResultEntry, SearchResultPeer

if TYPE_CHECKING:
    from src.peer.chunk_manager import ChunkManager
    from src.peer.storage import Storage
    from src.peer.tcp_client import PeerTCPClient
    from src.peer.tracker_client import PeerTrackerClient

logger = logging.getLogger(__name__)

#: Mapa fonte → chunks anunciados: ``(ip, porta) -> [indices]``.
ChunksPorFonte = dict[tuple[str, int], list[int]]


class Downloader:
    """Coordena busca, transferência sequencial, retomada e re-registro."""

    def __init__(
        self,
        nome_peer: str,
        tracker_client: PeerTrackerClient,
        tcp_client: PeerTCPClient,
        storage: Storage,
        chunk_manager: ChunkManager,
        max_tentativas: int = 3,
        retry_delay_seconds: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Recebe todas as dependências por parâmetro (§14.4).

        Args:
            nome_peer: Nome deste peer (excluído das fontes; re-registro).
            tracker_client: Cliente REST do tracker.
            tcp_client: Cliente TCP peer↔peer.
            storage: Armazenamento local de chunks/arquivos.
            chunk_manager: Rastreador de progresso em memória.
            max_tentativas: Tentativas por chunk quando todas as fontes
                falham (cada tentativa percorre todas as fontes de novo).
            retry_delay_seconds: Espera entre tentativas de um mesmo chunk.
            sleep: Função de espera, injetável em testes (§10).
        """
        self.nome_peer = nome_peer
        self.tracker_client = tracker_client
        self.tcp_client = tcp_client
        self.storage = storage
        self.chunk_manager = chunk_manager
        self.max_tentativas = max_tentativas
        self.retry_delay_seconds = retry_delay_seconds
        self._sleep = sleep

    def download_file(self, hash_arquivo: str, nome_musica: str) -> Path | None:
        """Baixa ``hash_arquivo`` e devolve o caminho do arquivo montado.

        Args:
            hash_arquivo: Hash SHA-256 escolhido pelo usuário.
            nome_musica: Nome legível associado (vindo da busca anterior),
                usado para refazer o ``SEARCH_FILE`` e obter fontes frescas.

        Returns:
            Caminho do arquivo completo, ou ``None`` em falha (já logada).
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

        logger.info(
            "download de %s: %d chunks, %d fontes",
            hash_arquivo,
            entrada.n_chunks,
            len(fontes),
        )
        if not self._baixar_sequencial(
            hash_arquivo, entrada.n_chunks, chunks_por_fonte
        ):
            return None
        return self._finalizar(hash_arquivo, entrada.n_chunks)

    # ------------------------------------------------------------------
    # Etapas
    # ------------------------------------------------------------------

    def _buscar_entrada(
        self, hash_arquivo: str, nome_musica: str
    ) -> SearchResultEntry | None:
        """SEARCH_FILE no tracker; seleciona a entrada do hash pedido."""
        resultado = self.tracker_client.search_file(nome_musica, str(uuid.uuid4()))
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
        """CHUNK_LIST_REQUEST a cada fonte; ignora as sem chunks/sem resposta."""
        chunks_por_fonte: ChunksPorFonte = {}
        for fonte in fontes:
            indices = self.tcp_client.request_chunk_list(
                fonte.ip, fonte.porta, hash_arquivo
            )
            if indices:
                chunks_por_fonte[(fonte.ip, fonte.porta)] = indices
                logger.info(
                    "fonte %s (%s:%d) tem %d chunks",
                    fonte.nome_peer,
                    fonte.ip,
                    fonte.porta,
                    len(indices),
                )
        return chunks_por_fonte

    def _reconciliar_progresso(self, hash_arquivo: str, n_chunks: int) -> None:
        """Marca como recebidos os chunks que já estão no disco (retomada).

        A fonte de verdade do progresso é o disco, não a memória: assim a
        retomada funciona inclusive após reinício do processo.
        """
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

    def _baixar_sequencial(
        self, hash_arquivo: str, n_chunks: int, chunks_por_fonte: ChunksPorFonte
    ) -> bool:
        """Baixa os chunks faltantes em ordem, um por vez (Fase 3)."""
        self._reconciliar_progresso(hash_arquivo, n_chunks)
        for chunk_index in self.chunk_manager.missing_chunks(hash_arquivo, n_chunks):
            if not self._baixar_chunk_com_retry(
                hash_arquivo, chunk_index, chunks_por_fonte
            ):
                # Chunks já gravados ficam no disco para retomada futura.
                logger.error(
                    "chunk %d de %s esgotou %d tentativas; download abortado "
                    "(progresso mantido para retomada)",
                    chunk_index,
                    hash_arquivo,
                    self.max_tentativas,
                )
                return False
        return True

    def _baixar_chunk_com_retry(
        self, hash_arquivo: str, chunk_index: int, chunks_por_fonte: ChunksPorFonte
    ) -> bool:
        """Retenta o chunk após pequena espera quando todas as fontes falham."""
        for tentativa in range(1, self.max_tentativas + 1):
            if self._baixar_chunk(hash_arquivo, chunk_index, chunks_por_fonte):
                return True
            if tentativa < self.max_tentativas:
                logger.warning(
                    "chunk %d de %s falhou (tentativa %d/%d); aguardando %.1fs",
                    chunk_index,
                    hash_arquivo,
                    tentativa,
                    self.max_tentativas,
                    self.retry_delay_seconds,
                )
                self._sleep(self.retry_delay_seconds)
        return False

    def _baixar_chunk(
        self, hash_arquivo: str, chunk_index: int, chunks_por_fonte: ChunksPorFonte
    ) -> bool:
        """Tenta cada fonte que anunciou o chunk até uma servir (§7.4)."""
        for (ip, porta), indices in chunks_por_fonte.items():
            if chunk_index not in indices:
                continue
            dados = self.tcp_client.download_chunk(ip, porta, hash_arquivo, chunk_index)
            if dados is None:
                continue
            self.storage.save_chunk(hash_arquivo, chunk_index, dados)
            self.chunk_manager.mark_received(hash_arquivo, chunk_index)
            return True
        return False

    def _finalizar(self, hash_arquivo: str, n_chunks: int) -> Path | None:
        """Monta + valida SHA-256; re-registra no tracker como nova fonte."""
        try:
            caminho = self.storage.assemble_file(hash_arquivo, n_chunks)
        except (FileNotFoundError, InvalidHashError):
            # Conteúdo corrompido: aqui sim descarta tudo (§7.4 passo 6) —
            # reter chunks que não batem com o hash não permite retomada.
            logger.exception("validação falhou para %s; descartando", hash_arquivo)
            self.storage.remove_file(hash_arquivo)
            self.chunk_manager.reset(hash_arquivo)
            return None

        self.chunk_manager.reset(hash_arquivo)
        if self.tracker_client.register_file(self.nome_peer, hash_arquivo) is None:
            logger.warning(
                "re-registro de %s falhou; arquivo local mantido", hash_arquivo
            )
        logger.info("download concluído: %s -> %s", hash_arquivo, caminho)
        return caminho
