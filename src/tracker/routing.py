"""Roteamento de buscas entre trackers — SEARCH_FORWARD com TTL (§6.4).

Quando a busca local não tem hit e ``ttl > 0``, o tracker de origem envia
``SEARCH_FORWARD(ttl-1)`` em paralelo (uma thread por destino) aos trackers
ainda não consultados para aquele ``query_id`` e agrega os ``SEARCH_RESULT``
que chegarem dentro de ``search_forward_timeout_seconds`` (default 2s);
respostas atrasadas são descartadas.

O ``SEARCH_RESULT`` volta NA MESMA conexão TCP do ``SEARCH_FORWARD``: em
topologia totalmente conectada, a conexão parte do próprio tracker de
origem, então responder nela é "devolver direto ao origem_tracker"
(Listing 7.2) sem precisar de um dispatcher de respostas por query_id.

O receptor (:func:`handle_search_forward`) só busca localmente e NUNCA
re-encaminha: o originador já consulta todos os trackers conhecidos de uma
vez (mesma razão do "não é preciso re-flood" do main.tex §8) e o TTL
controla a profundidade.
"""

from __future__ import annotations

import logging
import queue
import socket
import threading
import time
from collections import OrderedDict

from pydantic import ValidationError

from src.common.messages import (
    SearchFile,
    SearchForward,
    SearchResult,
    SearchResultEntry,
    SearchResultPeer,
)
from src.common.protocol import MessageReader, ProtocolError, send_json_line
from src.tracker.index import Index
from src.tracker.sync_client import KnownTracker

logger = logging.getLogger(__name__)

#: Limite do cache LRU de query_id (proteção contra crescimento sem fim).
_QUERY_CACHE_MAXSIZE = 1024


class QueryForwardCache:
    """Cache LRU de ``query_id → set[tracker_id_já_consultado]`` (§6.4).

    Evita laços: um mesmo ``query_id`` nunca é re-encaminhado ao mesmo
    tracker, mesmo que o peer repita a busca. LRU limitado a
    ``maxsize`` entradas; o acesso é protegido por lock próprio.
    """

    def __init__(self, maxsize: int = _QUERY_CACHE_MAXSIZE) -> None:
        self._maxsize = maxsize
        self._lock = threading.Lock()
        self._consultados: OrderedDict[str, set[str]] = OrderedDict()

    def consultados(self, query_id: str) -> set[str]:
        """Trackers já consultados para ``query_id`` (cópia)."""
        with self._lock:
            consultados = self._consultados.get(query_id)
            if consultados is None:
                return set()
            self._consultados.move_to_end(query_id)
            return set(consultados)

    def registrar(self, query_id: str, tracker_ids: set[str]) -> None:
        """Acrescenta trackers ao conjunto consultado de ``query_id``."""
        with self._lock:
            self._consultados.setdefault(query_id, set()).update(tracker_ids)
            self._consultados.move_to_end(query_id)
            while len(self._consultados) > self._maxsize:
                self._consultados.popitem(last=False)


def handle_search_forward(msg: SearchForward, index: Index) -> SearchResult:
    """Atende um ``SEARCH_FORWARD`` recebido: busca local, sem re-encaminhar.

    O TTL controla a profundidade e o originador já consulta todos os
    trackers conhecidos em paralelo — re-encaminhar duplicaria consultas.
    """
    resultados = index.search_by_name(msg.query)
    logger.info(
        "SEARCH_FORWARD: query_id=%s query=%r origem_tracker=%s hits=%d",
        msg.query_id,
        msg.query,
        msg.origem_tracker,
        len(resultados),
    )
    return SearchResult(query_id=msg.query_id, resultados=resultados)


class SearchRouter:
    """Origina ``SEARCH_FORWARD`` quando a busca local não tem hit (§6.4)."""

    def __init__(
        self,
        tracker_id: str,
        known_trackers: list[KnownTracker],
        index: Index,
        timeout_seconds: float = 2.0,
        cache: QueryForwardCache | None = None,
    ) -> None:
        self.tracker_id = tracker_id
        self.known_trackers = list(known_trackers)
        self.index = index
        self.timeout_seconds = timeout_seconds
        self.cache = cache if cache is not None else QueryForwardCache()

    def handle_search_file_with_forwarding(self, msg: SearchFile) -> SearchResult:
        """Busca local primeiro; sem hit e com ``ttl > 0``, consulta os demais.

        Respostas que chegarem após ``timeout_seconds`` são descartadas; se
        nada chegar a tempo, devolve ``resultados=[]`` (main.tex §7.2).
        """
        resultados = self.index.search_by_name(msg.query)
        if resultados or msg.ttl <= 0:
            return SearchResult(query_id=msg.query_id, resultados=resultados)
        alvos = self._alvos_nao_consultados(msg.query_id)
        if not alvos:
            return SearchResult(query_id=msg.query_id, resultados=[])
        forward = SearchForward(
            query_id=msg.query_id,
            query=msg.query,
            ttl=msg.ttl - 1,
            origem_tracker=self.tracker_id,
        )
        agregados = self._coletar_respostas(alvos, forward)
        logger.info(
            "SEARCH_FILE roteada: query_id=%s query=%r alvos=%d hits_remotos=%d",
            msg.query_id,
            msg.query,
            len(alvos),
            len(agregados),
        )
        return SearchResult(query_id=msg.query_id, resultados=agregados)

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _alvos_nao_consultados(self, query_id: str) -> list[KnownTracker]:
        ja_consultados = self.cache.consultados(query_id)
        alvos = [t for t in self.known_trackers if t.tracker_id not in ja_consultados]
        self.cache.registrar(query_id, {t.tracker_id for t in alvos})
        return alvos

    def _coletar_respostas(
        self, alvos: list[KnownTracker], forward: SearchForward
    ) -> list[SearchResultEntry]:
        """Dispara as consultas em paralelo e agrega até o deadline (§6.4)."""
        respostas: queue.Queue[list[SearchResultEntry]] = queue.Queue()
        for alvo in alvos:
            threading.Thread(
                target=self._consultar_tracker,
                args=(alvo, forward, respostas),
                name=f"search-fwd-{alvo.tracker_id}",
                daemon=True,
            ).start()
        deadline = time.monotonic() + self.timeout_seconds
        recebidas: list[SearchResultEntry] = []
        pendentes = len(alvos)
        while pendentes > 0:
            restante = deadline - time.monotonic()
            if restante <= 0:
                break  # respostas atrasadas são descartadas (§6.4)
            try:
                recebidas.extend(respostas.get(timeout=restante))
            except queue.Empty:
                break
            pendentes -= 1
        return _mesclar_por_hash(recebidas)

    def _consultar_tracker(
        self,
        alvo: KnownTracker,
        forward: SearchForward,
        respostas: queue.Queue[list[SearchResultEntry]],
    ) -> None:
        """Envia o SEARCH_FORWARD e aguarda o SEARCH_RESULT na mesma conexão."""
        try:
            with socket.create_connection(
                (alvo.ip, alvo.sync_port), timeout=self.timeout_seconds
            ) as sock:
                send_json_line(sock, forward.model_dump())
                header, _ = MessageReader(sock).recv_message(
                    timeout=self.timeout_seconds
                )
                resultado = SearchResult.model_validate(header)
        except (TimeoutError, ConnectionRefusedError, OSError) as exc:
            logger.warning(
                "tracker_id=%s SEARCH_FORWARD a %s falhou: %s",
                self.tracker_id,
                alvo.tracker_id,
                exc,
            )
            respostas.put([])
            return
        except (ProtocolError, ValidationError):
            logger.exception(
                "tracker_id=%s resposta inválida de %s ao SEARCH_FORWARD",
                self.tracker_id,
                alvo.tracker_id,
            )
            respostas.put([])
            return
        respostas.put(resultado.resultados)


def _mesclar_por_hash(entries: list[SearchResultEntry]) -> list[SearchResultEntry]:
    """Agrega respostas de vários trackers: um hash aparece uma única vez.

    Os índices são réplicas convergentes, então entradas repetidas tendem a
    ser idênticas; por robustez, a união dos peers (por ``nome_peer``) é
    preservada.
    """
    por_hash: OrderedDict[str, SearchResultEntry] = OrderedDict()
    for entry in entries:
        existente = por_hash.get(entry.hash)
        if existente is None:
            por_hash[entry.hash] = entry.model_copy(deep=True)
            continue
        conhecidos = {p.nome_peer for p in existente.peers}
        novos: list[SearchResultPeer] = [
            p for p in entry.peers if p.nome_peer not in conhecidos
        ]
        existente.peers.extend(novos)
    return list(por_hash.values())
