"""Propagação outbound entre trackers — flooding TCP unicast (§6.1, camada 4).

Espelha o Listing 8.1 do ``main.tex`` (``propagar_sync`` / ``enviar_unicast``)
com ``socket`` + ``threading`` — NUNCA asyncio (§11.2). Em vez das globals
do listing, as dependências entram por construtor (§14.4).

O envio é *transient asynchronous* (main.tex §6.1): uma thread por tracker
conhecido, sem aguardar confirmação. Falha de conexão marca o tracker como
suspeito e NÃO retransmite — a reconciliação fica a cargo do ``SEED_REPORT``
periódico (anti-entropy) e do ``FULL_SYNC`` na reintegração (Fase 5).
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.common.messages import (
    FullSync,
    FullSyncEntry,
    FullSyncPeer,
    FullSyncTracker,
    SyncTable,
    SyncTableEntry,
)
from src.common.protocol import send_json_line
from src.tracker.index import IndexSnapshot

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KnownTracker:
    """Endereço de sincronização de um tracker conhecido (YAML §6.6)."""

    tracker_id: str
    ip: str
    sync_port: int


class SyncClient:
    """Cliente de flooding TCP unicast para os demais trackers.

    Exemplo:
        >>> cliente = SyncClient("tracker-1", [KnownTracker("tracker-2", "127.0.0.1", 9002)])
        >>> cliente.propagar_sync([entry], timestamp=1000.0)  # não bloqueia
    """

    def __init__(
        self,
        tracker_id: str,
        known_trackers: list[KnownTracker],
        timeout_seconds: float = 3.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.tracker_id = tracker_id
        self.known_trackers = list(known_trackers)
        self.timeout_seconds = timeout_seconds
        self._clock = clock
        self._suspeitos_lock = threading.Lock()
        self._suspeitos: set[str] = set()

    # ------------------------------------------------------------------
    # Flooding (Listing 8.1 do main.tex)
    # ------------------------------------------------------------------

    def propagar_sync(
        self, entries: list[SyncTableEntry], timestamp: float | None = None
    ) -> None:
        """Dispara ``SYNC_TABLE`` em paralelo para cada tracker conhecido.

        Uma thread daemon por destino (flooding, Listing 8.1); o chamador
        não bloqueia esperando rede. ``timestamp`` deve ser o MESMO gravado
        no índice local, para que o LWW convirja entre as réplicas; quando
        omitido, usa o relógio corrente.
        """
        if not self.known_trackers:
            return
        mensagem = SyncTable(
            origem=self.tracker_id,
            timestamp=self._clock() if timestamp is None else timestamp,
            entries=entries,
        ).model_dump()
        for tracker in self.known_trackers:
            threading.Thread(
                target=self.enviar_unicast,
                args=(tracker, mensagem),
                name=f"sync-out-{tracker.tracker_id}",
                daemon=True,
            ).start()

    def enviar_unicast(self, tracker: KnownTracker, mensagem: dict[str, Any]) -> bool:
        """Envia uma mensagem JSON one-shot ao ``sync_port`` de um tracker.

        Em timeout/recusa de conexão, marca o tracker como suspeito e NÃO
        retransmite (Listing 8.1): a reconciliação ocorrerá via SEED_REPORT
        ou FULL_SYNC na reintegração.

        Returns:
            ``True`` se a mensagem foi entregue ao socket com sucesso.
        """
        try:
            with socket.create_connection(
                (tracker.ip, tracker.sync_port), timeout=self.timeout_seconds
            ) as sock:
                send_json_line(sock, mensagem)
        except (TimeoutError, ConnectionRefusedError, OSError) as exc:
            logger.warning(
                "tracker_id=%s destino=%s indisponível (%s): marcado suspeito; "
                "reconciliação via SEED_REPORT/FULL_SYNC",
                self.tracker_id,
                tracker.tracker_id,
                exc,
            )
            self.marcar_tracker_suspeito(tracker.tracker_id)
            return False
        self._desmarcar_suspeito(tracker.tracker_id)
        logger.debug(
            "tracker_id=%s enviou %s a %s",
            self.tracker_id,
            mensagem.get("type"),
            tracker.tracker_id,
        )
        return True

    def enviar_full_sync(
        self,
        tracker_destino: KnownTracker,
        snapshot: IndexSnapshot,
        trackers_conhecidos: list[FullSyncTracker] | None = None,
    ) -> bool:
        """Envia o estado completo do índice a um tracker (``FULL_SYNC``).

        Usada na reintegração (Fase 5); a montagem a partir do snapshot já
        fica pronta aqui. Cada arquivo carrega fontes ativas e tombstones
        ainda retidos, ambos com timestamps — o receptor aplica LWW.
        """
        mensagem = FullSync(
            origem=self.tracker_id,
            entries=_entries_do_snapshot(snapshot),
            trackers_conhecidos=trackers_conhecidos or [],
        ).model_dump()
        return self.enviar_unicast(tracker_destino, mensagem)

    def propagar_full_sync(
        self,
        snapshot: IndexSnapshot,
        trackers_conhecidos: list[FullSyncTracker] | None = None,
    ) -> None:
        """Faz *push* do estado completo (``FULL_SYNC``) a cada tracker conhecido.

        É a reconciliação anti-entropy periódica entre trackers (main.tex
        §"Reconciliação anti-entropy"): uma thread daemon por destino,
        *best-effort* igual ao :meth:`propagar_sync` — falha marca o destino
        suspeito e NÃO retransmite, pois a próxima rodada repara. A mensagem
        é montada uma única vez e reusada em todos os envios.
        """
        if not self.known_trackers:
            return
        mensagem = FullSync(
            origem=self.tracker_id,
            entries=_entries_do_snapshot(snapshot),
            trackers_conhecidos=trackers_conhecidos or [],
        ).model_dump()
        for tracker in self.known_trackers:
            threading.Thread(
                target=self.enviar_unicast,
                args=(tracker, mensagem),
                name=f"anti-entropy-{tracker.tracker_id}",
                daemon=True,
            ).start()

    # ------------------------------------------------------------------
    # Suspeitos (marcar_tracker_suspeito do Listing 8.1)
    # ------------------------------------------------------------------

    def marcar_tracker_suspeito(self, tracker_id: str) -> None:
        """Registra que um tracker não respondeu ao último envio."""
        with self._suspeitos_lock:
            self._suspeitos.add(tracker_id)

    def tracker_esta_suspeito(self, tracker_id: str) -> bool:
        """Consulta se um tracker está marcado como suspeito."""
        with self._suspeitos_lock:
            return tracker_id in self._suspeitos

    def trackers_suspeitos(self) -> set[str]:
        """Cópia do conjunto de trackers suspeitos (para status/testes)."""
        with self._suspeitos_lock:
            return set(self._suspeitos)

    def _desmarcar_suspeito(self, tracker_id: str) -> None:
        with self._suspeitos_lock:
            self._suspeitos.discard(tracker_id)


def _entries_do_snapshot(snapshot: IndexSnapshot) -> list[FullSyncEntry]:
    """Converte um ``IndexSnapshot`` nas entries do ``FULL_SYNC`` (Listing 7.2).

    Cada arquivo carrega fontes ativas (``ativo=True``) e tombstones ainda
    retidos (``ativo=False``), ambos com seus timestamps — o receptor aplica
    LWW normalmente.
    """
    entries: list[FullSyncEntry] = []
    for hash_arquivo, meta in snapshot.hash_to_metadata.items():
        peers = [
            FullSyncPeer(
                nome_peer=e.nome_peer,
                ip=e.ip,
                porta=e.porta,
                ativo=True,
                timestamp=e.timestamp,
                origem=e.origem,
            )
            for e in snapshot.hash_to_peers.get(hash_arquivo, {}).values()
        ] + [
            FullSyncPeer(
                nome_peer=t.nome_peer,
                ip=t.ip,
                porta=t.porta,
                ativo=False,
                timestamp=t.timestamp,
                origem=t.origem,
            )
            for t in snapshot.tombstones.get(hash_arquivo, {}).values()
        ]
        entries.append(
            FullSyncEntry(
                hash=hash_arquivo,
                nome=meta.nome,
                tamanho=meta.tamanho,
                n_chunks=meta.n_chunks,
                peers=peers,
            )
        )
    return entries
