"""Digest de versões periódico entre trackers.

A detecção inline de lacunas (SYNC_TABLE fora de sequência) tem um ponto
cego: se a ÚLTIMA escrita de um tracker se perde e ele fica em silêncio, não vem
um seq posterior para revelar o buraco. Esta thread fecha essa borda: a cada
interval_seconds (folgado, 5 min) faz push de um SYNC_DIGEST — apenas o
vetor de versões — a todos os trackers conhecidos. Quem recebe compara componente
a componente e puxa via SYNC_PULL o que o emissor tiver a mais. O custo é
O(n_trackers), não O(índice): nada do estado viaja no digest.

O intervalo deve ficar ABAIXO da retenção dos tombstones (600s / 10 min) para
repor uma remoção perdida antes de o tombstone expirar.

Espelha o padrão de thread do TombstoneReaper: a classe só dá o ritmo; toda a
lógica de estado vive no Index (versoes) e no SyncClient
(propagar_digest), o que mantém tudo testável sem dormir.
"""

from __future__ import annotations

import logging
import threading

from src.tracker.index import Index
from src.tracker.sync_client import SyncClient

logger = logging.getLogger(__name__)

#: Intervalo do digest: 5 min. DEVE ser menor que a retenção dos tombstones
#: (600s / 10 min) para repor uma remoção perdida antes de o tombstone expirar.
DIGEST_INTERVAL = 300.0


class DigestBroadcaster:
    """Thread daemon que faz push periódico de SYNC_DIGEST (backstop).

    Exemplo:
        >>> broadcaster = DigestBroadcaster("tracker-1", index, sync_client)
        >>> broadcaster.start()
        ...
        >>> broadcaster.stop()
    """

    def __init__(
        self,
        tracker_id: str,
        index: Index,
        sync_client: SyncClient,
        interval_seconds: float = DIGEST_INTERVAL,
    ) -> None:
        """Args:
        tracker_id: Identificador deste tracker (para logs).
        index: Índice cujo vetor de versões (versoes) é anunciado.
        sync_client: Cliente de flooding (faz o push paralelo do digest).
        interval_seconds: Intervalo entre digests. Deve ser menor que
            tombstone_retention_seconds para uma remoção perdida ser
            reposta antes de o tombstone expirar.
        """
        self.tracker_id = tracker_id
        self.index = index
        self.sync_client = sync_client
        self.interval_seconds = interval_seconds
        self._parar = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Dispara a thread de digest periódico."""
        self._thread = threading.Thread(
            target=self._loop,
            name=f"digest-{self.tracker_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Sinaliza parada e aguarda a thread encerrar."""
        self._parar.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def enviar_digest_agora(self) -> None:
        """Faz um push imediato de SYNC_DIGEST (inicialização e testes)."""
        versoes = self.index.versoes()
        self.sync_client.propagar_digest(versoes)
        logger.debug(
            "tracker_id=%s SYNC_DIGEST propagado: versoes=%s",
            self.tracker_id,
            versoes,
        )

    def _loop(self) -> None:
        # wait() em vez de sleep(): acorda imediatamente no stop().
        while not self._parar.wait(self.interval_seconds):
            self.enviar_digest_agora()
