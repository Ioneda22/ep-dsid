"""Expiração periódica de tombstones do índice.

Uma thread dedicada varre Index.tombstones a cada interval_seconds
(default 60s) e descarta os registros com idade maior que
tombstone_retention_seconds (default 600s = 10 min). A
varredura em si é src.tracker.index.Index.expire_tombstones, que
roda sob o lock do índice e usa o relógio injetado — esta thread só dá o
ritmo, o que mantém a lógica testável sem dormir.
"""

from __future__ import annotations

import logging
import threading

from src.tracker.index import Index

logger = logging.getLogger(__name__)


class TombstoneReaper:
    """Thread daemon que expira tombstones periodicamente.

    Exemplo:
        >>> reaper = TombstoneReaper("tracker-1", index, retention_seconds=600)
        >>> reaper.start()
        ...
        >>> reaper.stop()
    """

    def __init__(
        self,
        tracker_id: str,
        index: Index,
        retention_seconds: float = 600.0,
        interval_seconds: float = 60.0,
    ) -> None:
        self.tracker_id = tracker_id
        self.index = index
        self.retention_seconds = retention_seconds
        self.interval_seconds = interval_seconds
        self._parar = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Dispara a thread de varredura."""
        self._thread = threading.Thread(
            target=self._loop,
            name=f"tombstone-reaper-{self.tracker_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Sinaliza parada e aguarda a thread encerrar."""
        self._parar.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        # wait() em vez de sleep(): acorda imediatamente no stop().
        while not self._parar.wait(self.interval_seconds):
            removidos = self.index.expire_tombstones(self.retention_seconds)
            if removidos:
                logger.info(
                    "tracker_id=%s expirou %d tombstone(s) com idade > %.0fs",
                    self.tracker_id,
                    removidos,
                    self.retention_seconds,
                )
