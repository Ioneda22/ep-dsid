from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.peer.tracker_client import PeerTrackerClient

logger = logging.getLogger(__name__)


class ReassignPoller:
    def __init__(
        self,
        nome_peer: str,
        tracker_client: PeerTrackerClient,
        interval_seconds: int = 20,
    ) -> None:
        self.nome_peer = nome_peer
        self.tracker_client = tracker_client
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            logger.warning("reassign poller já iniciado")
            return
        self._thread = threading.Thread(
            target=self._run, name="reassign-poller", daemon=True
        )
        self._thread.start()
        logger.info("reassign poller iniciado (intervalo=%ds)", self.interval_seconds)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def poll_agora(self) -> None:
        try:
            self.tracker_client.poll_reassign(self.nome_peer)
        except Exception:  # noqa: BLE001
            logger.exception("reassign poller: falha ao consultar migração pendente")

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            self.poll_agora()
