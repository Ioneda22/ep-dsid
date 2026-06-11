"""Seed reporter — STUB na Fase 3 (implementação real na Fase 5, §9).

A thread daemon já existe para fixar a arquitetura de threads do peer
(§7.1), mas ainda não envia ``SEED_REPORT``: apenas dorme em ciclos de
``interval_seconds`` até :meth:`SeedReporter.stop`.
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)


class SeedReporter:
    """Thread periódica de ``SEED_REPORT`` (corpo vazio até a Fase 5)."""

    def __init__(self, interval_seconds: int = 180) -> None:
        """Args:
        interval_seconds: Intervalo entre relatórios (§7.6: 3 minutos).
        """
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Sobe a thread daemon do reporter."""
        if self._thread is not None:
            logger.warning("seed reporter já iniciado")
            return
        self._thread = threading.Thread(
            target=self._run, name="seed-reporter", daemon=True
        )
        self._thread.start()
        logger.info("seed reporter iniciado (stub — envio real na Fase 5)")

    def stop(self) -> None:
        """Sinaliza o encerramento da thread."""
        self._stop_event.set()

    def _run(self) -> None:
        # TODO(Fase 5): enviar SEED_REPORT com storage.list_local_files().
        while not self._stop_event.wait(self.interval_seconds):
            logger.debug("seed reporter: ciclo sem envio (stub da Fase 3)")
