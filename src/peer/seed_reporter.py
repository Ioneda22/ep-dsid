"""Seed reporter do peer — envio periódico de SEED_REPORT.

Thread daemon que, a cada interval_seconds (default 180s = 3 min), envia ao
tracker atual um SEED_REPORT com todos os hashes completos deste peer
(storage.list_local_files()). O relatório cumpre duplo papel:
anti-entropy do índice (hash omitido vira tombstone no tracker) e sinal de vida
(ausência por 2 rodadas marca o peer como falho no failure detector).

O envio vai por PeerTrackerClient, que já resolve o fallback entre trackers
— o reporter não conhece a lista de trackers, só dispara o relatório.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.peer.storage import Storage
    from src.peer.tracker_client import PeerTrackerClient

logger = logging.getLogger(__name__)


class SeedReporter:
    """Thread periódica que envia SEED_REPORT com os hashes locais do peer."""

    def __init__(
        self,
        nome_peer: str,
        ip: str,
        porta: int,
        storage: Storage,
        tracker_client: PeerTrackerClient,
        interval_seconds: int = 180,
    ) -> None:
        """Recebe as dependências por parâmetro.

        Args:
            nome_peer: Nome deste peer (identidade no relatório).
            ip: IP local (contato para as fontes).
            porta: Porta do TCP server de chunks deste peer.
            storage: Armazenamento local — fonte dos hashes completos.
            tracker_client: Cliente REST (já com fallback) que envia o relatório.
            interval_seconds: Intervalo entre relatórios (3 minutos).
        """
        self.nome_peer = nome_peer
        self.ip = ip
        self.porta = porta
        self.storage = storage
        self.tracker_client = tracker_client
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
        logger.info("seed reporter iniciado (intervalo=%ds)", self.interval_seconds)

    def stop(self) -> None:
        """Sinaliza o encerramento da thread."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def enviar_agora(self) -> bool:
        """Envia um SEED_REPORT imediato (inicialização e testes).

        Returns:
            True se o tracker respondeu; False em qualquer falha (já
            logada) — nunca levanta, para não derrubar a thread periódica.
        """
        hashes = self.storage.list_local_files()
        try:
            resposta = self.tracker_client.seed_report(
                self.nome_peer, self.ip, self.porta, hashes
            )
        except Exception:  # noqa: BLE001 — thread daemon nunca deve morrer por isto
            logger.exception("seed reporter: falha ao enviar SEED_REPORT")
            return False
        if resposta is None:
            logger.warning("seed reporter: tracker não respondeu ao SEED_REPORT")
            return False
        logger.debug("SEED_REPORT enviado: %d hash(es)", len(hashes))
        return True

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_seconds):
            self.enviar_agora()
