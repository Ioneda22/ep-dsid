"""Detecção de falha de peer por ausência de ``SEED_REPORT`` (§6.3 do CLAUDE.md).

Uma thread dedicada varre ``Index.nome_peer_to_endereco`` a cada
``interval_seconds`` (default 60s) e, para cada peer sem ``SEED_REPORT`` há mais
de ``seed_report_timeout_seconds`` (default 360s = 2 rodadas de 3 min), marca
todas as suas fontes como tombstone e propaga via ``SYNC_TABLE`` (main.tex §13.4).

Espelha o padrão do ``TombstoneReaper``: a classe só dá o ritmo; toda a lógica
de estado (comparação com ``last_seed_ts``, tombstones, alocação de ``seq``) vive
em :meth:`src.tracker.index.Index.detectar_peers_falhos`, sob o relógio injetado
do índice — o que mantém a detecção testável sem dormir (§10).
"""

from __future__ import annotations

import logging
import threading

from src.tracker.index import Index
from src.tracker.sync_client import SyncClient

logger = logging.getLogger(__name__)


class FailureDetector:
    """Thread daemon que tombstona peers silenciosos e propaga a remoção.

    Exemplo:
        >>> detector = FailureDetector("tracker-1", index, sync_client)
        >>> detector.start()
        ...
        >>> detector.stop()
    """

    def __init__(
        self,
        tracker_id: str,
        index: Index,
        sync_client: SyncClient | None = None,
        seed_report_timeout_seconds: float = 360.0,
        interval_seconds: float = 60.0,
    ) -> None:
        """Args:
        tracker_id: Identificador deste tracker (para logs).
        index: Índice varrido em busca de peers silenciosos.
        sync_client: Cliente de flooding para propagar os tombstones; ``None``
            desliga a propagação (tracker isolado/testes).
        seed_report_timeout_seconds: Silêncio máximo tolerado antes do
            tombstone (main.tex §13.4: 6 min = 2 rodadas perdidas).
        interval_seconds: Período da varredura.
        """
        self.tracker_id = tracker_id
        self.index = index
        self.sync_client = sync_client
        self.seed_report_timeout_seconds = seed_report_timeout_seconds
        self.interval_seconds = interval_seconds
        self._parar = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Dispara a thread de varredura."""
        self._thread = threading.Thread(
            target=self._loop,
            name=f"failure-detector-{self.tracker_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Sinaliza parada e aguarda a thread encerrar."""
        self._parar.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def detectar_agora(self) -> list[str]:
        """Executa uma varredura imediata (inicialização e testes §10).

        Returns:
            Nomes dos peers considerados falhos nesta varredura.
        """
        falhos = self.index.detectar_peers_falhos(self.seed_report_timeout_seconds)
        for nome_peer, delta in falhos:
            logger.info(
                "tracker_id=%s peer %s FALHO (>%.0fs sem SEED_REPORT): tombstone de %d fonte(s)",
                self.tracker_id,
                nome_peer,
                self.seed_report_timeout_seconds,
                len(delta.entries) if delta is not None else 0,
            )
            if delta is not None and self.sync_client is not None:
                self.sync_client.propagar_sync(
                    delta.entries, seq=delta.seq, timestamp=delta.timestamp
                )
        return [nome for nome, _ in falhos]

    def _loop(self) -> None:
        # wait() em vez de sleep(): acorda imediatamente no stop().
        while not self._parar.wait(self.interval_seconds):
            self.detectar_agora()
