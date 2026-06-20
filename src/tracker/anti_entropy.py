"""Reconciliação anti-entropy periódica entre trackers (main.tex §Replicação).

O ``SYNC_TABLE`` é incremental e *best-effort*: se um tracker perde um delta
(falha transitória de TCP) mas continua no ar, esse delta some do *flooding* e
ele ficaria desatualizado indefinidamente. Esta thread fecha esse buraco: a
cada ``interval_seconds`` faz *push* do estado completo do índice
(``FULL_SYNC``) a todos os trackers conhecidos. O receptor aplica via LWW
(:meth:`src.tracker.index.Index.apply_full_sync`), que é idempotente — então
reaplicar o estado só repara o que estiver divergente, qualquer que tenha sido
a causa (mensagem perdida, partição curta ou tracker reiniciado).

Espelha o padrão de thread do :class:`src.tracker.tombstone.TombstoneReaper`:
a classe só dá o ritmo; toda a lógica de estado vive no ``Index`` e no
``SyncClient``, o que mantém tudo testável sem dormir (§10 do CLAUDE.md).
"""

from __future__ import annotations

import logging
import threading

from src.tracker.index import Index
from src.tracker.sync_client import SyncClient

logger = logging.getLogger(__name__)


class AntiEntropyReconciler:
    """Thread daemon que faz *push* periódico de ``FULL_SYNC`` (anti-entropy).

    Exemplo:
        >>> reconciler = AntiEntropyReconciler("tracker-1", index, sync_client)
        >>> reconciler.start()
        ...
        >>> reconciler.stop()
    """

    def __init__(
        self,
        tracker_id: str,
        index: Index,
        sync_client: SyncClient,
        interval_seconds: float = 180.0,
    ) -> None:
        """Args:
        tracker_id: Identificador deste tracker (para logs).
        index: Índice cujo snapshot é propagado a cada rodada.
        sync_client: Cliente de flooding (faz o *push* paralelo).
        interval_seconds: Intervalo entre reconciliações. Deve ser bem menor
            que ``tombstone_retention_seconds`` para uma remoção perdida ser
            reposta antes de o tombstone expirar (main.tex §Replicação).
        """
        self.tracker_id = tracker_id
        self.index = index
        self.sync_client = sync_client
        self.interval_seconds = interval_seconds
        self._parar = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Dispara a thread de reconciliação."""
        self._thread = threading.Thread(
            target=self._loop,
            name=f"anti-entropy-{self.tracker_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Sinaliza parada e aguarda a thread encerrar."""
        self._parar.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def reconciliar_agora(self) -> None:
        """Executa um ciclo de *push* imediato (inicialização e testes §10)."""
        snapshot = self.index.get_snapshot()
        self.sync_client.propagar_full_sync(snapshot)
        logger.debug("tracker_id=%s anti-entropy: FULL_SYNC propagado", self.tracker_id)

    def _loop(self) -> None:
        # wait() em vez de sleep(): acorda imediatamente no stop().
        while not self._parar.wait(self.interval_seconds):
            self.reconciliar_agora()
