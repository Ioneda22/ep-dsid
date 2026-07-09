"""Comunicação outbound entre trackers — flooding, pull e digest.

Usa socket + threading (propagar_sync / enviar_unicast) — nunca
asyncio. As dependências entram por construtor, não por estado global.

O envio de SYNC_TABLE e SYNC_DIGEST é transient asynchronous: uma
thread por tracker conhecido, sem aguardar confirmação. Falha de conexão marca
o tracker como suspeito e NÃO retransmite — quem perdeu o delta o repara
sozinho puxando com SYNC_PULL (detecção inline ou digest periódico). O
SYNC_PULL em si é request/response na MESMA conexão TCP (padrão do
SEARCH_FORWARD): a resposta são uma ou mais SYNC_TABLE.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from src.common.messages import (
    SyncDigest,
    SyncPull,
    SyncPullItem,
    SyncTable,
    SyncTableEntry,
    TrackerList,
    TrackerListItem,
    TrackerRejoin,
)
from src.common.protocol import (
    ConnectionClosedError,
    MessageReader,
    ProtocolError,
    send_json_line,
)
from src.tracker.index import Index

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KnownTracker:
    """Endereço de sincronização de um tracker conhecido (lido do YAML)."""

    tracker_id: str
    ip: str
    sync_port: int


class SyncClient:
    """Cliente TCP para os demais trackers: flooding, digest e pull.

    Exemplo:
        >>> cliente = SyncClient(
        ...     "tracker-1", [KnownTracker("tracker-2", "127.0.0.1", 9002)], index
        ... )
        >>> cliente.propagar_sync([entry], seq=1, timestamp=1000.0)  # nao bloqueia
    """

    def __init__(
        self,
        tracker_id: str,
        known_trackers: list[KnownTracker],
        index: Index | None = None,
        timeout_seconds: float = 3.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.tracker_id = tracker_id
        self.known_trackers = list(known_trackers)
        self.index = index
        self.timeout_seconds = timeout_seconds
        self._clock = clock
        self._suspeitos_lock = threading.Lock()
        self._suspeitos: set[str] = set()

    # ------------------------------------------------------------------
    # Flooding de escritas (SYNC_TABLE)
    # ------------------------------------------------------------------

    def propagar_sync(
        self,
        entries: list[SyncTableEntry],
        seq: int,
        timestamp: float | None = None,
    ) -> None:
        """Dispara SYNC_TABLE em paralelo para cada tracker conhecido.

        Uma thread daemon por destino (flooding); o chamador não bloqueia
        esperando rede. seq é o contador local da escrita e
        timestamp deve ser o MESMO gravado no índice local, para o LWW
        convergir entre as réplicas; quando omitido, usa o relógio corrente.
        """
        if not self.known_trackers:
            return
        mensagem = SyncTable(
            origem=self.tracker_id,
            seq=seq,
            timestamp=self._clock() if timestamp is None else timestamp,
            entries=entries,
        ).model_dump()
        self._floodar(mensagem, rotulo="sync-out")

    def propagar_digest(self, versoes: dict[str, int]) -> None:
        """Floods SYNC_DIGEST (vetor de versões) a cada tracker conhecido.

        One-shot best-effort como propagar_sync — falha marca o destino
        suspeito e não retransmite; o próprio destino, ao comparar o digest,
        puxa o que faltar.
        """
        if not self.known_trackers:
            return
        mensagem = SyncDigest(origem=self.tracker_id, versoes=versoes).model_dump()
        self._floodar(mensagem, rotulo="digest-out")

    def _floodar(self, mensagem: dict[str, Any], *, rotulo: str) -> None:
        """Uma thread daemon por destino, fire-and-forget (flooding paralelo)."""
        for tracker in self.known_trackers:
            threading.Thread(
                target=self.enviar_unicast,
                args=(tracker, mensagem),
                name=f"{rotulo}-{tracker.tracker_id}",
                daemon=True,
            ).start()

    def enviar_unicast(self, tracker: KnownTracker, mensagem: dict[str, Any]) -> bool:
        """Envia uma mensagem JSON one-shot ao sync_port de um tracker.

        Em timeout/recusa de conexão, marca o tracker como suspeito e NÃO
        retransmite: a reconciliação ocorre via SYNC_DIGEST /
        SYNC_PULL puxados pelo próprio destino.

        Returns:
            True se a mensagem foi entregue ao socket com sucesso.
        """
        try:
            with socket.create_connection(
                (tracker.ip, tracker.sync_port), timeout=self.timeout_seconds
            ) as sock:
                send_json_line(sock, mensagem)
        except (TimeoutError, ConnectionRefusedError, OSError) as exc:
            logger.warning(
                "tracker_id=%s destino=%s indisponível (%s): marcado suspeito; "
                "reconciliação via SYNC_DIGEST/SYNC_PULL",
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

    # ------------------------------------------------------------------
    # Reparo direcionado (SYNC_PULL) — request/response na mesma conexão
    # ------------------------------------------------------------------

    def solicitar_pull_de(self, destino_id: str, faltando: list[SyncPullItem]) -> None:
        """Dispara (em thread daemon) um SYNC_PULL a destino_id.

        Fire-and-forget: não bloqueia o handler de sincronização que detectou a
        lacuna. Se o destino não for conhecido, apenas loga (não deveria
        ocorrer na topologia estática).
        """
        destino = self.tracker_por_id(destino_id)
        if destino is None:
            logger.warning(
                "tracker_id=%s não conhece origem %s para SYNC_PULL",
                self.tracker_id,
                destino_id,
            )
            return
        threading.Thread(
            target=self.solicitar_pull,
            args=(destino, faltando),
            name=f"sync-pull-{destino_id}",
            daemon=True,
        ).start()

    def solicitar_pull(
        self, destino: KnownTracker, faltando: list[SyncPullItem]
    ) -> int:
        """Envia SYNC_PULL a destino e aplica as SYNC_TABLE de resposta.

        A resposta vem na MESMA conexão TCP do pedido (padrão do
        SEARCH_FORWARD): uma ou mais SYNC_TABLE (um evento por seq),
        encerradas pelo fechamento da conexão pelo respondente. Cada uma é
        aplicada por LWW e AVANÇA o visto sem detecção de lacuna — o próprio
        reparo não deve disparar novos pulls. Ao fim, fecha as pendências das
        origens pedidas.

        Returns:
            Quantas entradas (hash, peer) foram efetivamente aplicadas.
        """
        if self.index is None or not faltando:
            return 0
        mensagem = SyncPull(faltando=faltando).model_dump()
        aplicadas = 0
        try:
            with socket.create_connection(
                (destino.ip, destino.sync_port), timeout=self.timeout_seconds
            ) as sock:
                send_json_line(sock, mensagem)
                reader = MessageReader(sock)
                while True:
                    header, _ = reader.recv_message(timeout=self.timeout_seconds)
                    aplicadas += self._aplicar_resposta_pull(header)
        except ConnectionClosedError:
            pass  # respondente fechou a conexão: fim normal das respostas
        except (TimeoutError, ConnectionRefusedError, OSError, ProtocolError) as exc:
            logger.warning(
                "tracker_id=%s SYNC_PULL a %s falhou (%s); pendência reaberta depois",
                self.tracker_id,
                destino.tracker_id,
                exc,
            )
            self.marcar_tracker_suspeito(destino.tracker_id)
            return aplicadas
        self._desmarcar_suspeito(destino.tracker_id)
        for item in faltando:
            self.index.resolver_pendencia(item.origem)
        logger.info(
            "SYNC_PULL: tracker_id=%s destino=%s pediu=%d aplicadas=%d",
            self.tracker_id,
            destino.tracker_id,
            len(faltando),
            aplicadas,
        )
        return aplicadas

    def _aplicar_resposta_pull(self, header: dict[str, Any]) -> int:
        """Aplica uma SYNC_TABLE de resposta: LWW + avanço de visto.

        O origem da resposta é o autor ORIGINAL das escritas (não o
        respondente), então o visto avança na origem certa.
        """
        assert self.index is not None
        msg = SyncTable.model_validate(header)
        self.index.avancar_visto(msg.origem, msg.seq)
        return sum(
            self.index.apply_sync_entry(entry, msg.origem, msg.timestamp, msg.seq)
            for entry in msg.entries
        )

    # ------------------------------------------------------------------
    # Reintegração (TRACKER_REJOIN -> TRACKER_LIST -> SYNC_PULL(0))
    # ------------------------------------------------------------------

    def reintegrar(self, meu_ip: str, meu_sync_port: int) -> int:
        """Reintegra este tracker via o PRIMEIRO conhecido que aceitar a conexão.

        O bootstrap não é um nó fixo: o tracker que volta percorre a sua lista de
        conhecidos (na ordem do YAML) e usa como ponto de entrada o primeiro
        reachable. Dele recebe a membership (TRACKER_LIST) e reconstrói o
        índice como caso particular do reparo — um SYNC_PULL(desde_seq=0) por
        origem conhecida (inclusive a própria). Ao aplicar as respostas, visto
        e meu_seq são inicializados (via avancar_visto), evitando reuso de
        seq após reinício sem persistir em disco.

        Returns:
            Quantas entradas foram reconstruídas (0 se nenhum conhecido respondeu).
        """
        for candidato in self.known_trackers:
            recebidos = self._solicitar_tracker_list(candidato, meu_ip, meu_sync_port)
            if recebidos is None:
                continue  # não aceitou a conexão / respondeu inválido: próximo
            origens = {self.tracker_id} | {t.tracker_id for t in self.known_trackers}
            for item in recebidos:
                if item.tracker_id == self.tracker_id:
                    continue
                self.adicionar_tracker(
                    KnownTracker(item.tracker_id, item.ip, item.porta)
                )
                origens.add(item.tracker_id)
            faltando = [SyncPullItem(origem=o, desde_seq=0) for o in sorted(origens)]
            aplicadas = self.solicitar_pull(candidato, faltando)
            logger.info(
                "tracker_id=%s reintegrado via %s: %d entradas reconstruídas (%d origens)",
                self.tracker_id,
                candidato.tracker_id,
                aplicadas,
                len(origens),
            )
            return aplicadas
        logger.warning(
            "tracker_id=%s nenhum conhecido aceitou o TRACKER_REJOIN; índice "
            "será reconstruído aos poucos via SEED_REPORT / SYNC_DIGEST",
            self.tracker_id,
        )
        return 0

    def _solicitar_tracker_list(
        self, candidato: KnownTracker, meu_ip: str, meu_sync_port: int
    ) -> list[TrackerListItem] | None:
        """Envia TRACKER_REJOIN a candidato e lê o TRACKER_LIST.

        Returns:
            A membership recebida, ou None se candidato não aceitou a
            conexão / respondeu inválido (o chamador tenta o próximo conhecido).
        """
        rejoin = TrackerRejoin(
            tracker_id=self.tracker_id, ip=meu_ip, porta=meu_sync_port
        ).model_dump()
        try:
            with socket.create_connection(
                (candidato.ip, candidato.sync_port), timeout=self.timeout_seconds
            ) as sock:
                send_json_line(sock, rejoin)
                header, _ = MessageReader(sock).recv_message(
                    timeout=self.timeout_seconds
                )
                return TrackerList.model_validate(header).trackers_conhecidos
        except (
            TimeoutError,
            ConnectionRefusedError,
            ConnectionClosedError,
            OSError,
        ) as exc:
            logger.warning(
                "tracker_id=%s TRACKER_REJOIN a %s falhou (%s); tentando o próximo",
                self.tracker_id,
                candidato.tracker_id,
                exc,
            )
            return None
        except (ProtocolError, ValidationError):
            logger.exception(
                "tracker_id=%s resposta inválida de %s ao TRACKER_REJOIN",
                self.tracker_id,
                candidato.tracker_id,
            )
            return None

    # ------------------------------------------------------------------
    # Membership e suspeitos
    # ------------------------------------------------------------------

    def tracker_por_id(self, tracker_id: str) -> KnownTracker | None:
        """Resolve um tracker_id no KnownTracker correspondente."""
        for tracker in self.known_trackers:
            if tracker.tracker_id == tracker_id:
                return tracker
        return None

    def adicionar_tracker(self, tracker: KnownTracker) -> None:
        """Acrescenta um tracker à membership se ainda não conhecido (idempotente).

        Usado ao processar TRACKER_ANNOUNCE. Na topologia estática do
        protótipo todos já se conhecem do YAML, então é normalmente um no-op.
        """
        if self.tracker_por_id(tracker.tracker_id) is None:
            self.known_trackers.append(tracker)

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
