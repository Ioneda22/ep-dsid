"""Rebalanceamento de peers após a reintegração de um tracker.

Quando um tracker volta à rede (TRACKER_REJOIN → TRACKER_LIST →
SYNC_PULL(desde_seq=0)), ele reconstrói o índice, mas começa sem nenhum peer
reportando a ele. Para redistribuir a carga, cada tracker ativo cede uma fração
dos SEUS peers locais ao reintegrado:

    cessao = floor(meus_peers_locais / n_trackers_total)

O gatilho é a membership: ao processar o TRACKER_REJOIN (no tracker de
entrada) ou o TRACKER_ANNOUNCE (nos demais), cada tracker agenda a cessão
para o recém-chegado. A migração em si vai ao peer como reassign_to na
resposta da sua próxima chamada REST — o Index guarda o pendente e o
handler o anexa ao ACK (simplificação aceita: sem push TCP tracker→peer).

Cada tracker calcula sua cessão de forma independente; eventual sobrecessão
transitória é aceitável e converge nas próximas rodadas de SEED_REPORT.
Nenhuma coordenação atômica entre trackers é necessária.
"""

from __future__ import annotations

import logging
import random
from collections.abc import Callable

from src.tracker.index import Index
from src.tracker.sync_client import SyncClient

logger = logging.getLogger(__name__)


class RebalanceManager:
    """Agenda a cessão de peers locais a um tracker recém-reintegrado."""

    def __init__(
        self,
        tracker_id: str,
        index: Index,
        sync_client: SyncClient,
        api_por_tracker_id: dict[str, tuple[str, int]],
        sorteador: Callable[[list[str]], str] = random.choice,
    ) -> None:
        """Args:
        tracker_id: Identificador deste tracker.
        index: Índice de onde saem os peers locais e onde a migração é agendada.
        sync_client: Fonte da membership atual (known_trackers) para o total.
        api_por_tracker_id: Mapa tracker_id -> (ip, api_port) com o endereço
            REST de cada tracker (do YAML), usado para dizer ao peer onde se
            reportar. Sem entrada para o alvo, a cessão é ignorada.
        sorteador: Escolha uniforme de um tracker_id, injetável para testes
            determinísticos; por padrão random.choice.
        """
        self.tracker_id = tracker_id
        self.index = index
        self.sync_client = sync_client
        self.api_por_tracker_id = api_por_tracker_id
        self.sorteador = sorteador

    def sortear_reassign(self) -> tuple[str, int] | None:
        """Sorteia inline um destino uniforme entre os N trackers (inclusive este).

        Devolve o endereço REST (ip, api_port) do tracker sorteado, ou None se o
        sorteio cair neste tracker (peer permanece) ou se não houver outro alvo
        com endereço conhecido. É decisão inline: o chamador coloca o resultado
        direto no reassign_to da resposta do PEER_HELLO, sem enfileirar em
        _reassign_pendente — assim o HELLO de migração não deixa pendência e o
        peer migra no máximo uma vez.
        """
        alvos = sorted(self.api_por_tracker_id)
        if len(alvos) <= 1:
            return None
        escolhido = self.sorteador(alvos)
        if escolhido == self.tracker_id:
            return None
        return self.api_por_tracker_id[escolhido]

    def ceder_peers_para(self, novo_tracker_id: str) -> list[str]:
        """Agenda a migração de floor(locais / N) peers ao tracker reintegrado.

        Returns:
            Nomes dos peers cedidos (vazio se nada a ceder ou alvo desconhecido).
        """
        if novo_tracker_id == self.tracker_id:
            return []
        alvo = self.api_por_tracker_id.get(novo_tracker_id)
        if alvo is None:
            logger.warning(
                "tracker_id=%s sem endereço REST de %s; rebalance ignorado",
                self.tracker_id,
                novo_tracker_id,
            )
            return []
        locais = self.index.listar_peers_locais()
        n_total = len(self.sync_client.known_trackers) + 1
        cessao = len(locais) // n_total
        if cessao <= 0:
            logger.info(
                "tracker_id=%s nada a ceder a %s (locais=%d, N=%d)",
                self.tracker_id,
                novo_tracker_id,
                len(locais),
                n_total,
            )
            return []
        escolhidos = locais[:cessao]
        for nome_peer in escolhidos:
            self.index.agendar_reassign(nome_peer, alvo[0], alvo[1])
        logger.info(
            "tracker_id=%s cedeu %d peer(s) a %s (%s:%d): %s",
            self.tracker_id,
            cessao,
            novo_tracker_id,
            alvo[0],
            alvo[1],
            escolhidos,
        )
        return escolhidos
