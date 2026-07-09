"""Integração da reintegração de tracker + rebalance.

A reconstrução do índice via TRACKER_REJOIN → TRACKER_LIST →
SYNC_PULL(desde_seq=0) já é coberta em test_sync_repair.py. Aqui o foco é
o passo NOVO: quando um tracker se reintegra, os trackers ativos cedem
parte dos seus peers a ele (REASSIGN_TRACKER), e o peer cedido migra sozinho
na sua próxima chamada REST.

Cenário: 3 trackers reais (flooding + rebalance). Três peers reportam ao
tracker-1; o tracker-2 se reintegra; o tracker-1 cede floor(3/3)=1 peer a ele.
O peer cedido recebe reassign_to na resposta seguinte e passa a falar com o
tracker-2.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from src.peer.tracker_client import PeerTrackerClient
from tests.integration.cluster import TrackerNode, aguardar, cluster_de_trackers

HASH_A = "aa" * 32


@pytest.fixture()
def trackers(tmp_path: Path) -> Iterator[dict[str, TrackerNode]]:
    with cluster_de_trackers(tmp_path) as nodes:
        yield nodes


def _cfg(node: TrackerNode) -> dict[str, object]:
    return {"tracker_id": node.tracker_id, "ip": "127.0.0.1", "api_port": node.api_port}


def _hello(api_url: str, nome_peer: str, porta: int) -> None:
    resposta = httpx.post(
        f"{api_url}/peers/hello",
        json={
            "type": "PEER_HELLO",
            "nome_peer": nome_peer,
            "ip": "127.0.0.1",
            "porta": porta,
        },
        timeout=5.0,
    )
    assert resposta.status_code == 200


def test_reintegracao_cede_peers_e_peer_migra(
    trackers: dict[str, TrackerNode],
) -> None:
    t1, t2, t3 = (trackers[f"tracker-{i}"] for i in (1, 2, 3))

    # alice é um peer REAL (recebe reassign_to e migra); bob e carol só elevam a
    # contagem local do tracker-1 a 3, para floor(3/3)=1 (há o que ceder).
    alice = PeerTrackerClient([_cfg(t1), _cfg(t2), _cfg(t3)], timeout=2.0)
    try:
        assert alice.peer_hello("alice", "127.0.0.1", 7001) is not None
        _hello(t1.api_url, "bob", 7002)
        _hello(t1.api_url, "carol", 7003)
        assert (
            alice.register_file("alice", HASH_A, nome="musica", tamanho=100, n_chunks=1)
            is not None
        )
        assert sorted(t1.index.listar_peers_locais()) == ["alice", "bob", "carol"]

        # Convergência antes da reintegração.
        assert aguardar(
            lambda: {e.hash for e in t2.index.search_by_name("musica")} == {HASH_A}
        )

        # tracker-2 se reintegra: TRACKER_REJOIN a um conhecido -> TRACKER_LIST +
        # ANNOUNCE + rebalance. tracker-1 cede floor(3/3)=1 peer (alice, 1º sorted).
        assert t2.sync_client is not None
        t2.sync_client.reintegrar("127.0.0.1", t2.sync_server.porta)

        # Como em produção: alice segue reportando ao tracker atual; em ALGUMA das
        # chamadas a resposta traz reassign_to e o cliente migra sozinho. Aqui
        # exercitamos exatamente esse caminho (SEED_REPORT REST -> reassign_to ->
        # migração), sem espiar o estado interno do tracker.
        def alice_reportou_e_migrou() -> bool:
            alice.seed_report("alice", "127.0.0.1", 7001, [HASH_A])
            return alice.current_tracker_index == 1

        assert aguardar(alice_reportou_e_migrou), "reassign_to não migrou alice"
        assert alice.tracker_id == "tracker-2"

        # O PEER_HELLO automático na migração registrou alice no tracker-2.
        assert aguardar(
            lambda: "alice" in t2.index.get_snapshot().nome_peer_to_endereco
        )
    finally:
        alice.close()


def test_rebalance_nao_cede_quando_ha_poucos_peers(
    trackers: dict[str, TrackerNode],
) -> None:
    """floor(meus_peers / N) == 0 não cede ninguém."""
    t1, t2 = trackers["tracker-1"], trackers["tracker-2"]
    _hello(t1.api_url, "alice", 7001)  # 1 peer local, N=3 -> floor(1/3)=0

    assert t1.rebalance is not None
    cedidos = t1.rebalance.ceder_peers_para("tracker-2")
    assert cedidos == []
    assert t1.index.consumir_reassign("alice") is None
    assert t2 is not None
