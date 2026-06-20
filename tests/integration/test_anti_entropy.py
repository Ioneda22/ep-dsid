"""Integração: reconciliação anti-entropy entre trackers via ``FULL_SYNC``.

Sobe 3 trackers reais (API uvicorn + sync server TCP) em portas dinâmicas.
Cria uma divergência escrevendo DIRETO no índice do tracker-1 (sem passar pela
API, logo sem flooding ``SYNC_TABLE``), de modo que tracker-2 e tracker-3 não
tomam conhecimento — exatamente o delta que o ``SYNC_TABLE`` best-effort
poderia perder. Um ciclo de anti-entropy (push de ``FULL_SYNC`` do tracker-1)
deve então reparar os demais (main.tex §"Reconciliação anti-entropy").
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from src.tracker.anti_entropy import AntiEntropyReconciler
from src.tracker.index import Index
from tests.integration.cluster import TrackerNode, aguardar, cluster_de_trackers

HASH = "ab" * 32


@pytest.fixture()
def trackers(tmp_path: Path) -> Iterator[dict[str, TrackerNode]]:
    with cluster_de_trackers(tmp_path, com_flooding=True) as nodes:
        yield nodes


def _busca_local(index: Index, nome: str) -> list[str]:
    return [entrada.hash for entrada in index.search_by_name(nome)]


def _reconciler(node: TrackerNode) -> AntiEntropyReconciler:
    """Reconciler com intervalo grande: dispara só via ``reconciliar_agora``."""
    assert node.sync_client is not None
    return AntiEntropyReconciler(
        node.tracker_id, node.index, node.sync_client, interval_seconds=999
    )


def _convergiu(trackers: dict[str, TrackerNode], esperado: list[str]) -> bool:
    return all(
        _busca_local(trackers[tid].index, "Imagine") == esperado
        for tid in ("tracker-2", "tracker-3")
    )


def test_anti_entropy_repara_delta_perdido(trackers: dict[str, TrackerNode]) -> None:
    t1 = trackers["tracker-1"]
    # Divergência: escrita DIRETA no índice do tracker-1 (sem flooding).
    t1.index.register_peer("alice", "127.0.0.1", 7001)
    t1.index.register_file("alice", HASH, nome="Imagine", tamanho=100, n_chunks=1)
    assert not _convergiu(trackers, [HASH]), "pré-condição: 2 e 3 ainda não sabem"

    # Anti-entropy: tracker-1 faz push do estado completo (FULL_SYNC).
    _reconciler(t1).reconciliar_agora()

    assert aguardar(
        lambda: _convergiu(trackers, [HASH]), timeout=3.0
    ), "anti-entropy não repôs o delta perdido em 3s"
    # A fonte chega com a origem correta (desempate LWW determinístico).
    entrada = trackers["tracker-2"].index.get_snapshot().hash_to_peers[HASH]["alice"]
    assert entrada.origem == "tracker-1"


def test_anti_entropy_repara_tombstone_perdido(
    trackers: dict[str, TrackerNode],
) -> None:
    t1 = trackers["tracker-1"]
    # Alinha as réplicas com uma primeira rodada de anti-entropy.
    t1.index.register_peer("alice", "127.0.0.1", 7001)
    t1.index.register_file("alice", HASH, nome="Imagine", tamanho=100, n_chunks=1)
    _reconciler(t1).reconciliar_agora()
    assert aguardar(lambda: _convergiu(trackers, [HASH]), timeout=3.0)

    # Remoção DIRETA no tracker-1 (vira tombstone), sem propagar.
    t1.index.remove_peer_from_hash(HASH, "alice")
    # Anti-entropy repara o tombstone nos demais.
    _reconciler(t1).reconciliar_agora()

    def _tombstone_em_todos() -> bool:
        for tid in ("tracker-2", "tracker-3"):
            snapshot = trackers[tid].index.get_snapshot()
            if "alice" not in snapshot.tombstones.get(HASH, {}):
                return False
            if _busca_local(trackers[tid].index, "Imagine") != []:
                return False
        return True

    assert aguardar(
        _tombstone_em_todos, timeout=3.0
    ), "anti-entropy não repôs o tombstone em 3s"
