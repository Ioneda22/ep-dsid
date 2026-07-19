"""Fallback de tracker no peer.

Dois trackers reais (com flooding) + um peer configurado com ambos. O peer
registra um arquivo no tracker-1; o tracker-1 cai; a próxima operação do peer
(uma busca) migra automaticamente para o tracker-2 (reenviando PEER_HELLO
lá) e tem sucesso, pois o índice já foi replicado.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from src.peer.tracker_client import PeerTrackerClient
from tests.integration.cluster import TrackerNode, aguardar, cluster_de_trackers

HASH_A = "aa" * 32


@pytest.fixture()
def par_de_trackers(tmp_path: Path) -> Iterator[dict[str, TrackerNode]]:
    with cluster_de_trackers(tmp_path, ids=("tracker-1", "tracker-2")) as nodes:
        yield nodes


def _cfg(node: TrackerNode) -> dict[str, object]:
    return {"tracker_id": node.tracker_id, "ip": "127.0.0.1", "api_port": node.api_port}


def test_peer_migra_para_backup_quando_tracker_cai(
    par_de_trackers: dict[str, TrackerNode],
) -> None:
    t1, t2 = par_de_trackers["tracker-1"], par_de_trackers["tracker-2"]
    client = PeerTrackerClient([_cfg(t1), _cfg(t2)], timeout=1.0)
    try:
        assert client.peer_hello("alice", "127.0.0.1", 7001) is not None
        assert (
            client.register_file(
                "alice", HASH_A, nome="musica", tamanho=100, n_chunks=1
            )
            is not None
        )
        assert client.current_tracker_index == 0  # ainda no tracker-1

        # O registro replica para o tracker-2 antes de derrubarmos o tracker-1.
        assert aguardar(
            lambda: {e.hash for e in t2.index.search_by_name("musica")} == {HASH_A}
        ), "registro não replicou ao tracker-2"

        # tracker-1 cai.
        t1.parar()

        # Próxima operação: a busca migra sozinha para o tracker-2 e encontra.
        resultado = client.search_file("musica", str(uuid.uuid4()))
        assert resultado is not None
        assert {e.hash for e in resultado.resultados} == {HASH_A}
        assert client.current_tracker_index == 1  # fallback efetivado

        # O fallback reapresentou alice ao tracker-2 (PEER_HELLO automático).
        assert "alice" in t2.index.get_snapshot().nome_peer_to_endereco
    finally:
        client.close()
