"""Teste de integração do roteamento SEARCH_FORWARD entre trackers.

Cluster SEM flooding (com_flooding=False): o registro feito no
tracker-1 NÃO chega aos demais via SYNC_TABLE, forçando o tracker-3 a
rotear a busca via SEARCH_FORWARD (TTL, timeout de 2s) para respondê-la.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from tests.integration.cluster import TrackerNode, cluster_de_trackers

HASH_MUSICA = "cd" * 32
TIMEOUT_FORWARD = 2.0


@pytest.fixture()
def trackers(tmp_path: Path) -> Iterator[dict[str, TrackerNode]]:
    with cluster_de_trackers(
        tmp_path, com_flooding=False, search_timeout=TIMEOUT_FORWARD
    ) as nodes:
        yield nodes


def _registrar_em(node: TrackerNode, nome_peer: str, porta: int) -> None:
    assert (
        httpx.post(
            f"{node.api_url}/peers/hello",
            json={
                "type": "PEER_HELLO",
                "nome_peer": nome_peer,
                "ip": "127.0.0.1",
                "porta": porta,
            },
            timeout=5.0,
        ).status_code
        == 200
    )
    assert (
        httpx.post(
            f"{node.api_url}/files/register",
            json={
                "type": "REGISTER_FILE",
                "nome_peer": nome_peer,
                "hash": HASH_MUSICA,
                "nome": "Imagine",
                "tamanho": 5_242_880,
                "n_chunks": 20,
            },
            timeout=5.0,
        ).status_code
        == 200
    )


def _search(node: TrackerNode, query: str, ttl: int = 3) -> dict:
    resposta = httpx.post(
        f"{node.api_url}/search",
        json={
            "type": "SEARCH_FILE",
            "query_id": str(uuid.uuid4()),
            "query": query,
            "ttl": ttl,
        },
        timeout=TIMEOUT_FORWARD + 5.0,
    )
    assert resposta.status_code == 200
    return resposta.json()


def test_busca_sem_hit_local_roteia_e_encontra(
    trackers: dict[str, TrackerNode],
) -> None:
    # Peer X registra no tracker-1; sem flooding, só ele conhece o hash.
    _registrar_em(trackers["tracker-1"], "peer-x", 7001)
    assert trackers["tracker-3"].index.search_by_name("Imagine") == []

    # Peer Y busca no tracker-3 → SEARCH_FORWARD → SEARCH_RESULT de volta.
    resultado = _search(trackers["tracker-3"], "Imagine")
    assert len(resultado["resultados"]) == 1
    entrada = resultado["resultados"][0]
    assert entrada["hash"] == HASH_MUSICA
    assert entrada["n_chunks"] == 20
    assert entrada["peers"] == [
        {"nome_peer": "peer-x", "ip": "127.0.0.1", "porta": 7001}
    ]
    # O roteamento responde sem materializar o hash no índice local do 3.
    assert trackers["tracker-3"].index.search_by_name("Imagine") == []


def test_ttl_zero_nao_roteia(trackers: dict[str, TrackerNode]) -> None:
    _registrar_em(trackers["tracker-1"], "peer-x", 7001)
    assert _search(trackers["tracker-3"], "Imagine", ttl=0)["resultados"] == []


def test_busca_inexistente_em_todos_retorna_vazio(
    trackers: dict[str, TrackerNode],
) -> None:
    assert _search(trackers["tracker-3"], "Nao Existe")["resultados"] == []


def test_tracker_remoto_caido_responde_vazio_dentro_do_timeout(
    trackers: dict[str, TrackerNode],
) -> None:
    """Fonte caída: a busca degrada para [] sem estourar o timeout de 2s."""
    _registrar_em(trackers["tracker-1"], "peer-x", 7001)
    trackers["tracker-1"].sync_server.stop()

    inicio = time.monotonic()
    resultado = _search(trackers["tracker-3"], "Imagine")
    duracao = time.monotonic() - inicio
    assert resultado["resultados"] == []
    assert duracao < TIMEOUT_FORWARD + 1.5, f"busca demorou {duracao:.1f}s"
