"""Teste de integração da Fase 4: flooding SYNC_TABLE entre 3 trackers.

Sobe 3 trackers reais (API uvicorn + sync server TCP) em portas dinâmicas
de 127.0.0.1, conectados entre si. Um peer fictício registra um arquivo no
tracker-1 via REST e, em até 3s, a busca por nome nos trackers 2 e 3
encontra o hash LOCALMENTE (sem SEARCH_FORWARD — flooding puro), com
``hash_to_peers`` idêntico nas três réplicas (mesmo timestamp/origem, LWW).
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from src.tracker.index import Index
from tests.integration.cluster import TrackerNode, aguardar, cluster_de_trackers

HASH_MUSICA = "ab" * 32


@pytest.fixture()
def trackers(tmp_path: Path) -> Iterator[dict[str, TrackerNode]]:
    with cluster_de_trackers(tmp_path, com_flooding=True) as nodes:
        yield nodes


def _peer_hello(api_url: str, nome_peer: str, porta: int) -> None:
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


def _register_file(api_url: str, nome_peer: str, hash_arquivo: str) -> httpx.Response:
    return httpx.post(
        f"{api_url}/files/register",
        json={
            "type": "REGISTER_FILE",
            "nome_peer": nome_peer,
            "hash": hash_arquivo,
            "nome": "Imagine",
            "tamanho": 5_242_880,
            "n_chunks": 20,
        },
        timeout=5.0,
    )


def _busca_local(index: Index, nome: str) -> list[str]:
    """Busca DIRETA no índice do tracker (sem REST → sem SEARCH_FORWARD)."""
    return [entrada.hash for entrada in index.search_by_name(nome)]


def _search_api(api_url: str, query: str) -> dict:
    resposta = httpx.post(
        f"{api_url}/search",
        json={
            "type": "SEARCH_FILE",
            "query_id": str(uuid.uuid4()),
            "query": query,
            "ttl": 3,
        },
        timeout=5.0,
    )
    assert resposta.status_code == 200
    return resposta.json()


def test_register_no_tracker_1_propaga_aos_demais_em_3s(
    trackers: dict[str, TrackerNode],
) -> None:
    t1 = trackers["tracker-1"]
    _peer_hello(t1.api_url, "alice", 7001)
    assert _register_file(t1.api_url, "alice", HASH_MUSICA).status_code == 200

    # Critério §12.3 do CLAUDE.md: hash presente nos índices LOCAIS de
    # tracker-2 e tracker-3 em < 3s (flooding, não forward).
    assert aguardar(
        lambda: all(
            _busca_local(trackers[tid].index, "Imagine") == [HASH_MUSICA]
            for tid in ("tracker-2", "tracker-3")
        ),
        timeout=3.0,
    ), "flooding não convergiu em 3s"

    # GET /search dos trackers 2 e 3 também responde (camada REST).
    for tid in ("tracker-2", "tracker-3"):
        resultado = _search_api(trackers[tid].api_url, "Imagine")
        assert resultado["resultados"][0]["hash"] == HASH_MUSICA
        assert resultado["resultados"][0]["peers"] == [
            {"nome_peer": "alice", "ip": "127.0.0.1", "porta": 7001}
        ]

    # As três réplicas têm a MESMA entrada (timestamp e origem inclusive).
    entradas = [
        trackers[tid].index.get_snapshot().hash_to_peers[HASH_MUSICA]
        for tid in ("tracker-1", "tracker-2", "tracker-3")
    ]
    assert entradas[0] == entradas[1] == entradas[2]


def test_peer_leave_file_propaga_tombstone(trackers: dict[str, TrackerNode]) -> None:
    t1 = trackers["tracker-1"]
    _peer_hello(t1.api_url, "alice", 7001)
    _peer_hello(t1.api_url, "bob", 7002)
    _register_file(t1.api_url, "alice", HASH_MUSICA)
    _register_file(t1.api_url, "bob", HASH_MUSICA)
    assert aguardar(
        lambda: all(
            len(trackers[tid].index.get_snapshot().hash_to_peers.get(HASH_MUSICA, {}))
            == 2
            for tid in ("tracker-2", "tracker-3")
        ),
        timeout=3.0,
    )

    resposta = httpx.post(
        f"{t1.api_url}/files/leave",
        json={"type": "PEER_LEAVE_FILE", "nome_peer": "alice", "hash": HASH_MUSICA},
        timeout=5.0,
    )
    assert resposta.status_code == 200

    # §12.9 do CLAUDE.md: em < 3s os demais registram tombstone de alice e
    # bob segue como fonte.
    def _tombstone_em_todos() -> bool:
        for tid in ("tracker-2", "tracker-3"):
            snapshot = trackers[tid].index.get_snapshot()
            if "alice" not in snapshot.tombstones.get(HASH_MUSICA, {}):
                return False
            if set(snapshot.hash_to_peers.get(HASH_MUSICA, {})) != {"bob"}:
                return False
        return True

    assert aguardar(_tombstone_em_todos, timeout=3.0), "tombstone não propagou em 3s"


def test_tracker_caido_nao_trava_register_e_vira_suspeito(
    trackers: dict[str, TrackerNode],
) -> None:
    t1 = trackers["tracker-1"]
    assert t1.sync_client is not None
    _peer_hello(t1.api_url, "alice", 7001)

    # Derruba só o sync server do tracker-2 (crash failure controlado).
    trackers["tracker-2"].sync_server.stop()

    inicio = time.monotonic()
    assert _register_file(t1.api_url, "alice", HASH_MUSICA).status_code == 200
    # Flooding é assíncrono (uma thread daemon por destino): a resposta REST
    # não espera o destino morto (Listing 8.1 / transient asynchronous).
    assert time.monotonic() - inicio < 2.0, "REGISTER_FILE bloqueou no tracker caído"

    # tracker-2 vira suspeito; tracker-3 recebe normalmente.
    assert aguardar(lambda: t1.sync_client.tracker_esta_suspeito("tracker-2"))
    assert aguardar(
        lambda: _busca_local(trackers["tracker-3"].index, "Imagine") == [HASH_MUSICA],
        timeout=3.0,
    )
    assert not t1.sync_client.tracker_esta_suspeito("tracker-3")
