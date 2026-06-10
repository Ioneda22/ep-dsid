"""Teste de integração da Fase 2: tracker REST real + cliente HTTP.

Sobe o app FastAPI num uvicorn real (thread, porta dinâmica em
127.0.0.1 — §10) e exercita o ciclo PEER_HELLO → REGISTER_FILE →
SEARCH_FILE → PEER_LEAVE_FILE → PEER_LEAVE via httpx.
"""

from __future__ import annotations

import socket
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import uvicorn

from src.tracker.api import create_app
from src.tracker.index import Index
from src.tracker.persistence import init_db

HASH_MUSICA = "ab" * 32


def _porta_livre() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def tracker_url(tmp_path: Path) -> Iterator[str]:
    """Tracker real servindo em porta dinâmica; derrubado ao fim do teste."""
    porta = _porta_livre()
    db = init_db(tmp_path / "peerspot.db")
    app = create_app(
        index=Index(),
        db=db,
        tracker_id="tracker-teste",
        trackers_conhecidos=[
            {"tracker_id": "tracker-teste", "ip": "127.0.0.1", "api_port": porta}
        ],
    )
    config = uvicorn.Config(
        app, host="127.0.0.1", port=porta, log_config=None, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    prazo = time.time() + 10
    while not server.started:
        if time.time() > prazo:
            pytest.fail("tracker não subiu em 10s")
        time.sleep(0.01)
    yield f"http://127.0.0.1:{porta}"
    server.should_exit = True
    thread.join(timeout=5)
    db.close()


@pytest.fixture()
def cliente(tracker_url: str) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=tracker_url, timeout=5.0) as http_client:
        yield http_client


def _peer_hello(cliente: httpx.Client, nome_peer: str, porta: int) -> httpx.Response:
    return cliente.post(
        "/peers/hello",
        json={
            "type": "PEER_HELLO",
            "nome_peer": nome_peer,
            "ip": "127.0.0.1",
            "porta": porta,
        },
    )


def _register_file(cliente: httpx.Client, nome_peer: str) -> httpx.Response:
    return cliente.post(
        "/files/register",
        json={
            "type": "REGISTER_FILE",
            "nome_peer": nome_peer,
            "hash": HASH_MUSICA,
            "nome": "Imagine",
            "tamanho": 5_242_880,
            "n_chunks": 20,
        },
    )


def _search(cliente: httpx.Client, query: str) -> dict:
    resposta = cliente.post(
        "/search",
        json={
            "type": "SEARCH_FILE",
            "query_id": str(uuid.uuid4()),
            "query": query,
            "ttl": 3,
        },
    )
    assert resposta.status_code == 200
    return resposta.json()


def test_health_e_trackers(cliente: httpx.Client) -> None:
    saude = cliente.get("/health")
    assert saude.status_code == 200
    assert saude.json() == {"status": "ok", "tracker_id": "tracker-teste"}

    trackers = cliente.get("/trackers")
    assert trackers.status_code == 200
    assert trackers.json()["trackers"][0]["tracker_id"] == "tracker-teste"


def test_fluxo_hello_register_search(cliente: httpx.Client) -> None:
    assert _peer_hello(cliente, "alice", 7001).status_code == 200
    assert _register_file(cliente, "alice").status_code == 200

    resultado = _search(cliente, "Imagine")
    assert resultado["type"] == "SEARCH_RESULT"
    assert len(resultado["resultados"]) == 1
    entrada = resultado["resultados"][0]
    assert entrada["hash"] == HASH_MUSICA
    assert entrada["nome"] == "Imagine"
    assert entrada["peers"] == [
        {"nome_peer": "alice", "ip": "127.0.0.1", "porta": 7001}
    ]


def test_search_correlaciona_query_id(cliente: httpx.Client) -> None:
    query_id = str(uuid.uuid4())
    resposta = cliente.post(
        "/search",
        json={"type": "SEARCH_FILE", "query_id": query_id, "query": "x", "ttl": 3},
    )
    assert resposta.json()["query_id"] == query_id


def test_busca_sem_hit_retorna_resultados_vazios(cliente: httpx.Client) -> None:
    assert _search(cliente, "Nao Existe")["resultados"] == []


def test_register_sem_hello_retorna_peer_unknown(cliente: httpx.Client) -> None:
    resposta = _register_file(cliente, "intrusa")
    assert resposta.status_code == 404
    corpo = resposta.json()
    assert corpo["type"] == "ERROR"
    assert corpo["code"] == "PEER_UNKNOWN"
    assert corpo["ref_type"] == "REGISTER_FILE"


def test_peer_leave_file_remove_apenas_aquela_fonte(cliente: httpx.Client) -> None:
    _peer_hello(cliente, "alice", 7001)
    _register_file(cliente, "alice")
    _peer_hello(cliente, "bob", 7002)
    _register_file(cliente, "bob")

    resposta = cliente.post(
        "/files/leave",
        json={"type": "PEER_LEAVE_FILE", "nome_peer": "alice", "hash": HASH_MUSICA},
    )
    assert resposta.status_code == 200

    peers = _search(cliente, "Imagine")["resultados"][0]["peers"]
    assert peers == [{"nome_peer": "bob", "ip": "127.0.0.1", "porta": 7002}]


def test_peer_leave_some_do_indice(cliente: httpx.Client) -> None:
    _peer_hello(cliente, "alice", 7001)
    _register_file(cliente, "alice")

    resposta = cliente.post(
        "/peers/leave", json={"type": "PEER_LEAVE", "nome_peer": "alice"}
    )
    assert resposta.status_code == 200
    assert _search(cliente, "Imagine")["resultados"] == []
    # Sem novo PEER_HELLO, o peer não pode registrar de novo.
    assert _register_file(cliente, "alice").status_code == 404


def test_update_ip_reflete_na_busca(cliente: httpx.Client) -> None:
    _peer_hello(cliente, "alice", 7001)
    _register_file(cliente, "alice")

    resposta = cliente.post(
        "/peers/update-ip",
        json={
            "type": "UPDATE_IP",
            "nome_peer": "alice",
            "novo_ip": "127.0.0.2",
            "porta": 7099,
        },
    )
    assert resposta.status_code == 200
    peers = _search(cliente, "Imagine")["resultados"][0]["peers"]
    assert peers == [{"nome_peer": "alice", "ip": "127.0.0.2", "porta": 7099}]


def test_seed_report_reconstroi_presenca_e_remove_omitidos(
    cliente: httpx.Client,
) -> None:
    _peer_hello(cliente, "alice", 7001)
    _register_file(cliente, "alice")

    # Relatório sem o hash: equivale a remoção (main.tex §7.2).
    resposta = cliente.post(
        "/peers/seed-report",
        json={
            "type": "SEED_REPORT",
            "nome_peer": "alice",
            "ip": "127.0.0.1",
            "porta": 7001,
            "hashes": [],
        },
    )
    assert resposta.status_code == 200
    assert _search(cliente, "Imagine")["resultados"] == []

    # Próximo relatório com o hash: alice volta como fonte.
    cliente.post(
        "/peers/seed-report",
        json={
            "type": "SEED_REPORT",
            "nome_peer": "alice",
            "ip": "127.0.0.1",
            "porta": 7001,
            "hashes": [HASH_MUSICA],
        },
    )
    peers = _search(cliente, "Imagine")["resultados"][0]["peers"]
    assert peers == [{"nome_peer": "alice", "ip": "127.0.0.1", "porta": 7001}]


def test_payload_malformado_retorna_error_400(cliente: httpx.Client) -> None:
    resposta = cliente.post("/peers/hello", json={"nome_peer": "alice"})
    assert resposta.status_code == 400
    corpo = resposta.json()
    assert corpo["type"] == "ERROR"
    assert corpo["code"] == "MALFORMED_MESSAGE"
    assert corpo["ref_type"] == "PEER_HELLO"
