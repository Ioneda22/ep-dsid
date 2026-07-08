"""Teste de integração da Fase 6: CRUD de playlists via REST.

Sobe o app FastAPI num uvicorn real (thread, porta dinâmica em
127.0.0.1 — §10) e exercita o ciclo completo de playlists via httpx:
criar → adicionar 3 hashes → listar → remover um → obter → deletar.

As playlists são dados de usuário LOCAIS ao tracker (não propagadas por
SYNC_TABLE), por isso um único tracker basta para o teste.
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import uvicorn

from src.tracker.api import create_app
from src.tracker.index import Index
from src.tracker.persistence import init_db

HASH_A = "aa" * 32
HASH_B = "bb" * 32
HASH_C = "cc" * 32


def _porta_livre() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def cliente(tmp_path: Path) -> Iterator[httpx.Client]:
    """Tracker real em porta dinâmica; cliente httpx apontando para ele."""
    porta = _porta_livre()
    db = init_db(tmp_path / "peerspot.db")
    app = create_app(
        index=Index(),
        db=db,
        tracker_id="tracker-teste",
        trackers_conhecidos=[],
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
    with httpx.Client(base_url=f"http://127.0.0.1:{porta}", timeout=5.0) as http_client:
        # A playlist referencia um dono; registra a presença primeiro (PEER_HELLO).
        http_client.post(
            "/peers/hello",
            json={
                "type": "PEER_HELLO",
                "nome_peer": "alice",
                "ip": "127.0.0.1",
                "porta": 7001,
            },
        )
        yield http_client
    server.should_exit = True
    thread.join(timeout=5)
    db.close()


def _criar(cliente: httpx.Client, nome: str = "favoritas") -> int:
    resposta = cliente.post("/playlists", json={"dono": "alice", "nome": nome})
    assert resposta.status_code == 200
    return int(resposta.json()["playlist_id"])


def _adicionar(cliente: httpx.Client, pid: int, hash_arquivo: str) -> httpx.Response:
    return cliente.post(f"/playlists/{pid}/items", json={"hash": hash_arquivo})


def test_ciclo_completo_de_playlist(cliente: httpx.Client) -> None:
    # Criar playlist.
    pid = _criar(cliente)

    # Adicionar 3 hashes.
    for hash_arquivo in (HASH_A, HASH_B, HASH_C):
        assert _adicionar(cliente, pid, hash_arquivo).status_code == 200

    # Listar → aparece na listagem do dono.
    listagem = cliente.get("/playlists/alice").json()["playlists"]
    assert listagem == [{"id": pid, "nome": "favoritas", "dono": "alice"}]

    # Obter → os 3 hashes na ordem de inserção.
    playlist = cliente.get(f"/playlists/{pid}").json()
    assert playlist == {
        "nome": "favoritas",
        "dono": "alice",
        "itens": [HASH_A, HASH_B, HASH_C],
    }

    # Remover um → restam 2 (na ordem original).
    assert cliente.delete(f"/playlists/{pid}/items/{HASH_B}").status_code == 200
    assert cliente.get(f"/playlists/{pid}").json()["itens"] == [HASH_A, HASH_C]

    # Deletar playlist → não aparece mais na listagem nem no GET.
    assert cliente.delete(f"/playlists/{pid}").status_code == 200
    assert cliente.get("/playlists/alice").json()["playlists"] == []
    assert cliente.get(f"/playlists/{pid}").status_code == 404


def test_obter_playlist_inexistente_retorna_404(cliente: httpx.Client) -> None:
    resposta = cliente.get("/playlists/12345")
    assert resposta.status_code == 404
    assert resposta.json()["type"] == "ERROR"
    assert resposta.json()["code"] == "NOT_FOUND"


def test_adicionar_item_em_playlist_inexistente_retorna_404(
    cliente: httpx.Client,
) -> None:
    assert _adicionar(cliente, 999, HASH_A).status_code == 404


def test_listar_playlists_de_dono_sem_playlists(cliente: httpx.Client) -> None:
    assert cliente.get("/playlists/bob").json()["playlists"] == []
