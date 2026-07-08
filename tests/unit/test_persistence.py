"""Testes unitários da persistência SQLite do tracker (Fase 2)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.tracker.persistence import TrackerDB, init_db


@pytest.fixture()
def db(tmp_path: Path) -> TrackerDB:
    tracker_db = init_db(tmp_path / "tracker" / "peerspot.db", clock=lambda: 1000.0)
    yield tracker_db
    tracker_db.close()


def test_init_db_cria_diretorios_e_arquivo(tmp_path: Path) -> None:
    caminho = tmp_path / "sub" / "dir" / "peerspot.db"
    tracker_db = init_db(caminho)
    try:
        assert caminho.exists()
    finally:
        tracker_db.close()


def test_registrar_usuario_e_listar(db: TrackerDB) -> None:
    db.registrar_usuario("alice")
    db.registrar_usuario("bob")
    assert db.listar_usuarios() == ["alice", "bob"]


def test_registrar_usuario_idempotente(db: TrackerDB) -> None:
    db.registrar_usuario("alice")
    db.registrar_usuario("alice")  # PEER_HELLO repetido não duplica
    assert db.listar_usuarios() == ["alice"]


def test_criar_e_obter_playlist(db: TrackerDB) -> None:
    pid = db.criar_playlist("alice", "favoritas")
    db.adicionar_item(pid, "a" * 64, db.proxima_ordem(pid))
    db.adicionar_item(pid, "b" * 64, db.proxima_ordem(pid))
    playlist = db.obter_playlist(pid)
    assert playlist == {
        "nome": "favoritas",
        "dono": "alice",
        "itens": ["a" * 64, "b" * 64],
    }


def test_obter_playlist_inexistente_e_none(db: TrackerDB) -> None:
    assert db.obter_playlist(999) is None


def test_listar_playlists_por_dono(db: TrackerDB) -> None:
    db.criar_playlist("alice", "rock")
    db.criar_playlist("alice", "jazz")
    db.criar_playlist("bob", "pop")
    nomes = [pl["nome"] for pl in db.listar_playlists("alice")]
    assert nomes == ["rock", "jazz"]
    assert db.listar_playlists("carol") == []


def test_remover_item_preserva_ordem_sem_colisao(db: TrackerDB) -> None:
    pid = db.criar_playlist("alice", "mix")
    for h in ("a" * 64, "b" * 64, "c" * 64):
        db.adicionar_item(pid, h, db.proxima_ordem(pid))
    db.remover_item(pid, "b" * 64)
    # proxima_ordem é MAX+1, então um novo add não colide com a ordem 2 de 'c'.
    db.adicionar_item(pid, "d" * 64, db.proxima_ordem(pid))
    assert db.obter_playlist(pid)["itens"] == ["a" * 64, "c" * 64, "d" * 64]


def test_deletar_playlist_remove_cabecalho_e_itens(db: TrackerDB) -> None:
    pid = db.criar_playlist("alice", "temp")
    db.adicionar_item(pid, "a" * 64, db.proxima_ordem(pid))
    db.deletar_playlist(pid)
    assert db.obter_playlist(pid) is None
    assert db.listar_playlists("alice") == []
