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


def test_playlists_sao_stub_ate_fase_6(db: TrackerDB) -> None:
    with pytest.raises(NotImplementedError):
        db.criar_playlist("alice", "favoritas")
    with pytest.raises(NotImplementedError):
        db.adicionar_item_playlist(1, "a" * 64)
    with pytest.raises(NotImplementedError):
        db.listar_playlist(1)
