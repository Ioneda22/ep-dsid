"""Testes unitários de src.peer.chunk_manager."""

from __future__ import annotations

from src.peer.chunk_manager import ChunkManager

HASH = "a" * 64


def test_missing_chunks_sem_download_retorna_todos() -> None:
    cm = ChunkManager()
    assert cm.missing_chunks(HASH, 4) == [0, 1, 2, 3]


def test_mark_received_remove_dos_faltantes() -> None:
    cm = ChunkManager()
    cm.start_download(HASH, 4)
    cm.mark_received(HASH, 1)
    cm.mark_received(HASH, 3)
    assert cm.missing_chunks(HASH, 4) == [0, 2]


def test_is_complete_apenas_com_todos_os_chunks() -> None:
    cm = ChunkManager()
    cm.start_download(HASH, 2)
    assert not cm.is_complete(HASH, 2)
    cm.mark_received(HASH, 0)
    assert not cm.is_complete(HASH, 2)
    cm.mark_received(HASH, 1)
    assert cm.is_complete(HASH, 2)


def test_is_complete_hash_desconhecido() -> None:
    assert not ChunkManager().is_complete(HASH, 1)


def test_progress_conta_recebidos() -> None:
    cm = ChunkManager()
    assert cm.progress(HASH, 3) == (0, 3)
    cm.mark_received(HASH, 0)
    assert cm.progress(HASH, 3) == (1, 3)


def test_reset_limpa_progresso() -> None:
    cm = ChunkManager()
    cm.mark_received(HASH, 0)
    cm.reset(HASH)
    assert cm.missing_chunks(HASH, 2) == [0, 1]


def test_downloads_de_hashes_distintos_sao_independentes() -> None:
    cm = ChunkManager()
    outro = "b" * 64
    cm.mark_received(HASH, 0)
    assert cm.missing_chunks(outro, 1) == [0]
