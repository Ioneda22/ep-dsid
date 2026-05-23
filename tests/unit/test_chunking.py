"""Testes unitários para src/common/chunking.py."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.common.chunking import (
    DEFAULT_CHUNK_SIZE,
    chunk_count,
    join_chunks,
    split_file,
)


def test_chunk_count_zero() -> None:
    assert chunk_count(0) == 0


def test_chunk_count_exato() -> None:
    assert chunk_count(DEFAULT_CHUNK_SIZE) == 1
    assert chunk_count(DEFAULT_CHUNK_SIZE * 4) == 4


def test_chunk_count_nao_multiplo() -> None:
    assert chunk_count(DEFAULT_CHUNK_SIZE + 1) == 2
    assert chunk_count(DEFAULT_CHUNK_SIZE * 3 + 100) == 4


def test_chunk_count_invalido() -> None:
    with pytest.raises(ValueError):
        chunk_count(-1)
    with pytest.raises(ValueError):
        chunk_count(10, chunk_size=0)


def test_split_join_multiplo_do_chunk_size(tmp_path: Path) -> None:
    chunk_size = 1024
    n_chunks = 5
    data = os.urandom(chunk_size * n_chunks)
    origem = tmp_path / "orig.bin"
    origem.write_bytes(data)

    chunks = list(split_file(origem, chunk_size=chunk_size))
    assert len(chunks) == n_chunks
    assert all(len(c) == chunk_size for c in chunks)

    destino = tmp_path / "reconstruido.bin"
    total = join_chunks(chunks, destino)
    assert total == len(data)
    assert destino.read_bytes() == data


def test_split_join_nao_multiplo_do_chunk_size(tmp_path: Path) -> None:
    """Tamanho NÃO múltiplo: último chunk é menor, reconstrução deve casar."""
    chunk_size = 1024
    tamanho = chunk_size * 3 + 137  # último chunk = 137 bytes
    data = os.urandom(tamanho)
    origem = tmp_path / "orig.bin"
    origem.write_bytes(data)

    chunks = list(split_file(origem, chunk_size=chunk_size))
    assert len(chunks) == 4
    assert all(len(c) == chunk_size for c in chunks[:-1])
    assert len(chunks[-1]) == 137

    destino = tmp_path / "rec.bin"
    join_chunks(chunks, destino)
    assert destino.read_bytes() == data


def test_split_arquivo_vazio(tmp_path: Path) -> None:
    f = tmp_path / "vazio.bin"
    f.write_bytes(b"")
    assert list(split_file(f)) == []


def test_split_chunk_size_invalido(tmp_path: Path) -> None:
    f = tmp_path / "x.bin"
    f.write_bytes(b"a")
    with pytest.raises(ValueError):
        list(split_file(f, chunk_size=0))


def test_join_em_diretorio_existente(tmp_path: Path) -> None:
    destino = tmp_path / "out.bin"
    total = join_chunks([b"abc", b"def", b""], destino)
    assert total == 6
    assert destino.read_bytes() == b"abcdef"
