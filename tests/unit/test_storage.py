"""Testes unitários de src.peer.storage — armazenamento local de chunks e montagem de arquivos no peer."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.common.errors import InvalidHashError
from src.common.hashing import sha256_bytes
from src.peer.storage import Storage

# Chunk pequeno para os testes não manipularem megabytes.
CHUNK = 16


@pytest.fixture()
def storage(tmp_path: Path) -> Storage:
    return Storage(tmp_path / "peer-teste", chunk_size=CHUNK)


def _conteudo(n_bytes: int) -> bytes:
    return bytes(i % 256 for i in range(n_bytes))


def test_save_e_load_chunk_roundtrip(storage: Storage) -> None:
    dados = _conteudo(CHUNK)
    storage.save_chunk("h1", 0, dados)
    assert storage.load_chunk("h1", 0) == dados


def test_load_chunk_inexistente_levanta(storage: Storage) -> None:
    with pytest.raises(FileNotFoundError):
        storage.load_chunk("h1", 0)


def test_has_chunk_durante_download(storage: Storage) -> None:
    assert not storage.has_chunk("h1", 0)
    storage.save_chunk("h1", 0, _conteudo(CHUNK))
    assert storage.has_chunk("h1", 0)
    assert not storage.has_chunk("h1", 1)


def test_assemble_valida_e_apaga_chunks(storage: Storage, tmp_path: Path) -> None:
    conteudo = _conteudo(CHUNK * 2 + 5)  # 3 chunks; último parcial
    hash_ok = sha256_bytes(conteudo)
    for i in range(3):
        storage.save_chunk(hash_ok, i, conteudo[i * CHUNK : (i + 1) * CHUNK])

    caminho = storage.assemble_file(hash_ok, 3)

    assert caminho.read_bytes() == conteudo
    # Chunks individuais apagados após a montagem (sem duplicar espaço).
    assert not (tmp_path / "peer-teste" / hash_ok / "chunks").exists()
    assert storage.list_local_files() == [hash_ok]


def test_assemble_chunk_faltando_levanta(storage: Storage) -> None:
    conteudo = _conteudo(CHUNK * 2)
    hash_ok = sha256_bytes(conteudo)
    storage.save_chunk(hash_ok, 0, conteudo[:CHUNK])  # falta o chunk 1
    with pytest.raises(FileNotFoundError):
        storage.assemble_file(hash_ok, 2)
    # Chunks preservados para retomada.
    assert storage.has_chunk(hash_ok, 0)


def test_assemble_hash_divergente_levanta_e_nao_monta(storage: Storage) -> None:
    storage.save_chunk("0" * 64, 0, _conteudo(CHUNK))
    with pytest.raises(InvalidHashError):
        storage.assemble_file("0" * 64, 1)
    assert not storage.assembled_path("0" * 64).exists()


def test_load_chunk_fatia_do_arquivo_montado(storage: Storage) -> None:
    conteudo = _conteudo(CHUNK * 2 + 7)
    hash_ok = sha256_bytes(conteudo)
    for i in range(3):
        storage.save_chunk(hash_ok, i, conteudo[i * CHUNK : (i + 1) * CHUNK])
    storage.assemble_file(hash_ok, 3)

    assert storage.load_chunk(hash_ok, 0) == conteudo[:CHUNK]
    assert storage.load_chunk(hash_ok, 2) == conteudo[2 * CHUNK :]
    assert storage.has_chunk(hash_ok, 2)
    assert not storage.has_chunk(hash_ok, 3)
    with pytest.raises(FileNotFoundError):
        storage.load_chunk(hash_ok, 3)


def test_get_chunk_count_montado_e_em_download(storage: Storage) -> None:
    conteudo = _conteudo(CHUNK + 1)  # 2 chunks
    hash_ok = sha256_bytes(conteudo)
    assert storage.get_chunk_count(hash_ok) == 0
    storage.save_chunk(hash_ok, 1, conteudo[CHUNK:])
    # Em download: maior índice presente + 1 (mesmo com buracos).
    assert storage.get_chunk_count(hash_ok) == 2
    storage.save_chunk(hash_ok, 0, conteudo[:CHUNK])
    storage.assemble_file(hash_ok, 2)
    assert storage.get_chunk_count(hash_ok) == 2


def test_import_file_calcula_hash_tamanho_chunks(
    storage: Storage, tmp_path: Path
) -> None:
    conteudo = _conteudo(CHUNK * 4 + 3)
    origem = tmp_path / "musica.bin"
    origem.write_bytes(conteudo)

    hash_arquivo, tamanho, n_chunks = storage.import_file(origem)

    assert hash_arquivo == sha256_bytes(conteudo)
    assert tamanho == len(conteudo)
    assert n_chunks == 5
    assert storage.assembled_path(hash_arquivo).read_bytes() == conteudo
    assert storage.list_local_files() == [hash_arquivo]


def test_remove_file_apaga_diretorio_do_hash(storage: Storage) -> None:
    conteudo = _conteudo(CHUNK)
    origem_hash = sha256_bytes(conteudo)
    storage.save_chunk(origem_hash, 0, conteudo)
    storage.assemble_file(origem_hash, 1)

    storage.remove_file(origem_hash)

    assert storage.list_local_files() == []
    assert not (storage.storage_dir / origem_hash).exists()


def test_remove_file_inexistente_nao_levanta(storage: Storage) -> None:
    storage.remove_file("f" * 64)  # idempotente


def test_list_local_files_ignora_downloads_incompletos(storage: Storage) -> None:
    storage.save_chunk("a" * 64, 0, _conteudo(CHUNK))
    assert storage.list_local_files() == []
