"""Testes das funções puras/resolução da PeerCLI (sem rede nem I/O de socket).

Exercita a resolução nome→hash (busca e local) e a formatação de tamanho —
a lógica nova que faz os comandos operarem por nome legível.
"""

from __future__ import annotations

from pathlib import Path

from src.common.messages import SearchResultEntry, SearchResultPeer
from src.peer.cli import PeerCLI
from src.peer.name_registry import NameRegistry

HASH_A = "a" * 64
HASH_B = "b" * 64


class _FakeStorage:
    """Storage mínimo: só o conjunto de arquivos completos locais."""

    def __init__(self, locais: list[str]) -> None:
        self._locais = locais

    def list_local_files(self) -> list[str]:
        return list(self._locais)


class _FakeChunkManager:
    downloads: dict[str, set[int]] = {}


class _FakeDownloader:
    chunk_manager = _FakeChunkManager()


def _entrada(hash_arquivo: str, nome: str) -> SearchResultEntry:
    peer = SearchResultPeer(nome_peer="alice", ip="127.0.0.1", porta=7001)
    return SearchResultEntry(hash=hash_arquivo, nome=nome, n_chunks=1, peers=[peer])


def _cli(tmp_path: Path, locais: list[str] | None = None) -> PeerCLI:
    return PeerCLI(
        nome_peer="bob",
        storage=_FakeStorage(locais or []),  # type: ignore[arg-type]
        tracker_client=object(),  # type: ignore[arg-type]
        downloader=_FakeDownloader(),  # type: ignore[arg-type]
        name_registry=NameRegistry(tmp_path),
    )


# ---------------------------------------------------------------------------
# _formatar_tamanho
# ---------------------------------------------------------------------------


def test_formatar_tamanho_unidades() -> None:
    assert PeerCLI._formatar_tamanho(0) == "0 B"
    assert PeerCLI._formatar_tamanho(512) == "512 B"
    assert PeerCLI._formatar_tamanho(1024) == "1.0 KB"
    assert PeerCLI._formatar_tamanho(5 * 1024 * 1024) == "5.0 MB"
    assert PeerCLI._formatar_tamanho(3 * 1024**3) == "3.0 GB"


# ---------------------------------------------------------------------------
# Resolução pela última busca (download / peers)
# ---------------------------------------------------------------------------


def test_resolver_busca_por_nome(tmp_path: Path) -> None:
    cli = _cli(tmp_path)
    cli._ultima_busca = [_entrada(HASH_A, "Imagine")]
    assert cli._resolver_busca("Imagine").hash == HASH_A
    assert cli._resolver_busca("imagine").hash == HASH_A  # case-insensitive


def test_resolver_busca_por_indice_e_hash(tmp_path: Path) -> None:
    cli = _cli(tmp_path)
    cli._ultima_busca = [_entrada(HASH_A, "Imagine"), _entrada(HASH_B, "Yesterday")]
    assert cli._resolver_busca("2").hash == HASH_B
    assert cli._resolver_busca(HASH_A).hash == HASH_A


def test_resolver_busca_sem_busca_previa(tmp_path: Path) -> None:
    assert _cli(tmp_path)._resolver_busca("Imagine") is None


def test_resolver_busca_indice_fora_do_intervalo(tmp_path: Path) -> None:
    cli = _cli(tmp_path)
    cli._ultima_busca = [_entrada(HASH_A, "Imagine")]
    assert cli._resolver_busca("5") is None


def test_resolver_busca_nome_ambiguo(tmp_path: Path) -> None:
    cli = _cli(tmp_path)
    cli._ultima_busca = [_entrada(HASH_A, "Imagine"), _entrada(HASH_B, "Imagine")]
    assert cli._resolver_busca("Imagine") is None  # ambíguo → exige nº/hash


# ---------------------------------------------------------------------------
# Resolução local (remove)
# ---------------------------------------------------------------------------


def test_resolver_local_por_nome(tmp_path: Path) -> None:
    cli = _cli(tmp_path, locais=[HASH_A])
    cli.name_registry.registrar(HASH_A, "Imagine")
    assert cli._resolver_local("Imagine") == HASH_A


def test_resolver_local_por_hash(tmp_path: Path) -> None:
    cli = _cli(tmp_path, locais=[HASH_A])
    assert cli._resolver_local(HASH_A) == HASH_A


def test_resolver_local_desconhecido(tmp_path: Path) -> None:
    cli = _cli(tmp_path, locais=[HASH_A])
    assert cli._resolver_local("Inexistente") is None


def test_resolver_local_nome_de_arquivo_ausente(tmp_path: Path) -> None:
    # Nome registrado, mas o arquivo não está mais entre os locais completos.
    cli = _cli(tmp_path, locais=[])
    cli.name_registry.registrar(HASH_A, "Imagine")
    assert cli._resolver_local("Imagine") is None
