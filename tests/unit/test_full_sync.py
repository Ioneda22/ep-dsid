"""Testes unitários de ``Index.apply_full_sync`` — reconciliação anti-entropy.

``apply_full_sync`` converte cada ``FullSyncPeer`` numa escrita LWW e
reaproveita ``apply_sync_entry``. Aqui validamos: registro + metadata,
tombstone, descarte da versão mais antiga (LWW), idempotência e desempate por
``origem`` — com relógio fake injetado (§10 do CLAUDE.md).
"""

from __future__ import annotations

import pytest

from src.common.messages import FullSyncEntry, FullSyncPeer
from src.tracker.index import Index

HASH = "c" * 64


class RelogioFake:
    """Relógio determinístico injetado no Index (§10)."""

    def __init__(self, inicio: float = 1000.0) -> None:
        self.agora = inicio

    def __call__(self) -> float:
        return self.agora

    def avancar(self, segundos: float) -> None:
        self.agora += segundos


@pytest.fixture()
def relogio() -> RelogioFake:
    return RelogioFake()


@pytest.fixture()
def indice(relogio: RelogioFake) -> Index:
    return Index(clock=relogio, tracker_id="tracker-1")


def _peer(
    nome_peer: str = "alice",
    ip: str = "127.0.0.1",
    porta: int = 7001,
    ativo: bool = True,
    timestamp: float = 1000.0,
    origem: str = "tracker-2",
) -> FullSyncPeer:
    return FullSyncPeer(
        nome_peer=nome_peer,
        ip=ip,
        porta=porta,
        ativo=ativo,
        timestamp=timestamp,
        origem=origem,
    )


def _entry(peers: list[FullSyncPeer]) -> FullSyncEntry:
    return FullSyncEntry(
        hash=HASH, nome="Imagine", tamanho=100, n_chunks=1, peers=peers
    )


def _fonte_local(indice: Index) -> None:
    """alice registrada localmente (origem tracker-1, timestamp do relógio)."""
    indice.register_peer("alice", "127.0.0.1", 7001)
    indice.register_file("alice", HASH, nome="Imagine", tamanho=100, n_chunks=1)


def test_registra_fonte_e_metadata(indice: Index) -> None:
    aplicadas = indice.apply_full_sync([_entry([_peer()])])
    assert aplicadas == 1
    resultados = indice.search_by_name("Imagine")
    assert [r.hash for r in resultados] == [HASH]
    assert resultados[0].n_chunks == 1
    assert [p.nome_peer for p in resultados[0].peers] == ["alice"]


def test_aplica_tombstone(indice: Index) -> None:
    aplicadas = indice.apply_full_sync([_entry([_peer(ativo=False)])])
    assert aplicadas == 1
    snapshot = indice.get_snapshot()
    assert "alice" in snapshot.tombstones[HASH]
    assert indice.search_by_name("Imagine") == []


def test_aplica_multiplos_peers_de_uma_entrada(indice: Index) -> None:
    entry = _entry([_peer("alice", porta=7001), _peer("bob", porta=7002)])
    assert indice.apply_full_sync([entry]) == 2
    fontes = {p.nome_peer for p in indice.search_by_name("Imagine")[0].peers}
    assert fontes == {"alice", "bob"}


def test_lww_descarta_versao_mais_antiga(relogio: RelogioFake, indice: Index) -> None:
    relogio.agora = 2000.0
    _fonte_local(indice)  # versão local em t=2000
    # FULL_SYNC com timestamp anterior (1000) é descartado pelo LWW.
    aplicadas = indice.apply_full_sync(
        [_entry([_peer(ip="10.0.0.9", timestamp=1000.0)])]
    )
    assert aplicadas == 0
    assert indice.get_peers_for_hash(HASH)[0].ip == "127.0.0.1"


def test_idempotente(indice: Index) -> None:
    entries = [_entry([_peer()])]
    assert indice.apply_full_sync(entries) == 1
    # Reaplicar o MESMO estado não muda nada (empate não-estrito) — é o que
    # torna o push periódico de anti-entropy seguro.
    assert indice.apply_full_sync(entries) == 0


def test_desempate_por_origem(relogio: RelogioFake, indice: Index) -> None:
    relogio.agora = 1000.0
    _fonte_local(indice)  # t=1000, origem tracker-1
    # origem tracker-0 < tracker-1 (local): empate perde, descartada.
    assert (
        indice.apply_full_sync(
            [_entry([_peer(ip="10.0.0.0", timestamp=1000.0, origem="tracker-0")])]
        )
        == 0
    )
    # origem tracker-2 > tracker-1: empate vence, sobrescreve.
    assert (
        indice.apply_full_sync(
            [_entry([_peer(ip="10.0.0.2", timestamp=1000.0, origem="tracker-2")])]
        )
        == 1
    )
    assert indice.get_peers_for_hash(HASH)[0].ip == "10.0.0.2"
