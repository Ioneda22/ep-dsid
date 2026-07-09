"""Testes de tombstone: marcação de remoção via SYNC_TABLE e expiração após a retenção.

Relógio injetado: o tempo "avança" sem dormir. A varredura periódica
(TombstoneReaper) é testada com intervalo curto de verdade, mas a regra
de idade continua vindo do relógio fake.
"""

from __future__ import annotations

import pytest

from src.common.messages import SyncTableEntry
from src.tracker.index import Index
from src.tracker.tombstone import TombstoneReaper
from tests.integration.cluster import aguardar

HASH = "a" * 64
RETENCAO = 600.0  # 10 min: janela de retenção do tombstone antes do descarte


class RelogioFake:
    """Relógio determinístico injetado no Index para tornar os testes de idade determinísticos."""

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
    indice = Index(clock=relogio, tracker_id="tracker-1")
    indice.register_peer("alice", "127.0.0.1", 7001)
    indice.register_file("alice", HASH, nome="Imagine", tamanho=100, n_chunks=1)
    return indice


def _tombstone_remoto(indice: Index, relogio: RelogioFake) -> None:
    relogio.avancar(1)
    entry = SyncTableEntry(
        hash=HASH, nome_peer="alice", ip="127.0.0.1", porta=7001, ativo=False
    )
    assert indice.apply_sync_entry(entry, "tracker-2", timestamp=relogio.agora, seq=1)


def test_sync_ativo_false_vira_tombstone(indice: Index, relogio: RelogioFake) -> None:
    _tombstone_remoto(indice, relogio)
    snapshot = indice.get_snapshot()
    assert "alice" in snapshot.tombstones[HASH]
    assert "alice" not in snapshot.hash_to_peers.get(HASH, {})
    assert indice.search_by_name("Imagine") == []


def test_tombstone_nao_expira_antes_da_retencao(
    indice: Index, relogio: RelogioFake
) -> None:
    _tombstone_remoto(indice, relogio)
    relogio.avancar(RETENCAO - 1)
    assert indice.expire_tombstones(RETENCAO) == 0
    assert "alice" in indice.get_snapshot().tombstones[HASH]


def test_tombstone_expira_apos_retencao(indice: Index, relogio: RelogioFake) -> None:
    _tombstone_remoto(indice, relogio)
    relogio.avancar(RETENCAO + 1)
    assert indice.expire_tombstones(RETENCAO) == 1
    # O hash some inteiro da tabela de tombstones (sem dicts vazios órfãos).
    assert indice.get_snapshot().tombstones == {}


def test_expiracao_e_seletiva_por_idade(indice: Index, relogio: RelogioFake) -> None:
    _tombstone_remoto(indice, relogio)  # alice em t=1001
    relogio.avancar(RETENCAO)  # bob será tombstonado agora (t=1601)
    indice.register_peer("bob", "127.0.0.1", 7002)
    indice.register_file("bob", HASH)
    indice.remove_peer_from_hash(HASH, "bob")
    relogio.avancar(2)  # alice: idade 602 > 600; bob: idade 2
    assert indice.expire_tombstones(RETENCAO) == 1
    tombstones = indice.get_snapshot().tombstones[HASH]
    assert set(tombstones) == {"bob"}


def test_reaper_thread_expira_em_background(
    indice: Index, relogio: RelogioFake
) -> None:
    """A thread dedicada (60s em produção; 0.05s aqui) chama a expiração."""
    _tombstone_remoto(indice, relogio)
    reaper = TombstoneReaper(
        "tracker-1", indice, retention_seconds=RETENCAO, interval_seconds=0.05
    )
    reaper.start()
    try:
        relogio.avancar(RETENCAO + 1)
        assert aguardar(lambda: indice.get_snapshot().tombstones == {}, timeout=3.0)
    finally:
        reaper.stop()
