"""Testes da resolução Last Write Wins do índice (§6.2; main.tex §12.2).

Aplica entradas de ``SYNC_TABLE`` concorrentes sobre o mesmo ``Index`` e
verifica as três regras: timestamp maior vence, menor é descartado e o
empate é resolvido pelo maior ``tracker_id`` — de forma determinística,
independente da ordem de chegada (senão as réplicas divergiriam).

O ``seq`` viaja junto de cada aplicação (proveniência), mas NÃO entra na
decisão LWW; aqui usamos um ``seq`` fixo, pois o que se testa é o desempate
por ``(timestamp, origem)``.
"""

from __future__ import annotations

import pytest

from src.common.messages import SyncTableEntry
from src.tracker.index import Index

HASH = "a" * 64


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


def _entry(
    nome_peer: str = "alice",
    ip: str = "127.0.0.1",
    porta: int = 7001,
    ativo: bool = True,
) -> SyncTableEntry:
    return SyncTableEntry(
        hash=HASH,
        nome_peer=nome_peer,
        ip=ip,
        porta=porta,
        ativo=ativo,
        nome="Imagine" if ativo else None,
        tamanho=100 if ativo else None,
        n_chunks=1 if ativo else None,
    )


def _aplicar(
    indice: Index,
    entry: SyncTableEntry,
    origem: str,
    timestamp: float,
    seq: int = 1,
) -> bool:
    """Aplica uma entry de SYNC_TABLE (o ``seq`` é proveniência, não LWW)."""
    return indice.apply_sync_entry(entry, origem, timestamp, seq)


def _com_fonte_local(indice: Index) -> Index:
    """alice registrada localmente em t=1000 com origem tracker-1."""
    indice.register_peer("alice", "127.0.0.1", 7001)
    indice.register_file("alice", HASH, nome="Imagine", tamanho=100, n_chunks=1)
    return indice


# ---------------------------------------------------------------------------
# Timestamp maior vence; menor é descartado
# ---------------------------------------------------------------------------


def test_timestamp_menor_perde(indice: Index) -> None:
    _com_fonte_local(indice)  # versão local: t=1000
    assert not _aplicar(indice, _entry(ativo=False), "tracker-2", timestamp=999.0)
    assert [p.nome_peer for p in indice.get_peers_for_hash(HASH)] == ["alice"]
    assert HASH not in indice.get_snapshot().tombstones


def test_timestamp_maior_vence(indice: Index) -> None:
    _com_fonte_local(indice)  # versão local: t=1000
    assert _aplicar(indice, _entry(ativo=False), "tracker-2", timestamp=1001.0)
    assert indice.get_peers_for_hash(HASH) == []
    assert indice.get_snapshot().tombstones[HASH]["alice"].timestamp == 1001.0


def test_atualizacao_de_endereco_com_timestamp_maior(indice: Index) -> None:
    _com_fonte_local(indice)
    nova_versao = _entry(ip="10.0.0.9", porta=7999)
    assert _aplicar(indice, nova_versao, "tracker-2", timestamp=2000.0)
    fonte = indice.get_peers_for_hash(HASH)[0]
    assert (fonte.ip, fonte.porta) == ("10.0.0.9", 7999)


# ---------------------------------------------------------------------------
# Empate de timestamp: maior tracker_id vence (lexicográfico), em qualquer ordem
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ordem", [("tracker-2", "tracker-3"), ("tracker-3", "tracker-2")]
)
def test_empate_vence_maior_tracker_id_independente_da_ordem(
    indice: Index, ordem: tuple[str, str]
) -> None:
    versoes = {
        "tracker-2": _entry(ip="10.0.0.2"),
        "tracker-3": _entry(ip="10.0.0.3"),
    }
    for origem in ordem:
        _aplicar(indice, versoes[origem], origem, timestamp=500.0)
    fonte = indice.get_snapshot().hash_to_peers[HASH]["alice"]
    # LWW: empate vence maior tracker_id — main.tex §12.2.
    assert (fonte.ip, fonte.origem) == ("10.0.0.3", "tracker-3")


def test_empate_com_escrita_local_compara_origens(indice: Index) -> None:
    _com_fonte_local(indice)  # t=1000, origem tracker-1
    # tracker-0 < tracker-1: empate perde, descartada.
    assert not _aplicar(indice, _entry(ip="10.0.0.0"), "tracker-0", timestamp=1000.0)
    # tracker-2 > tracker-1: empate vence, sobrescreve.
    assert _aplicar(indice, _entry(ip="10.0.0.2"), "tracker-2", timestamp=1000.0)
    assert indice.get_peers_for_hash(HASH)[0].ip == "10.0.0.2"


def test_replay_identico_e_descartado(indice: Index) -> None:
    entry = _entry()
    assert _aplicar(indice, entry, "tracker-2", timestamp=500.0)
    # Mesmo timestamp e mesma origem: empate não-estrito, descartado.
    assert not _aplicar(indice, entry, "tracker-2", timestamp=500.0)


# ---------------------------------------------------------------------------
# Interação registro ↔ tombstone (main.tex §12.3)
# ---------------------------------------------------------------------------


def test_registro_mais_recente_remove_tombstone(indice: Index) -> None:
    _aplicar(indice, _entry(ativo=False), "tracker-2", timestamp=1000.0)
    assert _aplicar(indice, _entry(), "tracker-3", timestamp=1001.0)
    snapshot = indice.get_snapshot()
    assert HASH not in snapshot.tombstones
    assert "alice" in snapshot.hash_to_peers[HASH]


def test_registro_atrasado_nao_ressuscita_removido(indice: Index) -> None:
    """A razão de existir do tombstone: segurar adds fora de ordem."""
    _aplicar(indice, _entry(ativo=False), "tracker-2", timestamp=1000.0)
    assert not _aplicar(indice, _entry(), "tracker-3", timestamp=999.0)
    snapshot = indice.get_snapshot()
    assert "alice" in snapshot.tombstones[HASH]
    assert "alice" not in snapshot.hash_to_peers.get(HASH, {})
