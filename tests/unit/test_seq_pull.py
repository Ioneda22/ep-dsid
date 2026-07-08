"""Testes unitários do mecanismo de ``seq`` / vetor de versões / reparo.

Cobre (main.tex §11.3): alocação de ``seq`` por escrita local, proveniência
gravada por ``apply_sync_entry`` sem afetar o LWW, detecção de lacuna com
``desde_seq`` capturado, avanço de ``visto`` por ``max``, pendências,
comparação de digest e montagem da resposta de ``SYNC_PULL`` (agrupada por
``seq``, com tombstones, só o estado atual). Relógio fake injetado (§10).
"""

from __future__ import annotations

import pytest

from src.common.messages import SyncPullItem, SyncTableEntry
from src.tracker.index import Index, PeerEntry

HASH_A = "a" * 64
HASH_B = "b" * 64


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


def _registra(indice: Index, nome_peer: str, hash_arquivo: str, nome: str) -> PeerEntry:
    indice.register_peer(nome_peer, "127.0.0.1", 7001)
    entry, _ = indice.register_file(
        nome_peer, hash_arquivo, nome=nome, tamanho=100, n_chunks=1
    )
    return entry


def _entry(
    nome_peer: str = "carol", hash_arquivo: str = HASH_A, ativo: bool = True
) -> SyncTableEntry:
    return SyncTableEntry(
        hash=hash_arquivo,
        nome_peer=nome_peer,
        ip="127.0.0.1",
        porta=7003,
        ativo=ativo,
        nome="Imagine" if ativo else None,
        tamanho=100 if ativo else None,
        n_chunks=1 if ativo else None,
    )


# ---------------------------------------------------------------------------
# Alocação de seq local (meu_seq == visto[tracker_id])
# ---------------------------------------------------------------------------


def test_escritas_locais_alocam_seq_monotonico(indice: Index) -> None:
    e1 = _registra(indice, "alice", HASH_A, "Imagine")
    e2 = _registra(indice, "bob", HASH_B, "Yesterday")
    assert (e1.seq, e2.seq) == (1, 2)
    assert indice.versoes()["tracker-1"] == 2  # meu_seq


def test_tombstone_local_aloca_seq(indice: Index) -> None:
    _registra(indice, "alice", HASH_A, "Imagine")  # seq 1
    tomb = indice.remove_peer_from_hash(HASH_A, "alice")  # seq 2
    assert tomb.seq == 2
    assert indice.versoes()["tracker-1"] == 2


def test_seed_report_sem_mudanca_nao_consome_seq(indice: Index) -> None:
    _registra(indice, "alice", HASH_A, "Imagine")  # seq 1
    # Reafirma o MESMO hash: sem mudança, meu_seq não avança (sem churn).
    indice.apply_seed_hashes("alice", {HASH_A})
    assert indice.versoes()["tracker-1"] == 1


def test_versoes_inclui_self_mesmo_zerado(indice: Index) -> None:
    assert indice.versoes() == {"tracker-1": 0}


# ---------------------------------------------------------------------------
# apply_sync_entry: grava seq como proveniência, sem entrar no LWW
# ---------------------------------------------------------------------------


def test_apply_sync_entry_grava_seq_sem_afetar_lww(indice: Index) -> None:
    assert indice.apply_sync_entry(_entry(), "tracker-2", timestamp=900.0, seq=7)
    fonte = indice.get_snapshot().hash_to_peers[HASH_A]["carol"]
    assert (fonte.seq, fonte.origem) == (7, "tracker-2")
    # seq NÃO desempata: timestamp menor perde mesmo com seq muito maior.
    assert not indice.apply_sync_entry(_entry(), "tracker-2", timestamp=800.0, seq=99)
    assert indice.get_snapshot().hash_to_peers[HASH_A]["carol"].seq == 7


# ---------------------------------------------------------------------------
# registrar_recepcao_flood: contiguidade, lacuna e pendência
# ---------------------------------------------------------------------------


def test_recepcao_contigua_nao_abre_pendencia(indice: Index) -> None:
    assert indice.registrar_recepcao_flood("tracker-2", 1) is None
    assert indice.registrar_recepcao_flood("tracker-2", 2) is None
    assert indice.versoes()["tracker-2"] == 2
    assert indice.pendencias() == {}


def test_lacuna_abre_pendencia_com_desde_capturado(indice: Index) -> None:
    assert indice.registrar_recepcao_flood("tracker-2", 1) is None  # visto=1
    # Recebe seq 11 (perdeu 2..10): desde_seq capturado = 1 (visto no instante).
    assert indice.registrar_recepcao_flood("tracker-2", 11) == 1
    # visto avança por max MESMO com a lacuna aberta (não trava em 1).
    assert indice.versoes()["tracker-2"] == 11
    assert indice.pendencias() == {"tracker-2": 1}
    # A escrita 12 é contígua com 11: aplicada na hora, sem nova pendência.
    assert indice.registrar_recepcao_flood("tracker-2", 12) is None
    assert indice.pendencias() == {"tracker-2": 1}  # desde_seq preserva o 1


def test_pendencia_mantem_menor_desde_seq(indice: Index) -> None:
    for seq in (1, 2, 3):
        assert indice.registrar_recepcao_flood("tracker-2", seq) is None
    assert indice.registrar_recepcao_flood("tracker-2", 7) == 3  # desde capturado = 3
    assert indice.pendencias() == {"tracker-2": 3}
    # Nova lacuna com visto já em 7: desde seria 7, mas mantém o MENOR (3).
    assert indice.registrar_recepcao_flood("tracker-2", 20) == 3
    assert indice.pendencias() == {"tracker-2": 3}
    assert indice.versoes()["tracker-2"] == 20


def test_resolver_pendencia(indice: Index) -> None:
    indice.registrar_recepcao_flood("tracker-2", 1)
    indice.registrar_recepcao_flood("tracker-2", 5)  # lacuna
    assert indice.pendencias() == {"tracker-2": 1}
    indice.resolver_pendencia("tracker-2")
    assert indice.pendencias() == {}


def test_avancar_visto_por_max_nao_abre_pendencia(indice: Index) -> None:
    indice.avancar_visto("tracker-2", 5)
    indice.avancar_visto("tracker-2", 3)  # menor: não regride
    assert indice.versoes()["tracker-2"] == 5
    assert indice.pendencias() == {}  # o reparo (pull) não dispara novos pulls


# ---------------------------------------------------------------------------
# comparar_digest
# ---------------------------------------------------------------------------


def test_comparar_digest_pede_onde_emissor_esta_a_frente(indice: Index) -> None:
    indice.avancar_visto("tracker-2", 3)
    faltando = indice.comparar_digest({"tracker-2": 5, "tracker-3": 2, "tracker-1": 0})
    # tracker-2: 5 > 3 -> pede desde 3; tracker-3: 2 > 0 -> desde 0;
    # tracker-1: 0 > 0 é falso -> não pede.
    assert {i.origem: i.desde_seq for i in faltando} == {
        "tracker-2": 3,
        "tracker-3": 0,
    }


# ---------------------------------------------------------------------------
# selecionar_para_pull: um evento por seq, só o estado atual, com tombstones
# ---------------------------------------------------------------------------


def test_pull_response_reflete_estado_atual_sem_historico(indice: Index) -> None:
    _registra(indice, "alice", HASH_A, "Imagine")  # seq 1
    _registra(indice, "bob", HASH_B, "Yesterday")  # seq 2
    indice.remove_peer_from_hash(HASH_A, "alice")  # tombstone, seq 3

    tabelas = indice.selecionar_para_pull(
        [SyncPullItem(origem="tracker-1", desde_seq=0)]
    )
    # O registro seq 1 foi SUPERADO pelo tombstone seq 3 (sem log histórico):
    # só sobram seq 2 (fonte ativa) e seq 3 (tombstone).
    por_seq = {t.seq: t for t in tabelas}
    assert sorted(por_seq) == [2, 3]
    assert all(t.origem == "tracker-1" for t in tabelas)
    assert por_seq[2].entries[0].ativo is True
    assert por_seq[2].entries[0].nome == "Yesterday"  # metadados p/ busca por nome
    assert por_seq[3].entries[0].ativo is False  # tombstone incluído


def test_pull_response_desde_seq_filtra(indice: Index) -> None:
    _registra(indice, "alice", HASH_A, "Imagine")  # seq 1
    _registra(indice, "bob", HASH_B, "Yesterday")  # seq 2
    tabelas = indice.selecionar_para_pull(
        [SyncPullItem(origem="tracker-1", desde_seq=1)]
    )
    assert [t.seq for t in tabelas] == [2]  # só o que veio depois de seq 1


def test_peer_leave_compartilha_um_seq_entre_tombstones(indice: Index) -> None:
    indice.register_peer("alice", "127.0.0.1", 7001)
    indice.register_file("alice", HASH_A, nome="A", tamanho=100, n_chunks=1)  # seq 1
    indice.register_file("alice", HASH_B, nome="B", tamanho=100, n_chunks=1)  # seq 2
    indice.remove_peer("alice")  # PEER_LEAVE: tombstona A e B com UM seq (3)

    assert indice.versoes()["tracker-1"] == 3
    tabelas = indice.selecionar_para_pull(
        [SyncPullItem(origem="tracker-1", desde_seq=0)]
    )
    # Um único evento (uma saída) -> um seq -> uma SYNC_TABLE com dois tombstones.
    assert [t.seq for t in tabelas] == [3]
    assert len(tabelas[0].entries) == 2
    assert all(e.ativo is False for e in tabelas[0].entries)
