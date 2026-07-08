"""Integração da Fase 5: detecção de falha de peer (§6.3, main.tex §13.4).

Um peer registra um arquivo e "morre" abruptamente (sem ``PEER_LEAVE``). Com o
relógio injetado avançado 7 min (> 6 min = 2 rodadas de ``SEED_REPORT``), o
failure detector marca todas as suas entradas como tombstone e as propaga via
``SYNC_TABLE`` (com ``seq`` — o mesmo caminho das escritas locais).
"""

from __future__ import annotations

from src.common.messages import SyncTableEntry
from src.tracker.failure_detector import FailureDetector
from src.tracker.index import Index

HASH_A = "aa" * 32
HASH_B = "bb" * 32


class _SyncClientFake:
    """Registra cada ``propagar_sync`` para inspeção do que seria flooded."""

    def __init__(self) -> None:
        self.known_trackers: list[object] = []
        self.chamadas: list[tuple[list[SyncTableEntry], int, float]] = []

    def propagar_sync(
        self, entries: list[SyncTableEntry], seq: int, timestamp: float | None = None
    ) -> None:
        self.chamadas.append((entries, seq, timestamp))


class _Relogio:
    """Relógio mutável injetável (§10): sem monkeypatch global."""

    def __init__(self, agora: float) -> None:
        self.agora = agora

    def __call__(self) -> float:
        return self.agora


def test_peer_silencioso_vira_tombstone_e_propaga() -> None:
    relogio = _Relogio(1000.0)
    index = Index(clock=relogio, tracker_id="tracker-1")
    sync = _SyncClientFake()
    detector = FailureDetector(
        "tracker-1", index, sync, seed_report_timeout_seconds=360.0
    )

    # alice presente (HELLO em t=1000) e fonte de dois arquivos.
    index.register_peer("alice", "127.0.0.1", 7001)
    index.register_file("alice", HASH_A, nome="Um", tamanho=10, n_chunks=1)
    index.register_file("alice", HASH_B, nome="Dois", tamanho=20, n_chunks=1)
    # bob também presente e reportando em dia.
    index.register_peer("bob", "127.0.0.1", 7002)

    # Antes do timeout, ninguém é considerado falho.
    assert detector.detectar_agora() == []

    # alice "morre": avançamos 7 min. bob renova o sinal de vida (segue vivo).
    relogio.agora = 1000.0 + 7 * 60
    index.update_last_seed("bob")

    falhos = detector.detectar_agora()

    assert falhos == ["alice"]
    # alice sai da presença; suas duas fontes viram tombstone.
    snap = index.get_snapshot()
    assert "alice" not in snap.nome_peer_to_endereco
    assert "bob" in snap.nome_peer_to_endereco
    assert "alice" in snap.tombstones[HASH_A]
    assert "alice" in snap.tombstones[HASH_B]
    assert "alice" not in snap.hash_to_peers.get(HASH_A, {})

    # Propagou UMA SYNC_TABLE (um evento -> um seq) com as duas entradas ativo=False.
    assert len(sync.chamadas) == 1
    entries, seq, timestamp = sync.chamadas[0]
    assert {e.hash for e in entries} == {HASH_A, HASH_B}
    assert all(e.ativo is False for e in entries)
    assert seq == index.versoes()["tracker-1"]  # seq local alocado
    assert timestamp == 1000.0 + 7 * 60

    # Idempotente: nova varredura não reencontra alice.
    assert detector.detectar_agora() == []
