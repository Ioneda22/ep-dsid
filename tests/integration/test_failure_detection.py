"""Integração da Fase 5: detecção de falha de peer, END-TO-END (§6.3, main.tex §13.4).

Dois trackers REAIS (API uvicorn + sync server TCP). alice registra dois arquivos
no tracker-1 e some (sem ``PEER_LEAVE``); bob renova a presença. Com o relógio
injetado avançado 7 min (> 6 min = 2 rodadas), o ``FailureDetector`` REAL do
tracker-1 tombstona alice e propaga via ``SYNC_TABLE`` — e verificamos o tombstone
CHEGANDO de fato ao índice do tracker-2 (sem nenhum dublê de sincronização).
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from src.tracker.failure_detector import FailureDetector
from tests.integration.cluster import TrackerNode, aguardar, cluster_de_trackers

HASH_A = "aa" * 32
HASH_B = "bb" * 32


class RelogioFake:
    """Relógio mutável injetável (§10): sem monkeypatch global."""

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
def trackers(tmp_path: Path, relogio: RelogioFake) -> Iterator[dict[str, TrackerNode]]:
    with cluster_de_trackers(
        tmp_path, ids=("tracker-1", "tracker-2"), clock=relogio
    ) as nodes:
        yield nodes


def _hello(api_url: str, nome_peer: str, porta: int) -> None:
    resposta = httpx.post(
        f"{api_url}/peers/hello",
        json={
            "type": "PEER_HELLO",
            "nome_peer": nome_peer,
            "ip": "127.0.0.1",
            "porta": porta,
        },
        timeout=5.0,
    )
    assert resposta.status_code == 200


def _register(api_url: str, nome_peer: str, hash_arquivo: str, nome: str) -> None:
    resposta = httpx.post(
        f"{api_url}/files/register",
        json={
            "type": "REGISTER_FILE",
            "nome_peer": nome_peer,
            "hash": hash_arquivo,
            "nome": nome,
            "tamanho": 100,
            "n_chunks": 1,
        },
        timeout=5.0,
    )
    assert resposta.status_code == 200


def _fontes(index, hash_arquivo: str) -> set[str]:
    return set(index.get_snapshot().hash_to_peers.get(hash_arquivo, {}))


def _tombstonado(index, hash_arquivo: str, nome_peer: str) -> bool:
    return nome_peer in index.get_snapshot().tombstones.get(hash_arquivo, {})


def test_peer_silencioso_vira_tombstone_e_propaga_ao_outro_tracker(
    trackers: dict[str, TrackerNode], relogio: RelogioFake
) -> None:
    t1, t2 = trackers["tracker-1"], trackers["tracker-2"]

    # alice (fonte de dois arquivos) e bob presentes no tracker-1, em t=1000.
    _hello(t1.api_url, "alice", 7001)
    _register(t1.api_url, "alice", HASH_A, "Um")
    _register(t1.api_url, "alice", HASH_B, "Dois")
    _hello(t1.api_url, "bob", 7002)

    # O registro replica ao tracker-2 (flooding real).
    assert aguardar(lambda: _fontes(t2.index, HASH_A) == {"alice"})
    assert aguardar(lambda: _fontes(t2.index, HASH_B) == {"alice"})

    detector = FailureDetector(
        "tracker-1", t1.index, t1.sync_client, seed_report_timeout_seconds=360.0
    )
    # Antes do timeout, ninguém falha.
    assert detector.detectar_agora() == []

    # alice "morre"; passam 7 min. bob renova a presença (segue vivo).
    relogio.avancar(7 * 60)
    _hello(t1.api_url, "bob", 7002)

    falhos = detector.detectar_agora()

    # No tracker-1: só alice caiu; suas duas fontes viraram tombstone.
    assert falhos == ["alice"]
    snap1 = t1.index.get_snapshot()
    assert "alice" not in snap1.nome_peer_to_endereco
    assert "bob" in snap1.nome_peer_to_endereco
    assert _tombstonado(t1.index, HASH_A, "alice")
    assert _tombstonado(t1.index, HASH_B, "alice")

    # END-TO-END: o tombstone chega ao tracker-2 via SYNC_TABLE (LWW: ts do
    # tombstone > ts do registro) — alice deixa de ser fonte lá também.
    assert aguardar(
        lambda: _tombstonado(t2.index, HASH_A, "alice")
        and _tombstonado(t2.index, HASH_B, "alice")
    ), "tombstone do failure detector não propagou ao tracker-2"
    assert t2.index.search_by_name("Um") == []
    assert t2.index.search_by_name("Dois") == []

    # Idempotente: nova varredura não reencontra alice.
    assert detector.detectar_agora() == []
