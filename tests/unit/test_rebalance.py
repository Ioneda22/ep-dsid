"""Testes do sorteio inline de reassign no PEER_HELLO (rebalance por contato).

O sorteador é injetado para determinismo (§10). O foco é garantir que o
espalhamento seja uma decisão ÚNICA e não enfileirada: o handler devolve
reassign_to na própria resposta do PEER_HELLO e NÃO deixa nada em
_reassign_pendente, de modo que o HELLO de migração não gere um novo reassign e
o peer migre no máximo uma vez.
"""

from __future__ import annotations

from pathlib import Path

from src.common.messages import PeerHello
from src.tracker import handlers
from src.tracker.index import Index
from src.tracker.persistence import init_db
from src.tracker.rebalance import RebalanceManager

API = {
    "tracker-1": ("127.0.0.1", 8001),
    "tracker-2": ("127.0.0.1", 8002),
    "tracker-3": ("127.0.0.1", 8003),
}


class _FakeSyncClient:
    """Só expõe known_trackers; sortear_reassign não o utiliza."""

    def __init__(self) -> None:
        self.known_trackers: list[object] = []


def _reb(
    index: Index,
    tracker_id: str,
    escolha: str,
    api: dict[str, tuple[str, int]] | None = None,
) -> RebalanceManager:
    return RebalanceManager(
        tracker_id=tracker_id,
        index=index,
        sync_client=_FakeSyncClient(),
        api_por_tracker_id=dict(API if api is None else api),
        sorteador=lambda _alvos: escolha,
    )


def test_sortear_self_mantem_peer() -> None:
    reb = _reb(Index(tracker_id="tracker-1"), "tracker-1", escolha="tracker-1")
    assert reb.sortear_reassign() is None


def test_sortear_outro_devolve_endereco_rest() -> None:
    reb = _reb(Index(tracker_id="tracker-1"), "tracker-1", escolha="tracker-2")
    assert reb.sortear_reassign() == ("127.0.0.1", 8002)


def test_sortear_sem_outros_alvos_nao_espalha() -> None:
    reb = _reb(
        Index(tracker_id="tracker-1"),
        "tracker-1",
        escolha="tracker-1",
        api={"tracker-1": ("127.0.0.1", 8001)},
    )
    assert reb.sortear_reassign() is None


def test_hello_espalha_inline_sem_enfileirar(tmp_path: Path) -> None:
    """reassign_to volta na resposta, mas nada fica pendente (migra 1x só)."""
    index = Index(tracker_id="tracker-1")
    db = init_db(tmp_path / "t1.db")
    reb = _reb(index, "tracker-1", escolha="tracker-2")

    msg = PeerHello(nome_peer="alice", ip="127.0.0.1", porta=7001)
    resposta = handlers.handle_peer_hello(msg, index, db, reb)

    assert resposta["reassign_to"] == {"ip": "127.0.0.1", "api_port": 8002}
    assert index.consumir_reassign("alice") is None


def test_hello_sem_rebalance_nao_espalha(tmp_path: Path) -> None:
    index = Index(tracker_id="tracker-1")
    db = init_db(tmp_path / "t1.db")

    msg = PeerHello(nome_peer="alice", ip="127.0.0.1", porta=7001)
    resposta = handlers.handle_peer_hello(msg, index, db)

    assert "reassign_to" not in resposta


def test_hello_reassign_enfileirado_tem_prioridade(tmp_path: Path) -> None:
    """A migração agendada pela reintegração vence o sorteio inline."""
    index = Index(tracker_id="tracker-1")
    db = init_db(tmp_path / "t1.db")
    index.agendar_reassign("alice", "127.0.0.1", 8003)
    reb = _reb(index, "tracker-1", escolha="tracker-2")

    msg = PeerHello(nome_peer="alice", ip="127.0.0.1", porta=7001)
    resposta = handlers.handle_peer_hello(msg, index, db, reb)

    assert resposta["reassign_to"] == {"ip": "127.0.0.1", "api_port": 8003}
    assert index.consumir_reassign("alice") is None
