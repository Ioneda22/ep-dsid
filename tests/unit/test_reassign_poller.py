from __future__ import annotations

from src.peer.reassign_poller import ReassignPoller


class _TrackerClientFake:
    def __init__(self, erro: Exception | None = None) -> None:
        self.chamadas: list[str] = []
        self._erro = erro

    def poll_reassign(self, nome_peer: str) -> None:
        self.chamadas.append(nome_peer)
        if self._erro is not None:
            raise self._erro


def test_poll_agora_consulta_o_tracker() -> None:
    fake = _TrackerClientFake()
    poller = ReassignPoller("alice", fake)  # type: ignore[arg-type]

    poller.poll_agora()

    assert fake.chamadas == ["alice"]


def test_poll_agora_engole_excecao() -> None:
    fake = _TrackerClientFake(erro=RuntimeError("tracker fora do ar"))
    poller = ReassignPoller("bob", fake)  # type: ignore[arg-type]

    poller.poll_agora()

    assert fake.chamadas == ["bob"]
