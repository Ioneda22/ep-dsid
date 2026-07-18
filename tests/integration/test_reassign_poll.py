from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from src.peer.tracker_client import PeerTrackerClient
from tests.integration.cluster import TrackerNode, cluster_de_trackers


@pytest.fixture()
def trackers(tmp_path: Path) -> Iterator[dict[str, TrackerNode]]:
    with cluster_de_trackers(tmp_path) as nodes:
        yield nodes


def _cfg(node: TrackerNode) -> dict[str, object]:
    return {"tracker_id": node.tracker_id, "ip": "127.0.0.1", "api_port": node.api_port}


def test_poll_entrega_reassign_e_peer_migra(trackers: dict[str, TrackerNode]) -> None:
    t1, t2, t3 = (trackers[f"tracker-{i}"] for i in (1, 2, 3))
    alice = PeerTrackerClient([_cfg(t1), _cfg(t2), _cfg(t3)], timeout=2.0)
    try:
        assert alice.peer_hello("alice", "127.0.0.1", 7001) is not None
        assert alice.current_tracker_index == 0

        t1.index.agendar_reassign("alice", "127.0.0.1", t2.api_port)
        alice.poll_reassign("alice")

        assert alice.current_tracker_index == 1
        assert alice.tracker_id == "tracker-2"
    finally:
        alice.close()


def test_poll_sem_pendencia_nao_migra(trackers: dict[str, TrackerNode]) -> None:
    t1, t2 = trackers["tracker-1"], trackers["tracker-2"]
    alice = PeerTrackerClient([_cfg(t1), _cfg(t2)], timeout=2.0)
    try:
        assert alice.peer_hello("alice", "127.0.0.1", 7001) is not None
        alice.poll_reassign("alice")
        assert alice.current_tracker_index == 0
        assert alice.tracker_id == "tracker-1"
    finally:
        alice.close()
