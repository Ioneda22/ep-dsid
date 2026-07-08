"""Integração da Fase 5: anti-entropy do ``SEED_REPORT`` (§9, main.tex §13.4).

Um peer registra um arquivo, apaga-o localmente SEM notificar o tracker e, no
ciclo seguinte de ``SEED_REPORT``, o hash omitido vira tombstone no índice — a
detecção não depende de ``PEER_LEAVE_FILE``.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import uvicorn

from src.peer.seed_reporter import SeedReporter
from src.peer.storage import Storage
from src.peer.tracker_client import PeerTrackerClient
from src.tracker.api import create_app
from src.tracker.index import Index
from src.tracker.persistence import init_db
from tests.integration.cluster import aguardar, porta_livre


@pytest.fixture()
def tracker(tmp_path: Path) -> Iterator[tuple[str, Index]]:
    """Um tracker uvicorn real em porta dinâmica; devolve (api_url, index)."""
    index = Index(tracker_id="tracker-1")
    porta = porta_livre()
    db = init_db(tmp_path / "tracker" / "peerspot.db")
    app = create_app(index=index, db=db, tracker_id="tracker-1", trackers_conhecidos=[])
    config = uvicorn.Config(
        app, host="127.0.0.1", port=porta, log_config=None, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    prazo = time.time() + 10
    while not server.started:
        if time.time() > prazo:
            pytest.fail("tracker não subiu em 10s")
        time.sleep(0.01)
    yield f"http://127.0.0.1:{porta}", index
    server.should_exit = True
    thread.join(timeout=5)
    db.close()


def _tem_fonte(index: Index, hash_arquivo: str, nome_peer: str) -> bool:
    fontes = index.get_snapshot().hash_to_peers.get(hash_arquivo, {})
    return nome_peer in fontes


def _tem_tombstone(index: Index, hash_arquivo: str, nome_peer: str) -> bool:
    return nome_peer in index.get_snapshot().tombstones.get(hash_arquivo, {})


def test_seed_report_omitido_vira_tombstone(
    tracker: tuple[str, Index], tmp_path: Path
) -> None:
    api_url, index = tracker
    porta_api = int(api_url.rsplit(":", 1)[1])
    trackers_cfg = [
        {"tracker_id": "tracker-1", "ip": "127.0.0.1", "api_port": porta_api}
    ]

    storage = Storage(tmp_path / "peer-alice")
    client = PeerTrackerClient(trackers_cfg)
    reporter = SeedReporter(
        nome_peer="alice",
        ip="127.0.0.1",
        porta=7001,
        storage=storage,
        tracker_client=client,
    )
    try:
        assert client.peer_hello("alice", "127.0.0.1", 7001) is not None
        # Arquivo de 1 chunk importado e registrado.
        origem = tmp_path / "musica.bin"
        origem.write_bytes(b"conteudo de teste")
        hash_arquivo, tamanho, n_chunks = storage.import_file(origem)
        assert (
            client.register_file(
                "alice", hash_arquivo, nome="musica", tamanho=tamanho, n_chunks=n_chunks
            )
            is not None
        )
        assert _tem_fonte(index, hash_arquivo, "alice")

        # Um SEED_REPORT ANTES de apagar mantém a fonte (relatório inclui o hash).
        assert reporter.enviar_agora()
        assert _tem_fonte(index, hash_arquivo, "alice")

        # Apaga o arquivo localmente sem notificar o tracker; o próximo relatório
        # omite o hash -> tombstone (main.tex §13.4).
        storage.remove_file(hash_arquivo)
        assert storage.list_local_files() == []
        assert reporter.enviar_agora()

        assert aguardar(lambda: _tem_tombstone(index, hash_arquivo, "alice"))
        assert not _tem_fonte(index, hash_arquivo, "alice")
    finally:
        client.close()
