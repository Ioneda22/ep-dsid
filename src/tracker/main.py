"""Entrypoint do tracker: python -m src.tracker.main --config config/tracker-1.yaml.

Sobe, no MESMO processo Python:

* a API REST (FastAPI/uvicorn) na api_port — atendimento aos peers;
* o servidor TCP de sincronização (SyncServer) na sync_port, em thread
  própria — flooding SYNC_TABLE, SYNC_PULL, SYNC_DIGEST e
  SEARCH_FORWARD;
* o TombstoneReaper (expiração de tombstones a cada 60s);
* o DigestBroadcaster (push periódico de SYNC_DIGEST — backstop que
  repara o último delta perdido de um tracker que ficou em silêncio);
* o FailureDetector (tombstona as fontes de um peer sem SEED_REPORT por
  tempo demais) e o RebalanceManager (cede peers via REASSIGN_TRACKER).

Um tracker não-bootstrap, ao subir, ainda se reintegra à rede em background:
TRACKER_REJOIN ao bootstrap, TRACKER_LIST de volta e SYNC_PULL(0) por
origem para reconstruir o índice, sem bloquear a subida da API.
"""

from __future__ import annotations

import argparse
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import uvicorn

from src.common.config import load_yaml, require_keys
from src.common.logging_config import setup_logging
from src.tracker.anti_entropy import DigestBroadcaster
from src.tracker.api import create_app
from src.tracker.failure_detector import FailureDetector
from src.tracker.index import Index
from src.tracker.persistence import init_db
from src.tracker.rebalance import RebalanceManager
from src.tracker.routing import SearchRouter
from src.tracker.sync_client import KnownTracker, SyncClient
from src.tracker.sync_server import SyncServer
from src.tracker.tombstone import TombstoneReaper

logger = logging.getLogger(__name__)

_CHAVES_OBRIGATORIAS = (
    "tracker_id",
    "ip",
    "api_port",
    "sync_port",
    "is_bootstrap",
    "db_path",
    "log_path",
    "log_level",
)


@dataclass
class TrackerSettings:
    """Configuração de um tracker carregada do YAML."""

    tracker_id: str
    ip: str
    api_port: int
    sync_port: int
    is_bootstrap: bool
    known_trackers: list[dict[str, Any]]
    db_path: Path
    log_path: Path
    log_level: str
    # Constantes operacionais.
    seed_report_timeout_seconds: int = 360
    tombstone_retention_seconds: int = 600
    sync_outbound_timeout_seconds: int = 3
    search_forward_timeout_seconds: int = 2
    # Digest periódico: 5 min, < retenção do tombstone (600s).
    digest_interval_seconds: int = 300


def load_tracker_settings(config_path: Path) -> TrackerSettings:
    """Carrega e valida o YAML de configuração do tracker.

    Raises:
        ConfigError: Se o arquivo não existir ou faltar chave obrigatória.
    """
    cfg = load_yaml(config_path)
    require_keys(cfg, _CHAVES_OBRIGATORIAS, str(config_path))
    return TrackerSettings(
        tracker_id=str(cfg["tracker_id"]),
        ip=str(cfg["ip"]),
        api_port=int(cfg["api_port"]),
        sync_port=int(cfg["sync_port"]),
        is_bootstrap=bool(cfg["is_bootstrap"]),
        known_trackers=list(cfg.get("known_trackers") or []),
        db_path=Path(cfg["db_path"]),
        log_path=Path(cfg["log_path"]),
        log_level=str(cfg["log_level"]),
        seed_report_timeout_seconds=int(cfg.get("seed_report_timeout_seconds", 360)),
        tombstone_retention_seconds=int(cfg.get("tombstone_retention_seconds", 600)),
        sync_outbound_timeout_seconds=int(cfg.get("sync_outbound_timeout_seconds", 3)),
        search_forward_timeout_seconds=int(
            cfg.get("search_forward_timeout_seconds", 2)
        ),
        digest_interval_seconds=int(cfg.get("digest_interval_seconds", 300)),
    )


def _trackers_conhecidos(settings: TrackerSettings) -> list[dict[str, Any]]:
    """Lista exposta em GET /trackers: este tracker + os do YAML."""
    proprio = {
        "tracker_id": settings.tracker_id,
        "ip": settings.ip,
        "api_port": settings.api_port,
        "sync_port": settings.sync_port,
    }
    return [proprio, *settings.known_trackers]


def _api_por_tracker_id(settings: TrackerSettings) -> dict[str, tuple[str, int]]:
    """Mapa tracker_id -> (ip, api_port) para o rebalance apontar peers.

    Inclui este tracker e os known_trackers do YAML que declararem api_port
    (necessário para dizer ao peer cedido onde se reportar via REST).
    """
    mapa: dict[str, tuple[str, int]] = {
        settings.tracker_id: (settings.ip, settings.api_port)
    }
    for entrada in settings.known_trackers:
        if "api_port" in entrada:
            mapa[str(entrada["tracker_id"])] = (
                str(entrada["ip"]),
                int(entrada["api_port"]),
            )
    return mapa


def _known_trackers_sync(settings: TrackerSettings) -> list[KnownTracker]:
    """Converte os known_trackers do YAML em destinos de sincronização.

    Raises:
        ConfigError: Se alguma entrada não tiver tracker_id/ip/sync_port.
    """
    destinos: list[KnownTracker] = []
    for entrada in settings.known_trackers:
        require_keys(
            entrada, ("tracker_id", "ip", "sync_port"), origem="known_trackers"
        )
        destinos.append(
            KnownTracker(
                tracker_id=str(entrada["tracker_id"]),
                ip=str(entrada["ip"]),
                sync_port=int(entrada["sync_port"]),
            )
        )
    return destinos


def main(argv: list[str] | None = None) -> None:
    """Carrega config, sobe sync server + reaper e serve a API REST."""
    parser = argparse.ArgumentParser(description="Tracker do PeerSpot")
    parser.add_argument(
        "--config", required=True, type=Path, help="caminho do YAML (config/*.yaml)"
    )
    args = parser.parse_args(argv)

    settings = load_tracker_settings(args.config)
    setup_logging(settings.log_path, settings.log_level)
    logger.info(
        "tracker_id=%s subindo API REST em %s:%d e sync em %s:%d (bootstrap=%s)",
        settings.tracker_id,
        settings.ip,
        settings.api_port,
        settings.ip,
        settings.sync_port,
        settings.is_bootstrap,
    )

    index = Index(tracker_id=settings.tracker_id)
    db = init_db(settings.db_path)
    known_trackers = _known_trackers_sync(settings)
    sync_client = SyncClient(
        tracker_id=settings.tracker_id,
        known_trackers=known_trackers,
        index=index,
        timeout_seconds=settings.sync_outbound_timeout_seconds,
    )
    search_router = SearchRouter(
        tracker_id=settings.tracker_id,
        known_trackers=known_trackers,
        index=index,
        timeout_seconds=settings.search_forward_timeout_seconds,
    )
    rebalance = RebalanceManager(
        tracker_id=settings.tracker_id,
        index=index,
        sync_client=sync_client,
        api_por_tracker_id=_api_por_tracker_id(settings),
    )
    sync_server = SyncServer(
        tracker_id=settings.tracker_id,
        ip=settings.ip,
        sync_port=settings.sync_port,
        index=index,
        sync_client=sync_client,
        rebalance=rebalance,
    )
    sync_server.start()
    reaper = TombstoneReaper(
        tracker_id=settings.tracker_id,
        index=index,
        retention_seconds=settings.tombstone_retention_seconds,
    )
    reaper.start()
    failure_detector = FailureDetector(
        tracker_id=settings.tracker_id,
        index=index,
        sync_client=sync_client,
        seed_report_timeout_seconds=settings.seed_report_timeout_seconds,
    )
    failure_detector.start()
    broadcaster = DigestBroadcaster(
        tracker_id=settings.tracker_id,
        index=index,
        sync_client=sync_client,
        interval_seconds=settings.digest_interval_seconds,
    )
    broadcaster.start()
    # Reintegração em background: TRACKER_REJOIN -> TRACKER_LIST -> SYNC_PULL(0)
    # pelo primeiro conhecido reachable, sem bloquear a subida da API. Quem sobe
    # primeiro não acha ninguém e segue com índice vazio, reconstruído aos poucos
    # via SEED_REPORT / SYNC_DIGEST.
    threading.Thread(
        target=sync_client.reintegrar,
        args=(settings.ip, settings.sync_port),
        name=f"rejoin-{settings.tracker_id}",
        daemon=True,
    ).start()

    app = create_app(
        index=index,
        db=db,
        tracker_id=settings.tracker_id,
        trackers_conhecidos=_trackers_conhecidos(settings),
        sync_client=sync_client,
        search_router=search_router,
    )
    try:
        # log_config=None: mantém o logging configurado pelo setup_logging.
        uvicorn.run(app, host=settings.ip, port=settings.api_port, log_config=None)
    finally:
        broadcaster.stop()
        failure_detector.stop()
        reaper.stop()
        sync_server.stop()


if __name__ == "__main__":
    main()
