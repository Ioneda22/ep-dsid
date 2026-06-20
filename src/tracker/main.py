"""Entrypoint do tracker: ``python -m src.tracker.main --config config/tracker-1.yaml``.

Sobe, no MESMO processo Python (§8 da tarefa da Fase 4):

* a API REST (FastAPI/uvicorn) na ``api_port`` — atendimento aos peers;
* o servidor TCP de sincronização (``SyncServer``) na ``sync_port``,
  em thread própria — flooding ``SYNC_TABLE`` e ``SEARCH_FORWARD``;
* o ``TombstoneReaper`` (expiração de tombstones a cada 60s);
* o ``AntiEntropyReconciler`` (push periódico de ``FULL_SYNC`` aos demais
  trackers — reconciliação anti-entropy que repara deltas perdidos).

O failure detector e a reintegração (``TRACKER_REJOIN``) chegam na Fase 5.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import uvicorn

from src.common.config import load_yaml, require_keys
from src.common.logging_config import setup_logging
from src.tracker.anti_entropy import AntiEntropyReconciler
from src.tracker.api import create_app
from src.tracker.index import Index
from src.tracker.persistence import init_db
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
    """Configuração de um tracker carregada do YAML (§6.6)."""

    tracker_id: str
    ip: str
    api_port: int
    sync_port: int
    is_bootstrap: bool
    known_trackers: list[dict[str, Any]]
    db_path: Path
    log_path: Path
    log_level: str
    # Constantes operacionais (usadas nas Fases 4 e 5).
    seed_report_timeout_seconds: int = 360
    tombstone_retention_seconds: int = 600
    sync_outbound_timeout_seconds: int = 3
    search_forward_timeout_seconds: int = 2
    anti_entropy_interval_seconds: int = 180  # 3 min; < retenção do tombstone (600s)


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
        anti_entropy_interval_seconds=int(
            cfg.get("anti_entropy_interval_seconds", 180)
        ),
    )


def _trackers_conhecidos(settings: TrackerSettings) -> list[dict[str, Any]]:
    """Lista exposta em ``GET /trackers``: este tracker + os do YAML."""
    proprio = {
        "tracker_id": settings.tracker_id,
        "ip": settings.ip,
        "api_port": settings.api_port,
        "sync_port": settings.sync_port,
    }
    return [proprio, *settings.known_trackers]


def _known_trackers_sync(settings: TrackerSettings) -> list[KnownTracker]:
    """Converte os ``known_trackers`` do YAML em destinos de sincronização.

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
        timeout_seconds=settings.sync_outbound_timeout_seconds,
    )
    search_router = SearchRouter(
        tracker_id=settings.tracker_id,
        known_trackers=known_trackers,
        index=index,
        timeout_seconds=settings.search_forward_timeout_seconds,
    )
    sync_server = SyncServer(
        tracker_id=settings.tracker_id,
        ip=settings.ip,
        sync_port=settings.sync_port,
        index=index,
    )
    sync_server.start()
    reaper = TombstoneReaper(
        tracker_id=settings.tracker_id,
        index=index,
        retention_seconds=settings.tombstone_retention_seconds,
    )
    reaper.start()
    reconciler = AntiEntropyReconciler(
        tracker_id=settings.tracker_id,
        index=index,
        sync_client=sync_client,
        interval_seconds=settings.anti_entropy_interval_seconds,
    )
    reconciler.start()

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
        reconciler.stop()
        reaper.stop()
        sync_server.stop()


if __name__ == "__main__":
    main()
