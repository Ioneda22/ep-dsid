"""Entrypoint do tracker: ``python -m src.tracker.main --config config/tracker-1.yaml``.

Fase 2: sobe apenas a API REST (FastAPI/uvicorn). O servidor de
sincronização TCP (Fase 4) e o failure detector (Fase 5) ainda não
existem — ``sync_port`` é lido e reservado, mas não usado.
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
from src.tracker.api import create_app
from src.tracker.index import Index
from src.tracker.persistence import init_db

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


def main(argv: list[str] | None = None) -> None:
    """Carrega config, prepara logging/índice/banco e serve a API REST."""
    parser = argparse.ArgumentParser(description="Tracker do PeerSpot")
    parser.add_argument(
        "--config", required=True, type=Path, help="caminho do YAML (config/*.yaml)"
    )
    args = parser.parse_args(argv)

    settings = load_tracker_settings(args.config)
    setup_logging(settings.log_path, settings.log_level)
    logger.info(
        "tracker_id=%s subindo API REST em %s:%d (bootstrap=%s)",
        settings.tracker_id,
        settings.ip,
        settings.api_port,
        settings.is_bootstrap,
    )

    index = Index()
    db = init_db(settings.db_path)
    app = create_app(
        index=index,
        db=db,
        tracker_id=settings.tracker_id,
        trackers_conhecidos=_trackers_conhecidos(settings),
    )
    # log_config=None: mantém o logging configurado pelo setup_logging.
    uvicorn.run(app, host=settings.ip, port=settings.api_port, log_config=None)


if __name__ == "__main__":
    main()
