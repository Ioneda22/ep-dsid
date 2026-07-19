"""Entrypoint do peer: python -m src.peer.main --config config/peer-alice.yaml.

Sobe o servidor TCP de chunks (thread daemon) e o seed reporter, envia
PEER_HELLO ao primeiro tracker da lista e entra no loop da CLI. No
quit, envia PEER_LEAVE e encerra as threads e conexões.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.common.config import load_yaml, require_keys
from src.common.logging_config import setup_logging
from src.peer.chunk_manager import ChunkManager
from src.peer.cli import PeerCLI
from src.peer.downloader import Downloader
from src.peer.name_registry import NameRegistry
from src.peer.seed_reporter import SeedReporter
from src.peer.storage import Storage
from src.peer.tcp_client import PeerTCPClient
from src.peer.tcp_server import PeerTCPServer
from src.peer.tracker_client import PeerTrackerClient, TodosTrackersIndisponiveis

logger = logging.getLogger(__name__)

_CHAVES_OBRIGATORIAS = (
    "nome_peer",
    "ip",
    "porta",
    "storage_dir",
    "log_path",
    "log_level",
    "trackers",
)


@dataclass
class PeerSettings:
    """Configuração de um peer carregada do YAML."""

    nome_peer: str
    ip: str
    porta: int
    storage_dir: Path
    log_path: Path
    log_level: str
    trackers: list[dict[str, Any]]
    # Constantes operacionais com valores padrão.
    chunk_size_bytes: int = 262144
    seed_report_interval_seconds: int = 180
    chunk_request_timeout_seconds: int = 10
    download_pool_size: int = 8  # nº de threads do download paralelo


def load_peer_settings(config_path: Path) -> PeerSettings:
    """Carrega e valida o YAML de configuração do peer.

    Raises:
        ConfigError: Se o arquivo não existir ou faltar chave obrigatória.
    """
    cfg = load_yaml(config_path)
    require_keys(cfg, _CHAVES_OBRIGATORIAS, str(config_path))
    return PeerSettings(
        nome_peer=str(cfg["nome_peer"]),
        ip=str(cfg["ip"]),
        porta=int(cfg["porta"]),
        storage_dir=Path(cfg["storage_dir"]),
        log_path=Path(cfg["log_path"]),
        log_level=str(cfg["log_level"]),
        trackers=list(cfg["trackers"]),
        chunk_size_bytes=int(cfg.get("chunk_size_bytes", 262144)),
        seed_report_interval_seconds=int(cfg.get("seed_report_interval_seconds", 180)),
        chunk_request_timeout_seconds=int(cfg.get("chunk_request_timeout_seconds", 10)),
        download_pool_size=int(cfg.get("download_pool_size", 8)),
    )


def _montar_peer(
    settings: PeerSettings,
) -> tuple[PeerCLI, PeerTCPServer, PeerTCPClient, PeerTrackerClient, SeedReporter]:
    """Instancia e conecta os componentes do peer (injeção por parâmetro)."""
    storage = Storage(settings.storage_dir, settings.chunk_size_bytes)
    tracker_client = PeerTrackerClient(settings.trackers)
    tcp_client = PeerTCPClient(timeout=settings.chunk_request_timeout_seconds)
    downloader = Downloader(
        nome_peer=settings.nome_peer,
        tracker_client=tracker_client,
        tcp_client=tcp_client,
        storage=storage,
        chunk_manager=ChunkManager(),
        download_pool_size=settings.download_pool_size,
    )
    name_registry = NameRegistry(settings.storage_dir)
    cli = PeerCLI(
        settings.nome_peer, storage, tracker_client, downloader, name_registry
    )
    server = PeerTCPServer(settings.ip, settings.porta, storage)
    reporter = SeedReporter(
        nome_peer=settings.nome_peer,
        ip=settings.ip,
        porta=settings.porta,
        storage=storage,
        tracker_client=tracker_client,
        interval_seconds=settings.seed_report_interval_seconds,
    )
    return cli, server, tcp_client, tracker_client, reporter


def main(argv: list[str] | None = None) -> None:
    """Carrega config, sobe threads auxiliares e roda a CLI até o quit."""
    parser = argparse.ArgumentParser(description="Peer do PeerSpot")
    parser.add_argument(
        "--config", required=True, type=Path, help="caminho do YAML (config/*.yaml)"
    )
    args = parser.parse_args(argv)

    settings = load_peer_settings(args.config)
    setup_logging(settings.log_path, settings.log_level, console=False)
    logger.info(
        "nome_peer=%s subindo em %s:%d (storage=%s)",
        settings.nome_peer,
        settings.ip,
        settings.porta,
        settings.storage_dir,
    )

    cli, server, tcp_client, tracker_client, reporter = _montar_peer(settings)
    server.start()
    if not server.started.wait(timeout=5):
        print("Falha ao subir o servidor TCP de chunks (veja o log).")
        return
    reporter.start()

    try:
        resposta_hello = tracker_client.peer_hello(
            settings.nome_peer, settings.ip, settings.porta
        )
    except TodosTrackersIndisponiveis:
        resposta_hello = None
    if resposta_hello is None:
        print("Aviso: PEER_HELLO falhou — tracker indisponível? (veja o log)")

    try:
        cli.run()
    finally:
        try:
            tracker_client.peer_leave(settings.nome_peer)
        except TodosTrackersIndisponiveis:
            logger.warning("PEER_LEAVE não enviado: todos os trackers indisponíveis")
        reporter.stop()
        server.stop()
        tcp_client.close_all()
        tracker_client.close()
        logger.info("peer %s encerrado", settings.nome_peer)


if __name__ == "__main__":
    main()
