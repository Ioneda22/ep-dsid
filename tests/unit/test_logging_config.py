"""Testes unitários para src/common/logging_config.py."""

from __future__ import annotations

from typing import Iterator

import logging
from pathlib import Path

import pytest

from src.common.logging_config import setup_logging


# Fixture autouse: não há preparação antes do yield; só o teardown após cada
# teste, removendo os handlers do logger raiz para não vazarem entre casos.
@pytest.fixture(autouse=True)
def _limpa_logger_raiz() -> Iterator[None]:
    """Remove handlers do logger raiz após cada teste para evitar vazamento."""
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()


def test_setup_logging_cria_arquivo_e_escreve(tmp_path: Path) -> None:
    """setup_logging cria o arquivo de log (e diretórios intermediários) e a
    mensagem emitida aparece nele com o nível e o nome do logger."""
    log_path = tmp_path / "subdir" / "peerspot.log"
    setup_logging(log_path, level="DEBUG")

    logging.getLogger("teste").info("mensagem-marcadora")

    for h in logging.getLogger().handlers:
        h.flush()

    assert log_path.exists()
    conteudo = log_path.read_text(encoding="utf-8")
    assert "mensagem-marcadora" in conteudo
    assert "teste" in conteudo
    assert "INFO" in conteudo


def test_setup_logging_idempotente(tmp_path: Path) -> None:
    """Chamar duas vezes não duplica handlers do PeerSpot."""
    log_path = tmp_path / "log.log"
    setup_logging(log_path, level="INFO")
    setup_logging(log_path, level="INFO")

    assert len(_handlers_peerspot()) == 2  # 1 arquivo + 1 stderr


def test_setup_logging_nivel_invalido(tmp_path: Path) -> None:
    """Um nível de log desconhecido faz setup_logging levantar ValueError."""
    with pytest.raises(ValueError, match="inválido"):
        setup_logging(tmp_path / "x.log", level="VERBOSO")


def test_setup_logging_sem_console_nao_anexa_handler_de_stderr(tmp_path: Path) -> None:
    """console=False deixa só o handler de arquivo no logger raiz."""
    setup_logging(tmp_path / "peer.log", level="INFO", console=False)

    handlers = _handlers_peerspot()
    assert len(handlers) == 1
    assert isinstance(handlers[0], logging.FileHandler)


def test_setup_logging_sem_console_nao_escreve_no_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Regressão: exceção de thread de fundo do peer não vaza para o terminal
    da CLI (corrompia o prompt do input()); vai só para o arquivo de log."""
    log_path = tmp_path / "peer.log"
    setup_logging(log_path, level="INFO", console=False)

    try:
        raise RuntimeError("nenhum dos 3 trackers respondeu")
    except RuntimeError:
        logging.getLogger("seed-reporter").exception("falha ao enviar SEED_REPORT")

    for h in logging.getLogger().handlers:
        h.flush()

    assert capsys.readouterr().err == ""
    assert "falha ao enviar SEED_REPORT" in log_path.read_text(encoding="utf-8")


def test_setup_logging_com_console_escreve_no_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """O tracker (sem CLI) continua vendo os logs no terminal."""
    setup_logging(tmp_path / "tracker.log", level="INFO", console=True)

    logging.getLogger("tracker").info("sync server escutando")

    assert "sync server escutando" in capsys.readouterr().err


def _handlers_peerspot() -> list[logging.Handler]:
    """Handlers anexados pelo setup_logging (marcados com _peerspot_handler)."""
    return [
        h
        for h in logging.getLogger().handlers
        if getattr(h, "_peerspot_handler", False)
    ]
