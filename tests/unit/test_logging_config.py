"""Testes unitários para src/common/logging_config.py."""

from __future__ import annotations

from typing import Iterator

import logging
from pathlib import Path

import pytest

from src.common.logging_config import setup_logging

# fixture prepara limpa o ambiente de teste antes do yield seria a preparação antes
# do teste rodar depois do yield é limpando o ambiente
@pytest.fixture(autouse=True)
def _limpa_logger_raiz() -> Iterator[None]:
    """Remove handlers do logger raiz após cada teste para evitar vazamento."""
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()


def test_setup_logging_cria_arquivo_e_escreve(tmp_path: Path) -> None:
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

    handlers_peerspot = [
        h for h in logging.getLogger().handlers
        if getattr(h, "_peerspot_handler", False)
    ]
    assert len(handlers_peerspot) == 2  # 1 arquivo + 1 stderr


def test_setup_logging_nivel_invalido(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="inválido"):
        setup_logging(tmp_path / "x.log", level="VERBOSO")
