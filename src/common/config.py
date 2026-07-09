"""Parsing de arquivos de configuração YAML.

Os arquivos vivem em config/*.yaml e são lidos no boot de cada nó. Esta
camada apenas converte YAML em dict e oferece uma validação leve de
campos obrigatórios; consumidores (tracker/peer) traduzem o dict em uma
dataclass tipada antes de usar.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml


class ConfigError(Exception):
    """Erro ao carregar ou validar configuração."""


def load_yaml(path: Path) -> dict[str, Any]:
    """Carrega um arquivo YAML e devolve seu conteúdo como dict.

    Args:
        path: Caminho do arquivo YAML.

    Returns:
        Conteúdo do arquivo como dicionário (chaves str).

    Raises:
        ConfigError: Se o arquivo não existir, falhar parsing, ou se o
            documento raiz não for um mapeamento.
    """
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except FileNotFoundError as exc:
        raise ConfigError(f"Arquivo de configuração não encontrado: {path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"YAML inválido em {path}: {exc}") from exc

    if data is None:
        raise ConfigError(f"Arquivo de configuração vazio: {path}")
    if not isinstance(data, dict):
        raise ConfigError(
            f"Raiz de {path} deve ser um mapeamento; recebido {type(data).__name__}"
        )
    return data


def require_keys(cfg: dict[str, Any], keys: Iterable[str], origem: str) -> None:
    """Valida que todas as chaves de keys estão presentes em cfg.

    Args:
        cfg: Dicionário de configuração já carregado.
        keys: Chaves obrigatórias.
        origem: Identificador legível usado nas mensagens de erro
            (geralmente o caminho do arquivo).

    Raises:
        ConfigError: Se uma ou mais chaves estiverem ausentes.
    """
    faltando = [k for k in keys if k not in cfg]
    if faltando:
        raise ConfigError(
            f"Configuração em {origem} sem chaves obrigatórias: {faltando}"
        )
