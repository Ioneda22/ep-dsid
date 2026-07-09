"""Configuração centralizada de logging para tracker e peer.

Formato: timestamp - nome_modulo - level - mensagem. Handler para arquivo
e, opcionalmente, para stderr. Idempotente — chamar duas vezes não duplica
handlers no logger raiz.

O peer desliga o handler de stderr (console=False): threads de fundo
(seed reporter, downloader, servidor TCP) logam a qualquer momento e
escreveriam por cima da linha que o usuário digita no input() da CLI.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    log_path: Path, level: str = "INFO", *, console: bool = True
) -> logging.Logger:
    """Configura o logger raiz com handler de arquivo e, se pedido, de stderr.

    Cria o diretório pai de log_path se necessário. Se já houver handlers
    do PeerSpot anexados ao logger raiz (marcados via atributo
    _peerspot_handler), eles são removidos antes — assim chamar
    setup_logging duas vezes não duplica saídas.

    Args:
        log_path: Caminho do arquivo de log. Diretórios pais são criados.
        level: Nível textual ("DEBUG", "INFO", "WARNING", ...).
        console: Se True, também emite em stderr. O peer passa False para
            não corromper o prompt da CLI (a única saída dele é o print).

    Returns:
        O logger raiz já configurado.

    Raises:
        ValueError: Se level não for um nível conhecido.

    Example:
        >>> setup_logging(Path("logs/peer-alice.log"), "INFO", console=False)
    """
    mapping = logging.getLevelNamesMapping()
    nivel_upper = level.upper()
    if nivel_upper not in mapping:
        raise ValueError(f"Nível de log inválido: {level!r}")
    numeric_level = mapping[nivel_upper]

    log_path.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(numeric_level)

    for h in list(root.handlers):
        if getattr(h, "_peerspot_handler", False):
            root.removeHandler(h)
            h.close()

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(numeric_level)
    file_handler._peerspot_handler = True  # type: ignore[attr-defined]
    root.addHandler(file_handler)

    if console:
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(numeric_level)
        stream_handler._peerspot_handler = True  # type: ignore[attr-defined]
        root.addHandler(stream_handler)

    return root
