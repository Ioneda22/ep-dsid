"""Cores ANSI mínimas para a CLI do peer (única camada com print, §4.3).

Sem dependência externa: usa só códigos ANSI da stdlib. As cores se
desativam automaticamente quando a saída não é um terminal (pytest,
redirecionamento para arquivo) ou quando NO_COLOR está no ambiente, de
modo que os logs e a captura dos testes ficam em texto puro. Em Windows,
habilita o processamento de sequências VT no console clássico.

Uso:
    print(console.ok("Upload concluído"))
    print(console.dim(hash_arquivo))
"""

from __future__ import annotations

import os
import sys


def _habilitar_vt_windows() -> bool:
    """Liga ENABLE_VIRTUAL_TERMINAL_PROCESSING no console do Windows."""
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        modo = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(modo)):
            return False
        kernel32.SetConsoleMode(handle, modo.value | 0x0004)
        return True
    except Exception:  # noqa: BLE001 — sem cor é degradação aceitável
        return False


def _suporta_cor() -> bool:
    """Decide uma única vez, na importação, se emitimos códigos ANSI."""
    if os.environ.get("NO_COLOR"):
        return False
    if not sys.stdout.isatty():
        return False
    if sys.platform == "win32":
        return _habilitar_vt_windows()
    return True


_ATIVO = _suporta_cor()


def _c(texto: str, codigo: str) -> str:
    """Envolve texto no código ANSI, ou o devolve cru se a cor está off."""
    if not _ATIVO:
        return texto
    return f"\033[{codigo}m{texto}\033[0m"


def titulo(texto: str) -> str:
    """Cabeçalho de seção (ciano em negrito)."""
    return _c(texto, "1;36")


def ok(texto: str) -> str:
    """Sucesso (verde)."""
    return _c(texto, "32")


def aviso(texto: str) -> str:
    """Advertência recuperável (amarelo)."""
    return _c(texto, "33")


def erro(texto: str) -> str:
    """Falha (vermelho)."""
    return _c(texto, "31")


def dim(texto: str) -> str:
    """Detalhe secundário como hashes (esmaecido)."""
    return _c(texto, "2")


def destaque(texto: str) -> str:
    """Realce de um valor no meio da linha (negrito)."""
    return _c(texto, "1")


def prompt(texto: str) -> str:
    """Prompt do loop de comandos (ciano)."""
    return _c(texto, "36")
