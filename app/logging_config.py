"""Configuration du logging — lisible en dev, exploitable par Railway en prod.

Deux formats, pilotés par `LOG_FORMAT` :
  • console (défaut local) : ligne colorée et alignée, facile à lire à l'œil.
  • json    (défaut prod / Railway) : une ligne JSON par log avec un champ
    `level` que Railway reconnaît pour colorer et filtrer par sévérité
    (debug | info | warn | error). Le champ `message` porte le texte.

Railway capture stdout/stderr : on n'écrit donc que sur stdout, et on mappe
WARNING→"warn" / les autres niveaux en minuscules attendus par Railway.
"""

from __future__ import annotations

import json
import logging
import sys

from app.config import settings

_CONFIGURED = False

# Mapping niveau Python → libellé attendu par Railway (et lisible).
_RAILWAY_LEVEL = {
    "DEBUG": "debug",
    "INFO": "info",
    "WARNING": "warn",
    "ERROR": "error",
    "CRITICAL": "error",
}

# Couleurs ANSI pour le format console (ignorées si non-TTY).
_COLORS = {
    "DEBUG": "\033[36m",     # cyan
    "INFO": "\033[32m",      # vert
    "WARNING": "\033[33m",   # jaune
    "ERROR": "\033[31m",     # rouge
    "CRITICAL": "\033[41m",  # fond rouge
}
_RESET = "\033[0m"


class JsonFormatter(logging.Formatter):
    """Une ligne JSON par enregistrement, avec `level` reconnu par Railway."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": _RAILWAY_LEVEL.get(record.levelname, "info"),
            "logger": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
        }
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


class ConsoleFormatter(logging.Formatter):
    """Format aligné et coloré, optimisé pour la lecture humaine en dev."""

    def __init__(self, color: bool) -> None:
        super().__init__(datefmt="%H:%M:%S")
        self._color = color

    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record, self.datefmt)
        level = record.levelname
        name = record.name.replace("app.", "")
        msg = record.getMessage()
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)
        if self._color:
            c = _COLORS.get(level, "")
            return f"\033[90m{ts}\033[0m {c}{level:<7}{_RESET} \033[1m{name}\033[0m  {msg}"
        return f"{ts} {level:<7} {name}  {msg}"


def _build_formatter() -> logging.Formatter:
    fmt = getattr(settings, "log_format", None) or _default_format()
    if fmt == "json":
        return JsonFormatter()
    return ConsoleFormatter(color=sys.stdout.isatty())


def _default_format() -> str:
    # JSON par défaut hors TTY (Railway), console en TTY (terminal local).
    return "console" if sys.stdout.isatty() else "json"


def setup_logging() -> None:
    """Configure le root logger une seule fois (idempotent)."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_build_formatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(settings.log_level.upper())

    # Réduire le bruit des libs tierces.
    for noisy in ("httpx", "websockets", "asyncio", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Raccourci pour récupérer un logger nommé après setup."""
    return logging.getLogger(name)
