"""
Logging utility — console + shared rotating file output.

All `get_logger()` names share one rotating file so disk use is bounded.
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from faralpha.config import LOGS_DIR

# Same basename as RotatingFileHandler target — used by /api/logs/*
APP_LOG_BASENAME = "faralpha_app.log"
MAX_LOG_BYTES = 10 * 1024 * 1024  # 10 MiB per file
LOG_BACKUP_COUNT = 10  # faralpha_app.log.1 … .10

_shared_rotating: RotatingFileHandler | None = None


def app_log_path() -> Path:
    """Absolute path to the primary application log file."""
    return LOGS_DIR / APP_LOG_BASENAME


def _get_rotating_handler() -> RotatingFileHandler:
    global _shared_rotating
    if _shared_rotating is not None:
        return _shared_rotating

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    path = app_log_path()
    _shared_rotating = RotatingFileHandler(
        str(path),
        maxBytes=MAX_LOG_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    fmt = logging.Formatter(
        "%(asctime)s | %(name)-30s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _shared_rotating.setLevel(logging.DEBUG)
    _shared_rotating.setFormatter(fmt)
    return _shared_rotating


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s | %(name)-30s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logger.addHandler(_get_rotating_handler())
    return logger
