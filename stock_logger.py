"""
统一日志模块。

用法：
    from stock_logger import get_logger
    logger = get_logger(__name__)
    logger.info("xxx")

日志同时输出到控制台和文件（data/logs/app.log），文件按天滚动保留 30 天。
环境变量 GUPPIAO_LOG_LEVEL 可以覆盖日志级别（DEBUG / INFO / WARNING / ERROR）。
"""
from __future__ import annotations

import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_LOG_DIR = Path(__file__).resolve().parent / "data" / "logs"
_LOG_FILE = _LOG_DIR / "app.log"
_INITIALIZED = False


def _ensure_log_dir() -> None:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)


def _resolve_level() -> int:
    raw = os.environ.get("GUPPIAO_LOG_LEVEL", "").strip().upper()
    return getattr(logging, raw, logging.INFO)


def _setup_root_logger() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return
    _INITIALIZED = True

    _ensure_log_dir()
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-7s %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 文件：按天滚动，保留 30 天
    fh = TimedRotatingFileHandler(
        str(_LOG_FILE),
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # 控制台
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(_resolve_level())
    sh.setFormatter(fmt)
    root.addHandler(sh)


def get_logger(name: str) -> logging.Logger:
    """获取模块级 logger，首次调用时自动初始化 root handler。"""
    _setup_root_logger()
    return logging.getLogger(name)
