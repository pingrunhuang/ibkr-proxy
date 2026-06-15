import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger

LOG_DIR = "logs"
STARTUP_TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")


class InterceptHandler(logging.Handler):
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def startup_log_path() -> Path:
    return Path(LOG_DIR) / f"ibkr_proxy.{STARTUP_TIMESTAMP}.log"


def remove_old_startup_logs(backup_count: int) -> None:
    old_logs = sorted(
        Path(LOG_DIR).glob("ibkr_proxy.*.log"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    previous_logs_to_keep = max(backup_count - 1, 0)
    for path in old_logs[previous_logs_to_keep:]:
        path.unlink(missing_ok=True)


def configure_logger() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    backup_count = int(os.getenv("LOG_BACKUP_COUNT", "30"))

    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    remove_old_startup_logs(backup_count)

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    logger.remove()
    logger.add(sys.stderr, level=log_level)
    logger.add(startup_log_path(), level=log_level, enqueue=True, encoding="utf-8")
