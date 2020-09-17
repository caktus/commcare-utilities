import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

CONSOLE_LOGGER_LEVEL = "INFO"
FILE_LOGGER_LEVEL = "WARNING"

console_logger = logging.StreamHandler(sys.stdout)
console_logger.setLevel(logging.DEBUG)
console_logger.setFormatter(formatter)


logger = logging.getLogger("main")
logger.setLevel(getattr(logging, CONSOLE_LOGGER_LEVEL))
logger.addHandler(console_logger)
log_file_path_from_env = os.environ.get("COMMCARE_UTILITIES_LOG_PATH")

if log_file_path_from_env:
    log_file_path = Path(log_file_path_from_env)
    log_file_path.mkdir(parents=True, exist_ok=True)
    log_file = log_file_path.joinpath("cc-utilities.log")
    file_logger = RotatingFileHandler(
        filename=log_file, mode="a", maxBytes=5 * 1024 * 1024, delay=0
    )
    file_logger.setLevel(getattr(logging, FILE_LOGGER_LEVEL))
    file_logger.setFormatter(formatter)
    logger.addHandler(file_logger)
