import http.client
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

http.client.HTTPConnection.debuglevel = 1


formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

CONSOLE_LOGGER_LEVEL = "DEBUG"
FILE_LOGGER_LEVEL = "INFO"

console_logger = logging.StreamHandler(sys.stdout)
console_logger.setLevel(logging.DEBUG)
console_logger.setFormatter(formatter)


logger = logging.getLogger("main")
logger.setLevel(getattr(logging, CONSOLE_LOGGER_LEVEL))
logger.addHandler(console_logger)

requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True


def get_full_log_file_path():
    """If env is configured for logging to file, generate full path to log file"""
    full_log_file_path = None
    log_file_path_from_env = os.environ.get("COMMCARE_UTILITIES_LOG_PATH")
    if log_file_path_from_env:
        log_file_path = Path(log_file_path_from_env)
        log_file_path.mkdir(parents=True, exist_ok=True)
        full_log_file_path = log_file_path.joinpath("cc-utilities.log")
    return full_log_file_path


full_log_file_path = get_full_log_file_path()
if full_log_file_path:
    file_logger = RotatingFileHandler(
        filename=full_log_file_path, mode="a", maxBytes=5 * 1024 * 1024, delay=0
    )
    file_logger.setLevel(getattr(logging, FILE_LOGGER_LEVEL))
    file_logger.setFormatter(formatter)
    logger.addHandler(file_logger)
