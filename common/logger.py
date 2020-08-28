import logging
from logging.handlers import RotatingFileHandler
import os
import sys
from pathlib import Path

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

log_file_path = os.environ.get(
    "COMMCARE_UTILITIES_LOG_PATH",
    Path(__file__).parent.absolute().joinpath("..", "logs"),
)
log_file_path.mkdir(parents=True, exist_ok=True)
log_file = log_file_path.joinpath("cc-utilities.log")
file_logger = RotatingFileHandler(
    filename=log_file, mode="a", maxBytes=5 * 1024 * 1024, delay=0
)
file_logger.setLevel(logging.WARNING)
file_logger.setFormatter(formatter)

console_logger = logging.StreamHandler(sys.stdout)
console_logger.setLevel(logging.DEBUG)
console_logger.setFormatter(formatter)


logger = logging.getLogger("main")
logger.setLevel(logging.INFO)
logger.addHandler(console_logger)
logger.addHandler(file_logger)
