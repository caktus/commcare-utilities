import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path

log_format = "[%(asctime)s] [%(levelname)s] - %(message)s"

log_level = os.environ.get("COMMCARE_UTILITIES_LOG_LEVEL", "INFO")
log_file_path = os.environ.get(
    "COMMCARE_UTILITIES_LOG_PATH",
    Path(__file__).parent.absolute().joinpath("..", "logs"),
)
log_file_path.mkdir(parents=True, exist_ok=True)

log_file = log_file_path.joinpath("cc-utilities.log")

rfh = RotatingFileHandler(
    filename=log_file, mode="a", maxBytes=5 * 1024 * 1024, delay=0
)

logging.basicConfig(level=log_level, format=log_format, handlers=[rfh])
logger = logging.getLogger("main")
