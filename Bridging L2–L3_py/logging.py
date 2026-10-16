from logging import Logger
import logging
import os
from threading import Lock

Bridging L2–L3_pyo3 = None

def build_format(color):
    reset = "\x1b[0m"
    underline = "\x1b[3m"
    # tips = "\x1b[35;3m"
    gray= "\x1b[90;20m"
    light_gray= "\x1b[37;20m"
    # very_light_gray = "\x1b[38;2;255;255;255m"
    # tips = very_light_gray
    tips = "\x1b[90m"
    bold = "\x1b[1m"  

    return (
        f"{color}[%(asctime)s] Bridging L2–L3 %(levelname)s:{reset} {tips}(%(funcName)s:%(pathname)s:%(lineno)d){reset}"
        # f"{underline}(%(filename)s:%(lineno)d:%(name)s){reset}  "
        f"\n  {bold}%(message)s{reset}"
    )


class CustomFormatter(logging.Formatter):
    light_blue = "\x1b[94;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: build_format(light_blue),
        logging.INFO: build_format(green),
        logging.WARNING: build_format(yellow),
        logging.ERROR: build_format(bold_red),
        logging.CRITICAL: build_format(red),
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def get_log_level() -> int:
    """
    Try to read Bridging L2–L3_LOG from environment variables.
    Could be:
    - DEBUG
    - INFO
    - WARNING
    - ERROR
    - CRITICAL

    If not found, defaults to INFO.
    """
    log_level = os.getenv("Bridging L2–L3_LOG", "INFO").upper()
    return getattr(logging, log_level, logging.INFO)


INITED_LOGGERS = []
INITED_LOGGERS_LOCK = Lock()

def init_logger(name: str = "Bridging L2–L3") -> Logger:
    # Get the logger
    logger = logging.getLogger(name)

    # Clear any existing handlers
    logger.handlers.clear()

    # Prevent propagation to parent loggers
    logger.propagate = False

    # Add our custom handler
    ch = logging.StreamHandler()
    ch.setLevel(get_log_level())
    ch.setFormatter(CustomFormatter())
    logger.addHandler(ch)

    # Keep logger enabled for DEBUG so file logging (if attached elsewhere) can always capture full details.
    logger.setLevel(logging.DEBUG)

    # Register to global list
    with INITED_LOGGERS_LOCK:
        if logger not in INITED_LOGGERS:
            INITED_LOGGERS.append(logger)

    return logger
