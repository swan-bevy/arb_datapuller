# logger.py
from loguru import logger

_logger = None


def get_logger():
    global _logger
    if not _logger:
        _logger = logger
        _logger.add("logs/error_code.log", level="ERROR", rotation="1 MB")
    return _logger


## FROM CHAT_GPT
# file1.py
# from logger import get_logger

# log = get_logger()
