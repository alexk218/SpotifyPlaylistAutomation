import logging
import os

LOGS_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))


def setup_logger(name, *path_parts, level=logging.INFO, clear_existing=True):
    log_file = build_log_path(*path_parts)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    if clear_existing and os.path.exists(log_file):
        open(log_file, 'w', encoding='utf-8').close()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    handler = logging.FileHandler(log_file, encoding='utf-8')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent duplicate handlers
    if not logger.handlers:
        logger.addHandler(handler)

    return logger


def build_log_path(*path_parts):
    """
    Build a full path inside the logs directory.
    Example: build_log_path("auth", "login.log") → /project/logs/auth/login.log
    """
    full_path = os.path.join(LOGS_ROOT_DIR, *path_parts)
    return os.path.abspath(full_path)
