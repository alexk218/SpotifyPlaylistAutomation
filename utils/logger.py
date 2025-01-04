import logging
import os

def setup_logger(name, log_file, level=logging.INFO):
    # Clear log file if it exists (by opening it in write mode)
    if os.path.exists(log_file):
        open(log_file, 'w', encoding='utf-8').close()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    handler = logging.FileHandler(log_file, encoding='utf-8')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        logger.addHandler(handler)

    return logger
