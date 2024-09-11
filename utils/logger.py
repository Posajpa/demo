import logging
import os
from logging.handlers import RotatingFileHandler

# Directory to save log files


import logging


def get_logger(logger_name, log_file=None, log_dir='logs', level=logging.DEBUG):

    # Ensure the log directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Full log file path
    log_path = os.path.join(log_dir, log_file)

    # Create a custom logger
    logger = logging.getLogger(logger_name)

    # Set the log level
    logger.setLevel(level)

    # Create handlers (file and console)
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    console_handler = logging.StreamHandler()

    # Set log level for handlers
    file_handler.setLevel(level)
    console_handler.setLevel(level)

    # Create a formatter and set it for the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    if not logger.hasHandlers():  # Avoid adding multiple handlers to the same logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
