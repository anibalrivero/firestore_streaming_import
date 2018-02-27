"""
This setups a logger for the imports. This is very important, since we do not
have standard output for the threads
"""
import logging


def get_logger(name, debug=False):
    """
    Setups the default config for the logger
    Returns:

    """
    formatter = logging.Formatter(
        "%(asctime)s %(processName)-10s %(levelname)s:%(name)s 	%(message)s")
    logger = logging.getLogger(name)
    level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(level)
    file_handler = logging.FileHandler('import.log')
    console_handler = logging.StreamHandler()
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
