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
        "%(asctime)s %(threadName)s %(levelname)s:%(name)s 	%(message)s")
    logger = logging.getLogger(name)
    level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(level)
    fh = logging.FileHandler('import.log', mode="w")
    ch = logging.StreamHandler()
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
