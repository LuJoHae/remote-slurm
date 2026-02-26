import logging
import sys

import remote_slurm.execute
import remote_slurm.ssh
import remote_slurm.slurmify


def setup_logging(level=logging.INFO, log_filepath=None):
    logger = logging.getLogger("remote_slurm")
    logger.setLevel(level)
    logger.propagate = False

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%H:%M:%S'
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if log_filepath:
        file_handler = logging.FileHandler(log_filepath)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
