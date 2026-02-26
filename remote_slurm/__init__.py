import logging
import sys

import remote_slurm.execute
import remote_slurm.ssh
import remote_slurm.slurmify


class PackageLogFilter(logging.Filter):
    """Handles name truncation (optional) and newline escaping."""

    def __init__(self, shorten=True):
        super().__init__()
        self.shorten = shorten

    def filter(self, record):
        max_len = 25

        # 1. Handle Name Formatting
        if self.shorten:
            if len(record.name) > max_len:
                record.short_name = record.name[:max_len - 3] + "..."
            else:
                record.short_name = record.name.ljust(max_len)
        else:
            # If shortening is disabled, just use the full name
            record.short_name = record.name

        # 2. Escape Newlines
        if isinstance(record.msg, str):
            record.msg = record.msg.replace('\n', '\\n')

        return True


def setup_logging(level=logging.INFO, shorten_names=True, log_file=None):
    logger = logging.getLogger("remote_slurm")
    logger.setLevel(level)
    logger.propagate = False

    if logger.hasHandlers():
        logger.handlers.clear()

    # Note: Changed %(name)s to %(short_name)s to use the filter's output
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(short_name)s | %(message)s',
        datefmt='%H:%M:%S'
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    # Pass the global flag into the filter
    logger.addFilter(PackageLogFilter(shorten=shorten_names))
    logger.addHandler(handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
