import logging
import sys

import remote_slurm.execute
import remote_slurm.ssh
import remote_slurm.slurmify


class PackageLogFilter(logging.Filter):
    """Handles both name truncation and newline escaping for clean logs."""

    def filter(self, record):
        # 1. Truncate the Logger Name
        max_len = 25
        if len(record.name) > max_len:
            record.short_name = record.name[:max_len - 3] + "..."
        else:
            record.short_name = record.name.ljust(max_len)

        # 2. Escape Newlines in the message
        # We convert actual newlines into the literal string "\n"
        if isinstance(record.msg, str):
            record.msg = record.msg.replace('\n', '\\n')

        return True


def setup_logging(level=logging.INFO):
    logger = logging.getLogger("remote_slurm")
    logger.setLevel(level)

    # Prevent logs from double-printing if the user has their own root logger
    logger.propagate = False

    # Clear existing handlers (crucial for Jupyter)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a consistent format
    # %(name)s will show 'mypackage.submodule'
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%H:%M:%S'
    )

    # Redirect to stdout (best for both Terminal and Jupyter)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addFilter(PackageLogFilter())
    logger.addHandler(handler)

    return logger
