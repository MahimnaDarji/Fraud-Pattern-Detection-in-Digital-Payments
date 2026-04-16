"""Project-wide logging configuration helpers."""

import logging


_LOGGING_CONFIGURED = False


def configure_logging(debug_mode: bool = False) -> None:
    """Configure application logging once with structured, concise output."""
    global _LOGGING_CONFIGURED
    level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )
    _LOGGING_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger instance for the module name."""
    if not _LOGGING_CONFIGURED:
        configure_logging(debug_mode=False)
    return logging.getLogger(name)
