"""Logging configuration using rich."""

import logging
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler

# Module-level logger
_logger: Optional[logging.Logger] = None
_console = Console(stderr=True)


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure logging with rich handler.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured logger instance
    """
    global _logger

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                console=_console,
                rich_tracebacks=True,
                show_path=False,
                markup=True,
            )
        ],
    )

    # Reduce noise from third-party libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    _logger = logging.getLogger("political_speeches")
    return _logger


def get_logger() -> logging.Logger:
    """Get the configured logger, initializing if needed.

    Returns:
        Logger instance
    """
    global _logger
    if _logger is None:
        _logger = setup_logging()
    return _logger


def get_console() -> Console:
    """Get the rich console for direct output.

    Returns:
        Rich console instance
    """
    return _console
