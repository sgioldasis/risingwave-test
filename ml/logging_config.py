"""Centralized logging configuration for ML modules."""
import logging
import sys
from typing import Optional


def setup_logging(
    name: Optional[str] = None,
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    stream=sys.stdout
) -> logging.Logger:
    """
    Configure and return a logger with consistent formatting.

    Args:
        name: Logger name (uses root logger if None)
        level: Logging level (default: INFO)
        format_string: Custom format string
        stream: Output stream (default: stdout)

    Returns:
        Configured logger instance
    """
    if format_string is None:
        format_string = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()

    # Create handler
    handler = logging.StreamHandler(stream)
    handler.setLevel(level)

    # Create formatter
    formatter = logging.Formatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name."""
    return logging.getLogger(name)
