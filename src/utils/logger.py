"""Logging configuration for ErgoStream."""

import sys
from loguru import logger
from .config import settings


def setup_logger():
    """Configure loguru logger with appropriate settings."""
    logger.remove()  # Remove default handler

    # Console handler with custom format
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level=settings.log_level,
        colorize=True,
    )

    # File handler for errors
    logger.add(
        "logs/ergostream_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="7 days",
        level="ERROR",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}",
    )

    return logger


# Initialize logger
log = setup_logger()
