import re
import sys
from datetime import datetime

import tomllib
from loguru import logger
from pydantic import BaseModel


def __load_config() -> dict:
    """
    Loads the configuration from pyproject.toml.
    :return: Dictionary with configuration data.
    """
    with open("./pyproject.toml", "rb") as f:
        return tomllib.load(f)


def configure_logger(logfile: str = None):
    """
    Configures the logger to output to stdout and optionally to a logfile.
    :param logfile:  Path to the logfile. If None, only stdout is used.
    """
    logger.remove()
    logger.add(
        sys.stdout,
        colorize=True,
        level="DEBUG",
        format=(
            "<green>{time:HH:mm:ss}</green> "
            "| <level>{level: <8}</level> "
            "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
            "- <level>{message}</level>"
        ),
    )
    if logfile:
        logger.add(
            logfile,
            rotation="100 MB",
            level="TRACE",
        )


def simplify_text(text):
    """
    Simplifies the text by removing extra spaces and newlines.
    """
    # Remove multiple spaces and newlines
    text = re.sub(r"\s+", " ", text)
    text = text.replace("\n", " ")
    text = remove_html_tags(text)
    # Strip leading and trailing spaces
    return text.strip()


def remove_html_tags(text):
    """
    Removes HTML tags from the text.
    """
    clean = re.compile("<.*?>")
    return re.sub(clean, "", text)


class JobOffer(BaseModel):
    """
    Represents a job offer with its URL and description.
    """

    name: str
    date: datetime
    source: str
    url: str
    description: str
    analysis: str = ""
    offer_rating: int = 0
    candidate_rating: int = 0


global_config = __load_config()
