import re
import sys
import time
from datetime import datetime

from loguru import logger
from pydantic import BaseModel


def configure_logger(logfile=None):
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


class PageOperations:
    def __init__(self, page):
        self.page = page

    def scroll_to_the_bottom(self, pause_time=3):
        while True:
            # Wysokość przed przewinięciem
            curr_height = self.page.evaluate("window.scrollY")

            # Przewiń na dół
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(pause_time)  # czas na doładowanie nowych elementów

            # Sprawdź nową wysokość
            new_height = self.page.evaluate("window.scrollY")

            # Jeśli wysokość się nie zmieniła -> koniec
            if new_height == curr_height:
                break

    def scroll_and_collect(self, collect_locator, extraction_data_fun, scroll_by=400, wait_between=250):
        logger.debug("Starting scroll and collect operation")
        scrollstart = 0
        extracted_data = []
        while True:
            curr_height = self.page.evaluate("window.scrollY")
            all_locators = self.page.locator(collect_locator)
            for el in all_locators.all():
                data = extraction_data_fun(el)
                if data:
                    extracted_data.append(data)
                else:
                    logger.warning("No searched element ", el.inner_text())
                    logger.debug("Element details: {}", el.inner_html())
            self.page.evaluate(f"window.scrollTo({scrollstart}, {scrollstart + scroll_by})")
            self.page.wait_for_timeout(wait_between)
            scrollstart += scroll_by
            new_height = self.page.evaluate("window.scrollY")
            if new_height <= curr_height:
                logger.debug("Reached the end of the page or no new elements found")
                break
        all_urls = list(set(extracted_data))
        logger.info(f"Collected {len(all_urls)} unique URLs")
        return all_urls


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
