# ruff: noqa: E501
import asyncio

from loguru import logger
from playwright.async_api import Page, async_playwright

from src_async.sites.common_async import PageOperationsAsync
from src_common.common_utils import JobOffer, simplify_text


class Hexagon(PageOperationsAsync):
    core = r"https://hexagon.com"

    def __init__(self, browser, job_list_url=None):
        self.browser = browser
        self.context = ...
        self.name = "Hexagon"
        self.cookie_accept_text = "Accept all"
        self.url = job_list_url or "https://hexagon.com/company/careers/job-listings#jl_country=Poland&jl_e=0"
        self.headless = False
        logger.success("Initialized Hexagon Scraper for name: {}, url: {}", self.name, self.url)

    async def url_extractor_pattern(self, page: Page) -> list[str]:
        """
        Pattern for data extraction for parents class.
        :param page:
        :return:
        """
        elements = await page.locator("div.job-url").all()
        logger.info(f"Found {len(elements)} job elements")
        urls = []
        for el in elements:
            a = el.locator("a")
            link = await a.get_attribute("href")
            if link:
                if "/c/new" in link:
                    link = link.replace("/c/new", "")
                logger.debug("Found job link: {}", link)
                urls.append(self.core + link if link.startswith("/") else link)
            else:
                logger.warning("Element nie ma atrybutu href: {}", await el.inner_text())
                logger.debug("Element details: {}", await el.inner_html())
        return urls

    async def job_description_extractor_pattern(self, page) -> None | str:
        """
        Pattern for data extraction for parents class.
        example: searching for locators, extracting text and returning it
        """

        desc = page.locator("span[itemprop=description]")
        if await desc.is_visible(timeout=5000):
            content_page = await desc.inner_text()
            return simplify_text(content_page)

        panels = page.locator("div[data-reach-tab-panels]")
        if await panels.is_visible():
            content_page = await panels.inner_text()
            return simplify_text(content_page)

        questions = page.locator("div.ng-scope[ng-repeat*='JobDetailQuestions']")
        all_q = await questions.all()
        if all_q:
            content_page = " ".join([await loc.inner_text() for loc in all_q])
            return simplify_text(content_page)

    async def perform_full_extraction(self) -> list[JobOffer]:
        """
        Peforms full extraction of job offers from Hexagon.
        :return: list of JobOffer objects with extracted data
        """
        urls = await super().extract_jobs_urls(self.url)
        urls = super().filter_only_not_analyzed_urls(urls)
        if not urls:
            logger.warning("No URLs extracted from Hexagon")
            return []

        jobs_data = await super().extract_jobs_details_from_urls(urls)
        return jobs_data or []


async def extract_asynchronious_hexagon():
    """
    Extracts job offers from Hexagon asynchronously.
    :return:
    """
    async with async_playwright() as p:
        logger.info("Starting Playwright to extract job URLs from Hexagon...")
        browser = await p.chromium.launch(headless=False)
        async with Hexagon(browser) as hexagon:
            logger.info("Initialized Hexagon Scraper")
            urls = await hexagon.perform_full_extraction()
            return urls


if __name__ == "__main__":
    logger.info("Starting asynchronous extraction of job offers from Hexagon...")
    x = asyncio.run(extract_asynchronious_hexagon())
    logger.success("Finished extracting job offers from Hexagon.")
