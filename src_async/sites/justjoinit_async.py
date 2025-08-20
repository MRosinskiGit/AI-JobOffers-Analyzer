import asyncio

from loguru import logger
from playwright.async_api import Browser, BrowserContext, async_playwright
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from src_async.sites.common_async import PageOperationsAsync


class JustJoinIt(PageOperationsAsync):
    core = r"https://justjoin.it"

    def __init__(self, browser: Browser, job_list_url, name="JustJoinIt"):
        self.browser = browser
        self.context: BrowserContext = ...
        self.name = name
        self.url = job_list_url
        self.headless = False  # Set to True for headless mode
        logger.success("Initialized JustJoinIt Scraper for name: {}, url: {}", self.name, self.url)

    async def url_extractor_pattern(self, page):
        urls_parts = await super().scroll_and_collect(
            page, collect_locator="a.offer-card", extraction_data_fun=lambda el: el.get_attribute("href")
        )
        return [self.core + url_part for url_part in urls_parts if url_part is not None]

    async def job_description_extractor_pattern(self, page) -> None | str:
        """
        Extracts job description and tech stack from the page.
        """
        try:
            desc_hdr = page.get_by_text("Job description").first
            tech_hdr = page.get_by_text("Tech stack").first

            parent_textbox = desc_hdr.locator("xpath=..").locator("xpath=..")
            parent_techstack = tech_hdr.locator("xpath=..")
            content_page = await parent_textbox.inner_text(timeout=10000)
            techstack = await parent_techstack.inner_text(timeout=10000)
            return f"{techstack} | {content_page}"
        except PlaywrightTimeoutError:
            return None

    @logger.catch(reraise=False, default=[])
    async def perform_full_extraction(self):
        urls = await super().extract_jobs_urls(self.url)
        urls = super().filter_only_not_analyzed_urls(urls)
        if not urls:
            logger.warning("No URLs extracted")
            return []
        jobs_data = await super().extract_jobs_details_from_urls(urls)
        return jobs_data or []


async def extract_asynchronious_jjit():
    async with async_playwright() as p:
        logger.info("Starting Playwright to extract job URLs from JJIT...")
        browser = await p.chromium.launch(headless=False)
        async with JustJoinIt(
            browser,
            r"https://justjoin.it/job-offers/remote/testing?employment-type=b2b,permanent&workplace=hybrid&working-hours=full-time&keyword=python&orderBy=DESC&sortBy=published",
        ) as JJIT:
            offers = await JJIT.perform_full_extraction()
        return offers


if __name__ == "__main__":
    logger.info("Starting asynchronous extraction of job offers from JustJoinIt...")
    x = asyncio.run(extract_asynchronious_jjit())
    logger.success("Finished extracting job offers from JustJoinIt.")
