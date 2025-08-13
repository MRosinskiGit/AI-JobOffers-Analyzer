import asyncio
import datetime
from abc import ABC, abstractmethod

from loguru import logger
from playwright.async_api import BrowserContext, Page

from src_common.common_utils import JobOffer, simplify_text


class PageOperationsAsync(ABC):
    def __init__(self, context: BrowserContext, name: str):
        self.context: str = context
        self.name: BrowserContext = name
        print("Constructor")

    async def scroll_to_the_bottom(self, page, pause_time=2000):
        while True:
            curr_height = await page.evaluate("window.scrollY")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(pause_time)
            new_height = await page.evaluate("window.scrollY")
            if new_height == curr_height:
                break

    @staticmethod
    async def scroll_and_collect(page, collect_locator, extraction_data_fun, scroll_by=400, wait_between=250):
        logger.debug("Starting scroll and collect operation")
        scrollstart = 0
        extracted_data = []
        while True:
            curr_height = await page.evaluate("window.scrollY")
            all_locators = page.locator(collect_locator)
            for el in await all_locators.all():
                data = await extraction_data_fun(el)
                if data:
                    extracted_data.append(data)
                else:
                    logger.warning("No searched element ", await el.inner_text())
                    logger.debug("Element details: {}", await el.inner_html())
            await page.evaluate(f"window.scrollTo({scrollstart}, {scrollstart + scroll_by})")
            await page.wait_for_timeout(wait_between)
            scrollstart += scroll_by
            new_height = await page.evaluate("window.scrollY")
            if new_height <= curr_height:
                logger.debug("Reached the end of the page or no new elements found")
                break
        all_urls = list(set(extracted_data))
        logger.debug(f"Collected {len(all_urls)} unique URLs")
        return all_urls

    @logger.catch()
    async def extract_jobs_urls(self, url):
        logger.info("Extracting Job Offers URLs from {}", url)
        page = await self.context.new_page()
        try:
            await page.goto(url)
            await page.get_by_role("button", name="Accept All").click()
            await page.wait_for_timeout(1500)
            urls = await self.url_extractor_pattern(page)
            logger.success("Extracted {} job URLs", len(urls))
            return urls
        except Exception as e:
            logger.exception(f"Unexpected exception during extracting URLs: {e}")
        finally:
            await page.close()

    @abstractmethod
    async def url_extractor_pattern(self, page: Page) -> list[str]:
        """
        Abstract method to extract list of job URLs from a page.
        :param page: Async Playwright Page object.
        :return:
            List of job URLs extracted from the page.
        """
        pass

    @logger.catch(reraise=False)
    async def extract_jobs_details_from_urls(self, urls, max_concurrency: int = 15):
        sem = asyncio.Semaphore(max_concurrency)

        @logger.catch()
        async def process_url(url):
            async with sem:
                logger.info("Scraping data from URL: {}", url)
                page = await self.context.new_page()
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=120_000)
                    await page.wait_for_timeout(1000)
                    for i in range(RETRY_ATTEMPTS := 3):
                        offer_descritpion = await self.job_description_extractor_pattern(page)
                        if offer_descritpion:
                            break
                        if i != RETRY_ATTEMPTS - 1:
                            logger.debug("No job description found. Retrying ({}/{})...", i + 1, RETRY_ATTEMPTS)
                            await page.reload(wait_until="domcontentloaded", timeout=60_000)
                            await page.wait_for_timeout(1000)

                    if offer_descritpion is None:
                        logger.warning("No job description found for URL: {}", url)
                        return None
                    offer_descritpion = simplify_text(offer_descritpion)
                    title = await page.title()
                    logger.success("JobOffer scraped with Title: {}", title)

                    return JobOffer(
                        name=title,
                        date=datetime.datetime.now(),
                        source=self.name,
                        url=url,
                        description=offer_descritpion,
                    )
                finally:
                    await page.close()

        results = await asyncio.gather(*(process_url(u) for u in urls), return_exceptions=True)

        jobs = []
        for u, r in zip(urls, results):
            if isinstance(r, Exception):
                logger.error("Failed to process {}: {}", u, r)
            elif isinstance(r, JobOffer):
                jobs.append(r)

        logger.info("Finished extracting job details from JJIT. Ok: {}, Fail: {}", len(jobs), len(urls) - len(jobs))
        return jobs

    @abstractmethod
    async def job_description_extractor_pattern(self, page) -> str:
        """
        Extracts job description and tech stack from the page.
        """
        pass
