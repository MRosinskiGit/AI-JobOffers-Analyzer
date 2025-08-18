import asyncio
import datetime
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Awaitable, Callable, List, Optional, Tuple, Union

from loguru import logger
from playwright._impl._errors import TargetClosedError
from playwright.async_api import Browser, BrowserContext, Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from src_common.common_utils import JobOffer, global_config, simplify_text
from src_common.database import DatabaseManager


class PageOperationsAsync(ABC):
    """
    Abstract base class for asynchronous page operations using Playwright.
    Provides methods for scraping job offers, handling browser context, and utility functions for scrolling and
    collecting data.
    """

    cookie_accept_text: str = "Accept All"

    def __init__(self, browser: Browser, name: str) -> None:
        """
        Initialize the PageOperationsAsync instance.
        Args:
            browser (Browser): Playwright browser instance.
            name (str): Name of the job site/source.
        """
        self.browser: Browser = browser
        self.context: BrowserContext = ...
        self.page: Page = ...
        self.name: str = name

    async def __aenter__(self) -> "PageOperationsAsync":
        """
        Async context manager entry. Creates a new browser context.
        Returns:
            PageOperationsAsync: Self instance with initialized context.
        """
        self.context = await self.browser.new_context()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """
        Async context manager exit. Closes the browser context.
        """
        if self.context:
            await self.context.close()

    async def restart_context(self) -> None:
        """
        Restarts the browser context by closing the current context and creating a new one.
        Handles exceptions during context closure and guarantees a new context is created.
        """
        try:
            logger.info("Attempting to restart browser context...")
            await self.context.close()
        except TargetClosedError as e:
            logger.exception("TargetClosedError exception: {}", e)
        finally:
            self.context = await self.browser.new_context()
            logger.success("Context restarted")

    async def scroll_to_the_bottom(self, page: Page, pause_time: int = 2000) -> None:
        """
        Scrolls to the bottom of the page, pausing between scrolls.
        Args:
            page (Page): Playwright page object.
            pause_time (int): Time to wait between scrolls in milliseconds.
        """
        while True:
            curr_height = await page.evaluate("window.scrollY")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(pause_time)
            new_height = await page.evaluate("window.scrollY")
            if new_height == curr_height:
                break

    @staticmethod
    async def scroll_and_collect(
        page: Page,
        collect_locator: str,
        extraction_data_fun: Callable[[Page], Awaitable[Optional[str]]],
        scroll_by: int = 400,
        wait_between: int = 250,
    ) -> List[str]:
        """
        Scrolls the page and collects data using the provided locator and extraction function.
        Args:
            page (Page): Playwright page object.
            collect_locator (str): Locator string for elements to collect.
            extraction_data_fun (Callable): Async function to extract data from each element.
            scroll_by (int): Amount to scroll by each iteration.
            wait_between (int): Time to wait between scrolls in milliseconds.
        Returns:
            List[str]: List of unique extracted data strings.
        """
        logger.debug("Starting scroll and collect operation")
        scrollstart = 0
        extracted_data: List[str] = []
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
    async def extract_jobs_urls(self, url: Union[str, List[str]]) -> List[str]:
        """
        Asynchronously extracts job offer URLs from one or more pages.
        Args:
            url (Union[str, List[str]]): Single URL or list of URLs to extract job offers from.
        Returns:
            List[str]: List of extracted job offer URLs.
        """
        semaphore = asyncio.Semaphore(global_config["MAX_ASYNC_PLAYWRIGHT_WORKERS"])
        if isinstance(url, str):
            url = [url]

        async def __extract_urls(__url: str) -> Optional[List[str]]:
            async with semaphore:
                logger.info("Extracting Job Offers URLs from {}", __url)
                page = await self.context.new_page()
                try:
                    await page.goto(__url, timeout=120_000)
                    try:
                        await page.get_by_role("button", name=self.cookie_accept_text).click(timeout=2000)
                    except PlaywrightTimeoutError:
                        pass
                    await page.wait_for_timeout(1500)
                    urls = await self._url_extractor_pattern(page)
                    logger.success("Extracted {} job URLs", len(urls))
                    return urls
                except Exception as e:
                    logger.exception(f"Unexpected exception during extracting URLs: {e}")
                finally:
                    await page.close()

        results = await asyncio.gather(*(__extract_urls(u) for u in url), return_exceptions=True)
        urls: List[str] = []
        for r in results:
            if isinstance(r, Exception):
                logger.error("Failed to extract URLs: {}", r)
            elif isinstance(r, list):
                urls.extend(r)
        logger.info("Finished extracting job URLs. Total: {}", len(urls))
        return urls

    @abstractmethod
    async def _url_extractor_pattern(self, page: Page) -> List[str]:
        """
        Abstract method to extract list of job URLs from a page.
        Args:
            page (Page): Async Playwright Page object.
        Returns:
            List[str]: List of job URLs extracted from the page.
        """

    def filter_only_not_analyzed_urls(self, urls: List[str]) -> List[str]:
        """
        Filters out URLs that have already been analyzed.
        This method should be implemented in subclasses to provide specific filtering logic.
        Returns:
            List[str]: List of URLs that have not been analyzed yet.
        """
        logger.info("Filtering URLs that have not been analyzed yet")
        DB_NAME = os.getenv("DB_NAME")
        TABLE_NAME = os.getenv("TABLE_NAME")
        RELPATH = os.getenv("RELPATH")
        db = DatabaseManager(DB_NAME, TABLE_NAME, RELPATH)
        filtered_urls: List[str] = []
        for url in urls:
            job_obj = JobOffer(
                name="",
                date=datetime.datetime.now(),
                source="",
                url=url,
                description="",
            )
            if not len(db.search_jobs(job_obj)):
                filtered_urls.append(job_obj.url)
        logger.success("Filtered URLs. Total: {} out of {}", len(filtered_urls), len(urls))
        return filtered_urls

    @logger.catch(reraise=False)
    async def extract_jobs_details_from_urls(
        self, urls: List[str], max_concurrency: int = global_config["MAX_ASYNC_PLAYWRIGHT_WORKERS"]
    ) -> List[JobOffer]:
        """
        Asynchronously extracts job details from a list of job offer URLs using Playwright.
        Args:
            urls (List[str]): List of job offer URLs to process.
            max_concurrency (int): Maximum number of concurrent Playwright workers.
        Returns:
            List[JobOffer]: List of extracted JobOffer objects.
        """
        sem = asyncio.Semaphore(max_concurrency)

        async def process_url(url: str) -> Optional[JobOffer]:
            """
            Processes a single job offer URL and extracts its details.
            Args:
                url (str): Job offer URL.
            Returns:
                Optional[JobOffer]: Extracted job offer details or None if not found.
            """
            async with sem:
                page: Optional[Page] = None
                title: Optional[str] = None
                logger.trace("Scraping data from URL: {}", url)
                page, title = await self._open_page_and_check_tile(url)
                try:
                    offer_description = await self._read_job_descritpion(page)
                    if not offer_description:
                        logger.warning("No job description found for URL: {}", url)
                        return None
                    logger.trace("SUCCESS: JobOffer scraped with Title: {}", title)

                    return JobOffer(
                        name=title,
                        date=datetime.datetime.now(),
                        source=self.name,
                        url=url,
                        description=simplify_text(offer_description),
                    )
                finally:
                    if page:
                        await page.close()

        @dataclass
        class TasksFactory:
            """
            Helper class for creating asyncio tasks for job offer processing.
            """

            func: Callable[..., Awaitable]
            args: tuple = field(default_factory=tuple)
            kwargs: dict = field(default_factory=dict)

            def start(self) -> Awaitable:
                """
                Starts the async function with provided arguments.
                Returns:
                    Awaitable: Awaitable object for the async function.
                """
                return self.func(*self.args, **self.kwargs)

            def create_task(self) -> asyncio.Task:
                """
                Creates an asyncio task for the async function.
                Returns:
                    asyncio.Task: Created asyncio task.
                """
                return asyncio.create_task(self.start())

        all_task_creators: List[TasksFactory] = [TasksFactory(process_url, (url,)) for url in urls]
        tasks_mapped = {job.create_task(): job for job in all_task_creators}
        all_tasks = list(tasks_mapped.keys())
        all_tasks_init_len = len(all_tasks)
        counter = 0
        results: List[JobOffer] = []
        while counter < 1:  # Fixme fix restarting context
            logger.info("Starting {} of collection iteration...", counter + 1)
            done, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                try:
                    if task.result() is None:
                        logger.warning("None returned by {}", tasks_mapped[task])
                        continue
                    results.append(task.result())
                except (BotManagemenetException, TargetClosedError) as e:
                    for task_unfinished in pending:
                        task_unfinished.cancel()
                    if isinstance(e, BotManagemenetException):
                        logger.warning("Bot management detected. Restarting context")
                    elif isinstance(e, TargetClosedError):
                        logger.warning("Target closed error. Restarting context")
                    await asyncio.gather(*pending, return_exceptions=True)
                except Exception as e:
                    logger.critical("Unexpected exception: {}", e)
            counter += 1
            if all_tasks_init_len == len(results):
                logger.success("Collecting done. Collected: {} out of {}", len(results), all_tasks_init_len)

        return results

    async def _open_page_and_check_tile(self, url: str, check_forbidden_titles: bool = True) -> Tuple[Page, str]:
        """
        Opens a page and checks its title for forbidden phrases.
        Args:
            url (str): URL to open.
            check_forbidden_titles (bool): Whether to check for forbidden phrases in the title.
        Returns:
            Tuple[Page, str]: Tuple of Playwright Page object and page title.
        Raises:
            BotManagemenetException: If forbidden phrases are found in the title.
        """
        page = await self.context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        await page.wait_for_timeout(1000)
        title = await page.title()
        if check_forbidden_titles:
            if any([phrase.lower() in title.lower() for phrase in ["Access denied", "used Cloudflare to"]]):
                logger.critical("Forbidden phrase found in page title {}", title)
                raise BotManagemenetException(f"Forbidden phrase found in page title {title}")
        return page, title

    async def _read_job_descritpion(self, page: Page, retry_attempts: int = 3) -> Optional[str]:
        """
        Reads the job description from the page, retrying if necessary.
        Args:
            page (Page): Playwright page object.
            retry_attempts (int): Number of retry attempts.
        Returns:
            Optional[str]: Extracted job description or None if not found.
        """
        offer_description: Optional[str] = None
        for i in range(retry_attempts):
            try:
                offer_description = await self._job_description_extractor_pattern(page)
            except PlaywrightTimeoutError:
                if i != retry_attempts - 1:
                    logger.debug("No job description found. Retrying ({}/{})...", i + 1, retry_attempts)
                    await page.reload(wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(1000)
            if offer_description:
                return offer_description

    @abstractmethod
    async def _job_description_extractor_pattern(self, page: Page) -> Optional[str]:
        """
        Abstract method to extract job description and tech stack from the page.
        Args:
            page (Page): Playwright page object.
        Returns:
            Optional[str]: Extracted job description or None if not found.
        """


class BotManagemenetException(Exception):
    """
    Exception raised when bot management or anti-bot measures are detected during scraping.
    """
