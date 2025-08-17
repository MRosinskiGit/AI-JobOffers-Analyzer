import asyncio
import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Union

from loguru import logger
from playwright._impl._errors import TargetClosedError
from playwright.async_api import Browser, BrowserContext, Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from src_common.common_utils import JobOffer, global_config, simplify_text


class PageOperationsAsync(ABC):
    cookie_accept_text = "Accept All"

    def __init__(self, browser: Browser, name: str):
        self.browser: Browser = browser
        self.context: BrowserContext = ...
        self.page: Page = ...
        self.name: str = name

    async def __aenter__(self):
        self.context = await self.browser.new_context()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.context:
            await self.context.close()

    async def restart_context(self):
        """
        Restarts the browser context by closing the current context and creating a new one.

        This method ensures that the browser context is reset, which can be useful for clearing
        session data, cookies, or other stateful information. It handles exceptions during the
        context closure and guarantees that a new context is created regardless of any errors.

        Logs:
            - Info: Indicates the attempt to restart the browser context.
            - Exception: Logs any unexpected exceptions that occur during the context closure.

        Raises:
            Any exceptions raised during the creation of a new browser context.
        """
        try:
            logger.info("Attempting to restart browser context...")
            await self.context.close()
        except Exception as e:
            logger.exception("Unexpected exception: {}", e)
        finally:
            self.context = await self.browser.new_context()
            logger.success("Context restarted")

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
    async def extract_jobs_urls(self, url: Union[str, list[str]]) -> list[str]:
        semaphore = asyncio.Semaphore(global_config["MAX_ASYNC_PLAYWRIGHT_WORKERS"])
        if isinstance(url, str):
            url = [url]

        async def __extract_urls(__url):
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
        urls = []
        for r in results:
            if isinstance(r, Exception):
                logger.error("Failed to extract URLs: {}", r)
            elif isinstance(r, list):
                urls.extend(r)
        logger.info("Finished extracting job URLs. Total: {}", len(urls))
        return urls

    @abstractmethod
    async def _url_extractor_pattern(self, page: Page) -> list[str]:
        """
        Abstract method to extract list of job URLs from a page.
        :param page: Async Playwright Page object.
        :return:
            List of job URLs extracted from the page.
        """
        pass

    # TODO CLEAN    THAT      PART
    @logger.catch(reraise=False)
    async def extract_jobs_details_from_urls(
        self, urls, max_concurrency: int = global_config["MAX_ASYNC_PLAYWRIGHT_WORKERS"]
    ):
        sem = asyncio.Semaphore(max_concurrency)

        async def process_url(url) -> JobOffer:
            async with sem:
                page = None
                title = None
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
            func: Callable[..., Awaitable]
            args: tuple = field(default_factory=tuple)
            kwargs: dict = field(default_factory=dict)

            def start(self) -> Awaitable:
                return self.func(*self.args, **self.kwargs)

            def create_task(self):
                return asyncio.create_task(self.start())

        all_task_creators = [TasksFactory(process_url, (url,)) for url in urls]
        tasks_mapped = {job.create_task(): job for job in all_task_creators}
        all_tasks = tasks_mapped.keys()
        all_tasks_init_len = len(all_tasks)
        counter = 0
        results = []
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
                    # pending.add(task)
                    # await self.restart_context()
                    # all_tasks = [TasksFactory(process_url, (tasks_mapped[task].args[0],)).create_task() for task in
                    #              pending]
                except Exception as e:
                    logger.critical("Unexpected exception: {}", e)
            counter += 1
            if all_tasks_init_len == len(results):
                logger.success("Collecting done. Collected: {} out of {}", len(results), all_tasks_init_len)

        return results

    async def _open_page_and_check_tile(self, url, check_forbidden_titles=True) -> tuple[Page, str]:
        page = await self.context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        await page.wait_for_timeout(1000)
        title = await page.title()
        if check_forbidden_titles:
            if any([phrase.lower() in title.lower() for phrase in ["Access denied", "used Cloudflare to"]]):
                logger.critical("Forbidden phrase found in page title {}", title)
                raise BotManagemenetException(f"Forbidden phrase found in page title {title}")
        return page, title

    async def _read_job_descritpion(self, page: Page, retry_attempts: int = 3) -> str:
        offer_description = None
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
    async def _job_description_extractor_pattern(self, page) -> str:
        """
        Extracts job description and tech stack from the page.
        """
        pass


class BotManagemenetException(Exception):
    pass
