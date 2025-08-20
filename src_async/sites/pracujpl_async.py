import asyncio
import re
from urllib.parse import urlparse, urlsplit, urlunsplit

from loguru import logger
from playwright.async_api import Page, async_playwright

from src_async.sites.common_async import PageOperationsAsync
from src_common.common_utils import JobOffer


class PracujPl(PageOperationsAsync):
    def __init__(self, browser, url, name="Pracujpl"):
        self.browser = browser
        self.context = ...
        self.url = url
        self.name = name
        self.cookie_accept_text = "Akceptuj wszystkie"
        self.headless = False
        logger.success("Initialized PracujPl Scraper")

    async def get_max_page_number(self) -> int | None:
        """
        From the main page with offers, get the maximum page number.
        :return:
        """
        page = await self.context.new_page()
        try:
            await page.goto(self.url)
            number_of_pages = await super().extract_text_from_locator(
                page, "span[data-test='top-pagination-max-page-number']"
            )
            return int(number_of_pages, 10)
        finally:
            await page.close()

    async def url_extractor_pattern(self, page: Page) -> list[str]:
        """
        Pattern for data extraction for parents class.

        :param page: Page
        :return:
        """
        locators = await page.locator("a[data-test='link-offer']").all()

        hrefs = await asyncio.gather(*(loc.get_attribute("href") for loc in locators))

        urls = [
            urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
            for href in hrefs
            if href
            for parts in [urlsplit(href)]
        ]

        return urls

    @logger.catch()
    async def job_description_extractor_pattern(self, page) -> None | str:
        """
        Pattern for data extraction for parents class.
        """

        expected = await super().extract_text_from_locator(page, 'div[data-scroll-id="technologies-expected-1"]')

        optional = await super().extract_text_from_locator(page, 'div[data-scroll-id="technologies-optional-1"]')

        about_project = await super().extract_text_from_locator(page, 'ul[data-test="text-about-project"]')

        responsibilities = await super().extract_text_from_locator(
            page, 'section[data-test="section-responsibilities"]'
        )

        requirements = await super().extract_text_from_locator(page, 'section[data-test="section-requirements"]')

        offers = await super().extract_text_from_locator(page, 'section[data-test="section-offered"]')

        benefits = await super().extract_text_from_locator(page, 'section[data-test="section-benefits"]')

        return f"{expected} {optional} {about_project} {responsibilities} {requirements} {offers} {benefits}"

    @logger.catch(reraise=False, default=[])
    async def perform_full_extraction(self) -> list[JobOffer]:
        """
        Peforms full extraction of job offers from Pracuj.pl.
        :return: list of JobOffer objects with extracted data
        """
        max_page_number = await self.get_max_page_number()
        urls = [
            f"https://it.pracuj.pl/praca/python;kw?sc=0&pn={i}&wm=hybrid%2Chome-office&itth=37"
            for i in range(1, max_page_number + 1)
        ]
        urls = await super().extract_jobs_urls(urls)
        urls = super().filter_only_not_analyzed_urls(urls)
        if not urls:
            logger.warning("No URLs extracted")
            return []
        urls = PracujPl.dedupe_pracuj_urls(urls, keep="max_id")
        jobs_data = await super().extract_jobs_details_from_urls(urls)
        return jobs_data or []

    @staticmethod
    def _canon_key_and_id(url: str) -> tuple[tuple[str, str], int | None]:
        """
        Extracts the canonical key and offer ID from a Pracuj.pl URL.
        :param url: string with url to extract key and ID from
        :return: tuple with (canonical key, offer ID)
        example:
        >>> PracujPl._canon_key_and_id("https://www.pracuj.pl/praca/data-engineer-warszawa-pulawska-2,oferta,1004285879")
        (('www.pracuj.pl', '/praca/data-engineer-warszawa-pulawska-2'), 1004285879)
        """
        ID_RE = re.compile(r",oferta,(\d+)$", re.IGNORECASE)
        p = urlparse(url)
        host = p.netloc.lower()
        path = p.path.rstrip("/")  # usuń końcowy slash
        m = ID_RE.search(path)
        offer_id = int(m.group(1)) if m else None

        core_path = ID_RE.sub("", path).lower()
        return (host, core_path), offer_id

    @staticmethod
    def dedupe_pracuj_urls(urls, keep="first") -> list[str]:
        """
        Remove all dusplicated URLs from the list, keeping only the first or last occurrence
        example:
        >>> urls = ["https://www.pracuj.pl/praca/data-engineer-warszawa-pulawska-2,oferta,1004285879",
        ... "https://www.pracuj.pl/praca/data-engineer-warszawa-pulawska-2,oferta,1004285880",]
        >>> PracujPl.dedupe_pracuj_urls(urls, keep="max_id")
        >>> Out: ["https://www.pracuj.pl/praca/data-engineer-warszawa-pulawska-2,oferta,1004285880"]

        """
        assert keep in {"first", "last", "max_id"}
        selected = {}

        for idx, u in enumerate(urls):
            key, oid = PracujPl._canon_key_and_id(u)
            if key not in selected:
                selected[key] = (idx, u, oid)
            else:
                if keep == "last":
                    selected[key] = (idx, u, oid)
                elif keep == "max_id":
                    prev = selected[key]
                    if (prev[2] or -1) < (oid or -1):
                        selected[key] = (idx, u, oid)

        # zwróć w kolejności pierwotnego pierwszego wystąpienia klucza
        return [v[1] for v in sorted(selected.values(), key=lambda t: t[0])]


async def extract_asynchronious_pracujpl() -> list[JobOffer]:
    """
    Extracts job offers from Pracuj.pl using Playwright asynchronously.
    :return: list of JobOffer objects with extracted data
    """
    async with async_playwright() as p:
        logger.info("Starting Playwright to extract job URLs from Pracuj.pl...")
        browser = await p.chromium.launch(headless=False)
        async with PracujPl(
            browser, r"https://it.pracuj.pl/praca/python;kw?sc=0&wm=hybrid%2Chome-office&itth=37"
        ) as pracujpl_scraper:
            jobs_data = await pracujpl_scraper.perform_full_extraction()
            logger.success("Extracted {} job offers from Pracuj.pl", len(jobs_data))
            return jobs_data


if __name__ == "__main__":
    logger.info("Starting asynchronous extraction of job offers from Pracuj.pl...")
    x = asyncio.run(extract_asynchronious_pracujpl())
    logger.success("Finished extracting job offers from Pracuj.pl")
