import asyncio
from typing import List

from loguru import logger
from playwright.async_api import async_playwright

from src_async.sites.justjoinit_async import JustJoinIt
from src_async.sites.pracujpl_async import PracujPl
from src_common.common_utils import JobOffer


@logger.catch(reraise=True)
async def extract_all_jobs() -> List[JobOffer]:
    async with async_playwright() as p:
        logger.info("Starting Playwright to extract job URLs...")
        browser = await p.chromium.launch(headless=True)
        all_jobs = []

        # context = await browser.new_context()
        # hexagon = Hexagon(context)
        # hexagon_jobs = await hexagon.extract_joboffers_objects()
        # for job in hexagon_jobs:
        #     if not isinstance(job, JobOffer):
        #         logger.warning(f"Skipping non-JobOffer object: {job}")
        #         continue
        #
        #     all_jobs.append(job)
        # await context.close()

        # Just Join It for testing
        url = "https://justjoin.it/job-offers/remote/testing?employment-type=b2b,permanent&workplace=hybrid&working-hours=full-time&keyword=python&orderBy=DESC&sortBy=published"
        async with JustJoinIt(browser, url, name="JustJoinIt-testing") as jjit:
            jjit_jobs = await jjit.perform_full_extraction()
            for job in jjit_jobs:
                if not isinstance(job, JobOffer):
                    logger.warning(f"Skipping non-JobOffer object: {job}")
                    continue
                all_jobs.append(job)

        # Just Join It for development
        url = r"https://justjoin.it/job-offers/remote/python?employment-type=b2b,permanent&experience-level=junior,mid&keyword=python&workplace=hybrid&working-hours=full-time&orderBy=DESC&sortBy=published"
        async with JustJoinIt(browser, url, name="JustJoinIt-development") as jjit:
            jjit_jobs = await jjit.perform_full_extraction()
            for job in jjit_jobs:
                if not isinstance(job, JobOffer):
                    logger.warning(f"Skipping non-JobOffer object: {job}")
                    continue
                all_jobs.append(job)

        # Just Join It for development
        url = r"https://justjoin.it/job-offers/remote/devops?employment-type=b2b,permanent&experience-level=junior,mid&workplace=hybrid&working-hours=full-time&keyword=python&orderBy=DESC&sortBy=published"

        async with JustJoinIt(browser, url, name="JustJoinIt-devops") as jjit:
            jjit_jobs = await jjit.perform_full_extraction()

            for job in jjit_jobs:
                if not isinstance(job, JobOffer):
                    logger.warning(f"Skipping non-JobOffer object: {job}")
                    continue
                all_jobs.append(job)

        # Just Join It for development
        url = r"https://justjoin.it/job-offers/remote/ai?employment-type=b2b,permanent&experience-level=junior,mid&workplace=hybrid&working-hours=full-time&keyword=python&orderBy=DESC&sortBy=published"

        async with JustJoinIt(browser, url, name="JustJoinIt-ml") as jjit:
            jjit_jobs = await jjit.perform_full_extraction()
            for job in jjit_jobs:
                if not isinstance(job, JobOffer):
                    logger.warning(f"Skipping non-JobOffer object: {job}")
                    continue
                all_jobs.append(job)

        # Pracuj.pl
        url = r"https://it.pracuj.pl/praca/python;kw?sc=0&wm=hybrid%2Chome-office&itth=37"
        async with PracujPl(browser, url) as ppl:
            ppl_jobs = await ppl.perform_full_extraction()
            for job in ppl_jobs:
                if not isinstance(job, JobOffer):
                    logger.warning(f"Skipping non-JobOffer object: {job}")
                    continue
                all_jobs.append(job)

        logger.success("Finished extracting All jobs.")
        return all_jobs


if __name__ == "__main__":
    logger.info("Starting asynchronous extraction of job offers from JustJoinIt...")
    jobs = asyncio.run(extract_all_jobs())
    for job in jobs:
        logger.info(f"Job URL: {job.url}\nName: {job.name}")
