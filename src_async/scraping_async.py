import asyncio
from typing import List

from loguru import logger
from playwright.async_api import async_playwright

from src_async.sites.hexagon_async import Hexagon
from src_async.sites.justjoinit_async import JustJoinIt
from src_async.sites.pracujpl_async import PracujPl
from src_common.common_utils import JobOffer


def extract_all_jobs() -> List[JobOffer]:
    """
    Asynchronously extracts job offers from multiple job sites using Playwright.

    This function launches a headless Chromium browser and collects job offers from several sources:
    - Just Join It (multiple categories)
    - Pracuj.pl
    The function can be extended to support more job sites.

    Returns:
        List[JobOffer]: List of all extracted job offers.
    """

    async def _extract() -> List[JobOffer]:
        async with async_playwright() as p:
            logger.info("Starting Playwright to extract job URLs...")
            browser = await p.chromium.launch(headless=True)
            all_jobs: List[JobOffer] = []

            # Just Join It for testing
            url = "https://justjoin.it/job-offers/remote/testing?employment-type=b2b,permanent&workplace=hybrid&working-hours=full-time&keyword=python&orderBy=DESC&sortBy=published"
            async with JustJoinIt(browser, url, name="JustJoinIt-testing") as jjit:
                jjit_jobs = await jjit.perform_full_extraction()
                for jjit_job in jjit_jobs:
                    if not isinstance(jjit_job, JobOffer):
                        logger.warning(f"Skipping non-JobOffer object: {jjit_job}")
                        continue
                    all_jobs.append(jjit_job)

            # Just Join It for development
            url = r"https://justjoin.it/job-offers/remote/python?employment-type=b2b,permanent&experience-level=junior,mid&keyword=python&workplace=hybrid&working-hours=full-time&orderBy=DESC&sortBy=published"
            async with JustJoinIt(browser, url, name="JustJoinIt-development") as jjit:
                jjit_jobs = await jjit.perform_full_extraction()
                for jjit_job in jjit_jobs:
                    if not isinstance(jjit_job, JobOffer):
                        logger.warning(f"Skipping non-JobOffer object: {jjit_job}")
                        continue
                    all_jobs.append(jjit_job)

            # Just Join It for development
            url = r"https://justjoin.it/job-offers/remote/devops?employment-type=b2b,permanent&experience-level=junior,mid&workplace=hybrid&working-hours=full-time&keyword=python&orderBy=DESC&sortBy=published"

            async with JustJoinIt(browser, url, name="JustJoinIt-devops") as jjit:
                jjit_jobs = await jjit.perform_full_extraction()

                for jjit_job in jjit_jobs:
                    if not isinstance(jjit_job, JobOffer):
                        logger.warning(f"Skipping non-JobOffer object: {jjit_job}")
                        continue
                    all_jobs.append(jjit_job)

            # Just Join It for development
            url = r"https://justjoin.it/job-offers/remote/ai?employment-type=b2b,permanent&experience-level=junior,mid&workplace=hybrid&working-hours=full-time&keyword=python&orderBy=DESC&sortBy=published"

            async with JustJoinIt(browser, url, name="JustJoinIt-ml") as jjit:
                jjit_jobs = await jjit.perform_full_extraction()
                for jjit_job in jjit_jobs:
                    if not isinstance(jjit_job, JobOffer):
                        logger.warning(f"Skipping non-JobOffer object: {jjit_job}")
                        continue
                    all_jobs.append(jjit_job)

            # Pracuj.pl
            url = r"https://it.pracuj.pl/praca/python;kw?sc=0&wm=hybrid%2Chome-office&itth=37"
            async with PracujPl(browser, url) as ppl:
                ppl_jobs = await ppl.perform_full_extraction()
                for ppl_job in ppl_jobs:
                    if not isinstance(ppl_job, JobOffer):
                        logger.warning(f"Skipping non-JobOffer object: {ppl_job}")
                        continue
                    all_jobs.append(ppl_job)

            # Hexagon
            url = r"https://hexagon.com/company/careers/job-listings#jl_country=Poland&jl_e=0"
            async with Hexagon(browser, url) as hexagon:
                hex_jobs = await hexagon.perform_full_extraction()
                for hex_job in hex_jobs:
                    if not isinstance(hex_job, JobOffer):
                        logger.warning(f"Skipping non-JobOffer object: {hex_job}")
                        continue
                    all_jobs.append(hex_job)

            logger.success("Finished extracting All jobs.")
            return all_jobs

    return asyncio.run(_extract())


if __name__ == "__main__":
    logger.info("Starting asynchronous extraction of job offers from JustJoinIt...")
    jobs: List[JobOffer] = extract_all_jobs()
    for job in jobs:
        logger.info(f"Job URL: {job.url}\nName: {job.name}")
