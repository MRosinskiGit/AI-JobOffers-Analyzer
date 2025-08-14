# ruff : noqa: E501
import asyncio
import concurrent.futures
import datetime
import json
import os
import re
import sys
import threading

from dotenv import load_dotenv
from json_repair import repair_json
from loguru import logger
from openai import OpenAI

from database import DatabaseManager
from src_async.scraping_async import extract_all_jobs
from src_common.common_utils import configure_logger

configure_logger("logs/log_async_main_{time}.log")

load_dotenv()
logger.info("Initializing OpenAI model with API key from environment variable.")
model = OpenAI(api_key=os.getenv("DEEPSEEK_API"), base_url="https://api.deepseek.com")


def extract_ratings(response):
    if isinstance(response, dict):
        return (
            response.get("ocena_oferty", 0),
            response.get("dopasowanie_kandydata", 0),
        )
    match = re.search(r"\[ocena_oferty=(\d+)", response)
    if match:
        return int(match.group(1)), None
    return None, None


def clean_deepseek_response(response):
    cleaned = re.sub(r"<think>.*?</think>\s*", "", response, flags=re.DOTALL)
    # extract the JSON part
    json_match = re.search(r"\{.*?\}", cleaned, flags=re.DOTALL)
    if json_match:
        cleaned = json_match.group(0)
        cleaned = repair_json(cleaned)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.error("JSON decoding error: {}", e)
            logger.error("Response content: {}", cleaned)
            return cleaned
    else:
        logger.error("No valid JSON found in the response")
        return cleaned


DB_NAME = "jobs.db"
TABLE_NAME = "job_offers_api"
RELPATH = "./db"
db = DatabaseManager(DB_NAME, TABLE_NAME, RELPATH)

all_jobs = asyncio.run(extract_all_jobs())


def process_job(data):
    db = DatabaseManager(DB_NAME, TABLE_NAME, RELPATH)
    if len(db.search_jobs(data)) != 0:
        logger.warning("Job already exists in the database: {}", data.name)
        return
    # logger.info("Processing job: {}", data.name)
    # response = model.chat.completions.create(
    #     model="deepseek-reasoner",
    #     messages=[
    #         {
    #             "role": "system",
    #             "content": (
    #                 "Jesteś narzędziem do oceny dopasowania ofert pracy IT. "
    #                 "Zawsze zwracasz WYŁĄCZNIE poprawny JSON w UTF-8, bez markdownu, komentarzy "
    #                 "i bez wyjaśniania rozumowania. Odpowiadasz po polsku."
    #             ),
    #         },
    #         {
    #             "role": "user",
    #             "content": f"""
    # {os.getenv("CANDIDATE_PROMPT")}
    #
    # WEJŚCIE:
    # OFERTA:
    # {data.description}
    #
    # ŹRÓDŁO:
    # {data.url}
    #
    # WYJŚCIE (TYLKO JSON; dokładnie te klucze):
    # {{
    #   "opinia": "maks 5 zdań; zwięzłe plusy i minusy; wskaż braki.",
    #   "ocena_oferty": 0,
    #   "dopasowanie_kandydata": 0,
    #   "braki": [],
    #   "techstack": [],
    #   "zrodlo": "{data.url}"
    # }}
    # """,
    #         },
    #     ],
    #     temperature=0.1,
    #     max_tokens=10000,
    #     response_format={"type": "json_object"},
    # )
    #
    # logger.success("Response for URL: {}", data.url)
    # logger.debug(response.choices[0].message.content)
    # response_formatted = clean_deepseek_response(str(response.choices[0].message.content))
    # offer_rating, candidate_rating = extract_ratings(response_formatted)
    # data.analysis = str(response_formatted)
    # if offer_rating:
    #     data.offer_rating = offer_rating
    # if candidate_rating:
    #     data.candidate_rating = candidate_rating
    # with db_access_lock:
    #     db.insert_job_offer(data)
    return data


db_access_lock = threading.Lock()
with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
    all_futures = {executor.submit(process_job, data): data for data in all_jobs}
    for future in concurrent.futures.as_completed(all_futures):
        res = all_futures[future]
        try:
            data = future.result()
        except Exception as exc:
            logger.error("Job processing generated an exception for {}: {}", res, exc)
        else:
            logger.success("All jobs processed and stored in the database")

# logger.info("Extracting jobs for today...")
# todays_job = db.extract_jobs_for_a_date(datetime.date.today())
#
# if not todays_job:
#     logger.warning("No jobs found for today.")
#     sys.exit(0)
# db.generate_report_html(todays_job)
