# ruff : noqa: E501
import datetime
import os
import sys

from dotenv import load_dotenv
from loguru import logger

from src_async.scraping_async import extract_all_jobs
from src_common.ai_analyzer import AIAnalyzer
from src_common.common_utils import configure_logger
from src_common.database import DatabaseManager

configure_logger("logs/log_async_main_{time}.log")

logger.info("Loading environment variables...")
load_dotenv()

logger.info("Starting async job extraction...")
all_jobs = extract_all_jobs()

logger.info("Request jobs analysis...")
AIAnalyzer(api_key=os.getenv("DEEPSEEK_API"), base_url="https://api.deepseek.com").request_jobs_ai_analyze(all_jobs)


logger.info("Extracting jobs for today...")
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")
RELPATH = os.getenv("RELPATH")
db = DatabaseManager(DB_NAME, TABLE_NAME, RELPATH)
todays_jobs = db.extract_jobs_for_a_date(datetime.date.today())

if not todays_jobs:
    logger.warning("No jobs found for today.")
    sys.exit(0)

logger.info("Generating HTML report for today's jobs...")
db.generate_report_html(todays_jobs)
