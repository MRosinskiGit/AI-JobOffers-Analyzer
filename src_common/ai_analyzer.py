import concurrent
import json
import os
import re
import threading
from typing import Optional

from json_repair import repair_json
from loguru import logger
from openai import APIError, AuthenticationError, OpenAI, RateLimitError

from src_common.common_utils import JobOffer, global_config
from src_common.database import DatabaseManager


class AIAnalyzer:
    """
    Class to handle AI analysis of job offers.
    """

    def __init__(self, api_key: str, base_url: str = "https://api.deepseek.com"):
        self.model = OpenAI(api_key=api_key, base_url=base_url)

    @staticmethod
    def build_prompt(job: JobOffer):
        """
        Build the prompt for the AI model based on the job offer.
        :param job:
        :return:
        """
        return [
            {  # rdzeń: tylko JSON PL
                "role": "system",
                "content": (
                    "Jesteś narzędziem do oceny dopasowania ofert pracy IT. "
                    "Zwracasz WYŁĄCZNIE poprawny JSON w UTF-8, bez markdownu i bez wyjaśniania rozumowania. "
                    "Odpowiadasz po polsku."
                ),
            },
            {  # schema i definicje
                "role": "system",
                "content": (
                    "Wyjście — dokładnie taki JSON:\n"
                    '{ "ocena_oferty": <int 0-100>, '
                    '"dopasowanie_kandydata": <int 0-100>, '
                    '"techstack": ["...", "..."], '
                    '"braki": ["...", "..."], '
                    '"opinia": "max 5 krótkich zdań" }\n'
                    "techstack: 1–20 unikalnych technologii z ogłoszenia, małymi literami; "
                    "braki: wymagania z ogłoszenia, których kandydat nie spełnia."
                ),
            },
            {  # profil kandydata (skondensowany)
                "role": "system",
                "content": (os.getenv("PROFILE")),
            },
            {  # normalizacja/synonimy
                "role": "system",
                "content": (
                    "Normalizacja techstack (zapisuj małymi literami): "
                    '"azure devops pipelines|azure pipelines|ado pipelines|azure devops ci/cd"→"azure devops"; '
                    '"qa automation|sdet|automated testing"→"test automation"; '
                    '"http api|web api"→"rest api"; '
                    '"hardware-in-the-loop|software-in-the-loop"→"hil/sil"; '
                    '"python backend|python scripting"→"python"; '
                    '"continuous integration|continuous delivery"→"ci/cd"; '
                    '"gh actions|github actions"→"github actions"; '
                    '"gitlab-ci|gitlab ci/cd"→"gitlab ci"; '
                    '"k8s|kubernetes"→"kubernetes"; '
                    '"ms azure|azure cloud"→"azure"; '
                    '"selenium webdriver"→"selenium".'
                ),
            },
            {  # scoring i reguły (szczegółowe)
                "role": "system",
                "content": (os.getenv("EXPECTATIONS")),
            },
            {  # wejście użytkownika
                "role": "user",
                "content": f"Pełny tekst ogłoszenia dla {job.url}:\n{job.description}",
            },
        ]

    @staticmethod
    def extract_ratings(response) -> tuple[int | None, int | None]:
        """
        Extracts the offer and candidate ratings from the AI response.
        :param response:
        :return: tuple with (offer_rating, candidate_rating)
        None if not found.
        """
        if isinstance(response, dict):
            return (
                response.get("ocena_oferty", 0),
                response.get("dopasowanie_kandydata", 0),
            )
        match = re.search(r"\[ocena_oferty=(\d+)", response)
        if match:
            return int(match.group(1)), None
        return None, None

    @staticmethod
    def clean_deepseek_response(response: str) -> dict | str:
        """
        Cleans the response from the DeepSeek model to extract the JSON part.
        :param response: Response string from the AI model.
        :return: dict with parsed JSON or cleaned string if JSON is not found.
        """
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

    def request_jobs_ai_analyze(self, all_jobs: list[JobOffer]):
        """
        Analyze job offers using the DeepSeek model.
        """
        db_access_lock = threading.Lock()

        def process_job(data: JobOffer) -> Optional[JobOffer]:
            """
            Process a single job offer by sending it to the AI model for analysis.
            :param data: JobOffer object containing job details.
            :return: JobOffer object with analysis and ratings or None if the job already exists in the database.
            """
            DB_PATH = os.getenv("DB_PATH")
            TABLE_NAME = os.getenv("TABLE_NAME")
            db = DatabaseManager(DB_PATH, TABLE_NAME)
            if db.search_jobs(data):
                logger.warning("Job already exists in the database: {}, SKIPPING", data.name)
                return None
            logger.info("Processing job: {}", data.name)
            try:
                response = self.model.chat.completions.create(
                    model="deepseek-reasoner",
                    messages=self.build_prompt(data),
                    temperature=0.0,
                    top_p=1.0,
                    max_tokens=10000,
                    response_format={"type": "json_object"},
                )
            except (AuthenticationError, RateLimitError, APIError) as e:
                logger.exception("e")
                raise e

            logger.success("Response for URL: {}", data.url)
            logger.debug(response.choices[0].message.content)
            logger.debug("Used Tokens: {}", response.usage.total_tokens)
            response_formatted = self.clean_deepseek_response(str(response.choices[0].message.content))
            offer_rating, candidate_rating = self.extract_ratings(response_formatted)
            data.analysis = str(response_formatted)
            if len(data.analysis) < 10:
                logger.warning("Analysis is too short for job: {}, SKIPPING", data)
                return None
            if offer_rating:
                data.offer_rating = offer_rating
            if candidate_rating:
                data.candidate_rating = candidate_rating
            with db_access_lock:
                db.insert_job_offer(data)
            return data

        with concurrent.futures.ThreadPoolExecutor(max_workers=global_config["MAX_REQUESTS_WORKERS"]) as executor:
            all_futures = {executor.submit(process_job, data): data for data in all_jobs}
            for future in concurrent.futures.as_completed(all_futures):
                try:
                    _ = future.result()
                except (APIError, AuthenticationError, RateLimitError) as e:
                    job = all_futures[future]
                    logger.exception("Job {} failed with exception: {}", job.name, e)

        logger.success("All jobs processed and stored in the database")
