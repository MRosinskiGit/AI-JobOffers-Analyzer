import concurrent
import json
import os
import re
import threading

from json_repair import repair_json
from loguru import logger
from openai import OpenAI

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

    @staticmethod
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

    def request_jobs_ai_analyze(self, all_jobs: list[JobOffer]):
        """
        Analyze job offers using the DeepSeek model.
        """
        db_access_lock = threading.Lock()

        def process_job(data):
            DB_NAME = os.getenv("DB_NAME")
            TABLE_NAME = os.getenv("TABLE_NAME")
            RELPATH = os.getenv("RELPATH")
            db = DatabaseManager(DB_NAME, TABLE_NAME, RELPATH)
            if len(db.search_jobs(data)) != 0:
                logger.warning("Job already exists in the database: {}, SKIPPING", data.name)
                return
            logger.info("Processing job: {}", data.name)
            response = self.model.chat.completions.create(
                model="deepseek-reasoner",
                messages=self.build_prompt(data),
                temperature=0.0,
                top_p=1.0,
                max_tokens=10000,
                response_format={"type": "json_object"},
            )

            logger.success("Response for URL: {}", data.url)
            logger.debug(response.choices[0].message.content)
            response_formatted = self.clean_deepseek_response(str(response.choices[0].message.content))
            offer_rating, candidate_rating = self.extract_ratings(response_formatted)
            data.analysis = str(response_formatted)
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
                res = all_futures[future]
                try:
                    _ = future.result()
                except Exception as exc:
                    logger.error("Job processing generated an exception for {}: {}", res, exc)
            else:
                logger.success("All jobs processed and stored in the database")
