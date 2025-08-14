import datetime
import os.path
import sqlite3

from loguru import logger

from src_common.common_utils import JobOffer


class DatabaseManager:
    def __init__(self, db_file, table_name, rel_path="."):
        logger.info("Connecting to database {} with table name {}", db_file, table_name)
        self.table_name = table_name
        if db_path := os.getenv("DB_PATH"):
            self.db_path = os.path.join(db_path, db_file)
        else:
            self.db_path = os.path.join(rel_path, db_file)
        """Initialize the DatabaseManager with the database file."""
        logger.debug("Database path: {}", self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.create_jobs_database()
        logger.success(f"Database connected: {self.db_path}")

    @logger.catch(reraise=True)
    def create_jobs_database(self):
        """Create a table in the database."""
        try:
            sql_create_jobs_table = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                name TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                description TEXT,
                analysis TEXT,
                offer_rating INTEGER DEFAULT 0,
                candidate_rating INTEGER DEFAULT 0,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor = self.conn.cursor()
            cursor.execute(sql_create_jobs_table)
        except sqlite3.Error as e:
            logger.error(f"Error creating table: {e}")

    def insert_job_offer(self, job_offer: JobOffer):
        """Insert a job into the jobs table."""
        logger.info("Inserting job with URL: {}", job_offer.url)
        sql_insert_job = f"""
        INSERT INTO {self.table_name} (source, name, url, description, analysis, offer_rating, candidate_rating)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        try:
            logger.info("Inserting job with URL: {}", job_offer.url)
            cursor = self.conn.cursor()
            cursor.execute(
                sql_insert_job,
                (
                    job_offer.source,
                    job_offer.name,
                    job_offer.url,
                    job_offer.description,
                    job_offer.analysis,
                    job_offer.offer_rating,
                    job_offer.candidate_rating,
                ),
            )
            self.conn.commit()
            logger.success("Job inserted successfully.")
        except sqlite3.Error as e:
            logger.error(f"Error inserting job: {e}")

    @logger.catch(reraise=True)
    def search_jobs(self, job_offer: JobOffer):
        """Search for jobs in the jobs table by name or description."""
        sql_search_jobs = f"""
        SELECT * FROM {self.table_name}
        WHERE url LIKE ?
        """
        logger.info("Looking for job with URL: {}", job_offer.url)
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_search_jobs, ("%" + job_offer.url + "%",))
            results = cursor.fetchall()
            logger.debug("Found matching results: {}", len(results))
            logger.trace("Search results: {}", results)
            return results
        except sqlite3.Error as e:
            logger.error(f"Error searching jobs: {e}")
            return None

    @logger.catch(reraise=True)
    def delete_database(self):
        """Delete the entire jobs table."""
        sql_delete_table = f"""
        DROP TABLE IF EXISTS {self.table_name};
        """
        try:
            response = input("Confirm deletion of the jobs table by typing 'y'...")
            if response.lower() != "y":
                logger.warning("Deletion cancelled.")
                return
            cursor = self.conn.cursor()
            cursor.execute(sql_delete_table)
            logger.success("Jobs table deleted successfully.")
        except sqlite3.Error as e:
            logger.error(f"Error deleting jobs table: {e}")

    from datetime import date, timedelta

    def extract_jobs_for_a_date(self, day: date):
        start = f"{day.isoformat()} 00:00:00"
        end = f"{(day + datetime.timedelta(days=1)).isoformat()} 00:00:00"

        sql = """
        SELECT id, source, name, url, description, analysis, offer_rating, candidate_rating, added_date
        FROM job_offers_api
        WHERE added_date >= ?
          AND added_date < ? order by candidate_rating DESC;
        """
        cur = self.conn.cursor()
        cur.execute(sql, (start, end))
        rows = cur.fetchall()
        logger.info("Extracted %d jobs for date: %s", len(rows), day)
        return rows

    def generate_report_html(self, jobs: list[tuple], output_path="./reports") -> bool:
        os.makedirs(output_path, exist_ok=True)
        report_file = os.path.join(output_path, f"report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
        try:
            with open(report_file, "w", encoding="utf-8") as f:
                f.write("<html><head><title>Job Offers Report</title></head><body>")
                f.write("<h1>Job Offers Report</h1>")
                f.write("<table border='1'>")
                f.write(
                    "<tr><th>ID</th><th>Source</th><th>Name</th><th>URL</th>"
                    "<th>Description</th><th>Analysis</th><th>Offer Rating</th>"
                    "<th>Candidate Rating</th><th>Added Date</th></tr>"
                )

                for job in jobs:
                    f.write("<tr>")
                    for idx, item in enumerate(job):
                        if idx == 3:  # Kolumna URL
                            f.write(f"<td><a href='{item}' target='_blank'>{item}</a></td>")
                        else:
                            f.write(f"<td>{item}</td>")

                f.write("</table></body></html>")
            logger.success(f"Report generated successfully: {report_file}")
            return True
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return False
