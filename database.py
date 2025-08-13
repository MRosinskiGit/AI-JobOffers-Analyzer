import sqlite3

from loguru import logger

from src_common.common_utils import JobOffer


class DatabaseManager:
    def __init__(self, db_file, table_name):
        logger.info("Connecting to database {} with table name {}", db_file, table_name)
        self.table_name = table_name
        self.db_file = db_file
        """Initialize the DatabaseManager with the database file."""
        self.conn = sqlite3.connect(self.db_file)
        self.create_jobs_database()
        logger.success(f"Database connected: {self.db_file}")

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
            logger.debug("Search results: {}", results)
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
