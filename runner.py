from tasks import run_job
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger("app")

def main():

    jobs = [
        {
            "job_name": "My Sql Server Job",
            "execution_script": "SELECT TOP 10 * FROM INFORMATION_SCHEMA.TABLES",
            "created_by": "sd",
        }
    ]

    results = []

    for job in jobs:
        async_result = run_job.delay(job)
        results.append(async_result)

    logger.info("Jobs dispatched")

    # Optional: Wait for results (blocking)
    for result in results:
        logger.info(result.get())  # Blocks until finished


if __name__ == "__main__":
    main()
