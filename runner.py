from tasks import run_job
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger("app")

def main():

    jobs = [
        {
            "job_name": "My Lambda Job",
            "connection_type": "snowflake",
            "execution_script": "select top 10 * from information_schema.tables",
            "created_by": "sd"
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
