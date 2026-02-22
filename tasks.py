from celery_app import celery_app
from jobs.job import Job
from connection import Connection
from connection_config import SqlServerConfig
import logging
from dotenv import load_dotenv
import debugpy

load_dotenv()

logger = logging.getLogger("app")

@celery_app.task(bind=True)
def run_job(self, job_payload: dict):
    """
    Celery task that reconstructs and runs a Job.
    """
    # # Listen on port 5678 (or any free port)
    # debugpy.listen(("0.0.0.0", 5678))
    # print("Waiting for debugger attach...")
    # debugpy.wait_for_client()  # pauses execution until debugger attaches

    try:
        job_connection = Connection.create(job_payload=job_payload)

        # Recreate Job object
        job = Job(
            task_id=self.request.id,
            job_name=job_payload["job_name"],
            job_connection=job_connection,
            execution_script=job_payload["execution_script"],
            created_by=job_payload["created_by"],
        )

        result = job.run()

        logger.info(f"Job finished: {job.job_name}")
        return result

    except Exception as exc:
        logger.exception(f"Job failed: {job_payload['job_name']}")
        # Immediately raise exception, no retries.
        raise exc
