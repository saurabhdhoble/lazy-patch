"""
Concurrent Snowflake Job Runner Script

This script creates one or more Job objects, alongwith their connections to Snowflake,
Sql Server etc., and executes them concurrently
using a ThreadPoolExecutor.

Concurrency Model:
- Uses asyncio as the entry point.
- Uses ThreadPoolExecutor to execute job runs each on its own thread
- Results and errors are logged.

Important Assumptions:
- This script blocks until all jobs complete.
"""

# Standard library imports
import os
import asyncio  # Used to run the async main entrypoint
import logging  # Logging framework for structured logs

# Internal / application-specific imports
from jobs.job import Job  # Job abstraction that encapsulates execution logic
from connection import *  # SnowflakeConnection and related connection utilities
from concurrent.futures import ThreadPoolExecutor, as_completed  # Thread-based concurrency
from connection_config import *  # SnowflakeConfig and connection configuration classes
from dotenv import load_dotenv

load_dotenv()

async def main():
    """
    Main asynchronous entry point.

    Responsibilities:
    - Define jobs to execute
    - Execute jobs concurrently using a thread pool
    - Log success or failure for each job

    Note:
    Although this function is async, it uses ThreadPoolExecutor
    because the Snowflake connector is blocking.
    """

    # Create Snowflake configuration object.
    # from_env() instantiates from environment variables using a classmethod
    snowconfig = SnowflakeConfig.from_env()
    sqlserverconfig = SqlServerConfig.from_env()

    # Define the list of jobs to execute.
    # Each Job encapsulates:
    # - A job name
    # - A Snowflake connection
    # - The SQL script to execute
    # - Metadata such as the creator
    jobs = [
        # Job
        # (
        #     job_name="My Job", 
        #     job_connection=SnowflakeConnection(connection_config=snowconfig), 
        #     execution_script="CALL PUBLIC.TEST_PROCEDURE()", 
        #     created_by="sd"
        # ),
        Job
        (
            job_name="My Sql Server Job", 
            job_connection=SqlConnection(connection_config=sqlserverconfig), 
            execution_script="SELECT TOP 10 * FROM INFORMATION_SCHEMA.TABLES", 
            created_by="sd"
        )
    ]

    # List to store Future objects returned by the executor
    futures = []

    # Create a thread pool sized to the number of jobs.
    # Each job is submitted as a separate thread.
    # This allows concurrent execution of blocking Snowflake calls.
    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:

        # Submit each job to the thread pool
        for job in jobs:
            # Submit the job's run() method for execution
            future = executor.submit(job.run)

            # Attach a callback to be executed when the future completes.
            # NOTE: job.job_callback() is invoked immediately here and its
            # return value is passed to add_done_callback().
            future.add_done_callback(job.job_callback())

            # Store future reference for tracking completion
            futures.append(future)

        # Iterate over futures as they complete (not submission order)
        for future in as_completed(futures):
            try:
                # Retrieve result of the completed job.
                # If the job raised an exception, this line will raise it.
                result = future.result()

                # Log successful job completion
                if result:
                    logger.info(f"Job Finished with result - {result}")
                else:
                    logger.info("No result returned from function callback")

            except Exception as e:
                # Log exception if job execution failed.
                # logger.exception logs full stack trace.
                # NOTE: 'result' may not be defined if exception occurred before assignment.
                logger.exception(f"Error executing job - {e}")


def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_to_file: bool = False,
    file_path: str | None = None,
) -> logging.Logger:
    """
    Configure and return a logger instance.

    Parameters:
    - name: Logger name (namespace)
    - level: Logging level (default: INFO)
    - log_to_file: Whether to log to a file
    - file_path: Path to log file if file logging is enabled

    Behavior:
    - Prevents duplicate handlers if logger already exists
    - Configures console logging
    - Optionally configures file logging
    """

    # Get or create a named logger
    logger = logging.getLogger(name)

    # Set logging level
    logger.setLevel(level)

    # Prevent duplicate handlers (important in larger applications
    # where setup_logger might be called multiple times).
    if logger.handlers:
        logger.propagate = False  # Prevent propagation to root logger
        return logger

    # Define log message format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
    )

    # Console handler for stdout logging
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Optional file handler for persistent logging
    if log_to_file and file_path:
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# Initialize application-level logger
logger = setup_logger(name='app')

# Explicitly set logging level to INFO
logger.setLevel(logging.INFO)


# Script entry point
if __name__ == '__main__':
    # Log process start
    logger.info('Starting process')

    # Run async main function
    asyncio.run(main())
