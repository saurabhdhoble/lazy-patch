from typing import Literal
from .connection import Connection
from models import ResponseModel
from utils import *
from connection_config import *
from snowflake import connector
from dataclasses import asdict
import logging
from concurrent.futures import Future
import json

# Create a module-level logger using the app namespace
logger = logging.getLogger(f"app.{__name__}")


class SnowflakeConnection(Connection):
    # Define the connection type as a literal for validation/type safety
    connection_type: Literal["snowflake"] = "snowflake"
    
    # Maximum number of rows to fetch per result set (None means no limit)
    MAX_ROW_SIZE: int | None = 100


    @classmethod
    def _from_payload(cls, job_payload):
        cfg = SnowflakeConfig.from_env()

        return SnowflakeConnection(
            connection_config = cfg
        )


    def test_connection(self):
        """
        Tests the Snowflake connection by:
        1. Creating a connection
        2. Executing a simple test query (SELECT 1)
        3. Logging the results
        4. Closing the connection
        """
        conn = None
        try:
            logger.info("Starting Snowflake connection test")
            
            # Create a Snowflake connection using configuration
            conn = connector.connect(**self.connection_config.model_dump())
            logger.info("Snowflake connection created")

            # Execute a simple validation query
            results = conn.execute_string('SELECT 1 as T')

            # Iterate through result cursors and log output
            for res in results:
                logger.info(res)

            logger.info("Snowflake connection test successful")

        except Exception as e:
            # Log and re-raise any exception encountered
            logger.error(f'Error during snowflake connection test - {e}')
            raise e
        finally:
            # Ensure connection is closed even if an error occurs
            if conn:
                try:
                    conn.close()
                    logger.info("Snowflake connection closed")
                except Exception:
                    logger.warning("Failed to close Snowflake connection")


    def _execute(self, script: str) -> ResponseModel:
        """
        Internal execution logic for Snowflake SQL script.
        Returns a ResponseModel (not serialized).
        """
        logger.info("Starting Snowflake script")

        conn = None
        try:
            # Create connection
            conn = connector.connect(**self.connection_config.model_dump())
            logger.info("Snowflake connection established")

            results_payload = []

            # Execute multi-statement script
            for cur in conn.execute_string(script):

                if cur.description:
                    columns = [col[0] for col in cur.description]
                    rows = cur.fetchmany(self.MAX_ROW_SIZE)

                    results_payload.append({
                        "columns": columns,
                        "rows": rows
                    })

            logger.info("Snowflake script executed successfully")

            return ResponseModel(
                status="pass",
                success_text="Snowflake script executed successfully",
                error_text="",
                data=results_payload
            )

        except Exception as e:
            """
            Let the parent execute() wrapper handle exceptions
            """
            raise e

        finally:
            if conn:
                conn.close()
                logger.info("Snowflake connection closed")

    
    def callback(self, future: Future) -> ResponseModel:
        """
        Callback method intended for async execution.
        Retrieves and returns the result from a Future object.
        """
        logger.info(f"Inside callback for {self.__class__.__name__}")
        
        # Return the result of the asynchronous execution
        return future.result()
