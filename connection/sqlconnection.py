from typing import Literal
from .connection import Connection
from models import ResponseModel
from utils import *
from connection_config import *
import pyodbc
import logging
from concurrent.futures import Future

logger = logging.getLogger(f"app.{__name__}")


class SqlConnection(Connection):
    connection_type: Literal["sql_server"] = "sql_server"
   # Maximum number of rows to fetch per result set (None means no limit)
    MAX_ROW_SIZE: int | None = 100

    def test_connection(self):
        try:
            cfg: SqlServerConfig = self.connection_config
            if cfg.trusted_connection:
                conn_str = (
                    f"DRIVER={{{cfg.driver}}};"
                    f"SERVER={cfg.server};"
                    # f"PORT={cfg.port};"
                    f"DATABASE={cfg.database};"
                    "Trusted_Connection=yes;"
                )
            else:
                conn_str = (
                    f"DRIVER={{{cfg.driver}}};"
                    f"SERVER={cfg.server};"
                    # f"PORT={cfg.port};"
                    f"DATABASE={cfg.database};"
                    f"UID={cfg.user};"
                    f"PWD={cfg.password};"
                )
            logger.info(f"Connection string - {conn_str}")
            conn = pyodbc.connect(conn_str, timeout=5)
            cursor = conn.cursor()

            cursor.execute("SELECT 1 AS T")
            for row in cursor.fetchall():
                print(row)

            cursor.close()
            conn.close()

        except Exception as e:
            raise RuntimeError(f"SQL Server connection test failed: {e}") from e

    def execute(self, script: str) -> ResponseModel:
        """
        Executes a SQL Server script (supports multiple statements).

        Returns:
            ResponseModel containing execution status, messages,
            and query results (if any).
        """
        logger.info("Starting SQL Server script execution")

        conn = None
        try:
            cfg: SqlServerConfig = self.connection_config

            # Build connection string
            if cfg.trusted_connection:
                conn_str = (
                    f"DRIVER={{{cfg.driver}}};"
                    f"SERVER={cfg.server};"
                    f"DATABASE={cfg.database};"
                    "Trusted_Connection=yes;"
                )
            else:
                conn_str = (
                    f"DRIVER={{{cfg.driver}}};"
                    f"SERVER={cfg.server};"
                    f"DATABASE={cfg.database};"
                    f"UID={cfg.user};"
                    f"PWD={cfg.password};"
                )

            # Connect to SQL Server
            logger.info(f"Connection string - {conn_str}")
            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()
            logger.info("SQL Server connection established")

            results_payload = []

            # Execute the script and collect the results
            cursor.execute(script)

            # Only fetch results if the statement returns rows
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchmany(self.MAX_ROW_SIZE)
                results_payload.append({
                    "columns": columns,
                    "rows": rows
                })

            logger.info("SQL Server script executed successfully")

            cursor.close()
            return ResponseModel(
                status="pass",
                success_text="SQL Server script executed successfully",
                error_text="",
                data=results_payload
            )

        except Exception as e:
            logger.exception("Error executing SQL Server script")
            return ResponseModel(
                status="fail",
                success_text="",
                error_text=str(e),
                data=None
            )

        finally:
            if conn:
                try:
                    conn.close()
                    logger.info("SQL Server connection closed")
                except Exception:
                    logger.warning("Failed to close SQL Server connection")

    
    def callback(self, future: Future) -> ResponseModel:
        """
        Callback method intended for async execution.
        Retrieves and returns the result from a Future object.
        """
        logger.info(f"Inside callback for {self.__class__.__name__}")
        
        # Return the result of the asynchronous execution
        return future.result()