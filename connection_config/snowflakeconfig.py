from pydantic import BaseModel, Field
import logging
import os

logger = logging.getLogger(f"app.{__name__}")

class SnowflakeConfig(BaseModel):
    user: str
    password: str
    account: str
    warehouse: str | None = None
    database: str | None = None
    schema_name: str | None = Field(None, alias="schema")
    role: str | None = None

    @classmethod
    def from_env(cls):
        return cls(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema_name=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
        )