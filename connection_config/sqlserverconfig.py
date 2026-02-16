from pydantic import BaseModel, model_validator
import logging
import os

logger = logging.getLogger(f"app.{__name__}")

class SqlServerConfig(BaseModel):
    server: str
    database: str
    user: str | None = None
    password: str | None = None
    port: int | None = 1433
    driver: str | None = "ODBC Driver 17 for SQL Server"
    trusted_connection: bool = False

    @classmethod
    def from_env(cls):
        return cls(
            server=os.getenv('SQL_SERVER_SERVER'),
            database=os.getenv('SQL_SERVER_DATABASE'),
            user=os.getenv('SQL_SERVER_USER'),
            password=os.getenv('SQL_SERVER_PASSWORD'),
            port=os.getenv('SQL_SERVER_PORT'),
            driver=os.getenv('SQL_SERVER_DRIVER'),
            trusted_connection=os.getenv('SQL_SERVER_TRUSTED_CONNECTION').strip().lower() \
                in ['1','yes', 'true']
        )

    @model_validator(mode="after")
    def validate_authentication(self):
        if self.trusted_connection:
            if (self.user and self.user.strip()) or (self.password and self.password.strip()):
                raise ValueError(
                    "For trusted_connection=True, user and password must be Blank or None."
                )
        else:
            if not self.user or not self.password:
                raise ValueError(
                    "For trusted_connection=False, both user and password are required."
                )
        return self
