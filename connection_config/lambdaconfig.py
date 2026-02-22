from pydantic import BaseModel, Field
import logging
import os

logger = logging.getLogger(f"app.{__name__}")


class LambdaConfig(BaseModel):
    region_name: str
    function_name: str = Field(..., alias="function")
    invocation_type: str = "RequestResponse"  # Default sync call
    qualifier: str | None = None  # Alias or version

    # Optional explicit credentials (not recommended in production)
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None

    @classmethod
    def from_env(cls):
        """
        Create LambdaConfig from environment variables.
        IAM role-based authentication is preferred.
        """
        logger.info("Loading Lambda configuration from environment variables")

        return cls(
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            function_name=os.getenv("LAMBDA_FUNCTION_NAME"),
            invocation_type=os.getenv("LAMBDA_INVOCATION_TYPE", "RequestResponse"),
            qualifier=os.getenv("LAMBDA_QUALIFIER"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        )