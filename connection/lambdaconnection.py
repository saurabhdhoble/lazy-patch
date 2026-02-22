from typing import Literal
from .connection import Connection
from models import ResponseModel
from utils import *
from connection_config import *
from dataclasses import asdict
import logging
from concurrent.futures import Future
import boto3
import json

# Create a module-level logger using the app namespace
logger = logging.getLogger(f"app.{__name__}")


class LambdaConnection(Connection):
    # Define the connection type
    connection_type: Literal["lambda"] = "lambda"


    @classmethod
    def _from_payload(cls, job_payload):
        cfg = LambdaConnection.from_env()

        return LambdaConnection(
            connection_config = cfg
        )


    def test_connection(self):
        """
        Tests Lambda connectivity by invoking the function
        with a simple ping payload.
        """
        client = None
        try:
            logger.info("Starting Lambda connection test")

            client = boto3.client(
                "lambda",
                **self.connection_config.model_dump()
            )

            logger.info("Lambda client created")

            response = client.invoke(
                FunctionName=self.connection_config.function_name,
                InvocationType="RequestResponse",
                Payload=json.dumps({"ping": True})
            )

            payload = json.loads(response["Payload"].read())

            logger.info(f"Lambda test response: {payload}")
            logger.info("Lambda connection test successful")

        except Exception as e:
            logger.error(f"Error during lambda connection test - {e}")
            raise e


    def _execute(self, payload: dict) -> ResponseModel:
        """
        Internal execution logic for Lambda invocation.
        Returns a ResponseModel (not serialized).
        """
        logger.info("Starting Lambda invocation")

        try:
            client = boto3.client(
                "lambda",
                **self.connection_config.model_dump()
            )

            logger.info("Lambda client established")

            response = client.invoke(
                FunctionName=self.connection_config.function_name,
                InvocationType="RequestResponse",  # sync execution
                Payload=json.dumps(payload)
            )

            response_payload = json.loads(
                response["Payload"].read()
            )

            logger.info("Lambda executed successfully")

            return ResponseModel(
                status="pass",
                success_text="Lambda executed successfully",
                error_text="",
                data=response_payload
            )

        except Exception as e:
            raise e


    def callback(self, future: Future) -> ResponseModel:
        """
        Callback method intended for async execution.
        Retrieves and returns the result from a Future object.
        """
        logger.info(f"Inside callback for {self.__class__.__name__}")
        return future.result()