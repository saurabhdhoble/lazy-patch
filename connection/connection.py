from pydantic import BaseModel, PrivateAttr
from typing import Literal
from abc import ABC, abstractmethod
from models import ResponseModel
from inspect import iscoroutinefunction, unwrap
import logging
import json
from concurrent.futures import Future

logger = logging.getLogger(f"app.{__name__}")


class Connection(BaseModel, ABC):
    connection_type: Literal["sql_server", "snowflake", "http_rest", "shell"]
    connection_config: object


    def execute(self, script: str) -> str:
        """
        Wrapper method:
        - Calls internal _execute()
        - Serializes ResponseModel using model_dump()
        - Handles unexpected exceptions
        - Returns JSON string
        """
        try:
            response: ResponseModel = self._execute(script)

            # Serialize ResponseModel to JSON
            return response.model_dump_json()

        except Exception as e:
            logger.exception(
                f"Unhandled exception in {self.__class__.__name__}.execute: {str(e)}"
            )

            fallback_response = ResponseModel(
                status="fail",
                error_text=str(e)
            )

            return fallback_response.model_dump_json()
        

    @abstractmethod
    def _execute(self, script: str) -> ResponseModel:
        """
        Subclasses must implement execution logic.
        """
        pass
    

    @abstractmethod
    def callback(self, future: Future) -> ResponseModel:
        """Handle any callback housekeeping"""
        raise NotImplementedError
