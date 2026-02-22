from pydantic import BaseModel, PrivateAttr
from typing import Literal, ClassVar, Dict, Type
from abc import ABC, abstractmethod
from models import ResponseModel
from inspect import iscoroutinefunction, unwrap
import logging
import json
from concurrent.futures import Future

logger = logging.getLogger(f"app.{__name__}")


class Connection(BaseModel, ABC):
    connection_type: Literal["sql_server", "snowflake", "lambda"]
    connection_config: object
    _registry: ClassVar[Dict[str, Type['Connection']]] = {}


    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if hasattr(cls, 'connection_type'):
            if not issubclass(cls, Connection):
                raise TypeError(
                    f"Cannot register {cls.__name__} - not a Connection subclass"
                )
            Connection._registry[cls.connection_type] = cls


    @classmethod
    def create(cls, job_payload: dict):
        connection_type = job_payload.get('connection_type')
        if connection_type not in cls._registry:
            raise ValueError(f"Unsupported connection_type: {connection_type}")

        # get the subclass and let it build itself
        return cls._registry[connection_type]._from_payload(job_payload)
    

    @classmethod
    @abstractmethod
    def _from_payload(cls, job_payload: dict):
        """
        Each subclass is responsible for:
        - building its config
        - returning a fully constructed instance
        """
        raise NotImplementedError


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
