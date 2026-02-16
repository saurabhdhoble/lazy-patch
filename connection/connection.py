from pydantic import BaseModel, PrivateAttr
from typing import Literal
from abc import ABC, abstractmethod
from models import ResponseModel
from inspect import iscoroutinefunction, unwrap
import logging
from concurrent.futures import Future

logger = logging.getLogger(f"app.{__name__}")


class Connection(BaseModel, ABC):
    connection_type: Literal["sql_server", "snowflake", "http_rest", "shell"]
    connection_config: object

    @abstractmethod
    def execute(self, script: str) -> ResponseModel:
        """Execute the given script"""
        raise NotImplementedError
    

    @abstractmethod
    def callback(self, future: Future) -> ResponseModel:
        """Handle any callback housekeeping"""
        raise NotImplementedError
