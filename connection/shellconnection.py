from typing import Literal
from .connection import Connection
from models import ResponseModel
from utils import *
import logging
from concurrent.futures import Future

logger = logging.getLogger(f"app.{__name__}")


class ShellConnection(Connection):
    connection_type: Literal["shell"] = "shell"
    server_name: str

    def execute(self, script: str) -> ResponseModel:
        logger.info(f"Executing shell script on {self.server_name}: {script}")
        return ResponseModel(status="pass", success_text="Hello Shell Is Done", error_text="")
    
    def callback(self, future: Future) -> ResponseModel:
        logger.info(f"Inside callback for {self.__class__.__name__}")
        try:
            res = future.result()
            return res
        except Exception as exc:
            logger.error(f"Exception raised in callback {self.__class__.__name__}")
            
