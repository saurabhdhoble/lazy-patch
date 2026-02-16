from pydantic import BaseModel
from connection import *
import logging

logger = logging.getLogger(f"app.{__name__}")

class Job(BaseModel):
    job_name: str
    job_connection: AnyConnection
    execution_script: str
    created_by: str

    def run(self):
        self.job_connection.test_connection()
        return self.job_connection.execute(self.execution_script)
    
    def job_callback(self):
        return self.job_connection.callback
