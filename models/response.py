from pydantic import BaseModel, model_validator
from typing import Literal, Any

class ResponseModel(BaseModel):
    status: Literal["pass", "fail"]
    success_text: str | None = None
    error_text: str | None = None
    data: Any | None = None

    # @model_validator(mode="after")
    # def validate_status_fields(self):
    #     if self.status == "pass":
    #         if len(self.error_text) > 0:
    #             raise ValueError("When status is 'pass', error_text must be blank.")
    #         if len(self.success_text) == 0:
    #             raise ValueError("When status is 'pass', success_text must be provided.")

    #     if self.status == "fail":
    #         if len(self.success_text) > 0:
    #             raise ValueError("When status is 'fail', success_text must be blank.")
    #         if len(self.error_text) == 0:
    #             raise ValueError("When status is 'fail', error_text must be provided.")

    #     return self