from typing_extensions import Annotated
from typing import Union
from pydantic import Field
from .sqlconnection import SqlConnection
from .snowflakeconnection import SnowflakeConnection
from .shellconnection import ShellConnection
import logging

logger = logging.getLogger(f"app.{__name__}")


AnyConnection = Annotated[
    Union[
        SqlConnection,
        SnowflakeConnection,
        ShellConnection,
    ],
    Field(discriminator="connection_type"),
]
