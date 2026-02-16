from functools import wraps
from models import *

def enforce_responsemodel(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if not isinstance(result, ResponseModel):
            raise TypeError(f'Invalid return type for function call {func.__qualname__} - it should return a ResponseModel')
        return result
    return wrapper
