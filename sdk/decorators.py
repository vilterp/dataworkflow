"""Decorators for defining workflow stages."""
import functools
from typing import Callable, Any


class StageMetadata:
    """Metadata about a workflow stage."""
    def __init__(self, name: str, order: int = None):
        self.name = name
        self.order = order
        self.func = None


def stage(name: str = None, order: int = None):
    """
    Decorator to mark a function as a workflow stage.

    Usage:
        @stage()
        def extract_data():
            # Load data from source
            return data

        @stage(name="transform", order=2)
        def transform_data(data):
            # Transform the data
            return transformed_data

    Args:
        name: Optional name for the stage (defaults to function name)
        order: Optional execution order (defaults to definition order)

    Returns:
        Decorated function with stage metadata
    """
    def decorator(func: Callable) -> Callable:
        stage_name = name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # The actual function execution
            return func(*args, **kwargs)

        # Attach metadata to the function
        wrapper._is_stage = True
        wrapper._stage_name = stage_name
        wrapper._stage_order = order

        return wrapper

    return decorator
