"""Decorators for defining workflow stages."""
import functools
from typing import Callable, Any


class StageMetadata:
    """Metadata about a workflow stage."""
    def __init__(self, name: str, order: int = None):
        self.name = name
        self.order = order
        self.func = None


def stage(func: Callable = None, *, name: str = None, order: int = None):
    """
    Decorator to mark a function as a workflow stage.

    Usage:
        @stage
        def extract_data():
            # Load data from source
            return data

        @stage()
        def process_data():
            # Process the data
            return result

        @stage(name="transform", order=2)
        def transform_data(data):
            # Transform the data
            return transformed_data

    Args:
        func: The function being decorated (when used without parentheses)
        name: Optional name for the stage (defaults to function name)
        order: Optional execution order (defaults to definition order)

    Returns:
        Decorated function with stage metadata
    """
    def decorator(f: Callable) -> Callable:
        stage_name = name or f.__name__

        @functools.wraps(f)
        def wrapper(*args, **kwargs) -> Any:
            # The actual function execution
            return f(*args, **kwargs)

        # Attach metadata to the function
        wrapper._is_stage = True
        wrapper._stage_name = stage_name
        wrapper._stage_order = order

        return wrapper

    # Support both @stage and @stage()
    if func is not None:
        # Called as @stage without parentheses
        return decorator(func)
    else:
        # Called as @stage() with parentheses
        return decorator
