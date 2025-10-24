"""Decorators for defining workflow stages."""
import functools
from typing import Callable, Any


class StageMetadata:
    """Metadata about a workflow stage."""
    def __init__(self, name: str):
        self.name = name
        self.func = None


# Global reference to the runner's execute_stage method
# This will be set by the runner when it loads the workflow module
_runner_execute_stage = None


def set_runner_execute_stage(execute_stage_func):
    """Set the runner's execute_stage function."""
    global _runner_execute_stage
    _runner_execute_stage = execute_stage_func


def stage(func: Callable = None, *, name: str = None):
    """
    Decorator to mark a function as a workflow stage.

    When called from within a workflow, this will route execution through
    the runner to properly track parent-child relationships using a call stack.

    Usage:
        @stage
        def extract_data():
            # Load data from source
            return data

        @stage()
        def process_data():
            # Process the data
            return result

        @stage(name="transform")
        def transform_data(data):
            # Transform the data
            return transformed_data

    Args:
        func: The function being decorated (when used without parentheses)
        name: Optional name for the stage (defaults to function name)

    Returns:
        Decorated function with stage metadata
    """
    def decorator(f: Callable) -> Callable:
        stage_name = name or f.__name__

        @functools.wraps(f)
        def wrapper(*args, **kwargs) -> Any:
            from sdk.context import get_stage_context

            # Check if we're running inside a workflow (context exists)
            context = get_stage_context()
            current_frame = context.get_current_frame()

            if current_frame and _runner_execute_stage:
                # We're inside a workflow execution - route through runner
                # This will create a child stage run with proper parent tracking
                return _runner_execute_stage(
                    workflow_id=current_frame.workflow_id,
                    stage_name=stage_name,
                    stage_func=lambda: f(*args, **kwargs)
                )
            else:
                # Not inside a workflow or runner not set - just execute directly
                return f(*args, **kwargs)

        # Attach metadata to the function
        wrapper._is_stage = True
        wrapper._stage_name = stage_name
        wrapper._original_func = f  # Keep reference to original function

        return wrapper

    # Support both @stage and @stage()
    if func is not None:
        # Called as @stage without parentheses
        return decorator(func)
    else:
        # Called as @stage() with parentheses
        return decorator
