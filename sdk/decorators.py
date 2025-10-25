"""Decorators for defining workflow stages with distributed execution."""
import functools
import time
import requests
from typing import Callable, Any, Optional
from urllib.parse import urljoin


class StageMetadata:
    """Metadata about a workflow stage."""
    def __init__(self, name: str):
        self.name = name
        self.func = None


# Global configuration for the control plane API
_control_plane_url = None
_current_invocation_id = None


def set_control_plane_url(url: str):
    """Set the control plane API base URL."""
    global _control_plane_url
    _control_plane_url = url


def set_current_invocation_id(invocation_id: Optional[str]):
    """Set the current invocation ID (for tracking caller context)."""
    global _current_invocation_id
    _current_invocation_id = invocation_id


def get_current_invocation_id() -> Optional[str]:
    """Get the current invocation ID."""
    return _current_invocation_id


def create_call(function_name: str, arguments: dict, caller_id: Optional[str] = None) -> str:
    """
    Create a new call invocation via the control plane API.

    Args:
        function_name: Name of the function to invoke
        arguments: Dictionary of arguments to pass
        caller_id: ID of the calling invocation (None for root calls)

    Returns:
        The invocation ID for the new call

    Raises:
        RuntimeError: If control plane is not configured or request fails
    """
    if not _control_plane_url:
        raise RuntimeError("Control plane URL not configured. Call set_control_plane_url() first.")

    url = urljoin(_control_plane_url, '/api/call')
    payload = {
        'caller_id': caller_id,
        'function_name': function_name,
        'arguments': arguments
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()

    data = response.json()
    return data['invocation_id']


def poll_call_status(invocation_id: str, poll_interval: float = 0.5, timeout: float = 300) -> Any:
    """
    Poll the control plane for call completion and return the result.

    Args:
        invocation_id: The invocation ID to poll
        poll_interval: Seconds between polling attempts
        timeout: Maximum seconds to wait before giving up

    Returns:
        The result value from the completed call

    Raises:
        RuntimeError: If control plane is not configured, call fails, or timeout occurs
    """
    if not _control_plane_url:
        raise RuntimeError("Control plane URL not configured. Call set_control_plane_url() first.")

    url = urljoin(_control_plane_url, f'/api/call/{invocation_id}')
    start_time = time.time()

    while True:
        if time.time() - start_time > timeout:
            raise RuntimeError(f"Timeout waiting for call {invocation_id} to complete")

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        status = data['status']

        if status == 'completed':
            return data.get('result')
        elif status == 'failed':
            error = data.get('error', 'Unknown error')
            raise RuntimeError(f"Call {invocation_id} failed: {error}")
        elif status in ['pending', 'running']:
            # Still executing, wait and poll again
            time.sleep(poll_interval)
        else:
            raise RuntimeError(f"Unknown status for call {invocation_id}: {status}")


def stage(func: Callable = None, *, name: str = None):
    """
    Decorator to mark a function as a workflow stage with distributed execution.

    When a decorated function is called, it will:
    1. Send a POST /api/call request to the control plane with the function name and args
    2. Receive an invocation ID back
    3. Poll GET /api/call/<id> until the call is complete
    4. Return the result

    This enables each stage invocation to potentially run in a separate process.

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
            # Check if control plane is configured
            if not _control_plane_url:
                # No control plane configured - execute directly (standalone mode)
                return f(*args, **kwargs)

            # Serialize arguments to JSON-compatible dict
            # For simplicity, we'll combine args and kwargs into a single dict
            arguments = {
                'args': list(args),
                'kwargs': kwargs
            }

            # Get current caller context
            caller_id = get_current_invocation_id()

            # Create the call via control plane
            invocation_id = create_call(stage_name, arguments, caller_id)

            # Set this as the current invocation for any nested calls
            previous_invocation_id = get_current_invocation_id()
            set_current_invocation_id(invocation_id)

            try:
                # Poll for completion and get result
                result = poll_call_status(invocation_id)
                return result
            finally:
                # Restore previous invocation context
                set_current_invocation_id(previous_invocation_id)

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
