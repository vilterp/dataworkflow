"""Decorators for defining workflow stages with distributed execution."""
import functools
import time
import requests
from typing import Callable, Any, Optional
from urllib.parse import urljoin
import threading
import os


# Global execution context set by the runner
_execution_context = threading.local()


def set_execution_context(control_plane_url: str, invocation_id: Optional[str] = None,
                         repo_name: Optional[str] = None, commit_hash: Optional[str] = None,
                         workflow_file: Optional[str] = None):
    """
    Set the execution context for the current thread.

    Called by the runner before executing workflow code.

    Args:
        control_plane_url: URL of the control plane
        invocation_id: Current invocation ID (if executing within a stage)
        repo_name: Repository name for new calls
        commit_hash: Commit hash for new calls
        workflow_file: Workflow file path for new calls
    """
    _execution_context.control_plane_url = control_plane_url
    _execution_context.invocation_id = invocation_id
    _execution_context.repo_name = repo_name
    _execution_context.commit_hash = commit_hash
    _execution_context.workflow_file = workflow_file


def get_execution_context():
    """Get the current execution context."""
    return {
        'control_plane_url': getattr(_execution_context, 'control_plane_url', os.getenv('WORKFLOW_CONTROL_PLANE_URL', 'http://localhost:5001')),
        'invocation_id': getattr(_execution_context, 'invocation_id', None),
        'repo_name': getattr(_execution_context, 'repo_name', None),
        'commit_hash': getattr(_execution_context, 'commit_hash', None),
        'workflow_file': getattr(_execution_context, 'workflow_file', None),
    }


def _create_call(function_name: str, arguments: dict) -> str:
    """
    Create a new call invocation via the control plane API.

    Args:
        function_name: Name of the function to invoke
        arguments: Dictionary of arguments to pass

    Returns:
        The invocation ID for the new call

    Raises:
        RuntimeError: If request fails
    """
    ctx = get_execution_context()

    url = urljoin(ctx['control_plane_url'], '/api/call')
    payload = {
        'caller_id': ctx['invocation_id'],
        'function_name': function_name,
        'arguments': arguments,
        'repo_name': ctx['repo_name'],
        'commit_hash': ctx['commit_hash'],
        'workflow_file': ctx['workflow_file']
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()

    data = response.json()
    return data['invocation_id']


def _poll_call_status(invocation_id: str, poll_interval: float = 0.5, timeout: float = 300) -> Any:
    """
    Poll the control plane for call completion and return the result.

    Args:
        invocation_id: The invocation ID to poll
        poll_interval: Seconds between polls
        timeout: Maximum seconds to wait

    Returns:
        The result value from the completed call

    Raises:
        TimeoutError: If timeout is exceeded
        RuntimeError: If the call fails
    """
    ctx = get_execution_context()
    url = urljoin(ctx['control_plane_url'], f'/api/call/{invocation_id}')

    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise TimeoutError(f"Call {invocation_id} timed out after {timeout}s")

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        status = data.get('status')

        if status == 'completed':
            result = data.get('result')
            return result
        elif status == 'failed':
            error = data.get('error', 'Unknown error')
            raise RuntimeError(f"Call {invocation_id} failed: {error}")

        # Still pending or running, wait and poll again
        time.sleep(poll_interval)


def stage(func: Callable) -> Callable:
    """
    Decorator for workflow stages that execute via distributed control plane.

    When a decorated function is called, instead of executing locally:
    1. Creates a call via the control plane API
    2. Polls for completion
    3. Returns the result

    Usage:
        @stage
        def extract_data():
            return [1, 2, 3]

        @stage
        def main():
            data = extract_data()  # Executes via control plane
            return data
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        stage_name = func.__name__

        # Package arguments for the API
        arguments = {
            'args': list(args),
            'kwargs': kwargs
        }

        # Create the call
        invocation_id = _create_call(stage_name, arguments)

        # Poll for result
        result = _poll_call_status(invocation_id)

        return result

    # Store the original function so the runner can execute it
    wrapper.__wrapped_stage__ = func

    return wrapper
