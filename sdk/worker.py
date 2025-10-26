"""Worker that polls for and executes call invocations."""
import os
import sys
import time
import uuid
import json
import importlib.util
import requests
import tempfile
import shutil
import logging
import traceback
import threading
from typing import Optional, Any, List
from pathlib import Path

# Import API schemas and decorators - need to add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.models.api_schemas import CallInfo, GetCallsResponse
from sdk.decorators import set_execution_context

logger = logging.getLogger(__name__)


class CallWorker:
    """
    Worker that polls for pending call invocations and executes them.

    The worker:
    1. Polls the control plane for pending calls
    2. Claims a call by marking it as started
    3. Loads the workflow code from repo/commit (specified in the invocation)
    4. Executes the specified function
    5. Reports the result back to the control plane
    """

    def __init__(self, server_url: str, worker_id: str = None, poll_interval: int = 2):
        """
        Initialize the call worker.

        Args:
            server_url: Base URL of the control plane (e.g., "http://localhost:5001")
            worker_id: Unique identifier for this worker (generated if not provided)
            poll_interval: Seconds to wait between polling for new calls
        """
        self.server_url = server_url.rstrip('/')
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.poll_interval = poll_interval
        self.running = False
        self.module_cache = {}  # Cache loaded modules by (repo, commit, file)
        self.active_threads = []  # Track active execution threads

    def start(self):
        """Start the worker loop."""
        logger.info(f"[{self.worker_id}] Starting call worker")
        logger.info(f"[{self.worker_id}] Server: {self.server_url}")
        logger.info(f"[{self.worker_id}] Poll interval: {self.poll_interval}s")

        self.running = True
        try:
            while self.running:
                try:
                    self._poll_and_execute()
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logger.error(f"[{self.worker_id}] Error in worker loop: {e}", exc_info=True)

                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            logger.info(f"[{self.worker_id}] Shutting down...")
            self.running = False

    def stop(self):
        """Stop the worker."""
        self.running = False

    def _poll_and_execute(self):
        """Poll for pending calls and execute one if available."""
        # Clean up finished threads
        self.active_threads = [t for t in self.active_threads if t.is_alive()]

        # Get pending calls
        calls = self._get_pending_calls()

        if not calls:
            return

        # Take the first available call
        call = calls[0]
        invocation_id = call.invocation_id
        logger.info(f"[{self.worker_id}] Found pending call: {invocation_id[:16]}... ({call.function_name})")

        # Claim it by marking as started
        if not self._start_call(invocation_id):
            logger.warning(f"[{self.worker_id}] Failed to claim call {invocation_id[:16]}...")
            return

        # Execute it in a background thread so we can continue polling for more calls
        thread = threading.Thread(
            target=self._execute_call,
            args=(call,),
            name=f"call-{invocation_id[:8]}",
            daemon=True
        )
        thread.start()
        self.active_threads.append(thread)
        logger.info(f"[{self.worker_id}] Started execution thread for {invocation_id[:16]}... (active threads: {len(self.active_threads)})")

    def _get_pending_calls(self) -> List[CallInfo]:
        """Get list of pending calls from the control plane."""
        try:
            response = requests.get(
                f"{self.server_url}/api/calls",
                params={'status': 'pending', 'limit': 1},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            calls_response = GetCallsResponse(**data)
            return calls_response.calls
        except requests.RequestException as e:
            logger.error(f"[{self.worker_id}] Error fetching pending calls: {e}")
            return []

    def _start_call(self, invocation_id: str) -> bool:
        """Mark a call as started (claim it)."""
        try:
            response = requests.post(
                f"{self.server_url}/api/call/{invocation_id}/start",
                json={'worker_id': self.worker_id},
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"[{self.worker_id}] Claimed call {invocation_id[:16]}...")
            return True
        except requests.RequestException as e:
            logger.error(f"[{self.worker_id}] Error claiming call: {e}")
            return False

    def _finish_call(self, invocation_id: str, status: str, result: Any = None, error: str = None):
        """Mark a call as finished with result or error."""
        try:
            payload = {'status': status}
            if status == 'completed':
                payload['result'] = result
            elif status == 'failed':
                payload['error'] = error

            response = requests.post(
                f"{self.server_url}/api/call/{invocation_id}/finish",
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"[{self.worker_id}] Finished call {invocation_id[:16]}... with status: {status}")
        except requests.RequestException as e:
            logger.error(f"[{self.worker_id}] Error finishing call: {e}")

    def _download_workflow_file(self, repo_name: str, commit_hash: str, workflow_file: str) -> Optional[str]:
        """
        Download the workflow file from the repository.

        Returns:
            Path to the downloaded file, or None if download failed
        """
        try:
            # Use the API endpoint to get the file content
            response = requests.get(
                f"{self.server_url}/api/repos/{repo_name}/blob/{commit_hash}/{workflow_file}",
                timeout=30
            )
            response.raise_for_status()

            # Create a temporary file
            temp_dir = tempfile.mkdtemp(prefix='workflow_')
            file_path = os.path.join(temp_dir, os.path.basename(workflow_file))

            with open(file_path, 'wb') as f:
                f.write(response.content)

            return file_path
        except requests.RequestException as e:
            logger.error(f"[{self.worker_id}] Error downloading workflow file: {e}")
            return None

    def _load_workflow_module(self, repo_name: str, commit_hash: str, workflow_file: str):
        """Load the workflow module for the given repo/commit/file."""
        cache_key = (repo_name, commit_hash, workflow_file)

        # Check cache
        if cache_key in self.module_cache:
            logger.debug(f"[{self.worker_id}] Using cached module for {workflow_file}@{commit_hash[:8]}")
            return self.module_cache[cache_key]

        logger.info(f"[{self.worker_id}] Loading module: {repo_name}/{workflow_file}@{commit_hash[:8]}")

        # Download the workflow file
        file_path = self._download_workflow_file(repo_name, commit_hash, workflow_file)
        if not file_path:
            raise Exception("Failed to download workflow file")

        try:
            # Load the workflow module
            module_name = f"workflow_{uuid.uuid4().hex[:8]}"
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec is None or spec.loader is None:
                raise Exception("Failed to create module spec")

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            # Cache the module
            self.module_cache[cache_key] = module
            logger.info(f"[{self.worker_id}] Loaded and cached module: {workflow_file}")

            return module
        finally:
            # Clean up temp file
            if file_path and os.path.exists(file_path):
                temp_dir = os.path.dirname(file_path)
                shutil.rmtree(temp_dir, ignore_errors=True)

    def _execute_call(self, call: CallInfo):
        """
        Execute a call invocation.

        Args:
            call: Call metadata from the control plane
        """
        invocation_id = call.invocation_id
        function_name = call.function_name
        arguments = call.arguments
        repo_name = call.repo_name
        commit_hash = call.commit_hash
        workflow_file = call.workflow_file

        logger.info(f"[{self.worker_id}] Executing: {function_name}() from {workflow_file}@{commit_hash[:8]}")

        try:
            # Load the workflow module
            module = self._load_workflow_module(repo_name, commit_hash, workflow_file)

            # Get the function from the module
            if not hasattr(module, function_name):
                raise Exception(f"Function '{function_name}' not found in module")

            func = getattr(module, function_name)

            # If function is decorated with @stage, get the original unwrapped function
            if hasattr(func, '__wrapped_stage__'):
                func = func.__wrapped_stage__

            # Set execution context so nested stage calls work
            set_execution_context(
                control_plane_url=self.server_url,
                invocation_id=invocation_id,
                repo_name=repo_name,
                commit_hash=commit_hash,
                workflow_file=workflow_file
            )

            # Extract args and kwargs from the arguments dict
            args = arguments.get('args', [])
            kwargs = arguments.get('kwargs', {})

            # Execute the function
            result = func(*args, **kwargs)

            # Mark as completed
            self._finish_call(invocation_id, 'completed', result=result)
            logger.info(f"[{self.worker_id}] ✓ {function_name}() completed successfully")

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
            logger.error(f"[{self.worker_id}] ✗ {function_name}() failed: {e}")
            logger.error(traceback.format_exc())
            self._finish_call(invocation_id, 'failed', error=error_msg)
