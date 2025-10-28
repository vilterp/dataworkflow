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
from datetime import datetime, timezone
from io import StringIO
import queue

# Import API schemas and decorators - need to add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.models.api_schemas import CallInfo, GetCallsResponse
from sdk.decorators import set_execution_context
from sdk.context import StageContext

logger = logging.getLogger(__name__)


class LogCapture:
    """
    Captures stdout/stderr and batches log lines for sending to control plane.

    This class intercepts writes to stdout/stderr, buffers them by line,
    and periodically sends batches to the control plane.
    """

    def __init__(self, server_url: str, stage_run_id: str, original_stdout, original_stderr,
                 batch_size: int = 10, flush_interval: float = 1.0):
        """
        Initialize log capture.

        Args:
            server_url: Control plane URL
            stage_run_id: ID of the stage run
            original_stdout: Original stdout stream
            original_stderr: Original stderr stream
            batch_size: Number of log lines to batch before sending
            flush_interval: Seconds to wait before flushing incomplete batch
        """
        self.server_url = server_url.rstrip('/')
        self.stage_run_id = stage_run_id
        self.original_stdout = original_stdout
        self.original_stderr = original_stderr
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.log_queue = queue.Queue()
        self.log_index = 0
        self.running = False
        self.sender_thread = None
        self.buffer = StringIO()

    def write(self, text: str):
        """Write text to capture (called by sys.stdout/stderr redirect)."""
        # Also write to original streams for debugging
        self.original_stdout.write(text)
        self.original_stdout.flush()

        # Buffer the text
        self.buffer.write(text)

        # Check for complete lines
        value = self.buffer.getvalue()
        while '\n' in value:
            line, rest = value.split('\n', 1)

            # Queue the line with timestamp
            self.log_queue.put({
                'index': self.log_index,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'content': line
            })
            self.log_index += 1

            # Update buffer
            self.buffer = StringIO()
            self.buffer.write(rest)
            value = rest

    def flush(self):
        """Flush any remaining content in buffer."""
        self.original_stdout.flush()

        # Flush any remaining partial line
        remaining = self.buffer.getvalue()
        if remaining:
            self.log_queue.put({
                'index': self.log_index,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'content': remaining
            })
            self.log_index += 1
            self.buffer = StringIO()

    def start(self):
        """Start the background sender thread."""
        self.running = True
        self.sender_thread = threading.Thread(
            target=self._send_logs_loop,
            daemon=True,
            name=f"log-sender-{self.stage_run_id[:8]}"
        )
        self.sender_thread.start()

    def stop(self):
        """Stop the sender thread and flush remaining logs."""
        self.flush()
        self.running = False
        if self.sender_thread:
            self.sender_thread.join(timeout=5.0)
        # Send any remaining logs
        self._send_batch(force=True)

    def _send_logs_loop(self):
        """Background loop that sends log batches."""
        batch = []
        last_flush = time.time()

        while self.running:
            try:
                # Try to get a log line with timeout
                try:
                    log_line = self.log_queue.get(timeout=0.1)
                    batch.append(log_line)
                except queue.Empty:
                    pass

                # Send batch if it's full or flush interval elapsed
                now = time.time()
                if len(batch) >= self.batch_size or (batch and now - last_flush >= self.flush_interval):
                    if self._send_batch_data(batch):
                        batch = []
                        last_flush = now

            except Exception as e:
                logger.error(f"Error in log sender loop: {e}", exc_info=True)

        # Send final batch
        if batch:
            self._send_batch_data(batch)

    def _send_batch(self, force: bool = False):
        """Send any queued logs immediately."""
        batch = []
        while not self.log_queue.empty():
            try:
                batch.append(self.log_queue.get_nowait())
            except queue.Empty:
                break

        if batch:
            self._send_batch_data(batch)

    def _send_batch_data(self, batch: List[dict]) -> bool:
        """Send a batch of log lines to the control plane."""
        if not batch:
            return True

        try:
            response = requests.post(
                f"{self.server_url}/api/stages/{self.stage_run_id}/logs",
                json={'logs': batch},
                timeout=10
            )
            response.raise_for_status()
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to send log batch: {e}")
            return False


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

        # Set up log capture
        log_capture = LogCapture(
            server_url=self.server_url,
            stage_run_id=invocation_id,
            original_stdout=sys.stdout,
            original_stderr=sys.stderr
        )

        try:
            # Start log capture and redirect stdout/stderr
            log_capture.start()
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = log_capture
            sys.stderr = log_capture

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

                # Create context object for file I/O
                context = StageContext(
                    control_plane_url=self.server_url,
                    stage_run_id=invocation_id,
                    repo_name=repo_name,
                    commit_hash=commit_hash
                )

                # Extract args and kwargs from the arguments dict
                args = arguments.get('args', [])
                kwargs = arguments.get('kwargs', {})

                # Inject context as first argument
                result = func(context, *args, **kwargs)

                # Mark as completed
                self._finish_call(invocation_id, 'completed', result=result)
                logger.info(f"[{self.worker_id}] ✓ {function_name}() completed successfully")

            finally:
                # Restore stdout/stderr
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                log_capture.stop()

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
            logger.error(f"[{self.worker_id}] ✗ {function_name}() failed: {e}")
            logger.error(traceback.format_exc())
            self._finish_call(invocation_id, 'failed', error=error_msg)
