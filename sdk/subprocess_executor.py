#!/usr/bin/env python3
"""
Subprocess executor for stage invocations.

This script is invoked as a subprocess to execute a single stage function.
It handles:
- Loading the workflow module
- Executing the stage function
- Capturing and sending logs to the control plane
- Reporting results back to the control plane
"""

import os
import sys
import json
import importlib.util
import requests
import tempfile
import shutil
import logging
import traceback
import argparse
import uuid
from datetime import datetime, timezone
from io import StringIO
import queue
import threading
import time
from typing import Any

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from sdk.decorators import set_execution_context
from sdk.context import StageContext

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class LogCapture:
    """
    Captures stdout/stderr and batches log lines for sending to control plane.
    """

    def __init__(self, server_url: str, stage_run_id: str, original_stdout, original_stderr,
                 batch_size: int = 10, flush_interval: float = 1.0):
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

    def _send_batch_data(self, batch: list) -> bool:
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


def download_workflow_file(server_url: str, repo_name: str, commit_hash: str, workflow_file: str) -> str:
    """Download the workflow file from the control plane."""
    try:
        response = requests.get(
            f"{server_url}/api/repos/{repo_name}/blob/{commit_hash}/{workflow_file}",
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
        raise Exception(f"Error downloading workflow file: {e}")


def load_workflow_module(file_path: str):
    """Load the workflow module from file path."""
    module_name = f"workflow_{uuid.uuid4().hex[:8]}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise Exception("Failed to create module spec")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def finish_call(server_url: str, invocation_id: str, status: str, result: Any = None, error: str = None):
    """Report call completion to the control plane."""
    try:
        payload = {'status': status}
        if status == 'completed':
            payload['result'] = result
        elif status == 'failed':
            payload['error'] = error

        response = requests.post(
            f"{server_url}/api/call/{invocation_id}/finish",
            json=payload,
            timeout=10
        )
        response.raise_for_status()
        logger.info(f"Finished call {invocation_id[:16]}... with status: {status}")
    except requests.RequestException as e:
        logger.error(f"Error finishing call: {e}")
        raise


def execute_stage(
    server_url: str,
    invocation_id: str,
    function_name: str,
    arguments: dict,
    repo_name: str,
    commit_hash: str,
    workflow_file: str
):
    """Execute a stage function in this subprocess."""
    logger.info(f"Executing: {function_name}() from {workflow_file}@{commit_hash[:8]}")

    # Set up log capture
    log_capture = LogCapture(
        server_url=server_url,
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
            # Download and load the workflow module
            file_path = download_workflow_file(server_url, repo_name, commit_hash, workflow_file)
            try:
                module = load_workflow_module(file_path)
            finally:
                # Clean up temp file
                if file_path and os.path.exists(file_path):
                    temp_dir = os.path.dirname(file_path)
                    shutil.rmtree(temp_dir, ignore_errors=True)

            # Get the function from the module
            if not hasattr(module, function_name):
                raise Exception(f"Function '{function_name}' not found in module")

            func = getattr(module, function_name)

            # If function is decorated with @stage, get the original unwrapped function
            if hasattr(func, '__wrapped_stage__'):
                func = func.__wrapped_stage__

            # Set execution context so nested stage calls work
            set_execution_context(
                control_plane_url=server_url,
                invocation_id=invocation_id,
                repo_name=repo_name,
                commit_hash=commit_hash,
                workflow_file=workflow_file
            )

            # Create context object for file I/O
            context = StageContext(
                control_plane_url=server_url,
                stage_run_id=invocation_id,
                repo_name=repo_name,
                commit_hash=commit_hash
            )

            # Extract args and kwargs from the arguments dict
            args = arguments.get('args', [])
            kwargs = arguments.get('kwargs', {})

            # Execute the function
            result = func(context, *args, **kwargs)

            # Mark as completed
            finish_call(server_url, invocation_id, 'completed', result=result)
            logger.info(f"✓ {function_name}() completed successfully")

        finally:
            # Restore stdout/stderr
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            log_capture.stop()

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        logger.error(f"✗ {function_name}() failed: {e}")
        logger.error(traceback.format_exc())
        finish_call(server_url, invocation_id, 'failed', error=error_msg)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Execute a stage function in a subprocess')
    parser.add_argument('--server-url', required=True, help='Control plane URL')
    parser.add_argument('--invocation-id', required=True, help='Stage invocation ID')
    parser.add_argument('--function-name', required=True, help='Function name to execute')
    parser.add_argument('--arguments', required=True, help='JSON-encoded arguments')
    parser.add_argument('--repo-name', required=True, help='Repository name')
    parser.add_argument('--commit-hash', required=True, help='Git commit hash')
    parser.add_argument('--workflow-file', required=True, help='Workflow file path')

    args = parser.parse_args()

    # Parse arguments JSON
    try:
        arguments = json.loads(args.arguments)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse arguments JSON: {e}")
        sys.exit(1)

    # Execute the stage
    execute_stage(
        server_url=args.server_url,
        invocation_id=args.invocation_id,
        function_name=args.function_name,
        arguments=arguments,
        repo_name=args.repo_name,
        commit_hash=args.commit_hash,
        workflow_file=args.workflow_file
    )


if __name__ == '__main__':
    main()
