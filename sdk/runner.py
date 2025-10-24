"""Workflow runner that polls for and executes workflows."""
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
from pathlib import Path
from typing import Optional, Dict, Any, List
import traceback

logger = logging.getLogger(__name__)


class WorkflowRunner:
    """
    Runner that polls for pending workflows and executes them.

    The runner:
    1. Polls the server for pending workflows
    2. Claims a workflow
    3. Downloads the workflow file from the repository
    4. Executes each stage in order
    5. Reports results back to the server
    """

    def __init__(self, server_url: str, repo_name: str, runner_id: str = None, poll_interval: int = 5):
        """
        Initialize the workflow runner.

        Args:
            server_url: Base URL of the DataWorkflow server (e.g., "http://localhost:5001")
            repo_name: Name of the repository to run workflows for
            runner_id: Unique identifier for this runner (generated if not provided)
            poll_interval: Seconds to wait between polling for new workflows
        """
        self.server_url = server_url.rstrip('/')
        self.repo_name = repo_name
        self.runner_id = runner_id or f"runner-{uuid.uuid4().hex[:8]}"
        self.poll_interval = poll_interval
        self.running = False

    def start(self):
        """Start the runner loop."""
        logger.info(f"[{self.runner_id}] Starting workflow runner for repository: {self.repo_name}")
        logger.info(f"[{self.runner_id}] Server: {self.server_url}")
        logger.info(f"[{self.runner_id}] Poll interval: {self.poll_interval}s")

        self.running = True
        try:
            while self.running:
                try:
                    self._poll_and_execute()
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logger.error(f"[{self.runner_id}] Error in runner loop: {e}", exc_info=True)

                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            logger.info(f"[{self.runner_id}] Shutting down...")
            self.running = False

    def stop(self):
        """Stop the runner."""
        self.running = False

    def _poll_and_execute(self):
        """Poll for pending workflows and execute one if available."""
        # Get pending workflows
        workflows = self._get_pending_workflows()

        if not workflows:
            return

        # Take the first available workflow
        workflow = workflows[0]
        logger.info(f"[{self.runner_id}] Found pending workflow: {workflow['id']}")

        # Claim it
        if not self._claim_workflow(workflow['id']):
            logger.warning(f"[{self.runner_id}] Failed to claim workflow {workflow['id']}")
            return

        # Execute it
        self._execute_workflow(workflow)

    def _get_pending_workflows(self) -> List[Dict[str, Any]]:
        """Get list of pending workflows from the server."""
        try:
            response = requests.get(
                f"{self.server_url}/api/workflows/pending",
                params={'repo_name': self.repo_name},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            return data.get('workflows', [])
        except requests.RequestException as e:
            logger.error(f"[{self.runner_id}] Error fetching pending workflows: {e}")
            return []

    def _claim_workflow(self, workflow_id: int) -> bool:
        """Claim a workflow for execution."""
        try:
            response = requests.post(
                f"{self.server_url}/api/workflows/{workflow_id}/claim",
                json={'runner_id': self.runner_id},
                timeout=10
            )
            response.raise_for_status()
            logger.info(f"[{self.runner_id}] Claimed workflow {workflow_id}")
            return True
        except requests.RequestException as e:
            logger.error(f"[{self.runner_id}] Error claiming workflow: {e}")
            return False

    def _start_workflow(self, workflow_id: int):
        """Mark workflow as started."""
        try:
            response = requests.post(
                f"{self.server_url}/api/workflows/{workflow_id}/start",
                timeout=10
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"[{self.runner_id}] Error starting workflow: {e}")

    def _start_stage(self, workflow_id: int, stage_name: str, parent_stage_run_id: Optional[int] = None) -> Optional[int]:
        """
        Mark a stage as started.

        Returns:
            Stage run ID if successful, None otherwise
        """
        try:
            payload = {}
            if parent_stage_run_id is not None:
                payload['parent_stage_run_id'] = parent_stage_run_id

            response = requests.post(
                f"{self.server_url}/api/workflows/{workflow_id}/stages/{stage_name}/start",
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            return data.get('stage_run_id')
        except requests.RequestException as e:
            logger.error(f"[{self.runner_id}] Error starting stage: {e}")
            return None

    def _finish_stage(self, workflow_id: int, stage_name: str, status: str,
                     result_value: Any = None, error_message: str = None):
        """Mark a stage as finished."""
        try:
            payload = {'status': status}
            if result_value is not None:
                payload['result_value'] = result_value
            if error_message:
                payload['error_message'] = error_message

            response = requests.post(
                f"{self.server_url}/api/workflows/{workflow_id}/stages/{stage_name}/finish",
                json=payload,
                timeout=10
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"[{self.runner_id}] Error finishing stage: {e}")

    def _finish_workflow(self, workflow_id: int, status: str, error_message: str = None):
        """Mark workflow as finished."""
        try:
            payload = {'status': status}
            if error_message:
                payload['error_message'] = error_message

            response = requests.post(
                f"{self.server_url}/api/workflows/{workflow_id}/finish",
                json=payload,
                timeout=10
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"[{self.runner_id}] Error finishing workflow: {e}")

    def _download_workflow_file(self, workflow_file: str, commit_hash: str) -> Optional[str]:
        """
        Download the workflow file from the repository.

        Args:
            workflow_file: Path to the workflow file in the repo
            commit_hash: Commit hash to fetch the file from

        Returns:
            Path to the downloaded file, or None if download failed
        """
        try:
            # Use the API endpoint to get the file content
            response = requests.get(
                f"{self.server_url}/api/repos/{self.repo_name}/blob/{commit_hash}/{workflow_file}",
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
            logger.error(f"[{self.runner_id}] Error downloading workflow file: {e}")
            return None

    def _execute_workflow(self, workflow: Dict[str, Any]):
        """
        Execute a workflow.

        Args:
            workflow: Workflow metadata from the server
        """
        workflow_id = workflow['id']
        workflow_file = workflow['workflow_file']
        commit_hash = workflow['commit_hash']

        logger.info(f"[{self.runner_id}] Executing workflow {workflow_id}: {workflow_file} @ {commit_hash}")

        # Start the workflow
        self._start_workflow(workflow_id)

        temp_dir = None
        try:
            # Download the workflow file
            file_path = self._download_workflow_file(workflow_file, commit_hash)
            if not file_path:
                raise Exception("Failed to download workflow file")

            temp_dir = os.path.dirname(file_path)

            # Load the workflow module
            spec = importlib.util.spec_from_file_location("workflow_module", file_path)
            if spec is None or spec.loader is None:
                raise Exception("Failed to load workflow module")

            module = importlib.util.module_from_spec(spec)
            sys.modules['workflow_module'] = module
            spec.loader.exec_module(module)

            # Look for main() function
            if not hasattr(module, 'main'):
                raise Exception("Workflow must define a main() function")

            main_func = getattr(module, 'main')

            logger.info(f"[{self.runner_id}] Executing workflow main()")

            # Execute the main function
            self._execute_stage(workflow_id, 'main', main_func, parent_stage_run_id=None)

            # Mark workflow as completed
            self._finish_workflow(workflow_id, 'completed')
            logger.info(f"[{self.runner_id}] Workflow {workflow_id} completed successfully")

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
            logger.error(f"[{self.runner_id}] Workflow {workflow_id} failed: {e}")
            self._finish_workflow(workflow_id, 'failed', error_msg)

        finally:
            # Cleanup temporary directory
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    def _execute_stage(self, workflow_id: int, stage_name: str, stage_func: callable,
                      parent_stage_run_id: Optional[int] = None):
        """
        Execute a single stage.

        Args:
            workflow_id: ID of the workflow run
            stage_name: Name of the stage
            stage_func: Stage function to execute
            parent_stage_run_id: Optional ID of parent stage that invoked this stage
        """
        logger.info(f"[{self.runner_id}] Starting stage: {stage_name}")
        stage_run_id = self._start_stage(workflow_id, stage_name, parent_stage_run_id)

        try:
            # Execute the stage function
            result = stage_func()

            # Mark stage as completed
            self._finish_stage(workflow_id, stage_name, 'completed', result_value=result)
            logger.info(f"[{self.runner_id}] Stage {stage_name} completed")

            return stage_run_id

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
            logger.error(f"[{self.runner_id}] Stage {stage_name} failed: {e}")
            self._finish_stage(workflow_id, stage_name, 'failed', error_message=error_msg)
            raise  # Re-raise to fail the workflow
