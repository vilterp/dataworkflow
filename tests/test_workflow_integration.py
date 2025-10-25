"""
Integration test for workflow execution.

This test:
1. Sets up a repository with the core API
2. Commits a workflow file
3. Creates a workflow run with entry point stage run
4. Spins up an in-process Flask server (control plane)
5. Runs a worker to execute the workflow
6. Checks the results
"""
import tempfile
import threading
import time
from pathlib import Path

from src.models.base import Base, create_session
from src.models import Repository as RepositoryModel, WorkflowRun, StageRun, WorkflowStatus, StageRunStatus
from src.storage import FilesystemStorage
from src.core import Repository
from src.core.stage_operations import create_workflow_run_with_entry_point
from sdk.worker import CallWorker


def test_workflow_execution_integration(tmp_path):
    """Test complete workflow execution from commit to results."""

    # Setup storage and database path
    db_path = str(tmp_path / "test.db")
    database_url = f'sqlite:///{db_path}'
    storage_path = str(tmp_path / "objects")

    # Initialize database
    Base.metadata.create_all(create_session(database_url).bind)

    # Create session
    db = create_session(database_url)
    storage = FilesystemStorage(base_path=storage_path)

    try:
        # 1. Create repository
        repo_model = RepositoryModel(name='test-repo', description='Test repository')
        db.add(repo_model)
        db.commit()

        repo = Repository(db, storage, repo_model.id)

        # 2. Create and commit a test workflow file
        workflow_code = '''from sdk.decorators import stage

@stage
def main():
    """Main workflow entry point."""
    data = extract_data()
    transformed = transform_data(data)
    result = load_data(transformed)
    return {"status": "success", "count": result}

@stage
def extract_data():
    """Extract some test data."""
    return [1, 2, 3, 4, 5]

@stage
def transform_data(data):
    """Transform the data."""
    return [x * 2 for x in data]

@stage
def load_data(data):
    """Load (count) the data."""
    return len(data)
'''

        # Create blob for workflow file
        workflow_blob = repo.create_blob(workflow_code.encode('utf-8'))

        # Create tree with the workflow file
        tree = repo.create_tree([{
            'name': 'test_workflow.py',
            'type': 'blob',
            'hash': workflow_blob.hash,
            'mode': '100644'
        }])

        # Create commit
        commit = repo.create_commit(
            tree_hash=tree.hash,
            message='Add test workflow',
            author='Test User',
            author_email='test@example.com'
        )

        # Create main branch
        repo.create_or_update_ref('refs/heads/main', commit.hash)

        print(f"✓ Created repository and committed workflow")
        print(f"  Commit: {commit.hash[:12]}")

        # 3. Create a workflow run with entry point stage run using the new function
        workflow_run = create_workflow_run_with_entry_point(
            repo=repo,
            db=db,
            repo_name='test-repo',
            workflow_file='test_workflow.py',
            commit_hash=commit.hash,
            entry_point='main',
            arguments=None,
            triggered_by='integration_test',
            trigger_event='test'
        )
        workflow_run_id = workflow_run.id

        print(f"✓ Created workflow run ID: {workflow_run_id}")

        # Verify the initial stage run was created
        initial_stage_runs = db.query(StageRun).filter(
            StageRun.workflow_run_id == workflow_run_id
        ).all()
        print(f"✓ Initial stage runs created: {len(initial_stage_runs)}")
        assert len(initial_stage_runs) == 1, f"Expected 1 initial stage run, got {len(initial_stage_runs)}"
        assert initial_stage_runs[0].stage_name == 'main', f"Expected main stage, got {initial_stage_runs[0].stage_name}"
        assert initial_stage_runs[0].status == StageRunStatus.PENDING, f"Expected PENDING status, got {initial_stage_runs[0].status}"

        # Close this session - the app and worker will create their own
        db.close()

        # 4. Start Flask server (control plane) in a background thread
        from src.app import app

        # Configure app to use our test database and storage
        app.config['DATABASE_URL'] = database_url
        app.config['STORAGE_BASE_PATH'] = storage_path
        app.config['TESTING'] = True
        app.config['DEBUG'] = False

        server_thread = threading.Thread(
            target=lambda: app.run(host='127.0.0.1', port=5555, debug=False, use_reloader=False, threaded=True),
            daemon=True
        )
        server_thread.start()

        # Wait for server to start
        max_attempts = 20
        for attempt in range(max_attempts):
            print(f"  Attempt {attempt + 1}: Checking if server is up...")
            try:
                import requests
                requests.get('http://127.0.0.1:5555', timeout=0.5)
                print(f"✓ Started Flask control plane on http://127.0.0.1:5555")
                break
            except Exception as e:
                print(f"  Attempt {attempt + 1}: Server not ready yet ({e})")
                if attempt == max_attempts - 1:
                    raise Exception("Server failed to start")
                time.sleep(0.1)

        # 5. Run the worker to execute pending calls
        worker = CallWorker(
            server_url='http://127.0.0.1:5555',
            worker_id='test-worker',
            poll_interval=1
        )

        # Execute pending calls in a loop until workflow completes
        # We'll poll for up to 10 seconds
        max_worker_iterations = 50  # 50 * 0.2s = 10 seconds max
        for iteration in range(max_worker_iterations):
            print(f"  Worker iteration {iteration + 1}: Polling for pending calls...")
            # Poll and execute one pending call
            calls_response = requests.get('http://127.0.0.1:5555/api/calls?status=pending&limit=1')
            if calls_response.status_code == 200:
                calls = calls_response.json().get('calls', [])
                if calls:
                    print(f"  Worker iteration {iteration + 1}: Found {len(calls)} pending call(s), executing...")
                    # Let worker execute the call
                    worker._poll_and_execute()
                else:
                    print(f"  Worker iteration {iteration + 1}: No pending calls")
            else:
                print(f"  Worker iteration {iteration + 1}: Failed to fetch calls, status {calls_response.status_code}")
            time.sleep(0.2)

            # Check if workflow is done
            db = create_session(database_url)
            workflow_run = db.query(WorkflowRun).filter(WorkflowRun.id == workflow_run_id).first()
            db.close()

            if workflow_run and workflow_run.status in [WorkflowStatus.COMPLETED, WorkflowStatus.FAILED]:
                print(f"✓ Workflow completed after {iteration + 1} worker iterations")
                break

        # 6. Check the results
        db = create_session(database_url)
        workflow_run = db.query(WorkflowRun).filter(WorkflowRun.id == workflow_run_id).first()

        if not workflow_run or workflow_run.status not in [WorkflowStatus.COMPLETED, WorkflowStatus.FAILED]:
            raise Exception(f"Workflow did not complete in time. Status: {workflow_run.status if workflow_run else 'None'}")

        print(f"✓ Workflow execution completed")

        # Verify workflow run completed
        assert workflow_run.status == WorkflowStatus.COMPLETED, \
            f"Expected workflow status COMPLETED, got {workflow_run.status}"

        # Get all stage runs
        stage_runs = db.query(StageRun).filter(
            StageRun.workflow_run_id == workflow_run_id
        ).order_by(StageRun.id).all()

        print(f"\n✓ Workflow execution results:")
        print(f"  Workflow status: {workflow_run.status.value}")
        print(f"  Stage runs: {len(stage_runs)}")

        # Verify we have the expected stages
        stage_names = [sr.stage_name for sr in stage_runs]
        assert 'main' in stage_names, "Missing main stage"
        assert 'extract_data' in stage_names, "Missing extract_data stage"
        assert 'transform_data' in stage_names, "Missing transform_data stage"
        assert 'load_data' in stage_names, "Missing load_data stage"

        # Verify all stages completed
        for stage_run in stage_runs:
            assert stage_run.status == StageRunStatus.COMPLETED, \
                f"Stage {stage_run.stage_name} status: {stage_run.status}, error: {stage_run.error_message}"
            print(f"    - {stage_run.stage_name}: {stage_run.status.value}, result: {stage_run.result_value}")

        # Verify parent-child relationships
        main_stage = next(sr for sr in stage_runs if sr.stage_name == 'main')
        print(f"\n  Main stage ID: {main_stage.id}")
        print(f"  Stage parent_stage_run_ids:")
        for sr in stage_runs:
            print(f"    - {sr.stage_name}: parent={sr.parent_stage_run_id}")

        # Child stages are called by main, so they should have main as parent
        child_stages = [sr for sr in stage_runs if sr.parent_stage_run_id == main_stage.id]

        assert len(child_stages) == 3, \
            f"Expected 3 child stages for main, got {len(child_stages)}"

        child_stage_names = {sr.stage_name for sr in child_stages}
        assert child_stage_names == {'extract_data', 'transform_data', 'load_data'}, \
            f"Unexpected child stages: {child_stage_names}"

        print(f"\n✓ Parent-child relationships verified:")
        print(f"    main (ID: {main_stage.id}) has {len(child_stages)} children")
        for child in child_stages:
            print(f"      - {child.stage_name} (ID: {child.id}, parent: {child.parent_stage_run_id})")

        # Verify final result
        main_result_str = main_stage.result_value
        assert main_result_str is not None, "Main stage should have a result"
        assert 'success' in main_result_str, f"Expected success in result, got: {main_result_str}"
        assert 'count' in main_result_str, f"Expected count in result, got: {main_result_str}"

        print(f"\n✓ Final result: {main_result_str}")
        print(f"\n✅ Integration test passed!")

    finally:
        db.close()
