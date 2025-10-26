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
import pytest
import requests

from src.models.base import Base, create_session
from src.models import Repository as RepositoryModel, StageRun, StageRunStatus, StageFile
from src.storage import FilesystemStorage
from src.core import Repository
from src.core.repository import TreeEntryInput
from src.models.tree import EntryType
from src.core.workflows import create_stage_run_with_entry_point
from sdk.worker import CallWorker


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def test_database(tmp_path):
    """Create a test database and return database URL."""
    db_path = str(tmp_path / "test.db")
    database_url = f'sqlite:///{db_path}'
    Base.metadata.create_all(create_session(database_url).bind)
    return database_url


@pytest.fixture
def test_storage(tmp_path):
    """Create a test storage backend and return storage path."""
    storage_path = str(tmp_path / "objects")
    return FilesystemStorage(base_path=storage_path), storage_path


@pytest.fixture
def test_repository(test_database, test_storage):
    """Create a test repository and return (repo, db session)."""
    storage, _ = test_storage
    db = create_session(test_database)

    repo_model = RepositoryModel(name='test-repo', description='Test repository')
    db.add(repo_model)
    db.commit()

    repo = Repository(db, storage, repo_model.id)

    yield repo, db

    db.close()


@pytest.fixture
def control_plane_server(test_database, test_storage):
    """Start Flask control plane server in background thread."""
    _, storage_path = test_storage

    from src.app import app

    # Configure app to use test database and storage
    app.config['DATABASE_URL'] = test_database
    app.config['STORAGE_BASE_PATH'] = storage_path
    app.config['TESTING'] = True
    app.config['DEBUG'] = True

    # Enable debug logging
    import logging
    logging.basicConfig(level=logging.INFO)
    app.logger.setLevel(logging.INFO)
    logging.getLogger('sdk.worker').setLevel(logging.INFO)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

    # Start server in background thread
    server_thread = threading.Thread(
        target=lambda: app.run(host='127.0.0.1', port=5555, debug=True, use_reloader=False, threaded=True),
        daemon=True
    )
    server_thread.start()

    # Wait for server to start
    wait_for_server('http://127.0.0.1:5555')
    print(f"✓ Started Flask control plane on http://127.0.0.1:5555")

    yield 'http://127.0.0.1:5555'


# ============================================================================
# Helper Functions
# ============================================================================

def wait_for_server(url, max_attempts=20):
    """Wait for server to be ready."""
    for attempt in range(max_attempts):
        print(f"  Attempt {attempt + 1}: Checking if server is up...")
        try:
            requests.get(url, timeout=0.5)
            return
        except Exception as e:
            print(f"  Attempt {attempt + 1}: Server not ready yet ({e})")
            if attempt == max_attempts - 1:
                raise Exception("Server failed to start")
            time.sleep(0.1)


def commit_file_to_repo(repo, filename, content):
    """Commit a file to the repository and return commit hash."""
    # Create blob for file
    blob = repo.create_blob(content.encode('utf-8'))

    # Create tree with the file
    tree = repo.create_tree([
        TreeEntryInput(
            name=filename,
            type=EntryType.BLOB,
            hash=blob.hash,
            mode='100644'
        )
    ])

    # Create commit
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message=f'Add {filename}',
        author='Test User',
        author_email='test@example.com'
    )

    # Create/update main branch
    repo.create_or_update_ref('refs/heads/main', commit.hash)

    print(f"✓ Created repository and committed {filename}")
    print(f"  Commit: {commit.hash[:12]}")

    return commit.hash


def commit_multiple_files_to_repo(repo, files):
    """
    Commit multiple files to the repository.

    Args:
        repo: Repository instance
        files: List of tuples (filename, content)

    Returns:
        commit hash
    """
    # Create blobs and tree entries for all files
    tree_entries = []
    for filename, content in files:
        blob = repo.create_blob(content.encode('utf-8'))
        tree_entries.append(
            TreeEntryInput(
                name=filename,
                type=EntryType.BLOB,
                hash=blob.hash,
                mode='100644'
            )
        )

    # Create tree
    tree = repo.create_tree(tree_entries)

    # Create commit
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message=f'Add {len(files)} file(s)',
        author='Test User',
        author_email='test@example.com'
    )

    # Create/update main branch
    repo.create_or_update_ref('refs/heads/main', commit.hash)

    print(f"✓ Created repository and committed {len(files)} file(s)")
    print(f"  Commit: {commit.hash[:12]}")

    return commit.hash


def run_workflow_until_complete(server_url, database_url, root_stage_id, max_iterations=50):
    """
    Run worker iterations until workflow completes.

    Args:
        server_url: Control plane URL
        database_url: Database URL
        root_stage_id: Root stage run ID to monitor
        max_iterations: Maximum worker iterations

    Returns:
        Number of iterations executed
    """
    worker = CallWorker(
        server_url=server_url,
        worker_id='test-worker',
        poll_interval=1
    )

    for iteration in range(max_iterations):
        print(f"  Worker iteration {iteration + 1}: Polling for pending calls...")

        # Check for pending calls
        calls_response = requests.get(f'{server_url}/api/calls?status=pending&limit=1')
        if calls_response.status_code == 200:
            calls = calls_response.json().get('calls', [])
            if calls:
                print(f"  Worker iteration {iteration + 1}: Found {len(calls)} pending call(s), executing...")
                worker._poll_and_execute()
            else:
                print(f"  Worker iteration {iteration + 1}: No pending calls")
        else:
            print(f"  Worker iteration {iteration + 1}: Failed to fetch calls, status {calls_response.status_code}")

        time.sleep(0.2)

        # Check if root stage is done
        db = create_session(database_url)
        root_stage_check = db.query(StageRun).filter(StageRun.id == root_stage_id).first()
        db.close()

        if root_stage_check and root_stage_check.status in [StageRunStatus.COMPLETED, StageRunStatus.FAILED]:
            print(f"✓ Workflow completed after {iteration + 1} worker iterations")
            return iteration + 1

    raise Exception(f"Workflow did not complete in {max_iterations} iterations")


def get_all_stage_descendants(db, stage_id):
    """Recursively get all descendant stage runs."""
    descendants = []
    children = db.query(StageRun).filter(StageRun.parent_stage_run_id == stage_id).all()
    for child in children:
        descendants.append(child)
        descendants.extend(get_all_stage_descendants(db, child.id))
    return descendants


# ============================================================================
# Tests
# ============================================================================

def test_workflow_execution_integration(test_database, test_storage, test_repository, control_plane_server):
    """Test complete workflow execution from commit to results."""
    repo, db = test_repository
    _, storage_path = test_storage

    try:
        # Create and commit a test workflow file
        workflow_code = '''from sdk.decorators import stage
from sdk.context import StageContext

@stage
def main(ctx: StageContext):
    """Main workflow entry point."""
    data = extract_data()
    transformed = transform_data(data)
    result = load_data(transformed)
    return {"status": "success", "count": result}

@stage
def extract_data(ctx: StageContext):
    """Extract some test data."""
    return [1, 2, 3, 4, 5]

@stage
def transform_data(ctx: StageContext, data):
    """Transform the data."""
    return [x * 2 for x in data]

@stage
def load_data(ctx: StageContext, data):
    """Load (count) the data."""
    return len(data)
'''

        commit_hash = commit_file_to_repo(repo, 'test_workflow.py', workflow_code)

        # Create a root stage run (entry point) for the workflow
        root_stage, created = create_stage_run_with_entry_point(
            repo=repo,
            db=db,
            repo_name='test-repo',
            workflow_file='test_workflow.py',
            commit_hash=commit_hash,
            entry_point='main',
            arguments=None,
            triggered_by='integration_test',
            trigger_event='test'
        )
        root_stage_id = root_stage.id
        assert created, "Expected a new stage run to be created"

        print(f"✓ Created root stage run ID: {root_stage_id}")

        # Verify the root stage run was created
        db_check = create_session(test_database)
        root_check = db_check.query(StageRun).filter(StageRun.id == root_stage_id).first()
        db_check.close()

        assert root_check is not None, "Root stage run not found"
        assert root_check.stage_name == 'main', f"Expected main stage, got {root_check.stage_name}"
        assert root_check.status == StageRunStatus.PENDING, f"Expected PENDING status, got {root_check.status}"
        assert root_check.parent_stage_run_id is None, "Root stage should have no parent"

        # Close this session - the app and worker will create their own
        db.close()

        # Run the workflow
        run_workflow_until_complete(control_plane_server, test_database, root_stage_id)

        # Check the results
        db = create_session(test_database)
        root_stage_final = db.query(StageRun).filter(StageRun.id == root_stage_id).first()

        if not root_stage_final or root_stage_final.status not in [StageRunStatus.COMPLETED, StageRunStatus.FAILED]:
            raise Exception(f"Workflow did not complete in time. Status: {root_stage_final.status if root_stage_final else 'None'}")

        print(f"✓ Workflow execution completed")

        # Verify root stage completed
        assert root_stage_final.status == StageRunStatus.COMPLETED, \
            f"Expected root stage status COMPLETED, got {root_stage_final.status}"

        # Get all descendant stage runs
        stage_runs = [root_stage_final] + get_all_stage_descendants(db, root_stage_id)

        print(f"\n✓ Workflow execution results:")
        print(f"  Root stage status: {root_stage_final.status.value}")
        print(f"  Total stage runs: {len(stage_runs)}")

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


def test_workflow_file_io_integration(test_database, test_storage, test_repository, control_plane_server):
    """Test workflow file I/O capabilities."""
    repo, db = test_repository
    _, storage_path = test_storage

    try:
        # Create workflow that reads a file and writes an output file
        workflow_code = '''from sdk.decorators import stage
from sdk.context import StageContext

@stage
def process_file(ctx: StageContext):
    """Read input file, process it, and write output."""
    # Read input file from repository
    content = ctx.read_file("input.txt")
    lines = content.strip().split("\\n")

    # Process: count words in each line
    results = []
    for i, line in enumerate(lines, 1):
        word_count = len(line.split())
        results.append(f"Line {i}: {word_count} words")

    # Write output file
    output = "\\n".join(results)
    ctx.write_file("word_counts.txt", output)

    return {
        "lines_processed": len(lines),
        "output_file": "word_counts.txt"
    }

@stage
def main(ctx: StageContext):
    """Main workflow entry point."""
    result = process_file()
    return result
'''

        # Create input data file
        input_data = """Hello world
This is a test file
It has multiple lines
Each line has different word counts"""

        # Commit both files to repository
        commit_hash = commit_multiple_files_to_repo(repo, [
            ('test_file_workflow.py', workflow_code),
            ('input.txt', input_data)
        ])

        # Create a root stage run
        root_stage, created = create_stage_run_with_entry_point(
            repo=repo,
            db=db,
            repo_name='test-repo',
            workflow_file='test_file_workflow.py',
            commit_hash=commit_hash,
            entry_point='main',
            arguments=None,
            triggered_by='file_io_test',
            trigger_event='test'
        )
        root_stage_id = root_stage.id
        assert created, "Expected a new stage run to be created"

        print(f"✓ Created root stage run ID: {root_stage_id}")

        # Close this session
        db.close()

        # Run the workflow
        run_workflow_until_complete(control_plane_server, test_database, root_stage_id)

        # Check the results
        db = create_session(test_database)
        root_stage_final = db.query(StageRun).filter(StageRun.id == root_stage_id).first()

        assert root_stage_final is not None, "Root stage not found"
        assert root_stage_final.status == StageRunStatus.COMPLETED, \
            f"Expected COMPLETED, got {root_stage_final.status}. Error: {root_stage_final.error_message}"

        print(f"✓ Workflow execution completed")

        # Get all stage runs
        stage_runs = [root_stage_final] + get_all_stage_descendants(db, root_stage_id)

        # Find the process_file stage
        process_file_stage = next((sr for sr in stage_runs if sr.stage_name == 'process_file'), None)
        assert process_file_stage is not None, "process_file stage not found"
        assert process_file_stage.status == StageRunStatus.COMPLETED, \
            f"process_file failed: {process_file_stage.error_message}"

        print(f"✓ process_file stage completed")

        # Verify files were created
        stage_files = db.query(StageFile).filter(
            StageFile.stage_run_id == process_file_stage.id
        ).all()

        assert len(stage_files) > 0, "No files were created by the stage"
        print(f"✓ Found {len(stage_files)} file(s) created by stage")

        # Verify the output file was created
        output_file = next((f for f in stage_files if f.file_path == 'word_counts.txt'), None)
        assert output_file is not None, "word_counts.txt not found in stage files"

        print(f"✓ Output file 'word_counts.txt' was created")
        print(f"  File ID: {output_file.id}")
        print(f"  Size: {output_file.size} bytes")
        print(f"  Content hash: {output_file.content_hash[:16]}...")

        # Verify file metadata
        assert output_file.size > 0, "Output file is empty"
        assert output_file.content_hash is not None, "Output file has no content hash"
        assert output_file.storage_key is not None, "Output file has no storage key"

        # Download and verify file contents
        response = requests.get(f'{control_plane_server}/api/stage-files/{output_file.id}/download')
        assert response.status_code == 200, f"Failed to download file: {response.status_code}"

        file_content = response.content.decode('utf-8')
        print(f"\n✓ Downloaded file content:")
        print(f"--- BEGIN FILE ---")
        print(file_content)
        print(f"--- END FILE ---")

        # Verify content is correct
        assert "Line 1:" in file_content, "Line 1 not found in output"
        assert "Line 2:" in file_content, "Line 2 not found in output"
        assert "Line 3:" in file_content, "Line 3 not found in output"
        assert "Line 4:" in file_content, "Line 4 not found in output"
        assert "2 words" in file_content, "Expected '2 words' in output"
        assert "5 words" in file_content, "Expected '5 words' in output"

        # Verify result contains expected data
        import json
        result = json.loads(process_file_stage.result_value)
        assert result['lines_processed'] == 4, f"Expected 4 lines processed, got {result['lines_processed']}"
        assert result['output_file'] == 'word_counts.txt', f"Unexpected output file name: {result['output_file']}"

        print(f"\n✓ File I/O integration test results:")
        print(f"  Lines processed: {result['lines_processed']}")
        print(f"  Output file: {result['output_file']}")
        print(f"  File size: {output_file.size} bytes")

        print(f"\n✅ File I/O integration test passed!")

    finally:
        db.close()
