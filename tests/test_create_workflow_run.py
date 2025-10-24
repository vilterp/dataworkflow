"""
Test script to create a workflow run for testing the workflow engine.

This script:
1. Creates a repository (if it doesn't exist)
2. Commits the example workflow file to the repository
3. Creates a workflow run entry in the database
"""
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.base import create_session, init_db
from src.models import Repository as RepositoryModel, WorkflowRun, WorkflowStatus
from src.storage import FilesystemStorage
from src.core.repository import Repository
from src.config import Config


def create_workflow_test():
    """Create a test workflow run."""

    # Initialize database
    database_url = Config.DATABASE_URL
    print(f"Using database: {database_url}")

    db = create_session(database_url)
    storage = FilesystemStorage(base_path='.dataworkflow/objects')

    try:
        # Get or create repository
        repo_model = db.query(RepositoryModel).filter(
            RepositoryModel.name == 'test-repo'
        ).first()

        if not repo_model:
            print("Creating test repository...")
            repo_model = RepositoryModel(
                name='test-repo',
                description='Test repository for workflows'
            )
            db.add(repo_model)
            db.commit()
            print(f"Created repository: {repo_model.name}")
        else:
            print(f"Using existing repository: {repo_model.name}")

        # Create repository instance
        repo = Repository(db, storage, repo_model.id)

        # Check if we have commits
        main_ref = repo.get_ref('refs/heads/main')

        if not main_ref:
            print("\nCreating initial commit with example workflow...")

            # Read the example workflow file
            workflow_file_path = 'examples/example_workflow.py'
            with open(workflow_file_path, 'rb') as f:
                workflow_content = f.read()

            # Create blob for the workflow file
            blob = repo.create_blob(workflow_content)
            print(f"Created blob: {blob.hash[:12]}")

            # Create tree with the workflow file
            tree = repo.create_tree([{
                'name': workflow_file_path,
                'type': 'blob',
                'hash': blob.hash,
                'mode': '100644'
            }])
            print(f"Created tree: {tree.hash[:12]}")

            # Create initial commit
            commit = repo.create_commit(
                tree_hash=tree.hash,
                message='Add example workflow',
                author='Test User',
                author_email='test@example.com'
            )
            print(f"Created commit: {commit.hash[:12]}")

            # Create main branch
            repo.create_or_update_ref('refs/heads/main', commit.hash)
            print(f"Created branch: refs/heads/main")

            commit_hash = commit.hash
        else:
            commit_hash = main_ref.commit_hash
            print(f"Using existing commit: {commit_hash[:12]}")

        # Create a workflow run
        print("\nCreating workflow run...")
        workflow_run = WorkflowRun(
            repository_id=repo_model.id,
            workflow_file='examples/example_workflow.py',
            commit_hash=commit_hash,
            status=WorkflowStatus.PENDING,
            triggered_by='test_script',
            trigger_event='manual'
        )
        db.add(workflow_run)
        db.commit()

        print(f"✓ Created workflow run ID: {workflow_run.id}")
        print(f"  Workflow file: {workflow_run.workflow_file}")
        print(f"  Commit: {workflow_run.commit_hash[:12]}")
        print(f"  Status: {workflow_run.status.value}")
        print("\nYou can now start the workflow runner to execute this workflow:")
        print(f"  python sdk/run_workflows.py --server http://localhost:5001 --repo test-repo")

    finally:
        db.close()


if __name__ == '__main__':
    create_workflow_test()
