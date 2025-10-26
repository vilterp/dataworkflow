"""Workflow and stage operations for DataWorkflow - business logic without controller dependencies"""
import json
from typing import Dict, List, Optional, Any
from src.models import StageRun, StageRunStatus
from src.core import Repository


def create_stage_run_with_entry_point(
    repo: Repository,
    db,
    repo_name: str,
    workflow_file: str,
    commit_hash: str,
    entry_point: str = "main",
    arguments: Optional[Dict[str, Any]] = None,
    triggered_by: str = "manual",
    trigger_event: str = "manual"
) -> StageRun:
    """
    Create an initial stage run (entry point invocation) for a workflow.

    This is the primary way to dispatch a workflow. It creates a root StageRun
    record to invoke the entry point function (default: main()).

    Args:
        repo: Repository instance
        db: Database session
        repo_name: Name of the repository
        workflow_file: Path to workflow file in the repo (e.g., "examples/distributed_workflow.py")
        commit_hash: Commit hash to run workflow from
        entry_point: Entry point function name (default: "main")
        arguments: Arguments to pass to entry point function as {'args': [...], 'kwargs': {...}}
        triggered_by: User or system that triggered the workflow
        trigger_event: Event type that triggered the workflow

    Returns:
        StageRun instance (the root stage)
    """
    # Create initial stage run for entry point
    stage_run = StageRun(
        parent_stage_run_id=None,  # Entry point has no parent
        arguments=json.dumps(arguments or {}),
        repo_name=repo_name,
        commit_hash=commit_hash,
        workflow_file=workflow_file,
        stage_name=entry_point,
        status=StageRunStatus.PENDING,
        triggered_by=triggered_by,
        trigger_event=trigger_event
    )
    db.add(stage_run)
    db.commit()

    return stage_run


def create_stage_run(
    db,
    repo_name: str,
    commit_hash: str,
    workflow_file: str,
    stage_name: str,
    arguments: Dict[str, Any],
    parent_stage_run_id: Optional[int] = None
) -> StageRun:
    """
    Create a new stage run (call invocation).

    This is used to create follow-up stage runs when a stage function
    calls other stage functions. The initial entry point stage run is
    created by create_stage_run_with_entry_point().

    Args:
        db: Database session
        repo_name: Repository name
        commit_hash: Commit hash the workflow is running from
        workflow_file: Path to workflow file
        stage_name: Name of the function to invoke
        arguments: Function arguments as {'args': [...], 'kwargs': {...}}
        parent_stage_run_id: Optional parent stage run ID (for call chains)

    Returns:
        StageRun instance
    """
    stage_run = StageRun(
        parent_stage_run_id=parent_stage_run_id,
        arguments=json.dumps(arguments),
        repo_name=repo_name,
        commit_hash=commit_hash,
        workflow_file=workflow_file,
        stage_name=stage_name,
        status=StageRunStatus.PENDING
    )
    db.add(stage_run)
    db.commit()

    return stage_run


def find_python_files_in_tree(repo: Repository, tree_hash: str, prefix: str = '') -> List[str]:
    """
    Recursively find all Python files in a tree.

    Args:
        repo: Repository instance
        tree_hash: Hash of the tree to search
        prefix: Path prefix for nested files

    Returns:
        List of Python file paths (e.g., ["examples/workflow.py", "main.py"])
    """
    files = []
    entries = repo.get_tree_contents(tree_hash)

    for entry in entries:
        full_path = f"{prefix}/{entry.name}" if prefix else entry.name
        if entry.type.value == 'blob' and entry.name.endswith('.py'):
            files.append(full_path)
        elif entry.type.value == 'tree':
            files.extend(find_python_files_in_tree(repo, entry.hash, full_path))

    return files
