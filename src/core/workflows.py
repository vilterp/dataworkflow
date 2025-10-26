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
    Create or retrieve an initial stage run (entry point invocation) for a workflow.

    This is the primary way to dispatch a workflow. Due to content-addressable IDs,
    if an identical workflow invocation already exists, it will be returned instead
    of creating a new one.

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
        StageRun instance (the root stage) - either newly created or existing
    """
    # Serialize arguments deterministically
    args_json = json.dumps(arguments or {}, sort_keys=True, separators=(',', ':'))

    # Compute content-addressable ID
    stage_id = StageRun.compute_id(
        parent_stage_run_id=None,
        commit_hash=commit_hash,
        workflow_file=workflow_file,
        stage_name=entry_point,
        arguments=args_json
    )

    # Check if this exact invocation already exists
    existing = db.query(StageRun).filter(StageRun.id == stage_id).first()
    if existing:
        return existing

    # Create new stage run
    stage_run = StageRun(
        id=stage_id,
        parent_stage_run_id=None,  # Entry point has no parent
        arguments=args_json,
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
    parent_stage_run_id: Optional[str] = None
) -> StageRun:
    """
    Create or retrieve a stage run (call invocation).

    This is used to create follow-up stage runs when a stage function
    calls other stage functions. Due to content-addressable IDs, if an
    identical invocation already exists, it will be returned instead.

    Args:
        db: Database session
        repo_name: Repository name
        commit_hash: Commit hash the workflow is running from
        workflow_file: Path to workflow file
        stage_name: Name of the function to invoke
        arguments: Function arguments as {'args': [...], 'kwargs': {...}}
        parent_stage_run_id: Optional parent stage run ID (for call chains)

    Returns:
        StageRun instance - either newly created or existing
    """
    # Serialize arguments deterministically
    args_json = json.dumps(arguments, sort_keys=True, separators=(',', ':'))

    # Compute content-addressable ID
    stage_id = StageRun.compute_id(
        parent_stage_run_id=parent_stage_run_id,
        commit_hash=commit_hash,
        workflow_file=workflow_file,
        stage_name=stage_name,
        arguments=args_json
    )

    # Check if this exact invocation already exists
    existing = db.query(StageRun).filter(StageRun.id == stage_id).first()
    if existing:
        return existing

    # Create new stage run
    stage_run = StageRun(
        id=stage_id,
        parent_stage_run_id=parent_stage_run_id,
        arguments=args_json,
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
