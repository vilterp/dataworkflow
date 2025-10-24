"""Stage operations for DataWorkflow - business logic without controller dependencies"""
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from src.models import Stage, StageFile
from src.core import Repository


def commit_stage(
    repo: Repository,
    db,
    stage: Stage,
    message: str,
    author: str,
    author_email: str,
    target_ref: str,
    parent_hash: Optional[str] = None
) -> Tuple[str, str]:
    """
    Commit a stage to create a new commit.

    This function merges all files from the base tree with staged files,
    creates a new commit, and updates the target ref.

    Args:
        repo: Repository instance
        db: Database session
        stage: Stage to commit
        message: Commit message
        author: Author name
        author_email: Author email
        target_ref: Full ref name to update (e.g., 'refs/heads/main')
        parent_hash: Optional parent commit hash (if None, uses stage.base_ref)

    Returns:
        Tuple of (commit_hash, target_ref)

    Raises:
        ValueError: If stage has no files or stage is already committed
    """
    # Validate stage
    if stage.committed:
        raise ValueError('Stage has already been committed')

    # Get files in the stage
    files = db.query(StageFile).filter(StageFile.stage_id == stage.id).all()
    if not files:
        raise ValueError('Cannot commit an empty stage')

    # Determine parent hash
    if parent_hash is None:
        base_ref = repo.get_ref(stage.base_ref)
        parent_hash = base_ref.commit_hash if base_ref else None

    # Get all files from the base commit's tree
    base_files = _get_base_tree_files(repo, stage.base_ref)

    # Merge staged files with base files (staged files override base files)
    all_files = base_files.copy()
    for file in files:
        all_files[file.path] = file.blob_hash

    # Create tree from merged files
    tree_entries = []
    for path, blob_hash in all_files.items():
        tree_entries.append({
            'name': path,
            'type': 'blob',
            'hash': blob_hash,
            'mode': '100644'
        })

    tree = repo.create_tree(tree_entries)

    # Create commit
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message=message,
        author=author,
        author_email=author_email,
        parent_hash=parent_hash
    )

    # Update the ref to point to the new commit
    repo.create_or_update_ref(target_ref, commit.hash)

    # Mark stage as committed
    stage.committed = True
    stage.committed_at = datetime.now(timezone.utc)
    stage.commit_hash = commit.hash
    stage.committed_ref = target_ref
    db.commit()

    return commit.hash, target_ref


def _get_base_tree_files(repo: Repository, base_ref: str) -> Dict[str, str]:
    """
    Recursively get all files from a base ref's tree.

    Args:
        repo: Repository instance
        base_ref: Full ref name (e.g., 'refs/heads/main')

    Returns:
        Dictionary mapping file paths to blob hashes
    """
    base_files = {}
    ref = repo.get_ref(base_ref)

    if ref:
        base_commit = repo.get_commit(ref.commit_hash)
        if base_commit:
            def get_all_files_in_tree(tree_hash: str, prefix: str = '') -> Dict[str, str]:
                """Recursively traverse tree and collect all blob entries."""
                files_dict = {}
                entries = repo.get_tree_contents(tree_hash)

                for entry in entries:
                    full_path = f"{prefix}/{entry.name}" if prefix else entry.name

                    if entry.type.value == 'blob':
                        files_dict[full_path] = entry.hash
                    elif entry.type.value == 'tree':
                        files_dict.update(get_all_files_in_tree(entry.hash, full_path))

                return files_dict

            base_files = get_all_files_in_tree(base_commit.tree_hash)

    return base_files


def get_stage_file_statuses(
    repo: Repository,
    db,
    stage: Stage
) -> Dict[str, str]:
    """
    Determine the status of each file in a stage relative to the base branch.

    Args:
        repo: Repository instance
        db: Database session
        stage: Stage to analyze

    Returns:
        Dictionary mapping file paths to status strings:
        - 'added': File doesn't exist in base
        - 'modified': File exists in base but with different content
        - 'unchanged': File exists in base with same content
    """
    file_statuses = {}

    # Get files in the stage
    files = db.query(StageFile).filter(StageFile.stage_id == stage.id).all()

    # Get all files from base tree
    base_files = _get_base_tree_files(repo, stage.base_ref)

    # Check each staged file
    for file in files:
        if file.path in base_files:
            # File exists in base - check if modified
            if base_files[file.path] != file.blob_hash:
                file_statuses[file.path] = 'modified'
            else:
                file_statuses[file.path] = 'unchanged'
        else:
            file_statuses[file.path] = 'added'

    return file_statuses
