"""Workflow and stage operations for DataWorkflow - business logic without controller dependencies"""
from typing import List
from src.core import Repository

# Re-export stage run functions from stage_runs module for backwards compatibility
from src.core.stage_runs import create_stage_run_with_entry_point, create_stage_run

__all__ = ['create_stage_run_with_entry_point', 'create_stage_run', 'find_python_files_in_tree']


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
