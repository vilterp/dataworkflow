"""
Virtual File System abstraction that unifies git objects and derived workflow data.

This module provides a unified tree structure that combines:
- Base git objects: trees and blobs
- Derived data: StageRuns and their output files (StageFiles)

The VFS allows treating the entire repository + workflow outputs as a single
hierarchical tree structure that can be traversed uniformly.
"""
from abc import ABC, abstractmethod
from typing import Optional, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from src.core.repository import Repository
    from src.models import Blob


class VirtualTreeNode(ABC):
    """
    Abstract base class for nodes in the virtual file system tree.

    All nodes can be traversed uniformly via get_children() and
    accessed via get_content() for leaf nodes.
    """

    def __init__(self, name: str, repo: 'Repository', path: str = ""):
        """
        Initialize a virtual tree node.

        Args:
            name: Name of this node
            repo: Repository instance for lazy loading
            path: Full path from repository root (e.g., "workflows/process.py")
        """
        self.name = name
        self._repo = repo
        self.path = path

    @abstractmethod
    def get_children(self) -> List[Tuple[str, 'VirtualTreeNode']]:
        """
        Get children of this node as a list of (name, node) tuples.

        Returns:
            List of child nodes. Empty for leaf nodes.
        """
        pass

    @abstractmethod
    def get_content(self) -> Optional['Blob']:
        """
        Get the content of this node as a Blob.

        Returns:
            Blob object for file nodes, None for containers.
        """
        pass

    @property
    @abstractmethod
    def node_type_name(self) -> str:
        """
        Get the name of this node type for display.

        Returns:
            Human-readable type name (e.g., "base blob", "StageRun")
        """
        pass


class TreeNode(VirtualTreeNode):
    """A git tree node (directory)."""

    def __init__(self, name: str, repo: 'Repository', tree_hash: str, commit_hash: str, path: str = ""):
        super().__init__(name, repo, path)
        self.tree_hash = tree_hash
        self.commit_hash = commit_hash

    def get_children(self) -> List[Tuple[str, VirtualTreeNode]]:
        from src.models.tree import EntryType

        entries = self._repo.get_tree_contents(self.tree_hash)
        children = []

        for entry in entries:
            # Build child path
            child_path = f"{self.path}/{entry.name}" if self.path else entry.name

            if entry.type == EntryType.BLOB:
                child = BlobNode(
                    name=entry.name,
                    repo=self._repo,
                    blob_hash=entry.hash,
                    commit_hash=self.commit_hash,
                    path=child_path
                )
            else:  # entry.type == EntryType.TREE
                child = TreeNode(
                    name=entry.name,
                    repo=self._repo,
                    tree_hash=entry.hash,
                    commit_hash=self.commit_hash,
                    path=child_path
                )

            children.append((entry.name, child))

        return children

    def get_content(self) -> Optional['Blob']:
        # Trees don't have content
        return None

    @property
    def node_type_name(self) -> str:
        return "base tree"


class BlobNode(VirtualTreeNode):
    """
    A git blob node (file).

    Blobs can have stage runs as children if they are workflow files.
    """

    def __init__(self, name: str, repo: 'Repository', blob_hash: str, commit_hash: str, path: str = ""):
        super().__init__(name, repo, path)
        self.blob_hash = blob_hash
        self.commit_hash = commit_hash

    def get_children(self) -> List[Tuple[str, VirtualTreeNode]]:
        """
        Get stage runs attached to this workflow file.

        A blob can have stage runs as children if it's a workflow file.
        Each stage run appears as a virtual subdirectory.
        """
        # Look up stage runs for this blob (workflow file)
        # Use full path instead of just the name
        stage_runs = self._repo.get_stage_runs_for_path(
            commit_hash=self.commit_hash,
            workflow_file=self.path,  # Use full path from root
            parent_stage_run_id=None  # Only root stage runs
        )

        children = []
        for stage_run in stage_runs:
            # Build child path for stage run
            child_path = f"{self.path}/{stage_run.stage_name}"
            child = StageRunNode(
                name=stage_run.stage_name,
                repo=self._repo,
                stage_run_id=stage_run.id,
                commit_hash=self.commit_hash,
                path=child_path
            )
            children.append((stage_run.stage_name, child))

        return children

    def get_content(self) -> Optional['Blob']:
        return self._repo.get_blob(self.blob_hash)

    @property
    def node_type_name(self) -> str:
        return "base blob"


class StageRunNode(VirtualTreeNode):
    """
    A stage run node (derived directory).

    Stage runs contain stage files and can have child stage runs.
    """

    def __init__(self, name: str, repo: 'Repository', stage_run_id: str, commit_hash: str, path: str = ""):
        super().__init__(name, repo, path)
        self.stage_run_id = stage_run_id
        self.commit_hash = commit_hash

    def get_children(self) -> List[Tuple[str, VirtualTreeNode]]:
        """
        Get children for a stage run node.

        Children include:
        1. Stage files created by this stage run
        2. Child stage runs (nested stages)
        """
        from src.models import StageRun

        children = []

        # Get the stage run object
        stage_run = self._repo.db.query(StageRun).filter(
            StageRun.id == self.stage_run_id
        ).first()

        if not stage_run:
            return []

        # Add stage files as children
        for stage_file in stage_run.stage_files:
            child_path = f"{self.path}/{stage_file.file_path}"
            child = StageFileNode(
                name=stage_file.file_path,
                repo=self._repo,
                stage_file_id=stage_file.id,
                path=child_path
            )
            children.append((stage_file.file_path, child))

        # Add child stage runs as children
        for child_stage_run in stage_run.child_stage_runs:
            child_path = f"{self.path}/{child_stage_run.stage_name}"
            child = StageRunNode(
                name=child_stage_run.stage_name,
                repo=self._repo,
                stage_run_id=child_stage_run.id,
                commit_hash=self.commit_hash,
                path=child_path
            )
            children.append((child_stage_run.stage_name, child))

        return children

    def get_content(self) -> Optional['Blob']:
        # Stage runs don't have content
        return None

    @property
    def node_type_name(self) -> str:
        return "StageRun"


class StageFileNode(VirtualTreeNode):
    """A stage file node (derived file output)."""

    def __init__(self, name: str, repo: 'Repository', stage_file_id: str, path: str = ""):
        super().__init__(name, repo, path)
        self.stage_file_id = stage_file_id

    def get_children(self) -> List[Tuple[str, VirtualTreeNode]]:
        # Leaf node
        return []

    def get_content(self) -> Optional['Blob']:
        """
        Get a pseudo-Blob for the stage file.

        Stage files are not stored as git blobs, but we create a pseudo-Blob
        object with the stage file's content_hash as its hash for comparison purposes.
        """
        from src.models import StageFile, Blob

        stage_file = self._repo.db.query(StageFile).filter(
            StageFile.id == self.stage_file_id
        ).first()

        if not stage_file:
            return None

        # Create a pseudo-Blob object for comparison
        # The content_hash from the stage file acts as the blob hash
        pseudo_blob = Blob(
            repository_id=self._repo.repository_id,
            hash=stage_file.content_hash,
            size=stage_file.size,
            s3_key=stage_file.storage_key
        )
        return pseudo_blob

    @property
    def node_type_name(self) -> str:
        return "StageFile"


def get_virtual_tree_root(repo: 'Repository', commit_hash: str) -> VirtualTreeNode:
    """
    Get the root node of the virtual file system for a given commit.

    Args:
        repo: Repository instance
        commit_hash: Commit hash to create virtual tree for

    Returns:
        Root VirtualTreeNode representing the commit's tree

    Raises:
        ValueError: If commit is not found
    """
    commit = repo.get_commit(commit_hash)
    if not commit:
        raise ValueError(f"Commit {commit_hash} not found")

    return TreeNode(
        name="",  # Root has empty name
        repo=repo,
        tree_hash=commit.tree_hash,
        commit_hash=commit_hash
    )
