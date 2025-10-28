"""
Virtual File System diff implementation.

Provides streaming diff generation between two VFS trees, yielding events for
added, removed, and modified nodes. Works with both base git objects and
derived workflow data.
"""
from dataclasses import dataclass
from typing import Generator, Optional, TYPE_CHECKING, List
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from src.core.vfs import VirtualTreeNode
    from src.core.repository import Repository
    from src.models import Blob


# ============================================================================
# Path Segment Classes
# ============================================================================

@dataclass
class PathSegment(ABC):
    """Base class for path segments in a VFS diff path."""
    name: str  # Name of this path segment

    @property
    @abstractmethod
    def segment_type(self) -> str:
        """Get the type of this segment (tree, stagerun, file)."""
        pass


@dataclass
class TreeSegment(PathSegment):
    """A normal tree/directory path segment (base git data)."""

    @property
    def segment_type(self) -> str:
        return "tree"


@dataclass
class StageRunSegment(PathSegment):
    """A stage run path segment (derived data)."""
    status: str  # Status of the stage run (COMPLETED, FAILED, etc.)

    @property
    def segment_type(self) -> str:
        return "stagerun"


@dataclass
class FileSegment(PathSegment):
    """A file path segment (final segment in the path)."""
    is_derived: bool  # True if this is a derived file (stage output)

    @property
    def segment_type(self) -> str:
        return "file"


# ============================================================================
# Diff Event Classes
# ============================================================================

@dataclass
class DiffEvent(ABC):
    """
    Base class for diff events.

    A diff event represents a change between two trees at a specific path.
    The path is represented as a list of segments to distinguish between
    tree nodes (base data) and stage runs (derived data).
    """
    path: List[PathSegment]  # Path segments from root to the changed node

    @property
    @abstractmethod
    def event_type(self) -> str:
        """Get the type of this event (added, removed, modified)."""
        pass


@dataclass
class AddedEvent(DiffEvent):
    """
    Event for a node that was added in the new tree.
    """
    node: 'VirtualTreeNode'  # The newly added node
    after_blob: Optional['Blob'] = None  # Content of the added node (if it's a file)

    @property
    def event_type(self) -> str:
        return "added"


@dataclass
class RemovedEvent(DiffEvent):
    """
    Event for a node that was removed from the old tree.
    """
    node: 'VirtualTreeNode'  # The removed node
    before_blob: Optional['Blob'] = None  # Content of the removed node (if it was a file)

    @property
    def event_type(self) -> str:
        return "removed"


@dataclass
class ModifiedEvent(DiffEvent):
    """
    Event for a node that was modified between old and new trees.
    """
    old_node: 'VirtualTreeNode'  # The node in the old tree
    new_node: 'VirtualTreeNode'  # The node in the new tree
    before_blob: Optional['Blob'] = None  # Content before modification
    after_blob: Optional['Blob'] = None   # Content after modification

    @property
    def event_type(self) -> str:
        return "modified"


def diff_trees(
    old_root: 'VirtualTreeNode',
    new_root: 'VirtualTreeNode',
    path_prefix: List[PathSegment] = None
) -> Generator[DiffEvent, None, None]:
    """
    Generate diff events between two VFS trees.

    This performs a streaming comparison of two virtual file system trees,
    yielding events for added, removed, and modified nodes. It recursively
    traverses both trees in parallel, comparing nodes at each level.

    The diff treats all node types uniformly - base git objects (trees/blobs)
    and derived data (stage runs/stage files) are diffed the same way.

    Args:
        old_root: Root of the old tree (base ref)
        new_root: Root of the new tree (comparison ref)
        path_prefix: Current path segments for recursive calls

    Yields:
        DiffEvent instances (AddedEvent, RemovedEvent, ModifiedEvent)

    Example:
        >>> for event in diff_trees(old_root, new_root):
        ...     if isinstance(event, AddedEvent):
        ...         print(f"Added: {'/'.join(seg.name for seg in event.path)}")
    """
    from src.core.vfs import TreeNode, BlobNode, StageRunNode, StageFileNode

    if path_prefix is None:
        path_prefix = []

    # Get children from both trees
    old_children = {name: node for name, node in old_root.get_children()}
    new_children = {name: node for name, node in new_root.get_children()}

    # Find all unique child names
    all_names = set(old_children.keys()) | set(new_children.keys())

    # Process each child name in sorted order for deterministic output
    for name in sorted(all_names):
        old_child = old_children.get(name)
        new_child = new_children.get(name)

        # Determine the node type and create appropriate path segment
        # Use new_child if available, otherwise old_child
        node = new_child if new_child is not None else old_child

        # Create path segment based on node type
        if isinstance(node, TreeNode):
            segment = TreeSegment(name=name)
        elif isinstance(node, StageRunNode):
            # Get status from the stage run
            from src.models import StageRun
            stage_run = node._repo.db.query(StageRun).filter(
                StageRun.id == node.stage_run_id
            ).first()
            status = stage_run.status.value if stage_run else "UNKNOWN"
            segment = StageRunSegment(name=name, status=status)
        elif isinstance(node, (BlobNode, StageFileNode)):
            # For files, we'll determine if it's derived based on the parent path
            is_derived = any(isinstance(seg, StageRunSegment) for seg in path_prefix)
            segment = FileSegment(name=name, is_derived=is_derived)
        else:
            # Fallback to tree segment
            segment = TreeSegment(name=name)

        # Build full path
        full_path = path_prefix + [segment]

        old_child = old_children.get(name)
        new_child = new_children.get(name)

        if old_child is None and new_child is not None:
            # Node was added
            yield from _handle_added(full_path, new_child)

        elif old_child is not None and new_child is None:
            # Node was removed
            yield from _handle_removed(full_path, old_child)

        else:
            # Node exists in both trees - check if modified
            yield from _handle_potential_modification(
                full_path, old_child, new_child
            )


def _handle_added(path: List[PathSegment], node: 'VirtualTreeNode') -> Generator[DiffEvent, None, None]:
    """
    Handle an added node and all its descendants.

    When a node is added, we emit an AddedEvent for it, and then recursively
    emit AddedEvents for all its children (if it's a container).

    Args:
        path: Path segments to the added node
        node: The newly added node

    Yields:
        AddedEvent for this node and all descendants
    """
    from src.core.vfs import TreeNode, BlobNode, StageRunNode, StageFileNode

    # Get content if this is a file node
    blob = node.get_content()

    # Yield event for this node
    yield AddedEvent(path=path, node=node, after_blob=blob)

    # If this node has children, recursively handle them as added too
    children = node.get_children()
    if children:
        for child_name, child_node in children:
            # Create appropriate segment for the child
            if isinstance(child_node, TreeNode):
                segment = TreeSegment(name=child_name)
            elif isinstance(child_node, StageRunNode):
                from src.models import StageRun
                stage_run = child_node._repo.db.query(StageRun).filter(
                    StageRun.id == child_node.stage_run_id
                ).first()
                status = stage_run.status.value if stage_run else "UNKNOWN"
                segment = StageRunSegment(name=child_name, status=status)
            elif isinstance(child_node, (BlobNode, StageFileNode)):
                is_derived = any(isinstance(seg, StageRunSegment) for seg in path)
                segment = FileSegment(name=child_name, is_derived=is_derived)
            else:
                segment = TreeSegment(name=child_name)

            child_path = path + [segment]
            yield from _handle_added(child_path, child_node)


def _handle_removed(path: List[PathSegment], node: 'VirtualTreeNode') -> Generator[DiffEvent, None, None]:
    """
    Handle a removed node and all its descendants.

    When a node is removed, we emit a RemovedEvent for it, and then recursively
    emit RemovedEvents for all its children (if it's a container).

    Args:
        path: Path segments to the removed node
        node: The removed node

    Yields:
        RemovedEvent for this node and all descendants
    """
    from src.core.vfs import TreeNode, BlobNode, StageRunNode, StageFileNode

    # Get content if this is a file node
    blob = node.get_content()

    # Yield event for this node
    yield RemovedEvent(path=path, node=node, before_blob=blob)

    # If this node has children, recursively handle them as removed too
    children = node.get_children()
    if children:
        for child_name, child_node in children:
            # Create appropriate segment for the child
            if isinstance(child_node, TreeNode):
                segment = TreeSegment(name=child_name)
            elif isinstance(child_node, StageRunNode):
                from src.models import StageRun
                stage_run = child_node._repo.db.query(StageRun).filter(
                    StageRun.id == child_node.stage_run_id
                ).first()
                status = stage_run.status.value if stage_run else "UNKNOWN"
                segment = StageRunSegment(name=child_name, status=status)
            elif isinstance(child_node, (BlobNode, StageFileNode)):
                is_derived = any(isinstance(seg, StageRunSegment) for seg in path)
                segment = FileSegment(name=child_name, is_derived=is_derived)
            else:
                segment = TreeSegment(name=child_name)

            child_path = path + [segment]
            yield from _handle_removed(child_path, child_node)


def _handle_potential_modification(
    path: List[PathSegment],
    old_node: 'VirtualTreeNode',
    new_node: 'VirtualTreeNode'
) -> Generator[DiffEvent, None, None]:
    """
    Handle a node that exists in both trees - check if it was modified.

    A node is considered modified if:
    1. Its content changed (for file nodes)
    2. Its type changed (e.g., file -> directory)
    3. It's a container and its children changed

    Args:
        path: Path segments to the node
        old_node: Node in the old tree
        new_node: Node in the new tree

    Yields:
        ModifiedEvent if the node changed, or recursively yields events for children
    """
    from src.core.vfs import TreeNode, BlobNode, StageRunNode, StageFileNode

    # Check if the node type changed
    if type(old_node) != type(new_node):
        # Type changed - treat as removed + added
        yield from _handle_removed(path, old_node)
        yield from _handle_added(path, new_node)
        return

    # For file nodes (BlobNode, StageFileNode), check if content changed
    if isinstance(old_node, (BlobNode, StageFileNode)):
        old_blob = old_node.get_content()
        new_blob = new_node.get_content()

        # Compare by hash
        old_hash = old_blob.hash if old_blob else None
        new_hash = new_blob.hash if new_blob else None

        if old_hash != new_hash:
            # Content changed
            yield ModifiedEvent(
                path=path,
                old_node=old_node,
                new_node=new_node,
                before_blob=old_blob,
                after_blob=new_blob
            )

    # For BlobNode, also check if children changed (stage runs)
    # This needs to happen even if the blob content didn't change!
    if isinstance(old_node, BlobNode):
        # Recursively diff children
        yield from diff_trees(old_node, new_node, path)

    # For container nodes (TreeNode, StageRunNode), recursively diff children
    elif isinstance(old_node, (TreeNode, StageRunNode)):
        # Recursively diff children
        yield from diff_trees(old_node, new_node, path)


def diff_commits(
    repo: 'Repository',
    old_commit_hash: str,
    new_commit_hash: str
) -> Generator[DiffEvent, None, None]:
    """
    Generate diff events between two commits.

    This is a convenience wrapper around diff_trees that gets the VFS roots
    for two commits and diffs them.

    Args:
        repo: Repository instance
        old_commit_hash: Hash of the base commit
        new_commit_hash: Hash of the comparison commit

    Yields:
        DiffEvent instances for all changes between the commits

    Example:
        >>> repo = Repository(...)
        >>> for event in diff_commits(repo, base_hash, new_hash):
        ...     print(f"{event.event_type}: {event.path}")
    """
    old_root = repo.get_root(old_commit_hash)
    new_root = repo.get_root(new_commit_hash)

    yield from diff_trees(old_root, new_root)


def commit_affects_path(repo: 'Repository', commit_hash: str, path: str) -> bool:
    """
    Check if a commit affects a specific file or directory path.

    Args:
        repo: Repository instance
        commit_hash: Hash of the commit to check
        path: File or directory path to check (as string, e.g., "src/main.py")

    Returns:
        True if the commit modifies the path or any files within it

    Example:
        >>> if commit_affects_path(repo, commit_hash, "src/main.py"):
        ...     print("Commit affects src/main.py")
    """
    commit = repo.get_commit(commit_hash)
    if not commit or not commit.parent_hash:
        return False

    # Check all diff events for this commit
    for event in diff_commits(repo, commit.parent_hash, commit_hash):
        # Convert path segments to string for comparison
        event_path = '/'.join(seg.name for seg in event.path)

        # Check if the event path matches exactly or is within the directory
        if event_path == path:
            return True
        # Check if this is a file within the directory
        if event_path.startswith(path + '/'):
            return True

    return False
