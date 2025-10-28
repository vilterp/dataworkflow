"""
View models for rendering VFS diffs in templates.

This module provides simple data structures optimized for template rendering,
without the baggage of the old FileDiff format.
"""
import difflib
from dataclasses import dataclass
from typing import List, Optional
from src.core.vfs_diff import diff_commits, AddedEvent, RemovedEvent, ModifiedEvent
from src.core.vfs import BlobNode, StageFileNode
from src.core.repository import Repository


@dataclass
class DiffLine:
    """A single line in a diff view."""
    line_number_old: Optional[int]
    line_number_new: Optional[int]
    content: str
    change_type: str  # 'add', 'remove', 'context'


@dataclass
class FileDiffView:
    """
    View model for a single file's diff.

    Optimized for template rendering with all the data needed to display
    a file change in the UI.
    """
    path: str
    event_type: str  # 'added', 'removed', 'modified'
    old_hash: Optional[str]
    new_hash: Optional[str]
    lines: List[DiffLine]
    is_binary: bool = False

    @property
    def change_type_display(self) -> str:
        """Get display name for change type."""
        return self.event_type


def get_commit_diff_view(
    repo: Repository,
    commit_hash: str,
    parent_hash: Optional[str] = None,
    context_lines: int = 3
) -> List[FileDiffView]:
    """
    Get commit diff as view models ready for template rendering.

    This uses the new VFS diff engine and converts events into simple
    view models that templates can easily consume.

    Args:
        repo: Repository instance
        commit_hash: Hash of the commit to diff
        parent_hash: Optional parent commit hash (uses commit's parent if None)
        context_lines: Number of context lines for unified diffs

    Returns:
        List of FileDiffView objects for file-level changes only
    """
    commit = repo.get_commit(commit_hash)
    if not commit:
        return []

    # Determine parent
    if parent_hash is None:
        parent_hash = commit.parent_hash

    # If no parent (initial commit), show all files as added
    if parent_hash is None:
        views = []
        root = repo.get_root(commit_hash)
        for event in _traverse_tree_as_events(root, ""):
            if isinstance(event, (AddedEvent,)) and isinstance(event.node, (BlobNode, StageFileNode)):
                view = _convert_added_event_to_view(event, repo, context_lines)
                if view:
                    views.append(view)
        return views

    # Normal diff between two commits
    views = []
    for event in diff_commits(repo, parent_hash, commit_hash):
        # Only process file-level events
        if isinstance(event, AddedEvent) and isinstance(event.node, (BlobNode, StageFileNode)):
            view = _convert_added_event_to_view(event, repo, context_lines)
            if view:
                views.append(view)
        elif isinstance(event, RemovedEvent) and isinstance(event.node, (BlobNode, StageFileNode)):
            view = _convert_removed_event_to_view(event, repo, context_lines)
            if view:
                views.append(view)
        elif isinstance(event, ModifiedEvent) and isinstance(event.old_node, (BlobNode, StageFileNode)):
            view = _convert_modified_event_to_view(event, repo, context_lines)
            if view:
                views.append(view)

    return views


def _traverse_tree_as_events(node, path_prefix):
    """Helper to traverse a tree and yield events for all files."""
    from src.core.vfs_diff import AddedEvent

    # Check if this node is a file
    if isinstance(node, (BlobNode, StageFileNode)):
        blob = node.get_content()
        yield AddedEvent(path=path_prefix, node=node, after_blob=blob)

    # Recursively process children
    for child_name, child_node in node.get_children():
        child_path = f"{path_prefix}/{child_name}" if path_prefix else child_name
        yield from _traverse_tree_as_events(child_node, child_path)


def _convert_added_event_to_view(event: AddedEvent, repo: Repository, context_lines: int) -> Optional[FileDiffView]:
    """Convert an AddedEvent to a FileDiffView."""
    if not event.after_blob:
        return None

    lines = []
    is_binary = False

    content = repo.get_blob_content(event.after_blob.hash)
    if content:
        try:
            text = content.decode('utf-8')
            for i, line in enumerate(text.splitlines(), 1):
                lines.append(DiffLine(
                    line_number_old=None,
                    line_number_new=i,
                    content=line,
                    change_type='add'
                ))
        except UnicodeDecodeError:
            is_binary = True

    return FileDiffView(
        path=event.path,
        event_type='added',
        old_hash=None,
        new_hash=event.after_blob.hash,
        lines=lines,
        is_binary=is_binary
    )


def _convert_removed_event_to_view(event: RemovedEvent, repo: Repository, context_lines: int) -> Optional[FileDiffView]:
    """Convert a RemovedEvent to a FileDiffView."""
    if not event.before_blob:
        return None

    lines = []
    is_binary = False

    content = repo.get_blob_content(event.before_blob.hash)
    if content:
        try:
            text = content.decode('utf-8')
            for i, line in enumerate(text.splitlines(), 1):
                lines.append(DiffLine(
                    line_number_old=i,
                    line_number_new=None,
                    content=line,
                    change_type='remove'
                ))
        except UnicodeDecodeError:
            is_binary = True

    return FileDiffView(
        path=event.path,
        event_type='removed',
        old_hash=event.before_blob.hash,
        new_hash=None,
        lines=lines,
        is_binary=is_binary
    )


def _convert_modified_event_to_view(event: ModifiedEvent, repo: Repository, context_lines: int) -> Optional[FileDiffView]:
    """Convert a ModifiedEvent to a FileDiffView."""
    if not event.before_blob or not event.after_blob:
        return None

    lines = []
    is_binary = False

    old_content = repo.get_blob_content(event.before_blob.hash)
    new_content = repo.get_blob_content(event.after_blob.hash)

    if old_content and new_content:
        try:
            old_text = old_content.decode('utf-8')
            new_text = new_content.decode('utf-8')

            old_lines = old_text.splitlines()
            new_lines = new_text.splitlines()

            lines = _generate_unified_diff(old_lines, new_lines, context_lines)
        except UnicodeDecodeError:
            is_binary = True

    return FileDiffView(
        path=event.path,
        event_type='modified',
        old_hash=event.before_blob.hash,
        new_hash=event.after_blob.hash,
        lines=lines,
        is_binary=is_binary
    )


def _generate_unified_diff(old_lines: List[str], new_lines: List[str], context_lines: int) -> List[DiffLine]:
    """Generate unified diff using difflib."""
    diff_lines = []
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)

    old_line_num = 1
    new_line_num = 1

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            for i in range(i1, i2):
                diff_lines.append(DiffLine(
                    line_number_old=old_line_num,
                    line_number_new=new_line_num,
                    content=old_lines[i],
                    change_type='context'
                ))
                old_line_num += 1
                new_line_num += 1

        elif tag == 'delete':
            for i in range(i1, i2):
                diff_lines.append(DiffLine(
                    line_number_old=old_line_num,
                    line_number_new=None,
                    content=old_lines[i],
                    change_type='remove'
                ))
                old_line_num += 1

        elif tag == 'insert':
            for j in range(j1, j2):
                diff_lines.append(DiffLine(
                    line_number_old=None,
                    line_number_new=new_line_num,
                    content=new_lines[j],
                    change_type='add'
                ))
                new_line_num += 1

        elif tag == 'replace':
            for i in range(i1, i2):
                diff_lines.append(DiffLine(
                    line_number_old=old_line_num,
                    line_number_new=None,
                    content=old_lines[i],
                    change_type='remove'
                ))
                old_line_num += 1
            for j in range(j1, j2):
                diff_lines.append(DiffLine(
                    line_number_old=None,
                    line_number_new=new_line_num,
                    content=new_lines[j],
                    change_type='add'
                ))
                new_line_num += 1

    return diff_lines
