"""
Adapter to convert new VFS diff events to old FileDiff format.

This provides backwards compatibility for the UI until we can fully migrate
to the new diff format.
"""
import difflib
from typing import List, Optional
from src.core.vfs_diff import diff_commits, AddedEvent, RemovedEvent, ModifiedEvent
from src.core.repository import Repository
from src.diff import FileDiff, LineDiff, FileChangeType


def get_commit_diff_legacy(
    repo: Repository,
    commit_hash: str,
    parent_hash: Optional[str] = None,
    context_lines: int = 3
) -> List[FileDiff]:
    """
    Get commit diff in the old FileDiff format using the new VFS diff engine.

    This is a compatibility adapter that converts new VFS diff events into the
    old FileDiff format expected by existing templates.

    Args:
        repo: Repository instance
        commit_hash: Hash of the commit to diff
        parent_hash: Optional parent commit hash (uses commit's parent if None)
        context_lines: Number of context lines for unified diffs

    Returns:
        List of FileDiff objects compatible with existing templates
    """
    from src.core.vfs import BlobNode, StageFileNode

    commit = repo.get_commit(commit_hash)
    if not commit:
        return []

    # Determine parent
    if parent_hash is None:
        parent_hash = commit.parent_hash

    # If no parent (initial commit), use empty tree logic
    if parent_hash is None:
        # For initial commits, all files are additions
        file_diffs = []
        for event in diff_commits(repo, commit_hash, commit_hash):
            # This won't yield anything, so we need a different approach
            pass

        # Get all files from the commit's tree and mark as added
        root = repo.get_root(commit_hash)
        for event in _traverse_tree_as_added(root, ""):
            file_diff = _convert_added_event(event, repo, context_lines)
            if file_diff:
                file_diffs.append(file_diff)
        return file_diffs

    # Normal diff between two commits
    file_diffs = []
    for event in diff_commits(repo, parent_hash, commit_hash):
        # Only process file-level events (not directories or stage runs)
        if isinstance(event, AddedEvent):
            if isinstance(event.node, (BlobNode, StageFileNode)):
                file_diff = _convert_added_event(event, repo, context_lines)
                if file_diff:
                    file_diffs.append(file_diff)
        elif isinstance(event, RemovedEvent):
            if isinstance(event.node, (BlobNode, StageFileNode)):
                file_diff = _convert_removed_event(event, repo, context_lines)
                if file_diff:
                    file_diffs.append(file_diff)
        elif isinstance(event, ModifiedEvent):
            if isinstance(event.old_node, (BlobNode, StageFileNode)):
                file_diff = _convert_modified_event(event, repo, context_lines)
                if file_diff:
                    file_diffs.append(file_diff)

    return file_diffs


def _traverse_tree_as_added(node, path_prefix):
    """Helper to traverse a tree and yield AddedEvents for all files"""
    from src.core.vfs import BlobNode, StageFileNode
    from src.core.vfs_diff import AddedEvent

    # Check if this node is a file
    if isinstance(node, (BlobNode, StageFileNode)):
        blob = node.get_content()
        yield AddedEvent(path=path_prefix, node=node, after_blob=blob)

    # Recursively process children
    for child_name, child_node in node.get_children():
        child_path = f"{path_prefix}/{child_name}" if path_prefix else child_name
        yield from _traverse_tree_as_added(child_node, child_path)


def _convert_added_event(event: AddedEvent, repo: Repository, context_lines: int) -> Optional[FileDiff]:
    """Convert an AddedEvent to a FileDiff"""
    if not event.after_blob:
        return None

    lines = []
    is_binary = False

    content = repo.get_blob_content(event.after_blob.hash)
    if content:
        try:
            text = content.decode('utf-8')
            for i, line in enumerate(text.splitlines(), 1):
                lines.append(LineDiff(
                    line_number_old=None,
                    line_number_new=i,
                    content=line,
                    change_type='add'
                ))
        except UnicodeDecodeError:
            is_binary = True

    return FileDiff(
        path=event.path,
        change_type=FileChangeType.ADDED,
        old_hash=None,
        new_hash=event.after_blob.hash,
        lines=lines,
        is_binary=is_binary
    )


def _convert_removed_event(event: RemovedEvent, repo: Repository, context_lines: int) -> Optional[FileDiff]:
    """Convert a RemovedEvent to a FileDiff"""
    if not event.before_blob:
        return None

    lines = []
    is_binary = False

    content = repo.get_blob_content(event.before_blob.hash)
    if content:
        try:
            text = content.decode('utf-8')
            for i, line in enumerate(text.splitlines(), 1):
                lines.append(LineDiff(
                    line_number_old=i,
                    line_number_new=None,
                    content=line,
                    change_type='remove'
                ))
        except UnicodeDecodeError:
            is_binary = True

    return FileDiff(
        path=event.path,
        change_type=FileChangeType.REMOVED,
        old_hash=event.before_blob.hash,
        new_hash=None,
        lines=lines,
        is_binary=is_binary
    )


def _convert_modified_event(event: ModifiedEvent, repo: Repository, context_lines: int) -> Optional[FileDiff]:
    """Convert a ModifiedEvent to a FileDiff"""
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

    return FileDiff(
        path=event.path,
        change_type=FileChangeType.MODIFIED,
        old_hash=event.before_blob.hash,
        new_hash=event.after_blob.hash,
        lines=lines,
        is_binary=is_binary
    )


def _generate_unified_diff(old_lines: List[str], new_lines: List[str], context_lines: int) -> List[LineDiff]:
    """
    Generate unified diff using difflib.

    This is copied from the old DiffGenerator to maintain identical behavior.
    """
    diff_lines = []

    # Use SequenceMatcher for more control
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)

    old_line_num = 1
    new_line_num = 1

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            # Context lines
            for i in range(i1, i2):
                diff_lines.append(LineDiff(
                    line_number_old=old_line_num,
                    line_number_new=new_line_num,
                    content=old_lines[i],
                    change_type='context'
                ))
                old_line_num += 1
                new_line_num += 1

        elif tag == 'delete':
            # Removed lines
            for i in range(i1, i2):
                diff_lines.append(LineDiff(
                    line_number_old=old_line_num,
                    line_number_new=None,
                    content=old_lines[i],
                    change_type='remove'
                ))
                old_line_num += 1

        elif tag == 'insert':
            # Added lines
            for j in range(j1, j2):
                diff_lines.append(LineDiff(
                    line_number_old=None,
                    line_number_new=new_line_num,
                    content=new_lines[j],
                    change_type='add'
                ))
                new_line_num += 1

        elif tag == 'replace':
            # Changed lines (show as delete + insert)
            for i in range(i1, i2):
                diff_lines.append(LineDiff(
                    line_number_old=old_line_num,
                    line_number_new=None,
                    content=old_lines[i],
                    change_type='remove'
                ))
                old_line_num += 1
            for j in range(j1, j2):
                diff_lines.append(LineDiff(
                    line_number_old=None,
                    line_number_new=new_line_num,
                    content=new_lines[j],
                    change_type='add'
                ))
                new_line_num += 1

    return diff_lines
