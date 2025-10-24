"""
Diff module for comparing commits and generating file diffs.

This module provides functionality to:
1. Compare two commits and identify added/removed/modified files
2. Generate line-by-line diffs for modified files
"""

import difflib
from dataclasses import dataclass
from typing import List, Optional, Dict
from enum import Enum

from src.repository.repository import Repository


class FileChangeType(Enum):
    """Type of file change in a commit"""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"


@dataclass
class FileChange:
    """Represents a file change in a commit"""
    path: str
    change_type: FileChangeType
    old_hash: Optional[str] = None
    new_hash: Optional[str] = None


@dataclass
class LineDiff:
    """Represents a line-level diff"""
    line_number_old: Optional[int]
    line_number_new: Optional[int]
    content: str
    change_type: str  # 'add', 'remove', 'context', 'header'


@dataclass
class FileDiff:
    """Represents a complete file diff"""
    path: str
    change_type: FileChangeType
    old_hash: Optional[str]
    new_hash: Optional[str]
    lines: List[LineDiff]
    is_binary: bool = False


class DiffGenerator:
    """Generate diffs between commits"""

    def __init__(self, repo: Repository):
        self.repo = repo

    def get_file_changes(self, commit_hash: str, parent_hash: Optional[str] = None) -> List[FileChange]:
        """
        Get list of file changes between a commit and its parent.

        Args:
            commit_hash: Hash of the commit to compare
            parent_hash: Hash of the parent commit (if None, uses commit's parent)

        Returns:
            List of FileChange objects
        """
        commit = self.repo.get_commit(commit_hash)
        if not commit:
            return []

        # If no parent specified, use commit's parent
        if parent_hash is None:
            parent_hash = commit.parent_hash

        # If still no parent (initial commit), all files are added
        if parent_hash is None:
            return self._get_all_files_as_added(commit.tree_hash)

        parent_commit = self.repo.get_commit(parent_hash)
        if not parent_commit:
            return []

        # Get file trees for both commits
        current_files = self._get_all_files_in_tree(commit.tree_hash)
        parent_files = self._get_all_files_in_tree(parent_commit.tree_hash)

        changes = []

        # Find added and modified files
        for path, hash in current_files.items():
            if path not in parent_files:
                changes.append(FileChange(
                    path=path,
                    change_type=FileChangeType.ADDED,
                    new_hash=hash
                ))
            elif parent_files[path] != hash:
                changes.append(FileChange(
                    path=path,
                    change_type=FileChangeType.MODIFIED,
                    old_hash=parent_files[path],
                    new_hash=hash
                ))

        # Find removed files
        for path, hash in parent_files.items():
            if path not in current_files:
                changes.append(FileChange(
                    path=path,
                    change_type=FileChangeType.REMOVED,
                    old_hash=hash
                ))

        return sorted(changes, key=lambda x: x.path)

    def get_file_diff(self, file_change: FileChange, context_lines: int = 3) -> FileDiff:
        """
        Generate line-by-line diff for a file change.

        Args:
            file_change: FileChange object
            context_lines: Number of context lines to show around changes

        Returns:
            FileDiff object with line-by-line changes
        """
        lines = []
        is_binary = False

        if file_change.change_type == FileChangeType.ADDED:
            # For added files, show all lines as additions
            content = self.repo.get_blob_content(file_change.new_hash)
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

        elif file_change.change_type == FileChangeType.REMOVED:
            # For removed files, show all lines as deletions
            content = self.repo.get_blob_content(file_change.old_hash)
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

        elif file_change.change_type == FileChangeType.MODIFIED:
            # For modified files, generate unified diff
            old_content = self.repo.get_blob_content(file_change.old_hash)
            new_content = self.repo.get_blob_content(file_change.new_hash)

            if old_content and new_content:
                try:
                    old_text = old_content.decode('utf-8')
                    new_text = new_content.decode('utf-8')

                    old_lines = old_text.splitlines()
                    new_lines = new_text.splitlines()

                    lines = self._generate_unified_diff(
                        old_lines, new_lines, context_lines
                    )
                except UnicodeDecodeError:
                    is_binary = True

        return FileDiff(
            path=file_change.path,
            change_type=file_change.change_type,
            old_hash=file_change.old_hash,
            new_hash=file_change.new_hash,
            lines=lines,
            is_binary=is_binary
        )

    def get_commit_diff(self, commit_hash: str, parent_hash: Optional[str] = None,
                       context_lines: int = 3) -> List[FileDiff]:
        """
        Get complete diff for a commit.

        Args:
            commit_hash: Hash of the commit
            parent_hash: Hash of parent commit (optional)
            context_lines: Number of context lines around changes

        Returns:
            List of FileDiff objects
        """
        file_changes = self.get_file_changes(commit_hash, parent_hash)
        return [self.get_file_diff(change, context_lines) for change in file_changes]

    def commit_affects_path(self, commit_hash: str, path: str) -> bool:
        """
        Check if a commit affects a specific file or directory path.

        Args:
            commit_hash: Hash of the commit to check
            path: File or directory path to check

        Returns:
            True if the commit modifies the path or any files within it
        """
        file_changes = self.get_file_changes(commit_hash)

        for change in file_changes:
            # Check if the change path matches exactly or is within the directory
            if change.path == path:
                return True
            # Check if this is a file within the directory
            if change.path.startswith(path + '/'):
                return True

        return False

    def get_latest_commit_for_path(self, branch_ref_hash: str, path: str, limit: int = 100):
        """
        Find the latest commit that affected a specific path.

        Args:
            branch_ref_hash: Hash of the branch's latest commit
            path: File or directory path to check
            limit: Maximum number of commits to search

        Returns:
            Commit object of the latest commit affecting the path, or None
        """
        commits = self.repo.get_commit_history(branch_ref_hash, limit=limit)

        for commit in commits:
            if self.commit_affects_path(commit.hash, path):
                return commit

        return None

    def _get_all_files_in_tree(self, tree_hash: str, prefix: str = "") -> Dict[str, str]:
        """
        Recursively get all files in a tree.

        Returns:
            Dict mapping file paths to blob hashes
        """
        files = {}
        entries = self.repo.get_tree_contents(tree_hash)

        for entry in entries:
            path = f"{prefix}/{entry.name}" if prefix else entry.name

            if entry.type.value == 'blob':
                files[path] = entry.hash
            elif entry.type.value == 'tree':
                # Recursively process subdirectories
                subfiles = self._get_all_files_in_tree(entry.hash, path)
                files.update(subfiles)

        return files

    def _get_all_files_as_added(self, tree_hash: str) -> List[FileChange]:
        """Get all files in tree as added files (for initial commit)"""
        files = self._get_all_files_in_tree(tree_hash)
        return [
            FileChange(path=path, change_type=FileChangeType.ADDED, new_hash=hash)
            for path, hash in sorted(files.items())
        ]

    def _generate_unified_diff(self, old_lines: List[str], new_lines: List[str],
                              context_lines: int) -> List[LineDiff]:
        """
        Generate unified diff using difflib.

        Args:
            old_lines: Lines from old file
            new_lines: Lines from new file
            context_lines: Number of context lines

        Returns:
            List of LineDiff objects
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
