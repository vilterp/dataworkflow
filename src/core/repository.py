import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, List
from dataclasses import dataclass
from sqlalchemy.orm import Session

from src.models import Blob, Tree, TreeEntry, Commit, Ref
from src.models.tree import EntryType
from src.storage import S3Storage


@dataclass
class TreeEntryInput:
    """
    Base tree entry with core fields for creating trees.

    This represents the minimal information needed to create a tree entry.
    """
    name: str
    type: str  # 'blob' or 'tree'
    hash: str
    mode: str = '100644'


@dataclass
class TreeEntryWithCommit(TreeEntryInput):
    """
    Tree entry with commit metadata for display purposes.

    Extends TreeEntryInput with the latest commit that modified this entry.
    Used when listing directory contents with commit information.
    """
    latest_commit: 'Commit | None' = None

    def __post_init__(self):
        # Convert string type to EntryType enum for display
        if isinstance(self.type, str):
            self.type = EntryType.BLOB if self.type == 'blob' else EntryType.TREE


@dataclass
class CommitStageRunStats:
    """
    Statistics about stage runs for a commit.
    """
    stage_run_count: int
    has_failed: bool
    has_running: bool
    has_completed: bool


class Repository:
    """
    Main repository class providing Git-like operations.
    Handles creating commits, managing refs, and traversing history.
    """

    def __init__(self, db: Session, storage: S3Storage, repository_id: int):
        self.db = db
        self.storage = storage
        self.repository_id = repository_id

    def create_blob(self, content: bytes) -> Blob:
        """
        Create a blob from content.

        Args:
            content: Binary content

        Returns:
            Blob object
        """
        # Store in S3
        hash, s3_key, size = self.storage.store(content)

        # Check if blob already exists in DB for this repository
        existing_blob = self.db.query(Blob).filter(
            Blob.repository_id == self.repository_id,
            Blob.hash == hash
        ).first()
        if existing_blob:
            return existing_blob

        # Create blob record
        blob = Blob(repository_id=self.repository_id, hash=hash, s3_key=s3_key, size=size)
        self.db.add(blob)
        self.db.commit()

        return blob

    def create_tree(self, entries: List[TreeEntryInput]) -> Tree:
        """
        Create a tree from a list of entries.

        Args:
            entries: List of TreeEntryInput objects

        Returns:
            Tree object
        """
        # Sort entries by name (git convention)
        sorted_entries = sorted(entries, key=lambda e: e.name)

        # Compute tree hash from entries (convert to dicts for hashing)
        entries_for_hash = [
            {'name': e.name, 'type': e.type, 'hash': e.hash, 'mode': e.mode}
            for e in sorted_entries
        ]
        tree_content = json.dumps(entries_for_hash, sort_keys=True)
        tree_hash = hashlib.sha256(tree_content.encode()).hexdigest()

        # Check if tree already exists for this repository
        existing_tree = self.db.query(Tree).filter(
            Tree.repository_id == self.repository_id,
            Tree.hash == tree_hash
        ).first()
        if existing_tree:
            return existing_tree

        # Create tree
        tree = Tree(repository_id=self.repository_id, hash=tree_hash)
        self.db.add(tree)
        self.db.flush()

        # Create tree entries
        for entry in sorted_entries:
            tree_entry = TreeEntry(
                repository_id=self.repository_id,
                tree_hash=tree_hash,
                name=entry.name,
                type=EntryType.BLOB if entry.type == 'blob' else EntryType.TREE,
                hash=entry.hash,
                mode=entry.mode
            )
            self.db.add(tree_entry)

        self.db.commit()
        return tree

    def create_commit(
        self,
        tree_hash: str,
        message: str,
        author: str,
        author_email: str,
        parent_hash: Optional[str] = None
    ) -> Commit:
        """
        Create a commit.

        Args:
            tree_hash: Hash of the tree
            message: Commit message
            author: Author name
            author_email: Author email
            parent_hash: Optional parent commit hash

        Returns:
            Commit object
        """
        # Compute commit hash
        commit_data = {
            'tree': tree_hash,
            'parent': parent_hash,
            'author': author,
            'author_email': author_email,
            'message': message,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        commit_content = json.dumps(commit_data, sort_keys=True)
        commit_hash = hashlib.sha256(commit_content.encode()).hexdigest()

        # Check if commit already exists for this repository
        existing_commit = self.db.query(Commit).filter(
            Commit.repository_id == self.repository_id,
            Commit.hash == commit_hash
        ).first()
        if existing_commit:
            return existing_commit

        # Create commit
        commit = Commit(
            repository_id=self.repository_id,
            hash=commit_hash,
            tree_hash=tree_hash,
            parent_hash=parent_hash,
            author=author,
            author_email=author_email,
            message=message
        )
        self.db.add(commit)
        self.db.commit()

        return commit

    def create_or_update_ref(self, ref_name: str, commit_hash: str) -> Ref:
        """
        Create or update a reference (branch/tag).

        Args:
            ref_name: Full ref name (e.g., 'refs/heads/main')
            commit_hash: Commit hash to point to

        Returns:
            Ref object
        """
        ref = self.db.query(Ref).filter(
            Ref.repository_id == self.repository_id,
            Ref.id == ref_name
        ).first()

        if ref:
            ref.commit_hash = commit_hash
        else:
            ref = Ref(repository_id=self.repository_id, id=ref_name, commit_hash=commit_hash)
            self.db.add(ref)

        self.db.commit()
        return ref

    def create_branch(self, branch_name: str, commit_hash: str) -> Ref:
        """
        Create a new branch.

        Args:
            branch_name: Short branch name (e.g., 'feature-x')
            commit_hash: Commit hash to point the branch to

        Returns:
            Ref object

        Raises:
            ValueError: If branch already exists
        """
        ref_name = f'refs/heads/{branch_name}'

        # Check if branch already exists
        existing = self.db.query(Ref).filter(
            Ref.repository_id == self.repository_id,
            Ref.id == ref_name
        ).first()

        if existing:
            raise ValueError(f"Branch '{branch_name}' already exists")

        # Create the new branch
        ref = Ref(repository_id=self.repository_id, id=ref_name, commit_hash=commit_hash)
        self.db.add(ref)
        self.db.commit()
        return ref

    def get_ref(self, ref_name: str) -> Optional[Ref]:
        """Get a reference by name"""
        return self.db.query(Ref).filter(
            Ref.repository_id == self.repository_id,
            Ref.id == ref_name
        ).first()

    def get_commit(self, commit_hash: str) -> Optional[Commit]:
        """Get a commit by hash"""
        return self.db.query(Commit).filter(
            Commit.repository_id == self.repository_id,
            Commit.hash == commit_hash
        ).first()

    def resolve_ref_or_commit(self, branch_or_hash: str) -> tuple[Optional[Commit], str]:
        """
        Resolve a branch name or commit hash to a commit.

        This method tries to resolve the input as a branch name first, then as a commit hash.
        It's useful for routes that accept either branch names or commit hashes.

        Args:
            branch_or_hash: Either a branch name or a commit hash

        Returns:
            A tuple of (commit, original_input) where:
            - commit is the resolved Commit object (or None if not found)
            - original_input is the branch_or_hash value passed in
        """
        # First try as a branch name
        ref_name = f'refs/heads/{branch_or_hash}' if not branch_or_hash.startswith('refs/') else branch_or_hash
        ref = self.get_ref(ref_name)

        if ref:
            # It's a valid branch
            commit = self.get_commit(ref.commit_hash)
            return commit, branch_or_hash

        # Try as a commit hash
        commit = self.get_commit(branch_or_hash)
        if commit:
            return commit, branch_or_hash

        # Neither branch nor commit found
        return None, branch_or_hash

    def get_tree(self, tree_hash: str) -> Optional[Tree]:
        """Get a tree by hash"""
        return self.db.query(Tree).filter(
            Tree.repository_id == self.repository_id,
            Tree.hash == tree_hash
        ).first()

    def get_blob(self, blob_hash: str) -> Optional[Blob]:
        """Get a blob by hash"""
        return self.db.query(Blob).filter(
            Blob.repository_id == self.repository_id,
            Blob.hash == blob_hash
        ).first()

    def get_blob_content(self, blob_hash: str) -> Optional[bytes]:
        """Get blob content from S3"""
        return self.storage.retrieve(blob_hash)

    def list_refs(self) -> List[Ref]:
        """List all refs for this repository"""
        return self.db.query(Ref).filter(
            Ref.repository_id == self.repository_id
        ).order_by(Ref.id).all()

    def list_branches(self) -> List[Ref]:
        """List all branches for this repository"""
        return self.db.query(Ref).filter(
            Ref.repository_id == self.repository_id,
            Ref.id.like('refs/heads/%')
        ).all()

    def list_tags(self) -> List[Ref]:
        """List all tags for this repository"""
        return self.db.query(Ref).filter(
            Ref.repository_id == self.repository_id,
            Ref.id.like('refs/tags/%')
        ).all()

    def get_commit_history(self, commit_hash: str, limit: int = 50) -> List[Commit]:
        """
        Get commit history starting from a commit.

        Args:
            commit_hash: Starting commit hash
            limit: Maximum number of commits to return

        Returns:
            List of commits in reverse chronological order
        """
        history = []
        current = self.get_commit(commit_hash)

        while current and len(history) < limit:
            history.append(current)
            if current.parent_hash:
                current = self.get_commit(current.parent_hash)
            else:
                break

        return history

    def get_tree_contents(self, tree_hash: str) -> List[TreeEntry]:
        """Get all entries in a tree"""
        tree = self.get_tree(tree_hash)
        if not tree:
            return []
        return tree.entries

    def get_tree_entries_with_commits(self, commit_hash: str, dir_path: str = '') -> List[TreeEntryWithCommit]:
        """
        Get tree entries for a directory path and their latest commit information.

        Args:
            commit_hash: The commit to start from
            dir_path: Directory path within the tree (empty for root)

        Returns:
            List of TreeEntryWithCommit objects with name, type, hash, mode, and latest commit information
        """
        from src.diff import DiffGenerator

        # Get the commit
        commit = self.get_commit(commit_hash)
        if not commit:
            return []

        # Navigate to the directory through the tree
        current_tree_hash = commit.tree_hash

        if dir_path:
            path_parts = dir_path.split('/')
            # Navigate through directories
            for part in path_parts:
                tree_entries = self.get_tree_contents(current_tree_hash)
                found = False
                for entry in tree_entries:
                    if entry.name == part and entry.type.value == 'tree':
                        current_tree_hash = entry.hash
                        found = True
                        break
                if not found:
                    return []

        # Get entries in the current directory
        entries = self.get_tree_contents(current_tree_hash)

        # Get latest commit info for each entry and create TreeEntryWithCommit objects
        diff_gen = DiffGenerator(self)
        tree_entries = []
        for entry in entries:
            entry_path = f"{dir_path}/{entry.name}" if dir_path else entry.name
            commit_for_entry = diff_gen.get_latest_commit_for_path(commit_hash, entry_path)

            # Create TreeEntryWithCommit from tree entry with commit metadata
            tree_entry = TreeEntryWithCommit(
                name=entry.name,
                type=entry.type.value,  # Convert EntryType enum to string for base class
                hash=entry.hash,
                mode=entry.mode,
                latest_commit=commit_for_entry
            )
            tree_entries.append(tree_entry)

        return tree_entries

    def delete_file(
        self,
        base_commit_hash: str,
        file_path: str,
        message: str,
        author: str,
        author_email: str
    ) -> Commit:
        """
        Delete a file from the repository by creating a new commit.

        Args:
            base_commit_hash: The commit to base the deletion on
            file_path: Path to the file to delete (e.g., "dir/file.txt")
            message: Commit message
            author: Author name
            author_email: Author email

        Returns:
            New commit with the file deleted

        Raises:
            ValueError: If file doesn't exist or path is invalid
        """
        # Get the base commit
        base_commit = self.get_commit(base_commit_hash)
        if not base_commit:
            raise ValueError(f"Commit {base_commit_hash} not found")

        # Parse the file path
        path_parts = file_path.split('/')
        file_name = path_parts[-1]
        dir_path = '/'.join(path_parts[:-1]) if len(path_parts) > 1 else ''

        # Build the new tree by recursively copying the old tree and removing the file
        new_tree_hash = self._delete_from_tree(base_commit.tree_hash, path_parts)

        # Create the commit
        return self.create_commit(
            tree_hash=new_tree_hash,
            message=message,
            author=author,
            author_email=author_email,
            parent_hash=base_commit_hash
        )

    def _delete_from_tree(self, tree_hash: str, path_parts: List[str]) -> str:
        """
        Recursively delete a file from a tree by creating new trees.

        Args:
            tree_hash: Current tree hash
            path_parts: Remaining path parts to navigate

        Returns:
            Hash of the new tree with the file deleted

        Raises:
            ValueError: If file doesn't exist
        """
        # Get current tree entries
        entries = self.get_tree_contents(tree_hash)

        # If this is the last part, remove the file
        if len(path_parts) == 1:
            target_name = path_parts[0]
            new_entries = []
            found = False

            for entry in entries:
                if entry.name == target_name:
                    found = True
                    # Skip this entry (delete it)
                    continue
                else:
                    new_entries.append(TreeEntryInput(
                        name=entry.name,
                        type=entry.type.value,
                        hash=entry.hash,
                        mode=entry.mode
                    ))

            if not found:
                raise ValueError(f"File {target_name} not found")

            # Create new tree without the deleted file
            return self.create_tree(new_entries).hash

        # Otherwise, navigate into the directory
        else:
            dir_name = path_parts[0]
            new_entries = []
            found = False

            for entry in entries:
                if entry.name == dir_name and entry.type.value == 'tree':
                    found = True
                    # Recursively delete from this subtree
                    new_subtree_hash = self._delete_from_tree(entry.hash, path_parts[1:])
                    new_entries.append(TreeEntryInput(
                        name=entry.name,
                        type='tree',
                        hash=new_subtree_hash,
                        mode=entry.mode
                    ))
                else:
                    new_entries.append(TreeEntryInput(
                        name=entry.name,
                        type=entry.type.value,
                        hash=entry.hash,
                        mode=entry.mode
                    ))

            if not found:
                raise ValueError(f"Directory {dir_name} not found")

            # Create new tree with updated subtree
            return self.create_tree(new_entries).hash

    def update_file(
        self,
        branch: str,
        file_path: str,
        content: bytes,
        commit_message: str,
        author_name: str,
        author_email: str
    ) -> Commit:
        """
        Update a file in the repository by creating a new commit.

        Args:
            branch: Branch name to update
            file_path: Path to the file to update (e.g., "dir/file.txt")
            content: New file content as bytes
            commit_message: Commit message
            author_name: Author name
            author_email: Author email

        Returns:
            New commit with the file updated

        Raises:
            ValueError: If branch doesn't exist or path is invalid
        """
        # Get the branch ref
        ref_name = f'refs/heads/{branch}' if not branch.startswith('refs/') else branch
        ref = self.get_ref(ref_name)
        if not ref:
            raise ValueError(f"Branch {branch} not found")

        # Get the base commit
        base_commit = self.get_commit(ref.commit_hash)
        if not base_commit:
            raise ValueError(f"Commit {ref.commit_hash} not found")

        # Store the new blob content
        blob = self.create_blob(content)

        # Parse the file path
        path_parts = file_path.split('/')

        # Build the new tree by recursively updating the old tree
        new_tree_hash = self._update_in_tree(base_commit.tree_hash, path_parts, blob.hash)

        # Create the commit
        new_commit = self.create_commit(
            tree_hash=new_tree_hash,
            message=commit_message,
            author=author_name,
            author_email=author_email,
            parent_hash=base_commit.hash
        )

        # Update the branch ref to point to the new commit
        self.create_or_update_ref(ref_name, new_commit.hash)

        return new_commit

    def _update_in_tree(self, tree_hash: str, path_parts: List[str], blob_hash: str) -> str:
        """
        Recursively update a file in a tree by creating new trees.

        Args:
            tree_hash: Current tree hash
            path_parts: Remaining path parts to navigate
            blob_hash: Hash of the new blob content

        Returns:
            Hash of the new tree with the file updated
        """
        # Get current tree entries
        entries = self.get_tree_contents(tree_hash)

        # If this is the last part, update or add the file
        if len(path_parts) == 1:
            target_name = path_parts[0]
            new_entries = []
            found = False

            for entry in entries:
                if entry.name == target_name:
                    found = True
                    # Update this entry with new blob
                    new_entries.append(TreeEntryInput(
                        name=entry.name,
                        type='blob',
                        hash=blob_hash,
                        mode=entry.mode
                    ))
                else:
                    new_entries.append(TreeEntryInput(
                        name=entry.name,
                        type=entry.type.value,
                        hash=entry.hash,
                        mode=entry.mode
                    ))

            # If file didn't exist, add it
            if not found:
                new_entries.append(TreeEntryInput(
                    name=target_name,
                    type='blob',
                    hash=blob_hash,
                    mode='100644'
                ))

            # Create new tree with updated file
            return self.create_tree(new_entries).hash

        # Otherwise, navigate into the directory
        else:
            dir_name = path_parts[0]
            new_entries = []
            found = False

            for entry in entries:
                if entry.name == dir_name and entry.type.value == 'tree':
                    found = True
                    # Recursively update in this subtree
                    new_subtree_hash = self._update_in_tree(entry.hash, path_parts[1:], blob_hash)
                    new_entries.append(TreeEntryInput(
                        name=entry.name,
                        type='tree',
                        hash=new_subtree_hash,
                        mode=entry.mode
                    ))
                else:
                    new_entries.append(TreeEntryInput(
                        name=entry.name,
                        type=entry.type.value,
                        hash=entry.hash,
                        mode=entry.mode
                    ))

            if not found:
                raise ValueError(f"Directory {dir_name} not found in path")

            # Create new tree with updated subtree
            return self.create_tree(new_entries).hash

    def get_blob_hash_from_path(self, tree_hash: str, file_path: str) -> Optional[str]:
        """
        Navigate through directories in a tree to find a blob hash for a file path.

        Args:
            tree_hash: The tree hash to start navigation from
            file_path: Path to the file (e.g., "dir/subdir/file.txt")

        Returns:
            Blob hash if found, None otherwise
        """
        path_parts = file_path.split('/')
        current_tree_hash = tree_hash

        # Navigate through directories
        for i, part in enumerate(path_parts[:-1]):
            tree_entries = self.get_tree_contents(current_tree_hash)
            found = False
            for entry in tree_entries:
                if entry.name == part and entry.type.value == 'tree':
                    current_tree_hash = entry.hash
                    found = True
                    break
            if not found:
                return None

        # Find the file in the final directory
        tree_entries = self.get_tree_contents(current_tree_hash)
        file_name = path_parts[-1]
        for entry in tree_entries:
            if entry.name == file_name and entry.type.value == 'blob':
                return entry.hash

        return None

    def get_path_commit_info(self, commit_hash: str, path: str, limit: int = 1000) -> tuple[Optional['Commit'], int]:
        """
        Get the latest commit affecting a path and the total count of commits affecting it.

        Args:
            commit_hash: The commit hash to start from
            path: File or directory path to check
            limit: Maximum number of commits to search through

        Returns:
            Tuple of (latest_commit, commit_count) where:
            - latest_commit is the most recent commit affecting the path (or None if not found)
            - commit_count is the total number of commits affecting the path
        """
        from src.diff import DiffGenerator

        diff_gen = DiffGenerator(self)

        # Get the latest commit for this path
        latest_commit = diff_gen.get_latest_commit_for_path(commit_hash, path, limit=limit)

        # Get all commits and filter to those affecting this path
        all_commits = self.get_commit_history(commit_hash, limit=limit)
        affecting_commits = [c for c in all_commits if diff_gen.commit_affects_path(c.hash, path)]

        return latest_commit, len(affecting_commits)

    def get_commit_stage_run_stats(self, commit_hash: str) -> CommitStageRunStats:
        """
        Get stage run statistics for a commit.

        Args:
            commit_hash: The commit hash to get stats for

        Returns:
            CommitStageRunStats with stage run information
        """
        from src.models import StageRun, StageRunStatus

        stage_runs = self.db.query(StageRun).filter(
            StageRun.commit_hash == commit_hash,
            StageRun.parent_stage_run_id == None
        ).all()

        return CommitStageRunStats(
            stage_run_count=len(stage_runs),
            has_failed=any(sr.status == StageRunStatus.FAILED for sr in stage_runs),
            has_running=any(sr.status == StageRunStatus.RUNNING for sr in stage_runs),
            has_completed=any(sr.status == StageRunStatus.COMPLETED for sr in stage_runs),
        )
