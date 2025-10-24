import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, Dict, List
from sqlalchemy.orm import Session

from src.models import Blob, Tree, TreeEntry, Commit, Ref
from src.models.tree import EntryType
from src.storage import S3Storage


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

    def create_tree(self, entries: List[Dict[str, str]]) -> Tree:
        """
        Create a tree from a list of entries.

        Args:
            entries: List of dicts with 'name', 'type', 'hash', 'mode'

        Returns:
            Tree object
        """
        # Sort entries by name (git convention)
        sorted_entries = sorted(entries, key=lambda e: e['name'])

        # Compute tree hash from entries
        tree_content = json.dumps(sorted_entries, sort_keys=True)
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
                name=entry['name'],
                type=EntryType.BLOB if entry['type'] == 'blob' else EntryType.TREE,
                hash=entry['hash'],
                mode=entry.get('mode', '100644')
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

    def get_tree_entries_with_commits(self, commit_hash: str, dir_path: str = '') -> tuple[List[TreeEntry], Dict[str, 'Commit']]:
        """
        Get tree entries for a directory path and their latest commit information.
        
        Args:
            commit_hash: The commit to start from
            dir_path: Directory path within the tree (empty for root)
            
        Returns:
            Tuple of (entries, entry_commits) where entry_commits maps entry names to their latest commits
        """
        from src.diff import DiffGenerator
        
        # Get the commit
        commit = self.get_commit(commit_hash)
        if not commit:
            return [], {}

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
                    return [], {}

        # Get entries in the current directory
        entries = self.get_tree_contents(current_tree_hash)

        # Get latest commit info for each entry
        diff_gen = DiffGenerator(self)
        entry_commits = {}
        for entry in entries:
            entry_path = f"{dir_path}/{entry.name}" if dir_path else entry.name
            commit_for_entry = diff_gen.get_latest_commit_for_path(commit_hash, entry_path)
            if commit_for_entry:
                entry_commits[entry.name] = commit_for_entry

        return entries, entry_commits
