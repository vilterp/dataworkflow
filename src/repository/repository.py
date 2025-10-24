import hashlib
import json
from datetime import datetime
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

    def __init__(self, db: Session, storage: S3Storage):
        self.db = db
        self.storage = storage

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

        # Check if blob already exists in DB
        existing_blob = self.db.query(Blob).filter(Blob.hash == hash).first()
        if existing_blob:
            return existing_blob

        # Create blob record
        blob = Blob(hash=hash, s3_key=s3_key, size=size)
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

        # Check if tree already exists
        existing_tree = self.db.query(Tree).filter(Tree.hash == tree_hash).first()
        if existing_tree:
            return existing_tree

        # Create tree
        tree = Tree(hash=tree_hash)
        self.db.add(tree)
        self.db.flush()

        # Create tree entries
        for entry in sorted_entries:
            tree_entry = TreeEntry(
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
            'timestamp': datetime.utcnow().isoformat()
        }
        commit_content = json.dumps(commit_data, sort_keys=True)
        commit_hash = hashlib.sha256(commit_content.encode()).hexdigest()

        # Check if commit already exists
        existing_commit = self.db.query(Commit).filter(Commit.hash == commit_hash).first()
        if existing_commit:
            return existing_commit

        # Create commit
        commit = Commit(
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
        ref = self.db.query(Ref).filter(Ref.id == ref_name).first()

        if ref:
            ref.commit_hash = commit_hash
        else:
            ref = Ref(id=ref_name, commit_hash=commit_hash)
            self.db.add(ref)

        self.db.commit()
        return ref

    def get_ref(self, ref_name: str) -> Optional[Ref]:
        """Get a reference by name"""
        return self.db.query(Ref).filter(Ref.id == ref_name).first()

    def get_commit(self, commit_hash: str) -> Optional[Commit]:
        """Get a commit by hash"""
        return self.db.query(Commit).filter(Commit.hash == commit_hash).first()

    def get_tree(self, tree_hash: str) -> Optional[Tree]:
        """Get a tree by hash"""
        return self.db.query(Tree).filter(Tree.hash == tree_hash).first()

    def get_blob(self, blob_hash: str) -> Optional[Blob]:
        """Get a blob by hash"""
        return self.db.query(Blob).filter(Blob.hash == blob_hash).first()

    def get_blob_content(self, blob_hash: str) -> Optional[bytes]:
        """Get blob content from S3"""
        return self.storage.retrieve(blob_hash)

    def list_refs(self) -> List[Ref]:
        """List all refs"""
        return self.db.query(Ref).order_by(Ref.id).all()

    def list_branches(self) -> List[Ref]:
        """List all branches"""
        return self.db.query(Ref).filter(Ref.id.like('refs/heads/%')).all()

    def list_tags(self) -> List[Ref]:
        """List all tags"""
        return self.db.query(Ref).filter(Ref.id.like('refs/tags/%')).all()

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
