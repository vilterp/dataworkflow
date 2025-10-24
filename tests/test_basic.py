"""
Basic test cases for repository operations.
Tests creating a repo, making commits, and listing them.
"""

import tempfile
import shutil

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.storage import FilesystemStorage
from src.repository import Repository


def test_create_blob_and_retrieve():
    """Test creating and retrieving a blob"""
    temp_dir = tempfile.mkdtemp()
    try:
        storage = FilesystemStorage(base_path=f"{temp_dir}/objects")

        # Create engine and initialize tables
        engine = create_engine('sqlite:///:memory:', echo=False)
        Base.metadata.create_all(engine)

        # Create session from the same engine
        Session = sessionmaker(bind=engine)
        db = Session()

        repo = Repository(db, storage)

        # Create blob
        content = b"Hello, World!"
        blob = repo.create_blob(content)

        assert blob is not None
        assert blob.size == len(content)
        assert len(blob.hash) == 64  # SHA-256

        # Retrieve content
        retrieved = repo.get_blob_content(blob.hash)
        assert retrieved == content

        db.close()
    finally:
        shutil.rmtree(temp_dir)


def test_create_commits_and_list():
    """Test creating commits and listing history"""
    temp_dir = tempfile.mkdtemp()
    try:
        storage = FilesystemStorage(base_path=f"{temp_dir}/objects")

        # Create engine and initialize tables
        engine = create_engine('sqlite:///:memory:', echo=False)
        Base.metadata.create_all(engine)

        # Create session from the same engine
        Session = sessionmaker(bind=engine)
        db = Session()

        repo = Repository(db, storage)

        # Create first commit
        blob1 = repo.create_blob(b"# README\nInitial version")
        tree1 = repo.create_tree([
            {'name': 'README.md', 'type': 'blob', 'hash': blob1.hash, 'mode': '100644'}
        ])
        commit1 = repo.create_commit(
            tree_hash=tree1.hash,
            message="Initial commit",
            author="Test User",
            author_email="test@example.com",
            parent_hash=None
        )

        # Create branch
        ref = repo.create_or_update_ref('refs/heads/main', commit1.hash)
        assert ref.name == 'main'
        assert ref.commit_hash == commit1.hash

        # Create second commit
        blob2 = repo.create_blob(b"# README\nUpdated version")
        tree2 = repo.create_tree([
            {'name': 'README.md', 'type': 'blob', 'hash': blob2.hash, 'mode': '100644'}
        ])
        commit2 = repo.create_commit(
            tree_hash=tree2.hash,
            message="Update README",
            author="Test User",
            author_email="test@example.com",
            parent_hash=commit1.hash
        )

        # Update branch
        repo.create_or_update_ref('refs/heads/main', commit2.hash)

        # List commits
        history = repo.get_commit_history(commit2.hash, limit=10)
        assert len(history) == 2
        assert history[0].message == "Update README"
        assert history[1].message == "Initial commit"

        # List branches
        branches = repo.list_branches()
        assert len(branches) == 1
        assert branches[0].name == 'main'

        print("\n✓ Test passed: Created 2 commits and listed them successfully")

        db.close()
    finally:
        shutil.rmtree(temp_dir)


def test_tree_with_multiple_files():
    """Test creating a tree with multiple files"""
    temp_dir = tempfile.mkdtemp()
    try:
        storage = FilesystemStorage(base_path=f"{temp_dir}/objects")

        # Create engine and initialize tables
        engine = create_engine('sqlite:///:memory:', echo=False)
        Base.metadata.create_all(engine)

        # Create session from the same engine
        Session = sessionmaker(bind=engine)
        db = Session()

        repo = Repository(db, storage)

        # Create multiple blobs
        blob1 = repo.create_blob(b"File 1 content")
        blob2 = repo.create_blob(b"File 2 content")
        blob3 = repo.create_blob(b"File 3 content")

        # Create tree
        tree = repo.create_tree([
            {'name': 'file1.txt', 'type': 'blob', 'hash': blob1.hash, 'mode': '100644'},
            {'name': 'file2.txt', 'type': 'blob', 'hash': blob2.hash, 'mode': '100644'},
            {'name': 'file3.txt', 'type': 'blob', 'hash': blob3.hash, 'mode': '100644'},
        ])

        # Verify tree contents
        entries = repo.get_tree_contents(tree.hash)
        assert len(entries) == 3
        assert entries[0].name == 'file1.txt'
        assert entries[1].name == 'file2.txt'
        assert entries[2].name == 'file3.txt'

        print("\n✓ Test passed: Created tree with 3 files")

        db.close()
    finally:
        shutil.rmtree(temp_dir)
