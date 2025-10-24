"""Tests for the diff module"""

import pytest
import tempfile
import shutil
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import Base, Repository as RepositoryModel
from src.repository.repository import Repository
from src.storage import FilesystemStorage
from src.diff import DiffGenerator, FileChangeType


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test storage"""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def db_session():
    """Create a test database session"""
    engine = create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def repo(db_session, temp_dir):
    """Create a test repository with sample commits"""
    # Create repository model
    repo_model = RepositoryModel(name='test-repo', description='Test repository')
    db_session.add(repo_model)
    db_session.commit()

    # Create repository
    storage = FilesystemStorage(base_path=f"{temp_dir}/objects")
    repo = Repository(db_session, storage, repo_model.id)

    # Create initial commit
    readme_v1 = repo.create_blob(b"# Test\nLine 1\nLine 2\nLine 3")
    license_v1 = repo.create_blob(b"MIT License\nCopyright 2024")

    tree_v1 = repo.create_tree([
        {'name': 'README.md', 'type': 'blob', 'hash': readme_v1.hash, 'mode': '100644'},
        {'name': 'LICENSE', 'type': 'blob', 'hash': license_v1.hash, 'mode': '100644'},
    ])

    commit_v1 = repo.create_commit(
        tree_hash=tree_v1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create second commit with modifications
    readme_v2 = repo.create_blob(b"# Test\nLine 1 modified\nLine 2\nLine 3\nLine 4")
    config = repo.create_blob(b"debug=true\nport=8000")

    tree_v2 = repo.create_tree([
        {'name': 'README.md', 'type': 'blob', 'hash': readme_v2.hash, 'mode': '100644'},
        {'name': 'config.txt', 'type': 'blob', 'hash': config.hash, 'mode': '100644'},
    ])

    commit_v2 = repo.create_commit(
        tree_hash=tree_v2.hash,
        message="Update README, add config, remove LICENSE",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit_v1.hash
    )

    repo.initial_commit = commit_v1
    repo.second_commit = commit_v2

    return repo


def test_file_changes_initial_commit(repo):
    """Test file changes for initial commit (no parent)"""
    diff_gen = DiffGenerator(repo)
    changes = diff_gen.get_file_changes(repo.initial_commit.hash)

    assert len(changes) == 2
    assert all(c.change_type == FileChangeType.ADDED for c in changes)

    paths = [c.path for c in changes]
    assert 'LICENSE' in paths
    assert 'README.md' in paths


def test_file_changes_with_modifications(repo):
    """Test file changes with added, modified, and removed files"""
    diff_gen = DiffGenerator(repo)
    changes = diff_gen.get_file_changes(repo.second_commit.hash)

    assert len(changes) == 3

    # Check for added file
    added = [c for c in changes if c.change_type == FileChangeType.ADDED]
    assert len(added) == 1
    assert added[0].path == 'config.txt'
    assert added[0].new_hash is not None
    assert added[0].old_hash is None

    # Check for modified file
    modified = [c for c in changes if c.change_type == FileChangeType.MODIFIED]
    assert len(modified) == 1
    assert modified[0].path == 'README.md'
    assert modified[0].old_hash is not None
    assert modified[0].new_hash is not None
    assert modified[0].old_hash != modified[0].new_hash

    # Check for removed file
    removed = [c for c in changes if c.change_type == FileChangeType.REMOVED]
    assert len(removed) == 1
    assert removed[0].path == 'LICENSE'
    assert removed[0].old_hash is not None
    assert removed[0].new_hash is None


def test_file_diff_added(repo):
    """Test diff for added file"""
    diff_gen = DiffGenerator(repo)
    changes = diff_gen.get_file_changes(repo.second_commit.hash)

    added_change = [c for c in changes if c.path == 'config.txt'][0]
    file_diff = diff_gen.get_file_diff(added_change)

    assert file_diff.path == 'config.txt'
    assert file_diff.change_type == FileChangeType.ADDED
    assert not file_diff.is_binary
    assert len(file_diff.lines) == 2

    # All lines should be additions
    assert all(line.change_type == 'add' for line in file_diff.lines)
    assert file_diff.lines[0].content == 'debug=true'
    assert file_diff.lines[1].content == 'port=8000'


def test_file_diff_removed(repo):
    """Test diff for removed file"""
    diff_gen = DiffGenerator(repo)
    changes = diff_gen.get_file_changes(repo.second_commit.hash)

    removed_change = [c for c in changes if c.path == 'LICENSE'][0]
    file_diff = diff_gen.get_file_diff(removed_change)

    assert file_diff.path == 'LICENSE'
    assert file_diff.change_type == FileChangeType.REMOVED
    assert not file_diff.is_binary
    assert len(file_diff.lines) == 2

    # All lines should be deletions
    assert all(line.change_type == 'remove' for line in file_diff.lines)
    assert file_diff.lines[0].content == 'MIT License'
    assert file_diff.lines[1].content == 'Copyright 2024'


def test_file_diff_modified(repo):
    """Test diff for modified file"""
    diff_gen = DiffGenerator(repo)
    changes = diff_gen.get_file_changes(repo.second_commit.hash)

    modified_change = [c for c in changes if c.path == 'README.md'][0]
    file_diff = diff_gen.get_file_diff(modified_change)

    assert file_diff.path == 'README.md'
    assert file_diff.change_type == FileChangeType.MODIFIED
    assert not file_diff.is_binary

    # Check that we have the right mix of changes
    change_types = [line.change_type for line in file_diff.lines]
    assert 'add' in change_types  # Line 4 added, Line 1 modified (add)
    assert 'remove' in change_types  # Line 1 original (remove)
    assert 'context' in change_types  # Unchanged lines


def test_file_diff_binary(repo):
    """Test diff for binary file"""
    # Create a binary blob
    binary_blob = repo.create_blob(b'\x00\x01\x02\xff\xfe')
    tree = repo.create_tree([
        {'name': 'binary.dat', 'type': 'blob', 'hash': binary_blob.hash, 'mode': '100644'}
    ])

    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Add binary file",
        author="Test User",
        author_email="test@example.com",
        parent_hash=repo.second_commit.hash
    )

    diff_gen = DiffGenerator(repo)
    changes = diff_gen.get_file_changes(commit.hash)

    binary_change = [c for c in changes if c.path == 'binary.dat'][0]
    file_diff = diff_gen.get_file_diff(binary_change)

    assert file_diff.is_binary
    assert len(file_diff.lines) == 0


def test_get_commit_diff(repo):
    """Test getting complete commit diff"""
    diff_gen = DiffGenerator(repo)
    file_diffs = diff_gen.get_commit_diff(repo.second_commit.hash)

    assert len(file_diffs) == 3

    paths = {diff.path for diff in file_diffs}
    assert paths == {'README.md', 'LICENSE', 'config.txt'}


def test_nested_directory_changes(db_session, temp_dir):
    """Test file changes with nested directories"""
    # Create repository
    repo_model = RepositoryModel(name='nested-repo', description='Nested test')
    db_session.add(repo_model)
    db_session.commit()

    storage = FilesystemStorage(base_path=f"{temp_dir}/objects")
    repo = Repository(db_session, storage, repo_model.id)

    # Create nested structure
    file1 = repo.create_blob(b"File 1")
    file2 = repo.create_blob(b"File 2")

    subtree = repo.create_tree([
        {'name': 'nested.txt', 'type': 'blob', 'hash': file2.hash, 'mode': '100644'}
    ])

    tree = repo.create_tree([
        {'name': 'root.txt', 'type': 'blob', 'hash': file1.hash, 'mode': '100644'},
        {'name': 'subdir', 'type': 'tree', 'hash': subtree.hash, 'mode': '040000'}
    ])

    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Nested structure",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    diff_gen = DiffGenerator(repo)
    changes = diff_gen.get_file_changes(commit.hash)

    assert len(changes) == 2
    paths = [c.path for c in changes]
    assert 'root.txt' in paths
    assert 'subdir/nested.txt' in paths


def test_get_latest_commit_for_path(repo):
    """Test finding the latest commit affecting a path"""
    diff_gen = DiffGenerator(repo)

    # Latest commit for README.md should be the second commit (modified it)
    latest = diff_gen.get_latest_commit_for_path(repo.second_commit.hash, 'README.md')
    assert latest is not None
    assert latest.hash == repo.second_commit.hash

    # Latest commit for LICENSE should be the first commit (was removed in second)
    latest_license = diff_gen.get_latest_commit_for_path(repo.second_commit.hash, 'LICENSE')
    assert latest_license is not None
    assert latest_license.hash == repo.second_commit.hash  # Removal counts as affecting

    # Latest commit for config.txt should be the second commit (was added)
    latest_config = diff_gen.get_latest_commit_for_path(repo.second_commit.hash, 'config.txt')
    assert latest_config is not None
    assert latest_config.hash == repo.second_commit.hash


def test_get_latest_commit_for_nonexistent_path(repo):
    """Test finding latest commit for a path that doesn't exist"""
    diff_gen = DiffGenerator(repo)

    # Nonexistent file should return None
    latest = diff_gen.get_latest_commit_for_path(repo.second_commit.hash, 'nonexistent.txt')
    assert latest is None
