"""
Tests for the VFS diff adapter.

The adapter converts new VFS diff events into the old FileDiff format
for backwards compatibility with existing templates.
"""
from src.core.repository import TreeEntryInput
from src.core.vfs_diff_adapter import get_commit_diff_legacy
from src.models.tree import EntryType
from src.diff import FileChangeType


def test_adapter_with_added_file(repo):
    """Test adapter converts file addition correctly"""
    # Create first commit
    blob1 = repo.create_blob(b"# README")
    tree1 = repo.create_tree([
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=blob1.hash, mode='100644')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create second commit with added file
    blob2 = repo.create_blob(b"print('hello')")
    tree2 = repo.create_tree([
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=blob1.hash, mode='100644'),
        TreeEntryInput(name='main.py', type=EntryType.BLOB, hash=blob2.hash, mode='100644'),
    ])
    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message="Add main.py",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )

    # Get diff using adapter
    file_diffs = get_commit_diff_legacy(repo, commit2.hash)

    # Should have one added file
    assert len(file_diffs) == 1
    assert file_diffs[0].path == "main.py"
    assert file_diffs[0].change_type == FileChangeType.ADDED
    assert file_diffs[0].new_hash == blob2.hash
    assert file_diffs[0].old_hash is None
    assert not file_diffs[0].is_binary
    assert len(file_diffs[0].lines) > 0
    assert all(line.change_type == 'add' for line in file_diffs[0].lines)

    print("\n✓ Adapter handles file addition")


def test_adapter_with_modified_file(repo):
    """Test adapter converts file modification correctly"""
    # Create first commit
    blob1 = repo.create_blob(b"version 1")
    tree1 = repo.create_tree([
        TreeEntryInput(name='file.txt', type=EntryType.BLOB, hash=blob1.hash, mode='100644')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create second commit with modified file
    blob2 = repo.create_blob(b"version 2")
    tree2 = repo.create_tree([
        TreeEntryInput(name='file.txt', type=EntryType.BLOB, hash=blob2.hash, mode='100644')
    ])
    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message="Update file",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )

    # Get diff using adapter
    file_diffs = get_commit_diff_legacy(repo, commit2.hash)

    # Should have one modified file
    assert len(file_diffs) == 1
    assert file_diffs[0].path == "file.txt"
    assert file_diffs[0].change_type == FileChangeType.MODIFIED
    assert file_diffs[0].old_hash == blob1.hash
    assert file_diffs[0].new_hash == blob2.hash
    assert not file_diffs[0].is_binary
    assert len(file_diffs[0].lines) > 0

    print("\n✓ Adapter handles file modification")


def test_adapter_with_removed_file(repo):
    """Test adapter converts file removal correctly"""
    # Create first commit with two files
    blob1 = repo.create_blob(b"# README")
    blob2 = repo.create_blob(b"print('hello')")
    tree1 = repo.create_tree([
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=blob1.hash, mode='100644'),
        TreeEntryInput(name='main.py', type=EntryType.BLOB, hash=blob2.hash, mode='100644'),
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create second commit with file removed
    tree2 = repo.create_tree([
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=blob1.hash, mode='100644'),
    ])
    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message="Remove main.py",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )

    # Get diff using adapter
    file_diffs = get_commit_diff_legacy(repo, commit2.hash)

    # Should have one removed file
    assert len(file_diffs) == 1
    assert file_diffs[0].path == "main.py"
    assert file_diffs[0].change_type == FileChangeType.REMOVED
    assert file_diffs[0].old_hash == blob2.hash
    assert file_diffs[0].new_hash is None
    assert not file_diffs[0].is_binary
    assert len(file_diffs[0].lines) > 0
    assert all(line.change_type == 'remove' for line in file_diffs[0].lines)

    print("\n✓ Adapter handles file removal")


def test_adapter_with_initial_commit(repo):
    """Test adapter handles initial commit (no parent)"""
    # Create initial commit
    blob1 = repo.create_blob(b"# README")
    blob2 = repo.create_blob(b"print('hello')")
    tree = repo.create_tree([
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=blob1.hash, mode='100644'),
        TreeEntryInput(name='main.py', type=EntryType.BLOB, hash=blob2.hash, mode='100644'),
    ])
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Get diff using adapter (no parent)
    file_diffs = get_commit_diff_legacy(repo, commit.hash)

    # Should have two added files
    assert len(file_diffs) == 2
    assert all(fd.change_type == FileChangeType.ADDED for fd in file_diffs)

    paths = {fd.path for fd in file_diffs}
    assert 'README.md' in paths
    assert 'main.py' in paths

    print("\n✓ Adapter handles initial commit")


def test_adapter_with_binary_file(repo):
    """Test adapter detects binary files"""
    # Create binary content (invalid UTF-8)
    binary_content = b'\x00\x01\x02\xff\xfe\xfd'
    blob = repo.create_blob(binary_content)
    tree = repo.create_tree([
        TreeEntryInput(name='binary.dat', type=EntryType.BLOB, hash=blob.hash, mode='100644')
    ])
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Add binary file",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Get diff using adapter
    file_diffs = get_commit_diff_legacy(repo, commit.hash)

    # Should detect as binary
    assert len(file_diffs) == 1
    assert file_diffs[0].is_binary
    assert len(file_diffs[0].lines) == 0  # No lines for binary files

    print("\n✓ Adapter detects binary files")


def test_adapter_with_nested_files(repo):
    """Test adapter handles nested directory structures"""
    # Create nested structure
    file1 = repo.create_blob(b"file1")
    file2 = repo.create_blob(b"file2")

    subdir = repo.create_tree([
        TreeEntryInput(name='file2.txt', type=EntryType.BLOB, hash=file2.hash, mode='100644')
    ])
    tree = repo.create_tree([
        TreeEntryInput(name='file1.txt', type=EntryType.BLOB, hash=file1.hash, mode='100644'),
        TreeEntryInput(name='subdir', type=EntryType.TREE, hash=subdir.hash, mode='040000')
    ])
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Get diff using adapter
    file_diffs = get_commit_diff_legacy(repo, commit.hash)

    # Should have two files (nested path included)
    assert len(file_diffs) == 2

    paths = {fd.path for fd in file_diffs}
    assert 'file1.txt' in paths
    assert 'subdir/file2.txt' in paths

    print("\n✓ Adapter handles nested directories")
