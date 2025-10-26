"""
Basic test cases for repository operations.
Tests creating a repo, making commits, and listing them.
"""
from src.core.repository import TreeEntryInput


def test_create_blob_and_retrieve(repo):
    """Test creating and retrieving a blob"""
    # Create blob
    content = b"Hello, World!"
    blob = repo.create_blob(content)

    assert blob is not None
    assert blob.size == len(content)
    assert len(blob.hash) == 64  # SHA-256

    # Retrieve content
    retrieved = repo.get_blob_content(blob.hash)
    assert retrieved == content


def test_create_commits_and_list(repo):
    """Test creating commits and listing history"""
    # Create first commit
    blob1 = repo.create_blob(b"# README\nInitial version")
    tree1 = repo.create_tree([
        TreeEntryInput(name='README.md', type='blob', hash=blob1.hash, mode='100644')
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
        TreeEntryInput(name='README.md', type='blob', hash=blob2.hash, mode='100644')
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


def test_tree_with_multiple_files(repo):
    """Test creating a tree with multiple files"""
    # Create multiple blobs
    blob1 = repo.create_blob(b"File 1 content")
    blob2 = repo.create_blob(b"File 2 content")
    blob3 = repo.create_blob(b"File 3 content")

    # Create tree
    tree = repo.create_tree([
        TreeEntryInput(name='file1.txt', type='blob', hash=blob1.hash, mode='100644'),
        TreeEntryInput(name='file2.txt', type='blob', hash=blob2.hash, mode='100644'),
        TreeEntryInput(name='file3.txt', type='blob', hash=blob3.hash, mode='100644'),
    ])

    # Verify tree contents
    entries = repo.get_tree_contents(tree.hash)
    assert len(entries) == 3
    assert entries[0].name == 'file1.txt'
    assert entries[1].name == 'file2.txt'
    assert entries[2].name == 'file3.txt'

    print("\n✓ Test passed: Created tree with 3 files")


def test_delete_file_from_root(repo):
    """Test deleting a file from the root directory"""
    # Create initial commit with multiple files
    blob1 = repo.create_blob(b"# README\nProject readme")
    blob2 = repo.create_blob(b"print('Hello, World!')")
    blob3 = repo.create_blob(b"# Config file\nkey=value")

    tree1 = repo.create_tree([
        TreeEntryInput(name='README.md', type='blob', hash=blob1.hash, mode='100644'),
        TreeEntryInput(name='main.py', type='blob', hash=blob2.hash, mode='100644'),
        TreeEntryInput(name='config.ini', type='blob', hash=blob3.hash, mode='100644'),
    ])

    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit with 3 files",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Delete main.py
    commit2 = repo.delete_file(
        base_commit_hash=commit1.hash,
        file_path="main.py",
        message="Delete main.py",
        author="Test User",
        author_email="test@example.com"
    )

    # Verify the file was deleted
    entries = repo.get_tree_contents(commit2.tree_hash)
    assert len(entries) == 2
    assert entries[0].name == 'README.md'
    assert entries[1].name == 'config.ini'

    # Verify original commit still has 3 files
    original_entries = repo.get_tree_contents(commit1.tree_hash)
    assert len(original_entries) == 3

    print("\n✓ Test passed: Successfully deleted file from root directory")


def test_delete_file_from_nested_directory(repo):
    """Test deleting a file from a nested directory"""
    # Create blobs
    readme_blob = repo.create_blob(b"# README")
    main_blob = repo.create_blob(b"def main(): pass")
    helper_blob = repo.create_blob(b"def helper(): pass")
    test_blob = repo.create_blob(b"def test(): pass")

    # Create nested tree structure: src/utils/helper.py
    utils_tree = repo.create_tree([
        TreeEntryInput(name='helper.py', type='blob', hash=helper_blob.hash, mode='100644'),
        TreeEntryInput(name='test.py', type='blob', hash=test_blob.hash, mode='100644'),
    ])

    src_tree = repo.create_tree([
        TreeEntryInput(name='main.py', type='blob', hash=main_blob.hash, mode='100644'),
        TreeEntryInput(name='utils', type='tree', hash=utils_tree.hash, mode='040000'),
    ])

    root_tree = repo.create_tree([
        TreeEntryInput(name='README.md', type='blob', hash=readme_blob.hash, mode='100644'),
        TreeEntryInput(name='src', type='tree', hash=src_tree.hash, mode='040000'),
    ])

    commit1 = repo.create_commit(
        tree_hash=root_tree.hash,
        message="Initial commit with nested structure",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Delete src/utils/helper.py
    commit2 = repo.delete_file(
        base_commit_hash=commit1.hash,
        file_path="src/utils/helper.py",
        message="Delete src/utils/helper.py",
        author="Test User",
        author_email="test@example.com"
    )

    # Navigate to src/utils and verify helper.py is gone
    new_root_entries = repo.get_tree_contents(commit2.tree_hash)
    assert len(new_root_entries) == 2

    # Find src tree
    src_entry = next(e for e in new_root_entries if e.name == 'src')
    src_entries = repo.get_tree_contents(src_entry.hash)

    # Find utils tree
    utils_entry = next(e for e in src_entries if e.name == 'utils')
    utils_entries = repo.get_tree_contents(utils_entry.hash)

    # Should only have test.py left
    assert len(utils_entries) == 1
    assert utils_entries[0].name == 'test.py'

    print("\n✓ Test passed: Successfully deleted file from nested directory")


def test_delete_nonexistent_file_fails(repo):
    """Test that deleting a nonexistent file raises an error"""
    # Create initial commit
    blob1 = repo.create_blob(b"# README")
    tree1 = repo.create_tree([
        TreeEntryInput(name='README.md', type='blob', hash=blob1.hash, mode='100644')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Try to delete a file that doesn't exist
    try:
        repo.delete_file(
            base_commit_hash=commit1.hash,
            file_path="nonexistent.txt",
            message="Delete nonexistent file",
            author="Test User",
            author_email="test@example.com"
        )
        assert False, "Expected ValueError to be raised"
    except ValueError as e:
        assert "not found" in str(e)

    print("\n✓ Test passed: Deleting nonexistent file raises ValueError")


def test_update_file_in_root(repo):
    """Test updating a file in the root directory"""
    # Create initial commit with multiple files
    blob1 = repo.create_blob(b"# README\nInitial version")
    blob2 = repo.create_blob(b"print('Hello, World!')")

    tree1 = repo.create_tree([
        TreeEntryInput(name='README.md', type='blob', hash=blob1.hash, mode='100644'),
        TreeEntryInput(name='main.py', type='blob', hash=blob2.hash, mode='100644'),
    ])

    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create branch
    repo.create_or_update_ref('refs/heads/main', commit1.hash)

    # Update README.md
    new_content = b"# README\nUpdated version with new content"
    commit2 = repo.update_file(
        branch='main',
        file_path='README.md',
        content=new_content,
        commit_message='Update README',
        author_name='Test User',
        author_email='test@example.com'
    )

    # Verify the file was updated
    entries = repo.get_tree_contents(commit2.tree_hash)
    assert len(entries) == 2

    # Find README.md entry
    readme_entry = next(e for e in entries if e.name == 'README.md')
    updated_content = repo.get_blob_content(readme_entry.hash)
    assert updated_content == new_content

    # Verify commit parent is correct
    assert commit2.parent_hash == commit1.hash

    # Verify branch ref was updated
    ref = repo.get_ref('refs/heads/main')
    assert ref.commit_hash == commit2.hash

    # Verify original commit unchanged
    original_entries = repo.get_tree_contents(commit1.tree_hash)
    original_readme = next(e for e in original_entries if e.name == 'README.md')
    original_content = repo.get_blob_content(original_readme.hash)
    assert original_content == b"# README\nInitial version"

    print("\n✓ Test passed: Successfully updated file in root directory")


def test_update_file_in_nested_directory(repo):
    """Test updating a file in a nested directory"""
    # Create blobs
    readme_blob = repo.create_blob(b"# README")
    main_blob = repo.create_blob(b"def main(): pass")
    helper_blob = repo.create_blob(b"def helper(): return 'original'")

    # Create nested tree structure: src/utils/helper.py
    utils_tree = repo.create_tree([
        TreeEntryInput(name='helper.py', type='blob', hash=helper_blob.hash, mode='100644'),
    ])

    src_tree = repo.create_tree([
        TreeEntryInput(name='main.py', type='blob', hash=main_blob.hash, mode='100644'),
        TreeEntryInput(name='utils', type='tree', hash=utils_tree.hash, mode='040000'),
    ])

    root_tree = repo.create_tree([
        TreeEntryInput(name='README.md', type='blob', hash=readme_blob.hash, mode='100644'),
        TreeEntryInput(name='src', type='tree', hash=src_tree.hash, mode='040000'),
    ])

    commit1 = repo.create_commit(
        tree_hash=root_tree.hash,
        message="Initial commit with nested structure",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create branch
    repo.create_or_update_ref('refs/heads/main', commit1.hash)

    # Update src/utils/helper.py
    new_content = b"def helper(): return 'updated'"
    commit2 = repo.update_file(
        branch='main',
        file_path='src/utils/helper.py',
        content=new_content,
        commit_message='Update helper function',
        author_name='Test User',
        author_email='test@example.com'
    )

    # Navigate to src/utils and verify helper.py was updated
    new_root_entries = repo.get_tree_contents(commit2.tree_hash)
    src_entry = next(e for e in new_root_entries if e.name == 'src')
    src_entries = repo.get_tree_contents(src_entry.hash)
    utils_entry = next(e for e in src_entries if e.name == 'utils')
    utils_entries = repo.get_tree_contents(utils_entry.hash)
    helper_entry = next(e for e in utils_entries if e.name == 'helper.py')

    updated_content = repo.get_blob_content(helper_entry.hash)
    assert updated_content == new_content

    # Verify commit parent
    assert commit2.parent_hash == commit1.hash

    print("\n✓ Test passed: Successfully updated file in nested directory")


def test_update_file_creates_new_file(repo):
    """Test that update_file creates a new file if it doesn't exist"""
    # Create initial commit with one file
    blob1 = repo.create_blob(b"# README")
    tree1 = repo.create_tree([
        TreeEntryInput(name='README.md', type='blob', hash=blob1.hash, mode='100644')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create branch
    repo.create_or_update_ref('refs/heads/main', commit1.hash)

    # Update (create) new file
    new_content = b"print('New file!')"
    commit2 = repo.update_file(
        branch='main',
        file_path='new.py',
        content=new_content,
        commit_message='Add new.py',
        author_name='Test User',
        author_email='test@example.com'
    )

    # Verify the file was created
    entries = repo.get_tree_contents(commit2.tree_hash)
    assert len(entries) == 2

    new_entry = next(e for e in entries if e.name == 'new.py')
    assert new_entry is not None

    content = repo.get_blob_content(new_entry.hash)
    assert content == new_content

    print("\n✓ Test passed: Successfully created new file with update_file")


def test_update_file_nonexistent_branch_fails(repo):
    """Test that updating a file on a nonexistent branch raises an error"""
    # Create initial commit
    blob1 = repo.create_blob(b"# README")
    tree1 = repo.create_tree([
        TreeEntryInput(name='README.md', type='blob', hash=blob1.hash, mode='100644')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Don't create any branches

    # Try to update file on nonexistent branch
    try:
        repo.update_file(
            branch='nonexistent',
            file_path='README.md',
            content=b"Updated",
            commit_message='Update',
            author_name='Test User',
            author_email='test@example.com'
        )
        assert False, "Expected ValueError to be raised"
    except ValueError as e:
        assert "not found" in str(e)

    print("\n✓ Test passed: Updating file on nonexistent branch raises ValueError")


def test_create_branch(repo):
    """Test creating a new branch"""
    # Create initial commit
    blob1 = repo.create_blob(b"# README")
    tree1 = repo.create_tree([
        TreeEntryInput(name='README.md', type='blob', hash=blob1.hash, mode='100644')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create main branch
    repo.create_or_update_ref('refs/heads/main', commit1.hash)

    # Create a new branch from commit1
    new_branch = repo.create_branch('feature-x', commit1.hash)

    assert new_branch is not None
    assert new_branch.name == 'feature-x'
    assert new_branch.commit_hash == commit1.hash
    assert new_branch.id == 'refs/heads/feature-x'

    # Verify we can retrieve the branch
    retrieved = repo.get_ref('refs/heads/feature-x')
    assert retrieved is not None
    assert retrieved.commit_hash == commit1.hash

    print("\n✓ Test passed: Successfully created new branch")


def test_create_branch_already_exists(repo):
    """Test that creating a branch that already exists raises an error"""
    # Create initial commit
    blob1 = repo.create_blob(b"# README")
    tree1 = repo.create_tree([
        TreeEntryInput(name='README.md', type='blob', hash=blob1.hash, mode='100644')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create main branch
    repo.create_or_update_ref('refs/heads/main', commit1.hash)

    # Create a branch
    repo.create_branch('feature-x', commit1.hash)

    # Try to create the same branch again
    try:
        repo.create_branch('feature-x', commit1.hash)
        assert False, "Expected ValueError to be raised"
    except ValueError as e:
        assert "already exists" in str(e)

    print("\n✓ Test passed: Creating duplicate branch raises ValueError")
