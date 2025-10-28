"""
Tests for VFS diff functionality.

Tests the streaming diff algorithm that compares two VFS trees and yields
events for added, removed, and modified nodes.
"""
from src.core.repository import TreeEntryInput
from src.core.vfs_diff import diff_commits, diff_trees, AddedEvent, RemovedEvent, ModifiedEvent
from src.models.tree import EntryType
from src.models import StageRun, StageFile, StageRunStatus


def test_simple_file_addition(repo):
    """Test diffing when a file is added"""
    # Create initial commit with one file
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

    # Create second commit with an added file
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

    # Diff the commits
    events = list(diff_commits(repo, commit1.hash, commit2.hash))

    # Should have one added event
    assert len(events) == 1
    assert isinstance(events[0], AddedEvent)
    assert events[0].path == "main.py"
    assert events[0].after_blob.hash == blob2.hash

    print("\n✓ Simple file addition diff works")


def test_simple_file_removal(repo):
    """Test diffing when a file is removed"""
    # Create initial commit with two files
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

    # Diff the commits
    events = list(diff_commits(repo, commit1.hash, commit2.hash))

    # Should have one removed event
    assert len(events) == 1
    assert isinstance(events[0], RemovedEvent)
    assert events[0].path == "main.py"
    assert events[0].before_blob.hash == blob2.hash

    print("\n✓ Simple file removal diff works")


def test_simple_file_modification(repo):
    """Test diffing when a file is modified"""
    # Create initial commit
    blob1 = repo.create_blob(b"# README\nVersion 1")
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

    # Create second commit with modified file
    blob2 = repo.create_blob(b"# README\nVersion 2")
    tree2 = repo.create_tree([
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=blob2.hash, mode='100644')
    ])
    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message="Update README",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )

    # Diff the commits
    events = list(diff_commits(repo, commit1.hash, commit2.hash))

    # Should have one modified event
    assert len(events) == 1
    assert isinstance(events[0], ModifiedEvent)
    assert events[0].path == "README.md"
    assert events[0].before_blob.hash == blob1.hash
    assert events[0].after_blob.hash == blob2.hash

    print("\n✓ Simple file modification diff works")


def test_branch_scenario(repo):
    """
    Test the full branching scenario:
    1. Create repo
    2. Add a file
    3. Make a branch
    4. Edit the file
    5. Diff that branch with the base branch
    """
    # Step 1-2: Create initial commit with a file
    initial_blob = repo.create_blob(b"def hello():\n    print('Hello')")
    tree1 = repo.create_tree([
        TreeEntryInput(name='main.py', type=EntryType.BLOB, hash=initial_blob.hash, mode='100644')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )
    repo.create_or_update_ref('refs/heads/main', commit1.hash)

    # Step 3: Create a branch
    repo.create_branch('feature', commit1.hash)

    # Step 4: Edit the file on the feature branch
    modified_blob = repo.create_blob(b"def hello():\n    print('Hello, World!')")
    tree2 = repo.create_tree([
        TreeEntryInput(name='main.py', type=EntryType.BLOB, hash=modified_blob.hash, mode='100644')
    ])
    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message="Update greeting",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )
    repo.create_or_update_ref('refs/heads/feature', commit2.hash)

    # Step 5: Diff feature branch with main branch
    events = list(diff_commits(repo, commit1.hash, commit2.hash))

    # Should have one modified event
    assert len(events) == 1
    assert isinstance(events[0], ModifiedEvent)
    assert events[0].path == "main.py"
    assert events[0].before_blob.hash == initial_blob.hash
    assert events[0].after_blob.hash == modified_blob.hash

    # Verify content
    old_content = repo.get_blob_content(events[0].before_blob.hash)
    new_content = repo.get_blob_content(events[0].after_blob.hash)
    assert old_content == b"def hello():\n    print('Hello')"
    assert new_content == b"def hello():\n    print('Hello, World!')"

    print("\n✓ Branch scenario diff works")


def test_nested_directory_changes(repo):
    """Test diffing with nested directory structure"""
    # Create initial commit with nested structure
    file1 = repo.create_blob(b"file1")
    file2 = repo.create_blob(b"file2")

    subdir_tree1 = repo.create_tree([
        TreeEntryInput(name='file2.txt', type=EntryType.BLOB, hash=file2.hash, mode='100644')
    ])
    root_tree1 = repo.create_tree([
        TreeEntryInput(name='file1.txt', type=EntryType.BLOB, hash=file1.hash, mode='100644'),
        TreeEntryInput(name='subdir', type=EntryType.TREE, hash=subdir_tree1.hash, mode='040000')
    ])
    commit1 = repo.create_commit(
        tree_hash=root_tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create second commit with modified nested file and new file
    file2_modified = repo.create_blob(b"file2 modified")
    file3 = repo.create_blob(b"file3")

    subdir_tree2 = repo.create_tree([
        TreeEntryInput(name='file2.txt', type=EntryType.BLOB, hash=file2_modified.hash, mode='100644'),
        TreeEntryInput(name='file3.txt', type=EntryType.BLOB, hash=file3.hash, mode='100644')
    ])
    root_tree2 = repo.create_tree([
        TreeEntryInput(name='file1.txt', type=EntryType.BLOB, hash=file1.hash, mode='100644'),
        TreeEntryInput(name='subdir', type=EntryType.TREE, hash=subdir_tree2.hash, mode='040000')
    ])
    commit2 = repo.create_commit(
        tree_hash=root_tree2.hash,
        message="Modify and add files",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )

    # Diff the commits
    events = list(diff_commits(repo, commit1.hash, commit2.hash))

    # Should have two events: modified and added
    assert len(events) == 2

    # Sort by path for consistent testing
    events.sort(key=lambda e: e.path)

    # First event should be modified file2.txt
    assert isinstance(events[0], ModifiedEvent)
    assert events[0].path == "subdir/file2.txt"
    assert events[0].before_blob.hash == file2.hash
    assert events[0].after_blob.hash == file2_modified.hash

    # Second event should be added file3.txt
    assert isinstance(events[1], AddedEvent)
    assert events[1].path == "subdir/file3.txt"
    assert events[1].after_blob.hash == file3.hash

    print("\n✓ Nested directory changes diff works")


def test_directory_added(repo):
    """Test when an entire directory is added"""
    # Create initial commit with one file
    file1 = repo.create_blob(b"file1")
    tree1 = repo.create_tree([
        TreeEntryInput(name='file1.txt', type=EntryType.BLOB, hash=file1.hash, mode='100644')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create second commit with added directory
    file2 = repo.create_blob(b"file2")
    file3 = repo.create_blob(b"file3")

    new_dir = repo.create_tree([
        TreeEntryInput(name='file2.txt', type=EntryType.BLOB, hash=file2.hash, mode='100644'),
        TreeEntryInput(name='file3.txt', type=EntryType.BLOB, hash=file3.hash, mode='100644')
    ])
    tree2 = repo.create_tree([
        TreeEntryInput(name='file1.txt', type=EntryType.BLOB, hash=file1.hash, mode='100644'),
        TreeEntryInput(name='newdir', type=EntryType.TREE, hash=new_dir.hash, mode='040000')
    ])
    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message="Add directory",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )

    # Diff the commits
    events = list(diff_commits(repo, commit1.hash, commit2.hash))

    # Should have 3 events: directory + 2 files
    assert len(events) == 3

    # Sort by path
    events.sort(key=lambda e: e.path)

    # All should be added events
    assert all(isinstance(e, AddedEvent) for e in events)

    # Check paths
    assert events[0].path == "newdir"
    assert events[1].path == "newdir/file2.txt"
    assert events[2].path == "newdir/file3.txt"

    print("\n✓ Directory addition diff works")


def test_directory_removed(repo):
    """Test when an entire directory is removed"""
    # Create initial commit with directory
    file1 = repo.create_blob(b"file1")
    file2 = repo.create_blob(b"file2")
    file3 = repo.create_blob(b"file3")

    subdir = repo.create_tree([
        TreeEntryInput(name='file2.txt', type=EntryType.BLOB, hash=file2.hash, mode='100644'),
        TreeEntryInput(name='file3.txt', type=EntryType.BLOB, hash=file3.hash, mode='100644')
    ])
    tree1 = repo.create_tree([
        TreeEntryInput(name='file1.txt', type=EntryType.BLOB, hash=file1.hash, mode='100644'),
        TreeEntryInput(name='subdir', type=EntryType.TREE, hash=subdir.hash, mode='040000')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create second commit with directory removed
    tree2 = repo.create_tree([
        TreeEntryInput(name='file1.txt', type=EntryType.BLOB, hash=file1.hash, mode='100644')
    ])
    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message="Remove directory",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )

    # Diff the commits
    events = list(diff_commits(repo, commit1.hash, commit2.hash))

    # Should have 3 events: directory + 2 files
    assert len(events) == 3

    # Sort by path
    events.sort(key=lambda e: e.path)

    # All should be removed events
    assert all(isinstance(e, RemovedEvent) for e in events)

    # Check paths
    assert events[0].path == "subdir"
    assert events[1].path == "subdir/file2.txt"
    assert events[2].path == "subdir/file3.txt"

    print("\n✓ Directory removal diff works")


def test_diff_with_stage_runs(repo):
    """Test diffing when stage runs are added to a workflow file"""
    # Create initial commit with workflow file
    workflow_blob = repo.create_blob(b"def process(): pass")
    tree1 = repo.create_tree([
        TreeEntryInput(name='workflow.py', type=EntryType.BLOB, hash=workflow_blob.hash, mode='100644')
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Add workflow",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create second commit (same tree, but we'll add stage runs)
    commit2 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Run workflow",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )

    # Add stage run to commit2
    stage_run = StageRun(
        id=StageRun.compute_id(
            parent_stage_run_id=None,
            commit_hash=commit2.hash,
            workflow_file='workflow.py',
            stage_name='process',
            arguments='{}'
        ),
        parent_stage_run_id=None,
        arguments='{}',
        repo_name='test-repo',
        commit_hash=commit2.hash,
        workflow_file='workflow.py',
        stage_name='process',
        status=StageRunStatus.COMPLETED,
        triggered_by='test',
        trigger_event='manual'
    )
    repo.db.add(stage_run)

    # Add stage file
    output_blob = repo.create_blob(b"output content")
    stage_file = StageFile(
        id=StageFile.compute_id(stage_run.id, 'output.txt'),
        stage_run_id=stage_run.id,
        file_path='output.txt',
        content_hash=output_blob.hash,
        storage_key=output_blob.s3_key,
        size=len(b"output content")
    )
    repo.db.add(stage_file)
    repo.db.commit()

    # Diff the commits
    events = list(diff_commits(repo, commit1.hash, commit2.hash))

    # Should have 2 added events: stage run + stage file
    assert len(events) == 2

    # Sort by path
    events.sort(key=lambda e: e.path)

    # Both should be added events
    assert all(isinstance(e, AddedEvent) for e in events)

    # Check paths
    assert events[0].path == "workflow.py/process"
    assert events[1].path == "workflow.py/process/output.txt"

    print("\n✓ Diff with stage runs works")


def test_diff_with_modified_stage_outputs(repo):
    """Test diffing when stage run outputs change between commits"""
    # Create workflow file
    workflow_blob = repo.create_blob(b"def process(): pass")
    tree = repo.create_tree([
        TreeEntryInput(name='workflow.py', type=EntryType.BLOB, hash=workflow_blob.hash, mode='100644')
    ])

    # Create first commit with stage run
    commit1 = repo.create_commit(
        tree_hash=tree.hash,
        message="First run",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    stage_run1 = StageRun(
        id=StageRun.compute_id(
            parent_stage_run_id=None,
            commit_hash=commit1.hash,
            workflow_file='workflow.py',
            stage_name='process',
            arguments='{}'
        ),
        parent_stage_run_id=None,
        arguments='{}',
        repo_name='test-repo',
        commit_hash=commit1.hash,
        workflow_file='workflow.py',
        stage_name='process',
        status=StageRunStatus.COMPLETED,
        triggered_by='test',
        trigger_event='manual'
    )
    repo.db.add(stage_run1)

    output1_blob = repo.create_blob(b"output v1")
    stage_file1 = StageFile(
        id=StageFile.compute_id(stage_run1.id, 'output.txt'),
        stage_run_id=stage_run1.id,
        file_path='output.txt',
        content_hash=output1_blob.hash,
        storage_key=output1_blob.s3_key,
        size=len(b"output v1")
    )
    repo.db.add(stage_file1)
    repo.db.commit()

    # Create second commit with different stage run output
    commit2 = repo.create_commit(
        tree_hash=tree.hash,
        message="Second run",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )

    stage_run2 = StageRun(
        id=StageRun.compute_id(
            parent_stage_run_id=None,
            commit_hash=commit2.hash,
            workflow_file='workflow.py',
            stage_name='process',
            arguments='{}'
        ),
        parent_stage_run_id=None,
        arguments='{}',
        repo_name='test-repo',
        commit_hash=commit2.hash,
        workflow_file='workflow.py',
        stage_name='process',
        status=StageRunStatus.COMPLETED,
        triggered_by='test',
        trigger_event='manual'
    )
    repo.db.add(stage_run2)

    output2_blob = repo.create_blob(b"output v2")
    stage_file2 = StageFile(
        id=StageFile.compute_id(stage_run2.id, 'output.txt'),
        stage_run_id=stage_run2.id,
        file_path='output.txt',
        content_hash=output2_blob.hash,
        storage_key=output2_blob.s3_key,
        size=len(b"output v2")
    )
    repo.db.add(stage_file2)
    repo.db.commit()

    # Diff the commits
    events = list(diff_commits(repo, commit1.hash, commit2.hash))

    # Should have 1 modified event for the stage file
    assert len(events) == 1
    assert isinstance(events[0], ModifiedEvent)
    assert events[0].path == "workflow.py/process/output.txt"
    assert events[0].before_blob.hash == output1_blob.hash
    assert events[0].after_blob.hash == output2_blob.hash

    print("\n✓ Diff with modified stage outputs works")


def test_empty_diff(repo):
    """Test diff when nothing changed"""
    # Create commit
    blob = repo.create_blob(b"content")
    tree = repo.create_tree([
        TreeEntryInput(name='file.txt', type=EntryType.BLOB, hash=blob.hash, mode='100644')
    ])
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Diff with itself
    events = list(diff_commits(repo, commit.hash, commit.hash))

    # Should have no events
    assert len(events) == 0

    print("\n✓ Empty diff works")


def test_streaming_diff(repo):
    """Test that diff is actually streaming (yields events one at a time)"""
    # Create two commits with many changes
    blobs1 = [repo.create_blob(f"content {i}".encode()) for i in range(10)]
    tree1 = repo.create_tree([
        TreeEntryInput(name=f'file{i}.txt', type=EntryType.BLOB, hash=blob.hash, mode='100644')
        for i, blob in enumerate(blobs1[:5])
    ])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    tree2 = repo.create_tree([
        TreeEntryInput(name=f'file{i}.txt', type=EntryType.BLOB, hash=blob.hash, mode='100644')
        for i, blob in enumerate(blobs1)
    ])
    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message="Add more files",
        author="Test User",
        author_email="test@example.com",
        parent_hash=commit1.hash
    )

    # Verify we get a generator
    diff_gen = diff_commits(repo, commit1.hash, commit2.hash)
    assert hasattr(diff_gen, '__next__'), "diff_commits should return a generator"

    # Consume events one by one
    event_count = 0
    for event in diff_gen:
        event_count += 1
        assert isinstance(event, (AddedEvent, RemovedEvent, ModifiedEvent))

    assert event_count == 5  # 5 files added

    print("\n✓ Streaming diff works")
