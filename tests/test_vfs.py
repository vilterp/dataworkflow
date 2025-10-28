"""
Tests for the Virtual File System (VFS) abstraction.

The VFS unifies git objects (trees, blobs) with workflow execution data
(stage runs, stage files) into a single traversable tree structure.
"""
from src.core.repository import TreeEntryInput
from src.core.vfs import TreeNode, BlobNode, StageRunNode, StageFileNode
from src.utils.vfs_pretty import pretty_print_tree
from src.models.tree import EntryType
from src.models import StageRun, StageFile, StageRunStatus


def test_simple_tree_structure(repo):
    """Test VFS with a simple tree of base files (no stage runs)"""
    # Create blobs
    blob1 = repo.create_blob(b"# README")
    blob2 = repo.create_blob(b"print('hello')")
    blob3 = repo.create_blob(b"def test(): pass")

    # Create tree
    tree = repo.create_tree([
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=blob1.hash, mode='100644'),
        TreeEntryInput(name='main.py', type=EntryType.BLOB, hash=blob2.hash, mode='100644'),
        TreeEntryInput(name='test.py', type=EntryType.BLOB, hash=blob3.hash, mode='100644'),
    ])

    # Create commit
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Get virtual tree root
    root = repo.get_root(commit.hash)

    # Check root
    assert isinstance(root, TreeNode)
    assert root.name == ""

    # Get children
    children = root.get_children()
    assert len(children) == 3

    # Verify children names and types
    child_dict = {name: node for name, node in children}
    assert 'README.md' in child_dict
    assert 'main.py' in child_dict
    assert 'test.py' in child_dict

    assert isinstance(child_dict['README.md'], BlobNode)
    assert isinstance(child_dict['main.py'], BlobNode)

    # Pretty print for visual inspection
    tree_str = pretty_print_tree(root)
    print("\n" + tree_str)

    # Assert on the structure
    assert "README.md/ # base blob" in tree_str
    assert "main.py/ # base blob" in tree_str
    assert "test.py/ # base blob" in tree_str


def test_nested_tree_structure(repo):
    """Test VFS with nested directories"""
    # Create blobs
    readme_blob = repo.create_blob(b"# README")
    main_blob = repo.create_blob(b"def main(): pass")
    helper_blob = repo.create_blob(b"def helper(): pass")

    # Create nested tree structure: src/utils/helper.py
    utils_tree = repo.create_tree([
        TreeEntryInput(name='helper.py', type=EntryType.BLOB, hash=helper_blob.hash, mode='100644'),
    ])

    src_tree = repo.create_tree([
        TreeEntryInput(name='main.py', type=EntryType.BLOB, hash=main_blob.hash, mode='100644'),
        TreeEntryInput(name='utils', type=EntryType.TREE, hash=utils_tree.hash, mode='040000'),
    ])

    root_tree = repo.create_tree([
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=readme_blob.hash, mode='100644'),
        TreeEntryInput(name='src', type=EntryType.TREE, hash=src_tree.hash, mode='040000'),
    ])

    commit = repo.create_commit(
        tree_hash=root_tree.hash,
        message="Nested structure",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Get virtual tree root
    root = repo.get_root(commit.hash)

    # Pretty print
    tree_str = pretty_print_tree(root)
    print("\n" + tree_str)

    # Assert on the structure
    assert "README.md/ # base blob" in tree_str
    assert "src # base tree" in tree_str
    assert "main.py/ # base blob" in tree_str
    assert "utils # base tree" in tree_str
    assert "helper.py/ # base blob" in tree_str


def test_tree_with_stage_runs(repo):
    """Test VFS with stage runs attached to workflow files"""
    # Create a workflow file
    workflow_blob = repo.create_blob(b"def process(): pass")
    tree = repo.create_tree([
        TreeEntryInput(name='workflow.py', type=EntryType.BLOB, hash=workflow_blob.hash, mode='100644'),
    ])

    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Add workflow",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create a stage run for this workflow
    stage_run = StageRun(
        id=StageRun.compute_id(
            parent_stage_run_id=None,
            commit_hash=commit.hash,
            workflow_file='workflow.py',
            stage_name='process',
            arguments='{}'
        ),
        parent_stage_run_id=None,
        arguments='{}',
        repo_name='test-repo',
        commit_hash=commit.hash,
        workflow_file='workflow.py',
        stage_name='process',
        status=StageRunStatus.COMPLETED,
        triggered_by='test',
        trigger_event='manual'
    )
    repo.db.add(stage_run)
    repo.db.commit()

    # Create stage files for this run
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

    # Get virtual tree root
    root = repo.get_root(commit.hash)

    # Pretty print
    tree_str = pretty_print_tree(root)
    print("\n" + tree_str)

    # Assert on the structure
    expected_structure = """└── workflow.py/ # base blob
    └── process/ # StageRun
        └── output.txt # StageFile"""

    # Check that workflow.py appears
    assert "workflow.py/ # base blob" in tree_str
    # Check that stage run appears as child
    assert "process/ # StageRun" in tree_str
    # Check that stage file appears
    assert "output.txt # StageFile" in tree_str


def test_tree_with_nested_stage_runs(repo):
    """Test VFS with nested stage runs (parent -> child stages)"""
    # Create a workflow file
    workflow_blob = repo.create_blob(b"def main(): nested()")
    tree = repo.create_tree([
        TreeEntryInput(name='workflow.py', type=EntryType.BLOB, hash=workflow_blob.hash, mode='100644'),
    ])

    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Add workflow",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create parent stage run
    parent_stage_run = StageRun(
        id=StageRun.compute_id(
            parent_stage_run_id=None,
            commit_hash=commit.hash,
            workflow_file='workflow.py',
            stage_name='main',
            arguments='{}'
        ),
        parent_stage_run_id=None,
        arguments='{}',
        repo_name='test-repo',
        commit_hash=commit.hash,
        workflow_file='workflow.py',
        stage_name='main',
        status=StageRunStatus.COMPLETED,
        triggered_by='test',
        trigger_event='manual'
    )
    repo.db.add(parent_stage_run)
    repo.db.commit()

    # Create child stage run
    child_stage_run = StageRun(
        id=StageRun.compute_id(
            parent_stage_run_id=parent_stage_run.id,
            commit_hash=commit.hash,
            workflow_file='workflow.py',
            stage_name='nested',
            arguments='{}'
        ),
        parent_stage_run_id=parent_stage_run.id,
        arguments='{}',
        repo_name='test-repo',
        commit_hash=commit.hash,
        workflow_file='workflow.py',
        stage_name='nested',
        status=StageRunStatus.COMPLETED,
        triggered_by='test',
        trigger_event='manual'
    )
    repo.db.add(child_stage_run)
    repo.db.commit()

    # Create stage files for both runs
    parent_output_blob = repo.create_blob(b"parent output")
    parent_stage_file = StageFile(
        id=StageFile.compute_id(parent_stage_run.id, 'output.txt'),
        stage_run_id=parent_stage_run.id,
        file_path='output.txt',
        content_hash=parent_output_blob.hash,
        storage_key=parent_output_blob.s3_key,
        size=len(b"parent output")
    )
    repo.db.add(parent_stage_file)

    child_output_blob = repo.create_blob(b"child output")
    child_stage_file = StageFile(
        id=StageFile.compute_id(child_stage_run.id, 'nested_output.txt'),
        stage_run_id=child_stage_run.id,
        file_path='nested_output.txt',
        content_hash=child_output_blob.hash,
        storage_key=child_output_blob.s3_key,
        size=len(b"child output")
    )
    repo.db.add(child_stage_file)
    repo.db.commit()

    # Get virtual tree root
    root = repo.get_root(commit.hash)

    # Pretty print
    tree_str = pretty_print_tree(root)
    print("\n" + tree_str)

    # Assert on the structure
    assert "workflow.py/ # base blob" in tree_str
    assert "main/ # StageRun" in tree_str
    assert "output.txt # StageFile" in tree_str
    assert "nested/ # StageRun" in tree_str
    assert "nested_output.txt # StageFile" in tree_str

    # Verify tree structure matches expected format
    lines = tree_str.split('\n')
    # Find the workflow.py line
    workflow_idx = next(i for i, line in enumerate(lines) if 'workflow.py' in line)
    # The next line should be main stage run
    assert 'main/ # StageRun' in lines[workflow_idx + 1]


def test_complex_tree_structure(repo):
    """
    Test a complex tree with:
    - Multiple files and directories
    - Multiple workflow files with stage runs
    - Nested stage runs
    """
    # Create blobs
    readme_blob = repo.create_blob(b"# README")
    workflow1_blob = repo.create_blob(b"def stage1(): pass")
    workflow2_blob = repo.create_blob(b"def stage2(): pass")
    data_blob = repo.create_blob(b"data,values")

    # Create tree structure
    data_tree = repo.create_tree([
        TreeEntryInput(name='input.csv', type=EntryType.BLOB, hash=data_blob.hash, mode='100644'),
    ])

    root_tree = repo.create_tree([
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=readme_blob.hash, mode='100644'),
        TreeEntryInput(name='workflow1.py', type=EntryType.BLOB, hash=workflow1_blob.hash, mode='100644'),
        TreeEntryInput(name='workflow2.py', type=EntryType.BLOB, hash=workflow2_blob.hash, mode='100644'),
        TreeEntryInput(name='data', type=EntryType.TREE, hash=data_tree.hash, mode='040000'),
    ])

    commit = repo.create_commit(
        tree_hash=root_tree.hash,
        message="Complex structure",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create stage runs for workflow1
    stage_run1 = StageRun(
        id=StageRun.compute_id(
            parent_stage_run_id=None,
            commit_hash=commit.hash,
            workflow_file='workflow1.py',
            stage_name='stage1',
            arguments='{}'
        ),
        parent_stage_run_id=None,
        arguments='{}',
        repo_name='test-repo',
        commit_hash=commit.hash,
        workflow_file='workflow1.py',
        stage_name='stage1',
        status=StageRunStatus.COMPLETED,
        triggered_by='test',
        trigger_event='manual'
    )
    repo.db.add(stage_run1)

    # Create stage runs for workflow2 with nested stage
    stage_run2 = StageRun(
        id=StageRun.compute_id(
            parent_stage_run_id=None,
            commit_hash=commit.hash,
            workflow_file='workflow2.py',
            stage_name='stage2',
            arguments='{}'
        ),
        parent_stage_run_id=None,
        arguments='{}',
        repo_name='test-repo',
        commit_hash=commit.hash,
        workflow_file='workflow2.py',
        stage_name='stage2',
        status=StageRunStatus.COMPLETED,
        triggered_by='test',
        trigger_event='manual'
    )
    repo.db.add(stage_run2)
    repo.db.commit()

    # Add nested stage for stage2
    nested_stage = StageRun(
        id=StageRun.compute_id(
            parent_stage_run_id=stage_run2.id,
            commit_hash=commit.hash,
            workflow_file='workflow2.py',
            stage_name='nested_stage',
            arguments='{}'
        ),
        parent_stage_run_id=stage_run2.id,
        arguments='{}',
        repo_name='test-repo',
        commit_hash=commit.hash,
        workflow_file='workflow2.py',
        stage_name='nested_stage',
        status=StageRunStatus.COMPLETED,
        triggered_by='test',
        trigger_event='manual'
    )
    repo.db.add(nested_stage)

    # Create stage files
    output1_blob = repo.create_blob(b"output1")
    stage_file1 = StageFile(
        id=StageFile.compute_id(stage_run1.id, 'out1.txt'),
        stage_run_id=stage_run1.id,
        file_path='out1.txt',
        content_hash=output1_blob.hash,
        storage_key=output1_blob.s3_key,
        size=len(b"output1")
    )
    repo.db.add(stage_file1)

    output2_blob = repo.create_blob(b"output2")
    stage_file2 = StageFile(
        id=StageFile.compute_id(nested_stage.id, 'out2.txt'),
        stage_run_id=nested_stage.id,
        file_path='out2.txt',
        content_hash=output2_blob.hash,
        storage_key=output2_blob.s3_key,
        size=len(b"output2")
    )
    repo.db.add(stage_file2)
    repo.db.commit()

    # Get virtual tree root
    root = repo.get_root(commit.hash)

    # Pretty print
    tree_str = pretty_print_tree(root)
    print("\n" + tree_str)

    # Assert expected structure components
    assert "README.md/ # base blob" in tree_str
    assert "workflow1.py/ # base blob" in tree_str
    assert "workflow2.py/ # base blob" in tree_str
    assert "data # base tree" in tree_str
    assert "input.csv/ # base blob" in tree_str

    # Assert stage runs and files
    assert "stage1/ # StageRun" in tree_str
    assert "out1.txt # StageFile" in tree_str
    assert "stage2/ # StageRun" in tree_str
    assert "nested_stage/ # StageRun" in tree_str
    assert "out2.txt # StageFile" in tree_str


def test_get_content_for_blobs(repo):
    """Test that get_content() works for blob nodes"""
    # Create blob
    blob = repo.create_blob(b"test content")
    tree = repo.create_tree([
        TreeEntryInput(name='test.txt', type=EntryType.BLOB, hash=blob.hash, mode='100644'),
    ])
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Test content",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Get virtual tree root
    root = repo.get_root(commit.hash)
    children = root.get_children()

    # Get the blob node
    test_node = next(node for name, node in children if name == 'test.txt')

    # Get content
    blob_obj = test_node.get_content()
    assert blob_obj is not None
    assert blob_obj.hash == blob.hash

    # Verify actual content
    content = repo.get_blob_content(blob_obj.hash)
    assert content == b"test content"


def test_get_content_for_stage_files(repo):
    """Test that get_content() works for stage file nodes"""
    # Create workflow
    workflow_blob = repo.create_blob(b"def process(): pass")
    tree = repo.create_tree([
        TreeEntryInput(name='workflow.py', type=EntryType.BLOB, hash=workflow_blob.hash, mode='100644'),
    ])
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Add workflow",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )

    # Create stage run
    stage_run = StageRun(
        id=StageRun.compute_id(
            parent_stage_run_id=None,
            commit_hash=commit.hash,
            workflow_file='workflow.py',
            stage_name='process',
            arguments='{}'
        ),
        parent_stage_run_id=None,
        arguments='{}',
        repo_name='test-repo',
        commit_hash=commit.hash,
        workflow_file='workflow.py',
        stage_name='process',
        status=StageRunStatus.COMPLETED,
        triggered_by='test',
        trigger_event='manual'
    )
    repo.db.add(stage_run)
    repo.db.commit()

    # Create stage file
    output_blob = repo.create_blob(b"stage output content")
    stage_file = StageFile(
        id=StageFile.compute_id(stage_run.id, 'output.txt'),
        stage_run_id=stage_run.id,
        file_path='output.txt',
        content_hash=output_blob.hash,
        storage_key=output_blob.s3_key,
        size=len(b"stage output content")
    )
    repo.db.add(stage_file)
    repo.db.commit()

    # Navigate to stage file node
    root = repo.get_root(commit.hash)
    workflow_node = next(node for name, node in root.get_children() if name == 'workflow.py')
    stage_run_node = next(node for name, node in workflow_node.get_children() if name == 'process')
    stage_file_node = next(node for name, node in stage_run_node.get_children() if name == 'output.txt')

    # Get content
    blob_obj = stage_file_node.get_content()
    assert blob_obj is not None
    assert blob_obj.hash == output_blob.hash

    # Verify actual content
    content = repo.get_blob_content(blob_obj.hash)
    assert content == b"stage output content"
