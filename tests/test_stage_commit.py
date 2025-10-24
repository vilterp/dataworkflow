"""
Tests for stage commit workflow - end to end integration test.
"""
import pytest
from src.models.base import create_session, init_db
# Import all models to ensure they're registered with Base.metadata
from src.models import (
    Repository as RepositoryModel,
    Commit, Tree, Blob, Ref,
    Stage, StageFile
)
from src.repository import Repository
from src.storage import FilesystemStorage


@pytest.fixture
def test_db():
    """Create a test database"""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    db_url = 'sqlite:///:memory:'

    # Create engine and session using the same engine instance
    # (critical for in-memory SQLite)
    engine = create_engine(db_url)

    # Create all tables
    from src.models.base import Base
    Base.metadata.create_all(bind=engine)

    # Create session from the same engine
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = Session()

    yield db
    db.close()


@pytest.fixture
def test_repo(test_db):
    """Create a test repository"""
    repo_model = RepositoryModel(name='test-repo', description='Test Repository')
    test_db.add(repo_model)
    test_db.commit()

    storage = FilesystemStorage()
    repo = Repository(repository_id=repo_model.id, db=test_db, storage=storage)

    return repo, test_db


def test_stage_commit_workflow(test_repo):
    """
    Test the complete workflow:
    1. Create an initial commit
    2. Create a stage
    3. Add files to the stage
    4. Commit the stage
    5. Verify ref was updated
    6. Verify new commit is visible
    """
    repo, db = test_repo

    # 1. Create initial commit
    blob1 = repo.create_blob(b'Initial content')
    tree1 = repo.create_tree([{
        'name': 'file1.txt',
        'type': 'blob',
        'hash': blob1.hash,
        'mode': '100644'
    }])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message='Initial commit',
        author='Test Author',
        author_email='test@example.com'
    )
    repo.create_or_update_ref('refs/heads/main', commit1.hash)

    # Verify initial state
    main_ref = repo.get_ref('refs/heads/main')
    assert main_ref.commit_hash == commit1.hash

    # 2. Create a stage
    stage = Stage(
        repository_id=repo.repository_id,
        name='test-stage',
        base_ref='refs/heads/main',
        description='Test stage'
    )
    db.add(stage)
    db.commit()

    # 3. Add files to the stage
    blob2 = repo.create_blob(b'New file content')
    blob3 = repo.create_blob(b'Modified content')

    stage_file1 = StageFile(
        stage_id=stage.id,
        path='file2.txt',
        blob_hash=blob2.hash
    )
    stage_file2 = StageFile(
        stage_id=stage.id,
        path='file1.txt',  # Modify existing file
        blob_hash=blob3.hash
    )
    db.add(stage_file1)
    db.add(stage_file2)
    db.commit()

    # 4. Commit the stage
    files = db.query(StageFile).filter(StageFile.stage_id == stage.id).all()
    assert len(files) == 2

    # Create tree from staged files
    tree_entries = []
    for file in files:
        tree_entries.append({
            'name': file.path,
            'type': 'blob',
            'hash': file.blob_hash,
            'mode': '100644'
        })

    tree2 = repo.create_tree(tree_entries)
    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message='Stage commit',
        author='Test Author',
        author_email='test@example.com',
        parent_hash=commit1.hash
    )

    # Update the ref
    repo.create_or_update_ref('refs/heads/main', commit2.hash)

    # Mark stage as committed
    stage.committed = True
    stage.commit_hash = commit2.hash
    stage.committed_ref = 'refs/heads/main'
    db.commit()

    # 5. Verify ref was updated
    main_ref = repo.get_ref('refs/heads/main')
    assert main_ref.commit_hash == commit2.hash
    assert main_ref.commit_hash != commit1.hash

    # 6. Verify new commit is visible and has correct content
    latest_commit = repo.get_commit(main_ref.commit_hash)
    assert latest_commit.hash == commit2.hash
    assert latest_commit.message == 'Stage commit'
    assert latest_commit.parent_hash == commit1.hash

    # Verify tree contents
    tree_contents = repo.get_tree_contents(latest_commit.tree_hash)
    assert len(tree_contents) == 2

    file_names = {entry.name for entry in tree_contents}
    assert 'file1.txt' in file_names
    assert 'file2.txt' in file_names

    # Verify file contents
    for entry in tree_contents:
        if entry.name == 'file1.txt':
            content = repo.get_blob_content(entry.hash)
            assert content == b'Modified content'
        elif entry.name == 'file2.txt':
            content = repo.get_blob_content(entry.hash)
            assert content == b'New file content'

    # Verify commit history
    history = repo.get_commit_history(main_ref.commit_hash, limit=10)
    assert len(history) == 2
    assert history[0].hash == commit2.hash
    assert history[1].hash == commit1.hash

    # Verify stage is marked as committed
    db.refresh(stage)
    assert stage.committed is True
    assert stage.commit_hash == commit2.hash
    assert stage.committed_ref == 'refs/heads/main'


def test_stage_commit_to_new_branch(test_repo):
    """
    Test committing a stage to a new branch.
    """
    repo, db = test_repo

    # Create initial commit on main
    blob1 = repo.create_blob(b'Main content')
    tree1 = repo.create_tree([{
        'name': 'file.txt',
        'type': 'blob',
        'hash': blob1.hash,
        'mode': '100644'
    }])
    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message='Main commit',
        author='Test',
        author_email='test@example.com'
    )
    repo.create_or_update_ref('refs/heads/main', commit1.hash)

    # Create a stage based on main
    stage = Stage(
        repository_id=repo.repository_id,
        name='feature-stage',
        base_ref='refs/heads/main'
    )
    db.add(stage)
    db.commit()

    # Add a file to the stage
    blob2 = repo.create_blob(b'Feature content')
    stage_file = StageFile(
        stage_id=stage.id,
        path='feature.txt',
        blob_hash=blob2.hash
    )
    db.add(stage_file)
    db.commit()

    # Commit to a new branch
    files = db.query(StageFile).filter(StageFile.stage_id == stage.id).all()
    tree_entries = [{'name': f.path, 'type': 'blob', 'hash': f.blob_hash, 'mode': '100644'} for f in files]

    tree2 = repo.create_tree(tree_entries)
    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message='Feature commit',
        author='Test',
        author_email='test@example.com',
        parent_hash=commit1.hash
    )

    # Create new branch
    new_branch_ref = 'refs/heads/feature-branch'
    repo.create_or_update_ref(new_branch_ref, commit2.hash)

    stage.committed = True
    stage.commit_hash = commit2.hash
    stage.committed_ref = new_branch_ref
    db.commit()

    # Verify main is unchanged
    main_ref = repo.get_ref('refs/heads/main')
    assert main_ref.commit_hash == commit1.hash

    # Verify new branch exists and points to new commit
    feature_ref = repo.get_ref(new_branch_ref)
    assert feature_ref is not None
    assert feature_ref.commit_hash == commit2.hash

    # Verify stage committed to correct branch
    assert stage.committed_ref == new_branch_ref
