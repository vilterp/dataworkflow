"""
Test Flask routes
"""
import pytest
from flask import Flask
from src.app import app as flask_app
from src.models import Repository as RepositoryModel
from src.repository import Repository
from src.storage import FilesystemStorage
from src.models.base import Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture
def app(temp_dir):
    """Create and configure a test Flask app"""
    flask_app.config['TESTING'] = True
    flask_app.config['DATABASE_URL'] = 'sqlite:///:memory:'

    # Setup database
    engine = create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db = Session()

    # Create repository
    repo_model = RepositoryModel(name='test-repo', description='Test repository')
    db.add(repo_model)
    db.commit()

    # Create sample data
    storage = FilesystemStorage(base_path=f"{temp_dir}/objects")
    repo = Repository(db, storage, repo_model.id)

    # Create a simple commit
    readme = repo.create_blob(b"# Test\nTest repository")
    tree = repo.create_tree([
        {'name': 'README.md', 'type': 'blob', 'hash': readme.hash, 'mode': '100644'}
    ])
    commit = repo.create_commit(
        tree_hash=tree.hash,
        message="Initial commit",
        author="Test User",
        author_email="test@example.com",
        parent_hash=None
    )
    repo.create_or_update_ref('refs/heads/main', commit.hash)

    db.close()

    yield flask_app


@pytest.fixture
def client(app):
    """Create a test client"""
    return app.test_client()


def test_repositories_list(client):
    """Test the repositories list page"""
    response = client.get('/')
    assert response.status_code == 200
    assert b'test-repo' in response.data


def test_repository_index(client):
    """Test repository homepage"""
    response = client.get('/test-repo')
    assert response.status_code == 200
    assert b'test-repo' in response.data
    assert b'README.md' in response.data


def test_branches_page(client):
    """Test branches list page"""
    response = client.get('/test-repo/branches')
    assert response.status_code == 200
    assert b'main' in response.data


def test_commits_page(client):
    """Test commits list page"""
    response = client.get('/test-repo/commits')
    assert response.status_code == 200
    assert b'Initial commit' in response.data


def test_commit_detail(client):
    """Test commit detail page"""
    # First get the commit hash from the commits page
    response = client.get('/test-repo/commits')
    # Extract commit hash from the response (this is a simple test, in production you'd get it from the DB)
    # For now we'll just test that the route exists

    # Get the repository to find the commit hash
    from src.config import Config
    from src.models.base import create_session

    db = create_session(Config.DATABASE_URL, echo=False)
    repo_model = db.query(RepositoryModel).filter(RepositoryModel.name == 'test-repo').first()
    from src.storage import FilesystemStorage
    import tempfile
    storage = FilesystemStorage(base_path=tempfile.mkdtemp())
    repo = Repository(db, storage, repo_model.id)

    ref = repo.get_ref('refs/heads/main')
    commit_hash = ref.commit_hash

    response = client.get(f'/test-repo/commit/{commit_hash}')
    assert response.status_code == 200
    assert b'Initial commit' in response.data
    db.close()


def test_blob_view(client):
    """Test blob view page with branch and path"""
    response = client.get('/test-repo/blob/main/README.md')
    assert response.status_code == 200
    assert b'Test repository' in response.data
    assert b'main' in response.data  # Branch should be displayed
    assert b'README.md' in response.data  # File path should be displayed


def test_404_repository_not_found(client):
    """Test 404 for non-existent repository"""
    response = client.get('/nonexistent-repo')
    assert response.status_code == 302  # Redirect
    # Follow redirect
    response = client.get('/nonexistent-repo', follow_redirects=True)
    assert b'not found' in response.data


def test_404_branch_not_found(client):
    """Test 404 for non-existent branch in blob view"""
    response = client.get('/test-repo/blob/nonexistent-branch/README.md')
    assert response.status_code == 302  # Redirect
    # Follow redirect
    response = client.get('/test-repo/blob/nonexistent-branch/README.md', follow_redirects=True)
    assert b'not found' in response.data


def test_404_file_not_found(client):
    """Test 404 for non-existent file in blob view"""
    response = client.get('/test-repo/blob/main/nonexistent.txt')
    assert response.status_code == 302  # Redirect


def test_tree_view(client):
    """Test tree view page with branch and path"""
    # Test root tree view
    response = client.get('/test-repo/tree/main/')
    assert response.status_code == 200
    assert b'main' in response.data
    assert b'README.md' in response.data
    assert b'Commits' in response.data  # Should have commit header


def test_404_tree_not_found(client):
    """Test 404 for non-existent directory in tree view"""
    response = client.get('/test-repo/tree/main/nonexistent-dir')
    assert response.status_code == 302  # Redirect
