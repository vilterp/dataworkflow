"""
Pytest configuration and shared fixtures.
"""

import tempfile
import shutil
import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base, init_db
from src.models import Repository as RepositoryModel
from src.storage import FilesystemStorage
from src.core import Repository


@pytest.fixture
def temp_dir():
    """Fixture that provides a temporary directory and cleans it up after test"""
    tmp = tempfile.mkdtemp()
    yield tmp
    shutil.rmtree(tmp)


@pytest.fixture
def repo(temp_dir):
    """Fixture that provides a configured repository instance"""
    storage = FilesystemStorage(base_path=f"{temp_dir}/objects")

    # Create engine and initialize tables
    engine = create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)

    # Create session from the same engine
    Session = sessionmaker(bind=engine)
    db = Session()

    # Create a repository model
    repo_model = RepositoryModel(name='test-repo', description='Test repository')
    db.add(repo_model)
    db.commit()

    repository = Repository(db, storage, repo_model.id)

    yield repository

    db.close()


@pytest.fixture
def app(temp_dir):
    """
    Create and configure a test Flask app.

    This fixture sets up a complete Flask application with:
    - Test database (SQLite)
    - Storage backend
    - Test repository with sample data
    """
    from src.app import app as flask_app
    from src.core.repository import TreeEntryInput
    from src.models.tree import EntryType

    # Use a persistent SQLite database file instead of in-memory
    db_path = f"{temp_dir}/test.db"
    database_url = f'sqlite:///{db_path}'

    flask_app.config['TESTING'] = True
    flask_app.config['DATABASE_URL'] = database_url
    flask_app.config['STORAGE_BASE_PATH'] = f"{temp_dir}/objects"

    # Setup database - create tables first
    init_db(database_url, echo=False)

    # Setup database session
    engine = create_engine(database_url, echo=False)
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
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=readme.hash, mode='100644')
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
    """
    Create a Flask test client.

    This fixture provides a test client for making HTTP requests to the Flask app.
    Automatically depends on the 'app' fixture.
    """
    return app.test_client()


@pytest.fixture
def db_session(app):
    """
    Create a database session for direct DB access in tests.

    This fixture provides a SQLAlchemy session that can be used to directly
    query and manipulate the database in tests. Automatically depends on the
    'app' fixture to ensure the database is initialized.
    """
    engine = create_engine(app.config['DATABASE_URL'], echo=False)
    Session = sessionmaker(bind=engine)
    db = Session()
    yield db
    db.close()
