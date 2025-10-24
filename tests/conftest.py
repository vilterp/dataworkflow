"""
Pytest configuration and shared fixtures.
"""

import tempfile
import shutil
import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.storage import FilesystemStorage
from src.repository import Repository


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

    repository = Repository(db, storage)

    yield repository

    db.close()
