#!/usr/bin/env python3
"""
Seed the database with sample data for demonstration.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import Config
from src.models.base import Base
from src.storage import FilesystemStorage
from src.repository import Repository


def seed_data():
    """Create sample commits and branches"""
    # Setup
    engine = create_engine(Config.DATABASE_URL, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db = Session()

    storage = FilesystemStorage()
    repo = Repository(db, storage)

    print("Creating sample data...\n")

    # Commit 1: Initial project setup
    print("1. Creating initial commit...")
    readme = repo.create_blob(b"# DataWorkflow\n\nA Git-like data versioning system.")
    gitignore = repo.create_blob(b"*.pyc\n__pycache__/\n.env\nvenv/")

    tree1 = repo.create_tree([
        {'name': '.gitignore', 'type': 'blob', 'hash': gitignore.hash, 'mode': '100644'},
        {'name': 'README.md', 'type': 'blob', 'hash': readme.hash, 'mode': '100644'},
    ])

    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit\n\nSet up project structure with README and gitignore",
        author="Alice Developer",
        author_email="alice@example.com",
        parent_hash=None
    )
    print(f"   Created: {commit1.hash[:7]} - {commit1.message.split(chr(10))[0]}")

    # Create main branch
    repo.create_or_update_ref('refs/heads/main', commit1.hash)
    print(f"   Created branch: main")

    # Commit 2: Add configuration
    print("\n2. Creating second commit...")
    config = repo.create_blob(b"DATABASE_URL=sqlite:///./dataworkflow.db\nDEBUG=true")
    readme_v2 = repo.create_blob(b"# DataWorkflow\n\nA Git-like data versioning system.\n\n## Features\n- Content-addressable storage\n- Git-like architecture")

    tree2 = repo.create_tree([
        {'name': '.gitignore', 'type': 'blob', 'hash': gitignore.hash, 'mode': '100644'},
        {'name': 'README.md', 'type': 'blob', 'hash': readme_v2.hash, 'mode': '100644'},
        {'name': 'config.env', 'type': 'blob', 'hash': config.hash, 'mode': '100644'},
    ])

    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message="Add configuration and update README",
        author="Bob Engineer",
        author_email="bob@example.com",
        parent_hash=commit1.hash
    )
    print(f"   Created: {commit2.hash[:7]} - {commit2.message}")

    # Update main
    repo.create_or_update_ref('refs/heads/main', commit2.hash)

    # Commit 3: Add code
    print("\n3. Creating third commit...")
    app_code = repo.create_blob(b"from flask import Flask\n\napp = Flask(__name__)\n\n@app.route('/')\ndef index():\n    return 'Hello, World!'\n")

    tree3 = repo.create_tree([
        {'name': '.gitignore', 'type': 'blob', 'hash': gitignore.hash, 'mode': '100644'},
        {'name': 'README.md', 'type': 'blob', 'hash': readme_v2.hash, 'mode': '100644'},
        {'name': 'app.py', 'type': 'blob', 'hash': app_code.hash, 'mode': '100755'},
        {'name': 'config.env', 'type': 'blob', 'hash': config.hash, 'mode': '100644'},
    ])

    commit3 = repo.create_commit(
        tree_hash=tree3.hash,
        message="Add Flask application",
        author="Alice Developer",
        author_email="alice@example.com",
        parent_hash=commit2.hash
    )
    print(f"   Created: {commit3.hash[:7]} - {commit3.message}")

    # Update main
    repo.create_or_update_ref('refs/heads/main', commit3.hash)

    # Create a tag
    repo.create_or_update_ref('refs/tags/v0.1.0', commit3.hash)
    print(f"   Created tag: v0.1.0")

    # Commit 4: Add tests
    print("\n4. Creating fourth commit...")
    test_code = repo.create_blob(b"import pytest\n\ndef test_example():\n    assert True\n")

    tree4 = repo.create_tree([
        {'name': '.gitignore', 'type': 'blob', 'hash': gitignore.hash, 'mode': '100644'},
        {'name': 'README.md', 'type': 'blob', 'hash': readme_v2.hash, 'mode': '100644'},
        {'name': 'app.py', 'type': 'blob', 'hash': app_code.hash, 'mode': '100755'},
        {'name': 'config.env', 'type': 'blob', 'hash': config.hash, 'mode': '100644'},
        {'name': 'test_app.py', 'type': 'blob', 'hash': test_code.hash, 'mode': '100644'},
    ])

    commit4 = repo.create_commit(
        tree_hash=tree4.hash,
        message="Add test suite",
        author="Bob Engineer",
        author_email="bob@example.com",
        parent_hash=commit3.hash
    )
    print(f"   Created: {commit4.hash[:7]} - {commit4.message}")

    # Update main
    repo.create_or_update_ref('refs/heads/main', commit4.hash)

    # Create develop branch from commit 3
    print("\n5. Creating develop branch...")
    repo.create_or_update_ref('refs/heads/develop', commit3.hash)
    print(f"   Created branch: develop (from {commit3.hash[:7]})")

    print("\n✅ Sample data created successfully!")
    print(f"\nCreated:")
    print(f"  - 4 commits")
    print(f"  - 2 branches (main, develop)")
    print(f"  - 1 tag (v0.1.0)")
    print(f"\nYou can now start the web server with:")
    print(f"  PYTHONPATH=. python src/app.py")

    db.close()


if __name__ == '__main__':
    seed_data()
