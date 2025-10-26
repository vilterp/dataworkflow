#!/usr/bin/env python3
"""
Seed the database with sample data for demonstration.
"""

from src.core.repository import TreeEntryInput
from src.models.tree import EntryType
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import Config
from src.models.base import Base
from src.models import Repository as RepositoryModel
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

    # Create repository model
    print("Creating sample repository...\n")
    repo_model = RepositoryModel(
        name='example-blog',
        description='A sample blog application'
    )
    db.add(repo_model)
    db.commit()
    print(f"Created repository: {repo_model.name}\n")

    repo = Repository(db, storage, repo_model.id)

    print("Creating sample commits...\n")

    # Commit 1: Initial project setup
    print("1. Creating initial commit...")
    readme = repo.create_blob(b"# My Blog\n\nA simple blog application built with Flask.\n\n## Features\n- Create and edit posts\n- User authentication\n- Markdown support")
    gitignore = repo.create_blob(b"*.pyc\n__pycache__/\n.env\nvenv/\n*.db")

    tree1 = repo.create_tree([
        TreeEntryInput(name='.gitignore', type=EntryType.BLOB, hash=gitignore.hash, mode='100644'),
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=readme.hash, mode='100644'),
    ])

    commit1 = repo.create_commit(
        tree_hash=tree1.hash,
        message="Initial commit\n\nSet up project structure with README and gitignore",
        author="Sarah Chen",
        author_email="sarah@example.com",
        parent_hash=None
    )
    print(f"   Created: {commit1.hash[:7]} - {commit1.message.split(chr(10))[0]}")

    # Create main branch
    repo.create_or_update_ref('refs/heads/main', commit1.hash)
    print(f"   Created branch: main")

    # Commit 2: Add basic project structure with nested directories
    print("\n2. Creating second commit...")
    requirements = repo.create_blob(b"flask==3.0.0\nmarkdown==3.5.1\nsqlalchemy==2.0.44")
    app_init = repo.create_blob(b"from flask import Flask\n\napp = Flask(__name__)\n")
    models_init = repo.create_blob(b"from .user import User\nfrom .post import Post\n")

    # Create nested directory structure
    models_tree = repo.create_tree([
        TreeEntryInput(name='__init__.py', type=EntryType.BLOB, hash=models_init.hash, mode='100644'),
    ])

    tree2 = repo.create_tree([
        TreeEntryInput(name='.gitignore', type=EntryType.BLOB, hash=gitignore.hash, mode='100644'),
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=readme.hash, mode='100644'),
        TreeEntryInput(name='app.py', type=EntryType.BLOB, hash=app_init.hash, mode='100755'),
        TreeEntryInput(name='models', type=EntryType.TREE, hash=models_tree.hash, mode='040000'),
        TreeEntryInput(name='requirements.txt', type=EntryType.BLOB, hash=requirements.hash, mode='100644'),
    ])

    commit2 = repo.create_commit(
        tree_hash=tree2.hash,
        message="Add basic project structure",
        author="Mike Johnson",
        author_email="mike@example.com",
        parent_hash=commit1.hash
    )
    print(f"   Created: {commit2.hash[:7]} - {commit2.message}")

    # Update main
    repo.create_or_update_ref('refs/heads/main', commit2.hash)

    # Commit 3: Add models with deeper nesting
    print("\n3. Creating third commit...")
    user_model = repo.create_blob(b"from sqlalchemy import Column, Integer, String\n\nclass User:\n    id = Column(Integer, primary_key=True)\n    username = Column(String(80), unique=True)\n    email = Column(String(120))\n")
    post_model = repo.create_blob(b"from sqlalchemy import Column, Integer, String, Text\n\nclass Post:\n    id = Column(Integer, primary_key=True)\n    title = Column(String(200))\n    content = Column(Text)\n")

    # Create deeper nested structure: models/blog/
    blog_init = repo.create_blob(b"# Blog models\n")
    blog_tree = repo.create_tree([
        TreeEntryInput(name='__init__.py', type=EntryType.BLOB, hash=blog_init.hash, mode='100644'),
        TreeEntryInput(name='post.py', type=EntryType.BLOB, hash=post_model.hash, mode='100644'),
    ])

    models_tree_v2 = repo.create_tree([
        TreeEntryInput(name='__init__.py', type=EntryType.BLOB, hash=models_init.hash, mode='100644'),
        TreeEntryInput(name='blog', type=EntryType.TREE, hash=blog_tree.hash, mode='040000'),
        TreeEntryInput(name='user.py', type=EntryType.BLOB, hash=user_model.hash, mode='100644'),
    ])

    tree3 = repo.create_tree([
        TreeEntryInput(name='.gitignore', type=EntryType.BLOB, hash=gitignore.hash, mode='100644'),
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=readme.hash, mode='100644'),
        TreeEntryInput(name='app.py', type=EntryType.BLOB, hash=app_init.hash, mode='100755'),
        TreeEntryInput(name='models', type=EntryType.TREE, hash=models_tree_v2.hash, mode='040000'),
        TreeEntryInput(name='requirements.txt', type=EntryType.BLOB, hash=requirements.hash, mode='100644'),
    ])

    commit3 = repo.create_commit(
        tree_hash=tree3.hash,
        message="Add blog models with nested structure",
        author="Sarah Chen",
        author_email="sarah@example.com",
        parent_hash=commit2.hash
    )
    print(f"   Created: {commit3.hash[:7]} - {commit3.message}")

    # Update main
    repo.create_or_update_ref('refs/heads/main', commit3.hash)

    # Create a tag
    repo.create_or_update_ref('refs/tags/v0.1.0', commit3.hash)
    print(f"   Created tag: v0.1.0")

    # Commit 4: Add templates with even deeper nesting
    print("\n4. Creating fourth commit...")
    base_template = repo.create_blob(b"<!DOCTYPE html>\n<html>\n<head><title>My Blog</title></head>\n<body>{% block content %}{% endblock %}</body>\n</html>")
    index_template = repo.create_blob(b"{% extends 'base.html' %}\n{% block content %}<h1>Welcome to My Blog</h1>{% endblock %}")
    post_list_template = repo.create_blob(b"{% extends 'base.html' %}\n{% block content %}<h1>All Posts</h1>{% endblock %}")

    # Create templates/blog/ directory structure
    blog_templates_tree = repo.create_tree([
        TreeEntryInput(name='index.html', type=EntryType.BLOB, hash=index_template.hash, mode='100644'),
        TreeEntryInput(name='post_list.html', type=EntryType.BLOB, hash=post_list_template.hash, mode='100644'),
    ])

    templates_tree = repo.create_tree([
        TreeEntryInput(name='base.html', type=EntryType.BLOB, hash=base_template.hash, mode='100644'),
        TreeEntryInput(name='blog', type=EntryType.TREE, hash=blog_templates_tree.hash, mode='040000'),
    ])

    tree4 = repo.create_tree([
        TreeEntryInput(name='.gitignore', type=EntryType.BLOB, hash=gitignore.hash, mode='100644'),
        TreeEntryInput(name='README.md', type=EntryType.BLOB, hash=readme.hash, mode='100644'),
        TreeEntryInput(name='app.py', type=EntryType.BLOB, hash=app_init.hash, mode='100755'),
        TreeEntryInput(name='models', type=EntryType.TREE, hash=models_tree_v2.hash, mode='040000'),
        TreeEntryInput(name='requirements.txt', type=EntryType.BLOB, hash=requirements.hash, mode='100644'),
        TreeEntryInput(name='templates', type=EntryType.TREE, hash=templates_tree.hash, mode='040000'),
    ])

    commit4 = repo.create_commit(
        tree_hash=tree4.hash,
        message="Add templates with nested structure",
        author="Mike Johnson",
        author_email="mike@example.com",
        parent_hash=commit3.hash
    )
    print(f"   Created: {commit4.hash[:7]} - {commit4.message}")

    # Update main
    repo.create_or_update_ref('refs/heads/main', commit4.hash)

    # Create develop branch from commit 2
    print("\n5. Creating develop branch...")
    repo.create_or_update_ref('refs/heads/develop', commit2.hash)
    print(f"   Created branch: develop (from {commit2.hash[:7]})")

    print("\n✅ Sample data created successfully!")
    print(f"\nCreated repository: {repo_model.name}")
    print(f"\nCreated:")
    print(f"  - 4 commits")
    print(f"  - 2 branches (main, develop)")
    print(f"  - 1 tag (v0.1.0)")
    print(f"  - Nested directories: models/blog/, templates/blog/")
    print(f"\nYou can now start the web server with:")
    print(f"  PYTHONPATH=. python src/app.py")
    print(f"\nThen visit: http://localhost:{Config.PORT}/example-blog")

    db.close()


if __name__ == '__main__':
    seed_data()
