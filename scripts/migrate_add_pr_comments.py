#!/usr/bin/env python3
"""
Migration script to add pull_request_comments table.
"""

from sqlalchemy import create_engine
from src.config import Config
from src.models.base import Base
from src.models import PullRequestComment  # Import to register table


def migrate():
    """Add pull request comments table to the database"""
    print("Running migration: add pull_request_comments table...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    # Create only the new table
    PullRequestComment.__table__.create(engine, checkfirst=True)

    print("\n✅ Migration completed successfully!")
    print("  - Created 'pull_request_comments' table")


if __name__ == '__main__':
    migrate()
