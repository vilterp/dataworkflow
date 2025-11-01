#!/usr/bin/env python3
"""
Migration script to add pull_requests and pull_request_checks tables.
"""

from sqlalchemy import create_engine
from src.config import Config
from src.models.base import Base
from src.models import PullRequest, PullRequestCheck  # Import to register tables


def migrate():
    """Add pull request tables to the database"""
    print("Running migration: add pull_requests tables...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    # Create only the new tables
    PullRequest.__table__.create(engine, checkfirst=True)
    PullRequestCheck.__table__.create(engine, checkfirst=True)

    print("\n✅ Migration completed successfully!")
    print("  - Created 'pull_requests' table")
    print("  - Created 'pull_request_checks' table")


if __name__ == '__main__':
    migrate()
