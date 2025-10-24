#!/usr/bin/env python3
"""
Migration script to add workflow_runs and stage_runs tables.
"""

from sqlalchemy import create_engine
from src.config import Config
from src.models.base import Base
from src.models import WorkflowRun, StageRun  # Import to register tables


def migrate():
    """Add workflow tables to the database"""
    print("Running migration: add workflow tables...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    # Create only the new tables
    WorkflowRun.__table__.create(engine, checkfirst=True)
    StageRun.__table__.create(engine, checkfirst=True)

    print("\n✅ Migration completed successfully!")
    print("  - Created 'workflow_runs' table")
    print("  - Created 'stage_runs' table")


if __name__ == '__main__':
    migrate()
