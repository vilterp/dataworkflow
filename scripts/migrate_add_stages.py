#!/usr/bin/env python3
"""
Migration script to add stages and stage_files tables.
"""

from sqlalchemy import create_engine
from src.config import Config
from src.models.base import Base
from src.models import Stage, StageFile  # Import to register tables


def migrate():
    """Add stages tables to the database"""
    print("Running migration: add stages tables...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    # Create only the new tables
    Stage.__table__.create(engine, checkfirst=True)
    StageFile.__table__.create(engine, checkfirst=True)

    print("\n✅ Migration completed successfully!")
    print("  - Created 'stages' table")
    print("  - Created 'stage_files' table")


if __name__ == '__main__':
    migrate()
