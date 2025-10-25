#!/usr/bin/env python3
"""
Migration script to add arguments field to stage_runs table.

This migration adds support for distributed execution by adding:
- arguments: JSON-encoded function arguments

The existing id and parent_stage_run_id fields are used for invocation tracking.
"""

from sqlalchemy import create_engine, text
from src.config import Config


def migrate():
    """Add arguments field to stage_runs table"""
    print("Running migration: add arguments field to stage_runs...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    with engine.begin() as conn:
        # Add arguments column
        print("\n  Adding arguments column...")
        conn.execute(text("""
            ALTER TABLE stage_runs
            ADD COLUMN arguments TEXT
        """))

        # Make workflow_run_id nullable (for standalone invocations)
        print("  Making workflow_run_id nullable...")
        conn.execute(text("""
            ALTER TABLE stage_runs
            ALTER COLUMN workflow_run_id DROP NOT NULL
        """))

    print("\n✅ Migration completed successfully!")
    print("  - Added 'arguments' column for storing function arguments")
    print("  - Made 'workflow_run_id' nullable for standalone invocations")
    print("\nThe stage_runs table now supports both:")
    print("  - Legacy workflow-based execution (with workflow_run_id)")
    print("  - New distributed call-based execution (workflow_run_id can be NULL)")
    print("  - Invocation tracking uses existing id and parent_stage_run_id columns")


if __name__ == '__main__':
    migrate()
