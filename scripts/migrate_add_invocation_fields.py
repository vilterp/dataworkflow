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
        # Check existing columns
        result = conn.execute(text("PRAGMA table_info(stage_runs)"))
        columns = [row[1] for row in result]

        # Add arguments column
        if 'arguments' not in columns:
            print("\n  Adding arguments column...")
            conn.execute(text("ALTER TABLE stage_runs ADD COLUMN arguments TEXT"))
            print("  ✓ Added arguments column")
        else:
            print("  ✓ Arguments column already exists")

        # Add repo_name column
        if 'repo_name' not in columns:
            print("  Adding repo_name column...")
            conn.execute(text("ALTER TABLE stage_runs ADD COLUMN repo_name VARCHAR(255)"))
            print("  ✓ Added repo_name column")
        else:
            print("  ✓ repo_name column already exists")

        # Add commit_hash column
        if 'commit_hash' not in columns:
            print("  Adding commit_hash column...")
            conn.execute(text("ALTER TABLE stage_runs ADD COLUMN commit_hash VARCHAR(64)"))
            print("  ✓ Added commit_hash column")
        else:
            print("  ✓ commit_hash column already exists")

        # Add workflow_file column
        if 'workflow_file' not in columns:
            print("  Adding workflow_file column...")
            conn.execute(text("ALTER TABLE stage_runs ADD COLUMN workflow_file VARCHAR(500)"))
            print("  ✓ Added workflow_file column")
        else:
            print("  ✓ workflow_file column already exists")

    print("\n✅ Migration completed successfully!")
    print("  Added columns:")
    print("    - arguments: JSON function arguments")
    print("    - repo_name: Repository containing workflow code")
    print("    - commit_hash: Git commit to load code from")
    print("    - workflow_file: Path to workflow file in repo")
    print("\nThe stage_runs table now supports distributed execution!")
    print("Each invocation knows where to find its code (repo + commit + file)")


if __name__ == '__main__':
    migrate()
