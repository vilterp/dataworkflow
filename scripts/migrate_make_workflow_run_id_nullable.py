#!/usr/bin/env python3
"""
Migration script to make workflow_run_id nullable in stage_runs table.

This migration supports distributed execution mode where stage runs
can exist independently without a workflow_run.
"""

from sqlalchemy import create_engine, text
from src.config import Config


def migrate():
    """Make workflow_run_id nullable in stage_runs table"""
    print("Running migration: make workflow_run_id nullable in stage_runs...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    with engine.begin() as conn:
        # Since SQLite doesn't support ALTER COLUMN, we need to:
        # 1. Create a new table with the updated schema
        # 2. Copy data from old table
        # 3. Drop old table
        # 4. Rename new table

        print("\n  Creating new stage_runs table with nullable workflow_run_id...")

        # Create new table
        conn.execute(text("""
            CREATE TABLE stage_runs_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_run_id INTEGER,
                parent_stage_run_id INTEGER,
                arguments TEXT,
                repo_name VARCHAR(255),
                commit_hash VARCHAR(64),
                workflow_file VARCHAR(500),
                stage_name VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                started_at DATETIME,
                completed_at DATETIME,
                result_value TEXT,
                error_message TEXT,
                created_at DATETIME,
                updated_at DATETIME,
                FOREIGN KEY (workflow_run_id) REFERENCES workflow_runs(id),
                FOREIGN KEY (parent_stage_run_id) REFERENCES stage_runs(id)
            )
        """))
        print("  ✓ Created new table")

        # Copy data from old table
        print("  Copying data from old table...")
        conn.execute(text("""
            INSERT INTO stage_runs_new (
                id, workflow_run_id, parent_stage_run_id, stage_name, status,
                started_at, completed_at, result_value, error_message,
                created_at, updated_at, arguments, repo_name, commit_hash, workflow_file
            )
            SELECT
                id, workflow_run_id, parent_stage_run_id, stage_name, status,
                started_at, completed_at, result_value, error_message,
                created_at, updated_at, arguments, repo_name, commit_hash, workflow_file
            FROM stage_runs
        """))
        print("  ✓ Data copied")

        # Drop old table
        print("  Dropping old table...")
        conn.execute(text("DROP TABLE stage_runs"))
        print("  ✓ Old table dropped")

        # Rename new table
        print("  Renaming new table...")
        conn.execute(text("ALTER TABLE stage_runs_new RENAME TO stage_runs"))
        print("  ✓ Table renamed")

    print("\n✅ Migration completed successfully!")
    print("  workflow_run_id is now nullable in stage_runs table")
    print("  Stage runs can now exist independently for distributed execution")


if __name__ == '__main__':
    migrate()
