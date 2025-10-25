#!/usr/bin/env python3
"""
Migration script to make invocation fields required (non-nullable) in stage_runs.

This migration enforces that all stage runs must have:
- arguments (required for distributed execution)
- repo_name (required to locate workflow code)
- commit_hash (required to load correct code version)
- workflow_file (required to load the workflow)

Note: This will delete any stage_runs records with NULL values in these fields.
"""

from sqlalchemy import create_engine, text
from src.config import Config


def migrate():
    """Make invocation fields required in stage_runs table"""
    print("Running migration: make invocation fields required in stage_runs...")
    print("⚠️  Warning: This will delete any records with NULL invocation fields")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    with engine.begin() as conn:
        # First, check if any records would be deleted
        result = conn.execute(text("""
            SELECT COUNT(*) FROM stage_runs
            WHERE arguments IS NULL
               OR repo_name IS NULL
               OR commit_hash IS NULL
               OR workflow_file IS NULL
        """))
        count = result.scalar()

        if count > 0:
            print(f"\n⚠️  Found {count} records with NULL invocation fields")
            print("  These records will be deleted before applying NOT NULL constraints")

            # Delete records with NULL values
            conn.execute(text("""
                DELETE FROM stage_runs
                WHERE arguments IS NULL
                   OR repo_name IS NULL
                   OR commit_hash IS NULL
                   OR workflow_file IS NULL
            """))
            print(f"  ✓ Deleted {count} incomplete records")
        else:
            print("  ✓ No incomplete records found")

        # SQLite doesn't support ALTER COLUMN to add NOT NULL
        # We need to recreate the table
        print("\n  Recreating table with NOT NULL constraints...")

        # Create new table with NOT NULL constraints
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS "stage_runs_new" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_run_id INTEGER,
                parent_stage_run_id INTEGER,
                arguments TEXT NOT NULL,
                repo_name VARCHAR(255) NOT NULL,
                commit_hash VARCHAR(64) NOT NULL,
                workflow_file VARCHAR(500) NOT NULL,
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

        # Copy data from old table to new table
        conn.execute(text("""
            INSERT INTO stage_runs_new
            SELECT * FROM stage_runs
        """))

        # Drop old table
        conn.execute(text("DROP TABLE stage_runs"))

        # Rename new table to original name
        conn.execute(text("ALTER TABLE stage_runs_new RENAME TO stage_runs"))

        print("  ✓ Table recreated with NOT NULL constraints")

    print("\n✅ Migration completed successfully!")
    print("  Updated constraints:")
    print("    - arguments: NOW REQUIRED")
    print("    - repo_name: NOW REQUIRED")
    print("    - commit_hash: NOW REQUIRED")
    print("    - workflow_file: NOW REQUIRED")
    print("\nAll stage runs must now have complete invocation metadata!")


if __name__ == '__main__':
    migrate()
