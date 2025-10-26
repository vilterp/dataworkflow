#!/usr/bin/env python3
"""
Migration script to remove WorkflowRun model and add trigger fields to StageRun.

This migration:
1. Adds triggered_by and trigger_event columns to stage_runs table
2. Copies values from workflow_runs to root stage_runs (where parent_stage_run_id is NULL)
3. Removes workflow_run_id foreign key from stage_runs table
4. Drops workflow_runs table

Note: This is a destructive migration. Backup your data before running.
"""

from sqlalchemy import create_engine, text
from src.config import Config


def migrate():
    """Remove WorkflowRun model and migrate data to StageRun"""
    print("Running migration: remove WorkflowRun model...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    with engine.begin() as conn:
        # Since SQLite doesn't support ALTER COLUMN, we need to:
        # 1. Create a new stage_runs table with the updated schema
        # 2. Copy data from old table (with trigger fields from workflow_runs)
        # 3. Drop old stage_runs table
        # 4. Rename new table
        # 5. Drop workflow_runs table

        print("\n  Creating new stage_runs table without workflow_run_id...")

        # Create new table
        conn.execute(text("""
            CREATE TABLE stage_runs_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_stage_run_id INTEGER,
                arguments TEXT NOT NULL,
                repo_name VARCHAR(255) NOT NULL,
                commit_hash VARCHAR(64) NOT NULL,
                workflow_file VARCHAR(500) NOT NULL,
                triggered_by VARCHAR(255),
                trigger_event VARCHAR(100),
                stage_name VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                started_at DATETIME,
                completed_at DATETIME,
                result_value TEXT,
                error_message TEXT,
                created_at DATETIME,
                updated_at DATETIME,
                FOREIGN KEY (parent_stage_run_id) REFERENCES stage_runs(id)
            )
        """))
        print("  ✓ Created new table")

        # Copy data from old table, joining with workflow_runs for root stages
        print("  Copying data from old table...")
        conn.execute(text("""
            INSERT INTO stage_runs_new (
                id, parent_stage_run_id, arguments, repo_name, commit_hash, workflow_file,
                triggered_by, trigger_event, stage_name, status,
                started_at, completed_at, result_value, error_message,
                created_at, updated_at
            )
            SELECT
                sr.id, sr.parent_stage_run_id, sr.arguments, sr.repo_name, sr.commit_hash, sr.workflow_file,
                COALESCE(wr.triggered_by, 'unknown') as triggered_by,
                COALESCE(wr.trigger_event, 'unknown') as trigger_event,
                sr.stage_name, sr.status,
                sr.started_at, sr.completed_at, sr.result_value, sr.error_message,
                sr.created_at, sr.updated_at
            FROM stage_runs sr
            LEFT JOIN workflow_runs wr ON sr.workflow_run_id = wr.id
        """))
        print("  ✓ Data copied")

        # Drop old table
        print("  Dropping old stage_runs table...")
        conn.execute(text("DROP TABLE stage_runs"))
        print("  ✓ Old table dropped")

        # Rename new table
        print("  Renaming new table...")
        conn.execute(text("ALTER TABLE stage_runs_new RENAME TO stage_runs"))
        print("  ✓ Table renamed")

        # Drop workflow_runs table
        print("  Dropping workflow_runs table...")
        conn.execute(text("DROP TABLE workflow_runs"))
        print("  ✓ workflow_runs table dropped")

    print("\n✅ Migration completed successfully!")
    print("  - workflow_run_id removed from stage_runs table")
    print("  - triggered_by and trigger_event added to stage_runs table")
    print("  - workflow_runs table dropped")
    print("  - Trigger metadata migrated to root stage runs")


if __name__ == '__main__':
    migrate()
