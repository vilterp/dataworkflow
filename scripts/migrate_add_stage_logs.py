#!/usr/bin/env python3
"""
Migration script to add stage_log_lines table.

This creates the stage_log_lines table to store log lines captured from
stage run executions.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.models.base import create_session
from src.config import Config
import sqlite3

def migrate_add_stage_logs():
    """Add stage_log_lines table."""
    database_url = Config.DATABASE_URL

    # Extract database file path from URL
    if database_url.startswith('sqlite:///'):
        db_path = database_url[10:]
    else:
        print(f"Unsupported database URL: {database_url}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check if table already exists
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='stage_log_lines'
        """)
        if cursor.fetchone():
            print("Table stage_log_lines already exists. Skipping migration.")
            return

        # Create stage_log_lines table
        print("Creating stage_log_lines table...")
        cursor.execute("""
            CREATE TABLE stage_log_lines (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                stage_run_id VARCHAR(64) NOT NULL,
                log_line_index INTEGER NOT NULL,
                timestamp DATETIME NOT NULL,
                log_contents TEXT NOT NULL,
                created_at DATETIME,
                FOREIGN KEY(stage_run_id) REFERENCES stage_runs (id)
            )
        """)

        # Create indices for faster lookups
        print("Creating index on stage_run_id...")
        cursor.execute("""
            CREATE INDEX ix_stage_log_lines_stage_run_id ON stage_log_lines (stage_run_id)
        """)

        print("Creating index on log_line_index for sorting...")
        cursor.execute("""
            CREATE INDEX ix_stage_log_lines_log_line_index ON stage_log_lines (log_line_index)
        """)

        # Composite index for efficient tailing queries
        print("Creating composite index for tailing queries...")
        cursor.execute("""
            CREATE INDEX ix_stage_log_lines_tailing ON stage_log_lines (stage_run_id, log_line_index)
        """)

        conn.commit()
        print("Migration completed successfully!")

    except Exception as e:
        conn.rollback()
        print(f"Error during migration: {e}")
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    migrate_add_stage_logs()
