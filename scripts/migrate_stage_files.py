#!/usr/bin/env python3
"""
Migration script to update stage_files table schema.

This updates the stage_files table to use the new schema with:
- String ID (hash-based)
- stage_run_id instead of stage_id
- file_path instead of path
- content_hash and storage_key for file storage
- size field
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.models.base import create_session
from src.config import Config
import sqlite3

def migrate_stage_files():
    """Migrate stage_files table to new schema."""
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
        # Drop the old stage_files table
        print("Dropping old stage_files table...")
        cursor.execute("DROP TABLE IF EXISTS stage_files")

        # Create new stage_files table
        print("Creating new stage_files table...")
        cursor.execute("""
            CREATE TABLE stage_files (
                id VARCHAR(64) NOT NULL,
                stage_run_id VARCHAR(64) NOT NULL,
                file_path VARCHAR(500) NOT NULL,
                content_hash VARCHAR(64) NOT NULL,
                storage_key VARCHAR(255) NOT NULL,
                size INTEGER NOT NULL,
                created_at DATETIME,
                PRIMARY KEY (id),
                FOREIGN KEY(stage_run_id) REFERENCES stage_runs (id)
            )
        """)

        # Create index on stage_run_id for faster lookups
        print("Creating index on stage_run_id...")
        cursor.execute("""
            CREATE INDEX ix_stage_files_stage_run_id ON stage_files (stage_run_id)
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
    migrate_stage_files()
