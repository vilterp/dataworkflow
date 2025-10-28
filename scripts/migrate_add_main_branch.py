#!/usr/bin/env python3
"""
Migration script to add main_branch field to repositories table.

This migration adds:
- main_branch: The default branch name for the repository (e.g., 'main', 'master')

All existing repositories will be set to 'main' as the default branch.
"""

from sqlalchemy import create_engine, text
from src.config import Config


def migrate():
    """Add main_branch field to repositories table"""
    print("Running migration: add main_branch field to repositories...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    with engine.begin() as conn:
        # Check existing columns
        result = conn.execute(text("PRAGMA table_info(repositories)"))
        columns = [row[1] for row in result]

        # Add main_branch column
        if 'main_branch' not in columns:
            print("\n  Adding main_branch column...")
            conn.execute(text("ALTER TABLE repositories ADD COLUMN main_branch VARCHAR(255) NOT NULL DEFAULT 'main'"))
            print("  ✓ Added main_branch column")

            # Set all existing repositories to 'main'
            print("  Setting all existing repositories to 'main'...")
            result = conn.execute(text("UPDATE repositories SET main_branch = 'main'"))
            print(f"  ✓ Updated {result.rowcount} repositories")
        else:
            print("  ✓ main_branch column already exists")

    print("\n✅ Migration completed successfully!")
    print("  Added column:")
    print("    - main_branch: Default branch name (VARCHAR(255), default='main')")
    print("\nAll existing repositories now have main_branch set to 'main'")


if __name__ == '__main__':
    migrate()
