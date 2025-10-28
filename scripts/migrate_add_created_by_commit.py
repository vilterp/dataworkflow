#!/usr/bin/env python3
"""
Migration script to add created_by_commit_hash field to blobs and trees tables.

This migration adds optimization support by tracking which commit first created each blob/tree:
- created_by_commit_hash: Commit hash that first introduced this object

This allows fast lookup of commit information for tree entries without scanning commit history.
"""

from sqlalchemy import create_engine, text
from src.config import Config


def migrate():
    """Add created_by_commit_hash field to blobs and trees tables"""
    print("Running migration: add created_by_commit_hash to blobs and trees...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    with engine.begin() as conn:
        # Check existing columns in blobs table
        result = conn.execute(text("PRAGMA table_info(blobs)"))
        blob_columns = [row[1] for row in result]

        # Add created_by_commit_hash column to blobs
        if 'created_by_commit_hash' not in blob_columns:
            print("\n  Adding created_by_commit_hash column to blobs...")
            conn.execute(text("ALTER TABLE blobs ADD COLUMN created_by_commit_hash VARCHAR(64)"))
            print("  ✓ Added created_by_commit_hash column to blobs")
        else:
            print("  ✓ created_by_commit_hash column already exists in blobs")

        # Check existing columns in trees table
        result = conn.execute(text("PRAGMA table_info(trees)"))
        tree_columns = [row[1] for row in result]

        # Add created_by_commit_hash column to trees
        if 'created_by_commit_hash' not in tree_columns:
            print("  Adding created_by_commit_hash column to trees...")
            conn.execute(text("ALTER TABLE trees ADD COLUMN created_by_commit_hash VARCHAR(64)"))
            print("  ✓ Added created_by_commit_hash column to trees")
        else:
            print("  ✓ created_by_commit_hash column already exists in trees")

    print("\n✅ Migration completed successfully!")
    print("  Added columns:")
    print("    - blobs.created_by_commit_hash: Commit that first created this blob")
    print("    - trees.created_by_commit_hash: Commit that first created this tree")
    print("\nThe blobs and trees tables now track their creating commits!")
    print("This enables fast commit lookup in get_tree_entries_with_commits.")


if __name__ == '__main__':
    migrate()
