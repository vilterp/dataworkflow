#!/usr/bin/env python3
"""
Initialize the database schema from scratch.

This script will:
1. Drop the existing database file (if --reset flag is provided)
2. Create all tables from the current SQLAlchemy models

Usage:
    python scripts/init_db.py          # Create tables (safe, doesn't drop)
    python scripts/init_db.py --reset  # Drop and recreate (DANGEROUS!)
"""

import argparse
import os
from src.config import Config
from src.models.base import init_db


def main():
    parser = argparse.ArgumentParser(description='Initialize database schema')
    parser.add_argument('--reset', action='store_true',
                        help='Drop existing database and recreate (DANGEROUS!)')
    args = parser.parse_args()

    db_path = Config.DATABASE_URL.replace('sqlite:///', '')

    if args.reset:
        if os.path.exists(db_path):
            print(f"⚠️  Dropping existing database: {db_path}")
            response = input("Are you sure? This will delete ALL data! (yes/no): ")
            if response.lower() != 'yes':
                print("Aborted.")
                return
            os.remove(db_path)
            print(f"✓ Deleted {db_path}")

    print(f"Creating database tables at: {db_path}")
    init_db(Config.DATABASE_URL, echo=True)
    print("\n✅ Database initialized successfully!")


if __name__ == '__main__':
    main()
