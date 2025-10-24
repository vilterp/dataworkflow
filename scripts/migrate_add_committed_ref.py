#!/usr/bin/env python3
"""
Migration script to add committed_ref column to stages table.
"""

from sqlalchemy import create_engine, text
from src.config import Config


def migrate():
    """Add committed_ref column to stages table"""
    print("Running migration: add committed_ref column to stages...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    with engine.connect() as conn:
        # Add the committed_ref column
        conn.execute(text("""
            ALTER TABLE stages
            ADD COLUMN committed_ref VARCHAR(255)
        """))
        conn.commit()

    print("\n✅ Migration completed successfully!")
    print("  - Added 'committed_ref' column to 'stages' table")


if __name__ == '__main__':
    migrate()
