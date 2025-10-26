#!/usr/bin/env python3
"""
Migration script to convert stage_runs to use content-addressable hash IDs.

This migration:
1. Creates a new stage_runs table with String(64) ID (hash-based)
2. Computes hash IDs for existing stage runs
3. Migrates data with new hash IDs, maintaining parent-child relationships
4. Drops old table and renames new table

Note: This is a destructive migration. Backup your data before running.
"""

from sqlalchemy import create_engine, text
from src.config import Config
import json
import hashlib


def compute_stage_id(parent_id, commit_hash, workflow_file, stage_name, arguments):
    """
    Compute content-addressable ID for a stage run.

    Must match the StageRun.compute_id() method exactly.
    """
    # Parse and re-serialize arguments to ensure deterministic JSON
    args_dict = json.loads(arguments)
    canonical_args = json.dumps(args_dict, sort_keys=True, separators=(',', ':'))

    # Compute hash of all execution parameters
    hash_input = f"{parent_id or ''}|{commit_hash}|{workflow_file}|{stage_name}|{canonical_args}"
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def migrate():
    """Convert stage_runs to use content-addressable hash IDs"""
    print("Running migration: convert to content-addressable IDs...")

    engine = create_engine(Config.DATABASE_URL, echo=True)

    with engine.begin() as conn:
        # Step 1: Get all existing stage runs ordered by creation (parents before children)
        print("\n  Fetching existing stage runs...")
        existing_runs = conn.execute(text("""
            SELECT id, parent_stage_run_id, arguments, repo_name, commit_hash, workflow_file,
                   triggered_by, trigger_event, stage_name, status,
                   started_at, completed_at, result_value, error_message,
                   created_at, updated_at
            FROM stage_runs
            ORDER BY created_at ASC
        """)).fetchall()

        print(f"  Found {len(existing_runs)} stage runs to migrate")

        # Step 2: Create new table with hash-based IDs
        print("\n  Creating new stage_runs table with hash-based IDs...")
        conn.execute(text("""
            CREATE TABLE stage_runs_new (
                id VARCHAR(64) PRIMARY KEY,
                parent_stage_run_id VARCHAR(64),
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
                FOREIGN KEY (parent_stage_run_id) REFERENCES stage_runs_new(id)
            )
        """))
        print("  ✓ Created new table")

        # Step 3: Migrate data with new hash IDs
        print("\n  Computing hash IDs and migrating data...")

        # Map old integer IDs to new hash IDs
        id_mapping = {}
        # Track which hash IDs we've already inserted (for deduplication)
        inserted_hashes = set()
        duplicates_skipped = 0

        for row in existing_runs:
            old_id = row[0]
            old_parent_id = row[1]
            arguments = row[2]
            repo_name = row[3]
            commit_hash = row[4]
            workflow_file = row[5]
            triggered_by = row[6]
            trigger_event = row[7]
            stage_name = row[8]
            status = row[9]
            started_at = row[10]
            completed_at = row[11]
            result_value = row[12]
            error_message = row[13]
            created_at = row[14]
            updated_at = row[15]

            # Look up parent's new hash ID if it exists
            new_parent_id = id_mapping.get(old_parent_id) if old_parent_id else None

            # Compute new hash ID for this stage run
            new_id = compute_stage_id(
                parent_id=new_parent_id,
                commit_hash=commit_hash,
                workflow_file=workflow_file,
                stage_name=stage_name,
                arguments=arguments
            )

            # Store mapping for children to reference
            id_mapping[old_id] = new_id

            # Skip if we've already inserted this hash (it's a duplicate)
            if new_id in inserted_hashes:
                duplicates_skipped += 1
                print(f"    Skipping duplicate: {stage_name} (hash {new_id[:12]}...)")
                continue

            inserted_hashes.add(new_id)

            # Insert with new hash ID
            conn.execute(text("""
                INSERT INTO stage_runs_new (
                    id, parent_stage_run_id, arguments, repo_name, commit_hash, workflow_file,
                    triggered_by, trigger_event, stage_name, status,
                    started_at, completed_at, result_value, error_message,
                    created_at, updated_at
                ) VALUES (
                    :id, :parent_id, :arguments, :repo_name, :commit_hash, :workflow_file,
                    :triggered_by, :trigger_event, :stage_name, :status,
                    :started_at, :completed_at, :result_value, :error_message,
                    :created_at, :updated_at
                )
            """), {
                'id': new_id,
                'parent_id': new_parent_id,
                'arguments': arguments,
                'repo_name': repo_name,
                'commit_hash': commit_hash,
                'workflow_file': workflow_file,
                'triggered_by': triggered_by,
                'trigger_event': trigger_event,
                'stage_name': stage_name,
                'status': status,
                'started_at': started_at,
                'completed_at': completed_at,
                'result_value': result_value,
                'error_message': error_message,
                'created_at': created_at,
                'updated_at': updated_at
            })

        migrated_count = len(existing_runs) - duplicates_skipped
        print(f"  ✓ Migrated {migrated_count} unique stage runs with new hash IDs")
        if duplicates_skipped > 0:
            print(f"  ℹ Skipped {duplicates_skipped} duplicate invocations")

        # Step 4: Drop old table
        print("\n  Dropping old stage_runs table...")
        conn.execute(text("DROP TABLE stage_runs"))
        print("  ✓ Old table dropped")

        # Step 5: Rename new table
        print("  Renaming new table...")
        conn.execute(text("ALTER TABLE stage_runs_new RENAME TO stage_runs"))
        print("  ✓ Table renamed")

    print("\n✅ Migration completed successfully!")
    print("  - Stage runs now use content-addressable hash IDs")
    print("  - Identical invocations will be deduplicated automatically")
    print(f"  - Migrated {migrated_count} unique stage runs")
    if duplicates_skipped > 0:
        print(f"  - Removed {duplicates_skipped} duplicate invocations")


if __name__ == '__main__':
    migrate()
