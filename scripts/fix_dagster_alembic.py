#!/usr/bin/env python3
"""
Fix Alembic migration issue in Dagster SQLite storage.

This script resolves the "Version table 'alembic_version' has more than one head" error
by merging the migration heads or resetting the migration state.
"""

import os
import sys
import sqlite3
from pathlib import Path

# Path to the Dagster SQLite database files
DAGSTER_STORAGE = Path("./dagster_storage")
RUNS_DB = DAGSTER_STORAGE / "runs.db"
EVENT_LOGS_DB = DAGSTER_STORAGE / "event_logs.db"
SCHEDULES_DB = DAGSTER_STORAGE / "schedules.db"


def check_database_exists(db_path: Path) -> bool:
    """Check if a database file exists and is readable."""
    return db_path.exists() and db_path.is_file()


def get_alembic_version(db_path: Path) -> list:
    """Get current Alembic version(s) from a database."""
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT version_num FROM alembic_version")
        versions = [row[0] for row in cursor.fetchall()]
        conn.close()
        return versions
    except sqlite3.Error as e:
        print(f"Error reading {db_path}: {e}")
        return []


def fix_alembic_heads(db_path: Path) -> bool:
    """
    Fix multiple Alembic heads by keeping only the latest version.
    
    This is a safe approach for Dagster's internal databases where we only
    need to ensure the schema is up to date.
    """
    versions = get_alembic_version(db_path)
    
    if len(versions) <= 1:
        print(f"  ✓ {db_path.name}: Already has {len(versions)} head(s), no fix needed")
        return True
    
    print(f"  ⚠ {db_path.name}: Found {len(versions)} heads: {versions}")
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Get the latest version (lexicographically highest revision)
        latest_version = max(versions)
        
        # Delete all versions and insert only the latest
        cursor.execute("DELETE FROM alembic_version")
        cursor.execute("INSERT INTO alembic_version (version_num) VALUES (?)", (latest_version,))
        conn.commit()
        conn.close()
        
        print(f"  ✓ Fixed: Kept version {latest_version}")
        return True
    except sqlite3.Error as e:
        print(f"  ✗ Error fixing {db_path}: {e}")
        return False


def main():
    """Main entry point."""
    print("Dagster Alembic Migration Fix Tool")
    print("=" * 50)
    
    # Check if running inside Docker or locally
    if os.path.exists("/workspace/dagster_storage"):
        # Running inside Docker container
        DAGSTER_STORAGE = Path("/workspace/dagster_storage")
        RUNS_DB = DAGSTER_STORAGE / "runs.db"
        EVENT_LOGS_DB = DAGSTER_STORAGE / "event_logs.db"
        SCHEDULES_DB = DAGSTER_STORAGE / "schedules.db"
        print("Detected Docker environment")
    else:
        print("Detected local environment")
    
    print(f"Storage path: {DAGSTER_STORAGE}")
    print()
    
    databases = []
    if check_database_exists(RUNS_DB):
        databases.append(RUNS_DB)
    if check_database_exists(EVENT_LOGS_DB):
        databases.append(EVENT_LOGS_DB)
    if check_database_exists(SCHEDULES_DB):
        databases.append(SCHEDULES_DB)
    
    if not databases:
        print("No Dagster SQLite databases found.")
        print("The databases will be created automatically when Dagster starts.")
        return 0
    
    print(f"Found {len(databases)} database(s) to check")
    print()
    
    all_fixed = True
    for db_path in databases:
        if not fix_alembic_heads(db_path):
            all_fixed = False
    
    print()
    if all_fixed:
        print("✓ All databases fixed successfully!")
        print("You can now restart Dagster.")
        return 0
    else:
        print("✗ Some databases could not be fixed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
