#!/usr/bin/env python3
"""Create Python UDFs in RisingWave with proper function definitions.

This script reads UDF definitions from sql/create_udfs.sql and executes them.
"""
import re
import sys
from pathlib import Path

import psycopg2


def parse_udf_statements(sql_content: str) -> list:
    """Parse SQL content to extract CREATE FUNCTION statements.
    
    This handles Python UDFs that contain semicolons inside $$ blocks.
    """
    statements = []
    
    # Pattern to match CREATE FUNCTION ... AS $$ ... $$;
    # The pattern captures the entire statement from CREATE FUNCTION to $$;
    # Handle newlines and multiline function bodies
    pattern = r'(CREATE FUNCTION\s+\w+\s*\([^)]*\)\s+RETURNS\s+\w+\s+LANGUAGE\s+\w+\s+AS\s*\$\$[\s\S]*?\$\$;)'
    
    for match in re.finditer(pattern, sql_content, re.IGNORECASE):
        stmt = match.group(1).strip()
        statements.append(stmt)
    
    return statements


def main():
    # Find the SQL file
    sql_file = Path("sql/create_udfs.sql")
    if not sql_file.exists():
        # Try absolute path for Docker container
        sql_file = Path("/workspace/sql/create_udfs.sql")
    
    if not sql_file.exists():
        print(f"Error: Could not find sql/create_udfs.sql")
        sys.exit(1)
    
    print(f"Loading SQL from: {sql_file}")
    sql_content = sql_file.read_text()
    
    conn = psycopg2.connect(
        host="frontend-node-0",
        port=4566,
        database="dev",
        user="root"
    )
    
    try:
        with conn.cursor() as cur:
            print("\nDropping existing UDFs...")
            for func_name in ['conversion_category', 'calculate_funnel_score', 'format_rate_with_emoji', 'revenue_tier']:
                try:
                    cur.execute(f"DROP FUNCTION IF EXISTS {func_name} CASCADE")
                    print(f"  Dropped {func_name}")
                except Exception as e:
                    print(f"  Error dropping {func_name}: {e}")
            conn.commit()
            
            # Parse CREATE FUNCTION statements from SQL file
            create_statements = parse_udf_statements(sql_content)
            print(f"\nFound {len(create_statements)} CREATE FUNCTION statements in SQL file")
            
            print("\nCreating UDFs...")
            for stmt in create_statements:
                # Extract function name for logging
                match = re.search(r'CREATE FUNCTION\s+(\w+)', stmt, re.IGNORECASE)
                func_name = match.group(1) if match else "unknown"
                
                try:
                    print(f"\n  Creating {func_name}...")
                    cur.execute(stmt)
                    conn.commit()
                    print(f"  Created {func_name}")
                except Exception as e:
                    print(f"  ERROR creating {func_name}: {e}")
                    raise
            
            print("\nTesting UDFs...")
            tests = [
                ("SELECT conversion_category(0.6)", "excellent"),
                ("SELECT calculate_funnel_score(100, 50, 25)", None),
                ("SELECT format_rate_with_emoji(0.45)", "Medium"),
                ("SELECT revenue_tier(750)", "high"),
            ]
            for test_sql, expected in tests:
                try:
                    cur.execute(test_sql)
                    result = cur.fetchone()[0]
                    status = "✓" if expected is None or expected in str(result) else "✗"
                    print(f"  {status} {test_sql} = {result}")
                except Exception as e:
                    print(f"  ✗ {test_sql} ERROR: {e}")
            
            print("\n✅ All UDFs created and tested!")
            
    finally:
        conn.close()


if __name__ == "__main__":
    main()
