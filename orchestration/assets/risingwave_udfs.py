"""Dagster asset for creating Python UDFs in RisingWave."""
import logging
import re
from pathlib import Path

from dagster import AssetExecutionContext, asset
import psycopg2

logger = logging.getLogger(__name__)


def parse_udf_statements(sql_content: str) -> list:
    """Parse SQL content to extract CREATE FUNCTION statements.
    
    This handles Python UDFs that contain semicolons inside $$ blocks.
    """
    statements = []
    
    # Pattern to match CREATE FUNCTION ... AS $$ ... $$;
    pattern = r'(CREATE FUNCTION\s+\w+\s*\([^)]*\)\s+RETURNS\s+\w+\s+LANGUAGE\s+\w+\s+AS\s*\$\$[\s\S]*?\$\$;)'
    
    for match in re.finditer(pattern, sql_content, re.IGNORECASE):
        stmt = match.group(1).strip()
        statements.append(stmt)
    
    return statements


@asset(
    group_name="risingwave",
    compute_kind="python",
    description="Creates Python UDFs in RisingWave for funnel analysis",
)
def risingwave_python_udfs(context: AssetExecutionContext) -> str:
    """Create Python UDFs in RisingWave by executing sql/create_udfs.sql."""
    # Find the SQL file
    sql_file = Path("/workspace/sql/create_udfs.sql")
    if not sql_file.exists():
        sql_file = Path("sql/create_udfs.sql")
    
    if not sql_file.exists():
        raise FileNotFoundError(f"Could not find sql/create_udfs.sql")
    
    sql_content = sql_file.read_text()
    context.log.info(f"Loaded SQL from: {sql_file}")
    
    # Parse CREATE FUNCTION statements from SQL file
    create_statements = parse_udf_statements(sql_content)
    context.log.info(f"Found {len(create_statements)} CREATE FUNCTION statements")
    
    # Connect to RisingWave
    conn = psycopg2.connect(
        host="frontend-node-0",
        port=4566,
        database="dev",
        user="root",
    )

    try:
        with conn.cursor() as cur:
            # Check existing UDFs
            cur.execute("""
                SELECT proname
                FROM pg_proc
                WHERE proname IN ('conversion_category', 'calculate_funnel_score', 'format_rate_with_emoji', 'revenue_tier', 'calculate_funnel_health')
            """)
            existing = [row[0] for row in cur.fetchall()]
            context.log.info(f"Existing UDFs before: {existing}")

            # Drop existing UDFs with all possible signatures
            drop_commands = [
                "DROP FUNCTION IF EXISTS conversion_category(float) CASCADE",
                "DROP FUNCTION IF EXISTS calculate_funnel_score(bigint, bigint, bigint) CASCADE",
                "DROP FUNCTION IF EXISTS calculate_funnel_score(numeric, numeric, numeric) CASCADE",
                "DROP FUNCTION IF EXISTS format_rate_with_emoji(float) CASCADE",
                "DROP FUNCTION IF EXISTS revenue_tier(numeric) CASCADE",
                "DROP FUNCTION IF EXISTS calculate_funnel_health(float, float) CASCADE",
                "DROP FUNCTION IF EXISTS calculate_funnel_health(double precision, double precision) CASCADE",
                "DROP FUNCTION IF EXISTS calculate_funnel_health(numeric, numeric) CASCADE",
            ]
            for cmd in drop_commands:
                try:
                    cur.execute(cmd)
                    conn.commit()
                    context.log.info(f"Executed: {cmd}")
                except Exception as e:
                    context.log.warning(f"Could not execute {cmd}: {e}")

            # Create each UDF from parsed statements
            created = []
            for stmt in create_statements:
                # Extract function name for logging
                match = re.search(r'CREATE FUNCTION\s+(\w+)', stmt, re.IGNORECASE)
                func_name = match.group(1) if match else "unknown"
                
                try:
                    context.log.info(f"Creating {func_name}...")
                    cur.execute(stmt)
                    conn.commit()
                    created.append(func_name)
                    context.log.info(f"Created {func_name}")
                except Exception as e:
                    context.log.error(f"Failed to create {func_name}: {e}")
                    context.log.error(f"SQL: {stmt[:200]}...")
                    raise

            # Verify by testing one UDF
            try:
                cur.execute("SELECT conversion_category(0.6)")
                result = cur.fetchone()
                context.log.info(f"Test: conversion_category(0.6) = {result[0]}")
            except Exception as test_err:
                context.log.error(f"UDF test failed: {test_err}")

            # List all UDFs
            cur.execute("""
                SELECT proname
                FROM pg_proc
                WHERE proname IN ('conversion_category', 'calculate_funnel_score', 'format_rate_with_emoji', 'revenue_tier', 'calculate_funnel_health')
            """)
            after = [row[0] for row in cur.fetchall()]
            context.log.info(f"UDFs after creation: {after}")

            return f"Created UDFs: {created}. Total: {len(after)} functions"
    except Exception as e:
        context.log.error(f"UDF creation failed: {e}")
        raise
    finally:
        conn.close()
