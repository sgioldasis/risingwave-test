"""Dagster asset for setting up PostgreSQL sink table."""
import logging
import os

from dagster import AssetExecutionContext, asset
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logger = logging.getLogger(__name__)

# SQL to create the funnel_summary_with_country table in PostgreSQL
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS funnel_summary_with_country (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP,
    country VARCHAR(2) NOT NULL,
    country_name VARCHAR,
    viewers BIGINT,
    carters BIGINT,
    purchasers BIGINT,
    view_to_cart_rate DOUBLE PRECISION,
    cart_to_buy_rate DOUBLE PRECISION,
    PRIMARY KEY (window_start, country)
);
"""


def get_postgres_connection():
    """Get a connection to the local PostgreSQL database.
    
    Uses environment variables or defaults to local devbox PostgreSQL.
    When running inside Docker (like Dagster), uses host.docker.internal
    to connect to the host's PostgreSQL.
    """
    # Check if we're running inside Docker by looking for .dockerenv
    in_docker = os.path.exists('/.dockerenv') or os.getenv('DOCKER_CONTAINER', 'false').lower() == 'true'
    
    if in_docker:
        # When inside Docker container, use host.docker.internal to reach host's PostgreSQL
        host = os.getenv("POSTGRES_HOST", "host.docker.internal")
        # In Docker, default to 'postgres' user since container user won't exist on host PG
        user = os.getenv("POSTGRES_USER", "postgres")
    else:
        # When running directly on host
        host = os.getenv("POSTGRES_HOST", "localhost")
        user = os.getenv("POSTGRES_USER", os.getenv("USER", "postgres"))
    
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )


@asset(
    group_name="postgres",
    compute_kind="python",
    description="Creates the funnel_summary_with_country table in local PostgreSQL for RisingWave sink",
)
def postgres_funnel_table(context: AssetExecutionContext) -> str:
    """Create the PostgreSQL table that will receive data from RisingWave sink.
    
    This table must exist before the RisingWave JDBC sink is created.
    """
    conn = None
    try:
        context.log.info("Connecting to PostgreSQL...")
        conn = get_postgres_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cur:
            context.log.info("Creating funnel_summary_with_country table if not exists...")
            cur.execute(CREATE_TABLE_SQL)
            
            # Verify table was created
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'funnel_summary_with_country'
                ORDER BY ordinal_position;
            """)
            columns = cur.fetchall()
            
        context.log.info(f"Table ready with columns: {[col[0] for col in columns]}")
        return "funnel_summary_with_country table created/verified successfully"
        
    except psycopg2.Error as e:
        context.log.error(f"PostgreSQL error: {e}")
        raise RuntimeError(f"Failed to create PostgreSQL table: {e}")
    finally:
        if conn:
            conn.close()
