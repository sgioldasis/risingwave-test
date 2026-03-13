#!/usr/bin/env python3
"""
Check funnel_training view structure and data availability.
Helps diagnose ML training issues.
"""

import logging
import psycopg2
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_funnel_training():
    """Check the funnel_training view for data and column issues."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=4566,
            database="dev",
            user="root",
            password=""
        )
        cursor = conn.cursor()
        
        logger.info("=" * 70)
        logger.info("FUNNEL_TRAINING VIEW DIAGNOSTICS")
        logger.info("=" * 70)
        
        # Check if view/materialized view exists
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.views
            WHERE table_name = 'funnel_training'
        """)
        view_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'funnel_training' AND table_type = 'BASE TABLE'
        """)
        table_count = cursor.fetchone()[0]
        
        if view_count == 0 and table_count == 0:
            logger.error("\n❌ ERROR: funnel_training view/materialized view does not exist!")
            logger.error("   Run: cd dbt && dbt run --models funnel_training")
            return
        logger.info(f"\n✓ funnel_training {'view' if view_count > 0 else 'materialized view'} exists")
        
        # Check column structure
        logger.info("\n📊 Checking column structure...")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'funnel_training'
            ORDER BY ordinal_position
        """)
        columns = [row[0] for row in cursor.fetchall()]
        
        required_lags = ['viewers_lag_1', 'viewers_lag_2', 'viewers_lag_3',
                        'carters_lag_1', 'carters_lag_2', 'carters_lag_3',
                        'purchasers_lag_1', 'purchasers_lag_2', 'purchasers_lag_3']
        
        missing = [col for col in required_lags if col not in columns]
        if missing:
            logger.error(f"\n❌ MISSING COLUMNS: {missing}")
            logger.error("   The ML predictor needs these lag features!")
            logger.error("   Update: dbt/models/funnel_training.sql")
        else:
            logger.info("✓ All required lag columns present")
        
        logger.info(f"\n   All columns: {', '.join(columns)}")
        
        # Check data availability
        logger.info("\n📈 Checking data availability...")
        cursor.execute("SELECT COUNT(*) FROM funnel_training")
        total_count = cursor.fetchone()[0]
        logger.info(f"   Total records: {total_count}")
        
        # Check recent data (last 5 minutes)
        cursor.execute("""
            SELECT COUNT(*) FROM funnel_training 
            WHERE window_start >= NOW() - INTERVAL '5 minutes'
        """)
        recent_count = cursor.fetchone()[0]
        logger.info(f"   Records in last 5 minutes: {recent_count}")
        
        # Check completed windows (window_end < NOW()) - what ML uses
        cursor.execute("""
            SELECT COUNT(*) FROM funnel_training 
            WHERE window_start >= NOW() - INTERVAL '5 minutes'
              AND window_end < NOW()
        """)
        completed_count = cursor.fetchone()[0]
        logger.info(f"   Completed windows (usable for ML): {completed_count}")
        
        if completed_count < 2:
            logger.warning("\n⚠️  WARNING: Not enough completed windows for training!")
            logger.warning("   Need at least 2 completed windows.")
            logger.warning("   Wait for the current minute to complete.")
        else:
            logger.info(f"\n✓ Sufficient data for training ({completed_count} windows)")
        
        # Show sample data
        logger.info("\n📋 Sample records (last 5):")
        cursor.execute("""
            SELECT window_start, window_end, viewers, carters, purchasers,
                   viewers_lag_1, viewers_lag_2, viewers_lag_3
            FROM funnel_training 
            ORDER BY window_start DESC
            LIMIT 5
        """)
        
        rows = cursor.fetchall()
        logger.info(f"{'window_start':<25} {'window_end':<25} {'viewers':>8} {'carters':>8} {'purchasers':>10}")
        logger.info("-" * 90)
        for row in rows:
            logger.info(f"{str(row[0]):<25} {str(row[1]):<25} {row[2]:>8} {row[3]:>8} {row[4]:>10}")
            logger.info(f"   lag1={row[5]}, lag2={row[6]}, lag3={row[7]}")
        
        cursor.close()
        conn.close()
        
        logger.info("\n" + "=" * 70)
        
    except Exception as e:
        logger.error(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    check_funnel_training()
