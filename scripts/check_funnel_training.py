#!/usr/bin/env python3
"""
Check funnel_training view structure and data availability.
Helps diagnose ML training issues.
"""

import psycopg2
import sys

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
        
        print("=" * 70)
        print("FUNNEL_TRAINING VIEW DIAGNOSTICS")
        print("=" * 70)
        
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
            print("\n❌ ERROR: funnel_training view/materialized view does not exist!")
            print("   Run: cd dbt && dbt run --models funnel_training")
            return
        print(f"\n✓ funnel_training {'view' if view_count > 0 else 'materialized view'} exists")
        
        # Check column structure
        print("\n📊 Checking column structure...")
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
            print(f"\n❌ MISSING COLUMNS: {missing}")
            print("   The ML predictor needs these lag features!")
            print("   Update: dbt/models/funnel_training.sql")
        else:
            print("✓ All required lag columns present")
        
        print(f"\n   All columns: {', '.join(columns)}")
        
        # Check data availability
        print("\n📈 Checking data availability...")
        cursor.execute("SELECT COUNT(*) FROM funnel_training")
        total_count = cursor.fetchone()[0]
        print(f"   Total records: {total_count}")
        
        # Check recent data (last 5 minutes)
        cursor.execute("""
            SELECT COUNT(*) FROM funnel_training 
            WHERE window_start >= NOW() - INTERVAL '5 minutes'
        """)
        recent_count = cursor.fetchone()[0]
        print(f"   Records in last 5 minutes: {recent_count}")
        
        # Check completed windows (window_end < NOW()) - what ML uses
        cursor.execute("""
            SELECT COUNT(*) FROM funnel_training 
            WHERE window_start >= NOW() - INTERVAL '5 minutes'
              AND window_end < NOW()
        """)
        completed_count = cursor.fetchone()[0]
        print(f"   Completed windows (usable for ML): {completed_count}")
        
        if completed_count < 2:
            print("\n⚠️  WARNING: Not enough completed windows for training!")
            print("   Need at least 2 completed windows.")
            print("   Wait for the current minute to complete.")
        else:
            print(f"\n✓ Sufficient data for training ({completed_count} windows)")
        
        # Show sample data
        print("\n📋 Sample records (last 5):")
        cursor.execute("""
            SELECT window_start, window_end, viewers, carters, purchasers,
                   viewers_lag_1, viewers_lag_2, viewers_lag_3
            FROM funnel_training 
            ORDER BY window_start DESC
            LIMIT 5
        """)
        
        rows = cursor.fetchall()
        print(f"{'window_start':<25} {'window_end':<25} {'viewers':>8} {'carters':>8} {'purchasers':>10}")
        print("-" * 90)
        for row in rows:
            print(f"{str(row[0]):<25} {str(row[1]):<25} {row[2]:>8} {row[3]:>8} {row[4]:>10}")
            print(f"   lag1={row[5]}, lag2={row[6]}, lag3={row[7]}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 70)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    check_funnel_training()
