#!/bin/bash
# Fix funnel_enriched view by dropping it so dbt can recreate it with correct UDFs

echo "Connecting to RisingWave to drop funnel_enriched view..."

# Use the dagster-daemon container which has psycopg2
 docker exec -i risingwave-test-dagster-daemon-1 python3 << 'EOF'
import psycopg2

conn = psycopg2.connect(
    host="frontend-node-0",
    port=4566,
    database="dev",
    user="root"
)

try:
    with conn.cursor() as cur:
        # Check if view exists
        cur.execute("""
            SELECT table_name FROM information_schema.views 
            WHERE table_name = 'funnel_enriched'
        """)
        if cur.fetchone():
            print("Dropping funnel_enriched view...")
            cur.execute("DROP VIEW IF EXISTS funnel_enriched")
            conn.commit()
            print("funnel_enriched view dropped successfully")
        else:
            print("funnel_enriched view does not exist")
        
        # Also check UDFs
        cur.execute("""
            SELECT proname FROM pg_proc 
            WHERE proname IN ('conversion_category', 'calculate_funnel_score', 'format_rate_with_emoji', 'revenue_tier')
        """)
        udfs = cur.fetchall()
        print(f"Current UDFs: {[u[0] for u in udfs]}")
finally:
    conn.close()
EOF

echo ""
echo "Done! Now re-materialize funnel_enriched in Dagster."
