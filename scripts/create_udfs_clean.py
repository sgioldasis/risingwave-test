#!/usr/bin/env python3
"""Create Python UDFs in RisingWave with proper code preservation."""
import psycopg2

UDFS = {
    "conversion_category": """CREATE FUNCTION conversion_category(rate float)
RETURNS varchar
LANGUAGE python AS $$
if rate is None:
    return 'unknown'
if rate >= 0.5:
    return 'excellent'
elif rate >= 0.3:
    return 'good'
elif rate >= 0.1:
    return 'average'
else:
    return 'needs_improvement'
$$;""",
    "calculate_funnel_score": """CREATE FUNCTION calculate_funnel_score(viewers bigint, carters bigint, purchasers bigint)
RETURNS float
LANGUAGE python AS $$
if viewers is None or viewers == 0:
    return 0.0
view_to_cart = carters / float(viewers) if carters else 0.0
cart_to_buy = purchasers / float(carters) if carters else 0.0
return round(view_to_cart * 0.4 + cart_to_buy * 0.6, 2)
$$;""",
    "format_rate_with_emoji": """CREATE FUNCTION format_rate_with_emoji(rate float)
RETURNS varchar
LANGUAGE python AS $$
if rate is None:
    return 'N/A'
percentage = round(rate * 100, 1)
if rate >= 0.5:
    return f'High {percentage}%'
elif rate >= 0.3:
    return f'Medium {percentage}%'
elif rate >= 0.1:
    return f'Low {percentage}%'
else:
    return f'Critical {percentage}%'
$$;""",
    "revenue_tier": """CREATE FUNCTION revenue_tier(amount numeric)
RETURNS varchar
LANGUAGE python AS $$
if amount is None:
    return 'unknown'
if amount >= 1000:
    return 'premium'
elif amount >= 500:
    return 'high'
elif amount >= 100:
    return 'medium'
else:
    return 'low'
$$;""",
}

def main():
    conn = psycopg2.connect(
        host="frontend-node-0",
        port=4566,
        database="dev",
        user="root"
    )
    
    try:
        with conn.cursor() as cur:
            print("Dropping existing UDFs...")
            for name in UDFS.keys():
                try:
                    cur.execute(f"DROP FUNCTION IF EXISTS {name} CASCADE")
                    print(f"  Dropped {name}")
                except Exception as e:
                    print(f"  Could not drop {name}: {e}")
            conn.commit()
            
            print("\nCreating UDFs...")
            for name, sql in UDFS.items():
                try:
                    print(f"\n  Creating {name}...")
                    print(f"  SQL preview: {sql[:80]}...")
                    cur.execute(sql)
                    conn.commit()
                    print(f"  Created {name}")
                    
                    # Test the UDF immediately
                    if name == "conversion_category":
                        cur.execute("SELECT conversion_category(0.6)")
                        result = cur.fetchone()
                        print(f"  Test conversion_category(0.6) = {result[0]}")
                except Exception as e:
                    print(f"  ERROR creating {name}: {e}")
                    raise
            
            print("\nAll UDFs created successfully!")
            
            # List all UDFs
            cur.execute("""
                SELECT proname FROM pg_proc 
                WHERE proname IN ('conversion_category', 'calculate_funnel_score', 'format_rate_with_emoji', 'revenue_tier')
            """)
            udfs = cur.fetchall()
            print(f"\nCurrent UDFs: {[u[0] for u in udfs]}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    main()
