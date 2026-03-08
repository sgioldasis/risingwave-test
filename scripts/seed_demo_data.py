#!/usr/bin/env python3
"""
Demo Data Seeder

This script seeds RisingWave with synthetic historical data so that
ML models can be trained immediately for live demos.

Generates ~60 minutes of realistic funnel data with:
- Time-based patterns (hourly cycles)
- Random variations
- Trend components
"""

import json
import random
import requests
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RISINGWAVE_URL = "postgresql://root@localhost:4566/dev"


def generate_historical_data(minutes=60, end_time=None):
    """
    Generate synthetic historical funnel data.
    
    Args:
        minutes: Number of minutes of data to generate
        end_time: The end timestamp (defaults to now)
    
    Returns:
        List of data records
    """
    if end_time is None:
        end_time = datetime.utcnow()
    
    data = []
    base_viewers = 100
    base_carters = 30
    base_purchasers = 8
    
    for i in range(minutes, 0, -1):
        timestamp = end_time - timedelta(minutes=i)
        
        # Add hourly pattern (more activity during "business hours")
        hour = timestamp.hour
        hour_factor = 1.0 + 0.3 * ((12 - abs(hour - 12)) / 12)  # Peak at noon
        
        # Add some trend (slight growth over time)
        trend_factor = 1.0 + (minutes - i) * 0.002
        
        # Random variation
        random_factor = random.uniform(0.85, 1.15)
        
        # Calculate values with noise
        viewers = int(base_viewers * hour_factor * trend_factor * random_factor)
        carters = int(base_carters * hour_factor * trend_factor * random_factor * random.uniform(0.9, 1.1))
        purchasers = int(base_purchasers * hour_factor * trend_factor * random_factor * random.uniform(0.8, 1.2))
        
        # Ensure logical consistency
        carters = min(carters, viewers)
        purchasers = min(purchasers, carters)
        
        # Calculate rates
        v2c_rate = round(carters / viewers, 4) if viewers > 0 else 0
        c2b_rate = round(purchasers / carters, 4) if carters > 0 else 0
        
        record = {
            "window_start": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "window_end": (timestamp + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "viewers": viewers,
            "carters": carters,
            "purchasers": purchasers,
            "view_to_cart_rate": v2c_rate,
            "cart_to_buy_rate": c2b_rate,
            "hour_of_day": timestamp.hour,
            "minute_of_hour": timestamp.minute,
            "day_of_week": timestamp.weekday(),
            "minute_sequence": int(timestamp.timestamp() / 60)
        }
        data.append(record)
    
    return data


def insert_into_risingwave(records):
    """
    Insert records directly into RisingWave.
    
    Note: Since we can't easily insert into materialized views,
    we'll create a temporary table and then use it to feed the funnel_training view.
    """
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not installed. Install with: pip install psycopg2-binary")
        return False
    
    try:
        conn = psycopg2.connect(RISINGWAVE_URL)
        cursor = conn.cursor()
        
        # Check if we can create a source table for demo data
        logger.info("Creating demo data source...")
        
        # First, try to create the table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS demo_funnel_data (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                viewers INT,
                carters INT,
                purchasers INT,
                view_to_cart_rate NUMERIC,
                cart_to_buy_rate NUMERIC,
                hour_of_day INT,
                minute_of_hour INT,
                day_of_week INT,
                minute_sequence BIGINT
            )
        """)
        
        # Clear existing demo data
        cursor.execute("DELETE FROM demo_funnel_data")
        
        # Insert records
        logger.info(f"Inserting {len(records)} records...")
        for record in records:
            cursor.execute("""
                INSERT INTO demo_funnel_data 
                (window_start, window_end, viewers, carters, purchasers, 
                 view_to_cart_rate, cart_to_buy_rate, hour_of_day, 
                 minute_of_hour, day_of_week, minute_sequence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record["window_start"],
                record["window_end"],
                record["viewers"],
                record["carters"],
                record["purchasers"],
                record["view_to_cart_rate"],
                record["cart_to_buy_rate"],
                record["hour_of_day"],
                record["minute_of_hour"],
                record["day_of_week"],
                record["minute_sequence"]
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully inserted {len(records)} records")
        return True
        
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        return False


def create_kafka_events_for_history(minutes=60, tps=100):
    """
    Alternative: Generate high-volume Kafka events to quickly build history.
    This is more realistic since it flows through the actual pipeline.
    """
    from kafka import KafkaProducer
    import time
    
    logger.info(f"Generating {minutes} minutes worth of data at {tps} TPS...")
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:19092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=minutes)
    
    # Generate events for each minute
    for minute_offset in range(minutes):
        current_time = start_time + timedelta(minutes=minute_offset)
        timestamp_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Calculate expected counts for this minute
        hour = current_time.hour
        hour_factor = 1.0 + 0.3 * ((12 - abs(hour - 12)) / 12)
        
        base_viewers = int(100 * hour_factor / 60 * tps)  # Scale to TPS
        base_carters = int(30 * hour_factor / 60 * tps)
        base_purchasers = int(8 * hour_factor / 60 * tps)
        
        # Generate page views and track viewer user IDs
        viewer_ids = []
        for _ in range(base_viewers):
            user_id = random.randint(1, 100000)
            viewer_ids.append(user_id)
            producer.send('page_views', value={
                "user_id": user_id,
                "page_id": f"page_{random.randint(1, 100)}",
                "event_time": timestamp_str
            })
        
        # Generate cart events only from users who viewed
        # Sample carters from the viewer pool to ensure carters <= viewers
        actual_carters = min(base_carters, len(viewer_ids))
        cart_user_ids = random.sample(viewer_ids, actual_carters) if viewer_ids else []
        for user_id in cart_user_ids:
            producer.send('cart_events', value={
                "user_id": user_id,
                "item_id": f"item_{random.randint(100, 1000)}",
                "event_time": timestamp_str
            })
        
        # Generate purchase events only from users who carted
        # Sample purchasers from the carter pool to ensure purchasers <= carters
        actual_purchasers = min(base_purchasers, len(cart_user_ids))
        purchase_user_ids = random.sample(cart_user_ids, actual_purchasers) if cart_user_ids else []
        for user_id in purchase_user_ids:
            producer.send('purchases', value={
                "user_id": user_id,
                "amount": round(random.uniform(10, 500), 2),
                "event_time": timestamp_str
            })
        
        if (minute_offset + 1) % 10 == 0:
            logger.info(f"Generated {minute_offset + 1}/{minutes} minutes of data")
    
    producer.flush()
    producer.close()
    logger.info("Done generating historical data via Kafka")


def fast_init_approach():
    """
    Fast initialization: Run producer at high speed for a few minutes.
    """
    logger.info("=" * 60)
    logger.info("FAST DEMO INITIALIZATION")
    logger.info("=" * 60)
    logger.info("")
    logger.info("This will generate ~30 minutes of data in ~2 minutes")
    logger.info("by running the producer at 50 TPS")
    logger.info("")
    
    import subprocess
    import time
    import signal
    
    # Start producer at high TPS
    logger.info("Starting high-speed producer (50 TPS)...")
    proc = subprocess.Popen(
        ["python3", "scripts/producer.py", "--tps", "50"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Let it run for 90 seconds to generate ~30 minutes of events
    logger.info("Generating data for 90 seconds...")
    time.sleep(90)
    
    # Stop producer
    logger.info("Stopping producer...")
    proc.send_signal(signal.SIGTERM)
    proc.wait()
    
    logger.info("Data generation complete!")
    logger.info("Waiting 30 seconds for RisingWave to process...")
    time.sleep(30)
    
    logger.info("")
    logger.info("Demo data ready! You can now start the dashboard.")
    logger.info("The ML models will auto-train when the dashboard starts.")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Seed demo data for ML training")
    parser.add_argument(
        "--method",
        choices=["kafka", "fast", "direct"],
        default="fast",
        help="Method to generate data: kafka (historical), fast (high TPS), direct (SQL)"
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=30,
        help="Minutes of data to generate"
    )
    
    args = parser.parse_args()
    
    if args.method == "kafka":
        create_kafka_events_for_history(minutes=args.minutes)
    elif args.method == "fast":
        fast_init_approach()
    elif args.method == "direct":
        records = generate_historical_data(minutes=args.minutes)
        insert_into_risingwave(records)
    
    logger.info("")
    logger.info("Next steps:")
    logger.info("1. Verify data in RisingWave:")
    logger.info("   psql postgresql://root@localhost:4566/dev -c 'SELECT COUNT(*) FROM funnel_training;'")
    logger.info("2. Start the dashboard - ML models will auto-train:")
    logger.info("   ./bin/4_run_modern.sh")


if __name__ == "__main__":
    main()
