#!/usr/bin/env python3
"""
Diagnostic script to check the entire data pipeline.
"""
import json
import sys


def check_kafka_connection():
    """Check if Kafka is accessible."""
    print("=" * 60)
    print("1. Checking Kafka Connection...")
    print("=" * 60)
    try:
        from confluent_kafka import Consumer, KafkaException

        conf = {
            'bootstrap.servers': 'localhost:19092',
            'group.id': 'check-pipeline',
            'auto.offset.reset': 'latest',
        }
        consumer = Consumer(conf)
        
        # Get cluster metadata (includes topics)
        metadata = consumer.list_topics(timeout=5)
        topics = [t.topic for t in iter(metadata.topics.values())]
        
        print(f"✅ Kafka is reachable")
        print(f"   Available topics: {sorted(topics) if topics else 'None'}")

        if 'funnel' in topics:
            print(f"✅ 'funnel' topic exists")
        else:
            print(f"❌ 'funnel' topic NOT found")
        consumer.close()
        return True
    except KafkaException as e:
        print(f"❌ Kafka connection failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        return False


def check_dashboard_api():
    """Check if dashboard API is serving data."""
    print("\n" + "=" * 60)
    print("2. Checking Dashboard API...")
    print("=" * 60)
    try:
        import requests

        # Check funnel endpoint
        try:
            resp = requests.get('http://localhost:8000/api/funnel', timeout=5)
            print(f"   /api/funnel status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    print(f"✅ Got {len(data)} funnel records")
                    if data:
                        print(f"   Latest record: {data[-1]}")
                else:
                    print(f"⚠️  Unexpected response: {data}")
        except Exception as e:
            print(f"❌ /api/funnel failed: {e}")

        # Check stats endpoint
        try:
            resp = requests.get('http://localhost:8000/api/stats', timeout=5)
            print(f"   /api/stats status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                print(f"   Stats: {json.dumps(data, indent=2)}")
        except Exception as e:
            print(f"❌ /api/stats failed: {e}")

    except Exception as e:
        print(f"❌ Dashboard API check failed: {e}")


def check_risingwave():
    """Check if RisingWave has funnel data."""
    print("\n" + "=" * 60)
    print("3. Checking RisingWave...")
    print("=" * 60)
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine("postgresql://root:root@localhost:4566/dev")
        with engine.connect() as conn:
            # Check funnel MV
            result = conn.execute(text("SELECT COUNT(*) FROM funnel"))
            count = result.fetchone()[0]
            print(f"✅ Funnel MV has {count} records")

            # Check sinks
            result = conn.execute(text("SHOW SINKS"))
            sinks = result.fetchall()
            print(f"   Available sinks: {[s[0] for s in sinks]}")

            # Check if funnel_kafka_sink exists
            if any('funnel_kafka' in str(s) for s in sinks):
                print(f"✅ funnel_kafka_sink exists")
            else:
                print(f"❌ funnel_kafka_sink NOT found")

    except Exception as e:
        print(f"❌ RisingWave check failed: {e}")


def main():
    print("\n🔍 Pipeline Diagnostic Tool\n")

    check_kafka_connection()
    check_dashboard_api()
    check_risingwave()

    print("\n" + "=" * 60)
    print("Diagnostic complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
