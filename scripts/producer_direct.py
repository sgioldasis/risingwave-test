#!/usr/bin/env python3
"""
Direct Producer for RisingWave

Generates rich clickstream data using the datagen.py pattern from:
https://github.com/risingwavelabs/awesome-stream-processing

Instead of producing to Kafka, this script inserts data directly into RisingWave
tables (users, devices, campaigns, sessions, page_catalog, clickstream_events).
"""

import argparse
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import psycopg2
from faker import Faker

faker = Faker("en_US")

# Data generation constants from datagen.py
COUNTRIES = ["US", "DE", "PK", "GB", "AE", "CA"]
CITIES = ["Karachi", "Lahore", "Berlin", "London", "Dubai", "Toronto", "New York", "Munich"]
REGIONS = ["Sindh", "Punjab", "Berlin", "England", "Dubai", "Ontario", "NY", "Bavaria"]

DEVICE_TYPES = ["desktop", "mobile", "tablet"]
OS_LIST = ["Windows", "macOS", "Linux", "Android", "iOS"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]

SOURCES = [
    ("google", "cpc"),
    ("twitter", "social"),
    ("newsletter", "email"),
    ("reddit", "social"),
    ("direct", "none"),
]
CAMPAIGNS = ["winter_sale", "onboarding", "flash_deal", "content_push", "retargeting"]
CONTENTS = ["banner_a", "banner_b", "cta_blue", "cta_orange"]
TERMS = ["running_shoes", "laptop_deals", "phone_discount", "streaming_sql", "none"]

PRODUCTS = [
    ("P-1001", "electronics", "/products/laptop"),
    ("P-1002", "electronics", "/products/phone"),
    ("P-2001", "apparel", "/products/shoes"),
    ("P-3001", "home", "/products/coffee-maker"),
]

STATIC_PAGES = [
    ("/", "home"),
    ("/search", "search"),
    ("/blog/streaming", "blog"),
    ("/cart", "cart"),
    ("/checkout", "checkout"),
]

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 4566,
    "database": "dev",
    "user": "root",
    "password": "root",
}


def now_utc():
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def connect_db():
    """Establish connection to RisingWave."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except psycopg2.Error as e:
        print(f"❌ Error: Could not connect to RisingWave. Details: {e}", file=sys.stderr, flush=True)
        print(f"👉 Please make sure services are started with './bin/1_up.sh'", file=sys.stderr, flush=True)
        sys.exit(1)


def check_tables_exist(conn) -> bool:
    """Check if required tables exist."""
    required_tables = ['users', 'devices', 'campaigns', 'sessions', 'page_catalog', 'clickstream_events']
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = ANY(%s)
        """, (required_tables,))
        existing = {row[0] for row in cursor.fetchall()}
        missing = set(required_tables) - existing
        if missing:
            print(f"❌ Error: Required tables do not exist: {missing}", file=sys.stderr, flush=True)
            print(f"👉 Please run dbt to create the tables: './bin/3_run_dbt.sh'", file=sys.stderr, flush=True)
            return False
        return True
    finally:
        cursor.close()


# Data generation functions (adapted from datagen.py)
def gen_user(user_id: int):
    signup = now_utc() - timedelta(days=random.randint(1, 365))
    return {
        "user_id": user_id,
        "full_name": faker.name(),
        "email": faker.email(),
        "country": random.choice(COUNTRIES),
        "signup_time": iso(signup),
        "marketing_opt_in": random.random() < 0.55,
        "ingested_at": iso(now_utc()),
    }


def gen_device():
    device_id = faker.bothify(text="DEV-########")
    dt = random.choice(DEVICE_TYPES)
    os_ = random.choice(OS_LIST)
    br = random.choice(BROWSERS)
    ua = f"{br}/{random.randint(90,130)} ({os_}; {dt})"
    return device_id, {
        "device_id": device_id,
        "device_type": dt,
        "os": os_,
        "browser": br,
        "user_agent": ua,
        "ingested_at": iso(now_utc()),
    }


def gen_campaign():
    campaign_id = faker.bothify(text="CMP-######")
    source, medium = random.choice(SOURCES)
    return campaign_id, {
        "campaign_id": campaign_id,
        "source": source,
        "medium": medium,
        "campaign": random.choice(CAMPAIGNS),
        "content": random.choice(CONTENTS),
        "term": random.choice(TERMS),
        "ingested_at": iso(now_utc()),
    }


def gen_session(session_id: str, user_id: int, device_id: str):
    start = now_utc() - timedelta(seconds=random.randint(0, 600))
    i = random.randrange(len(CITIES))
    return {
        "session_id": session_id,
        "user_id": user_id,
        "device_id": device_id,
        "session_start": iso(start),
        "ip_address": faker.ipv4_public(),
        "geo_city": CITIES[i],
        "geo_region": REGIONS[i],
        "ingested_at": iso(now_utc()),
    }


def _event(event_id: str, user_id: int, session_id: str, event_type: str, page_url: str,
           event_time: datetime, campaign_id: str, referrer: str, element_id: str, revenue: float):
    return {
        "event_id": event_id,
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "page_url": page_url,
        "element_id": element_id,
        "event_time": iso(event_time),
        "referrer": referrer,
        "campaign_id": campaign_id,
        "revenue_usd": revenue,
        "ingested_at": iso(now_utc()),
    }


def gen_session_journey(user_id: int, session_id: str, campaign_id: str) -> List[Dict]:
    """
    Generate a realistic user session journey with events.
    """
    t0 = now_utc() - timedelta(seconds=random.randint(0, 15))
    ref = faker.url() if random.random() < 0.65 else ""
    events = []

    # landing page
    landing_url, _ = random.choice(STATIC_PAGES[:3])  # home/search/blog
    events.append(_event(
        event_id=faker.bothify("EVT-########"),
        user_id=user_id,
        session_id=session_id,
        event_type="page_view",
        page_url=landing_url,
        event_time=t0,
        campaign_id=campaign_id,
        referrer=ref,
        element_id="",
        revenue=0.0
    ))

    # browse 1-4 products
    n_products = random.randint(1, 4)
    product_choices = random.sample(PRODUCTS, k=n_products)
    current = t0

    for pid, pcat, url in product_choices:
        current += timedelta(seconds=random.randint(3, 25))
        events.append(_event(
            faker.bothify("EVT-########"), user_id, session_id, "page_view",
            url, current, campaign_id, "", "", 0.0
        ))

        # click on CTA
        if random.random() < 0.75:
            current += timedelta(seconds=random.randint(1, 10))
            events.append(_event(
                faker.bothify("EVT-########"), user_id, session_id, "click",
                url, current, campaign_id, "", random.choice(["cta_add_to_cart", "cta_details", "cta_buy_now"]), 0.0
            ))

        # maybe add to cart
        if random.random() < 0.35:
            current += timedelta(seconds=random.randint(1, 8))
            events.append(_event(
                faker.bothify("EVT-########"), user_id, session_id, "add_to_cart",
                "/cart", current, campaign_id, "", f"add_{pid}", 0.0
            ))

            # maybe checkout
            if random.random() < 0.45:
                current += timedelta(seconds=random.randint(2, 15))
                events.append(_event(
                    faker.bothify("EVT-########"), user_id, session_id, "checkout_start",
                    "/checkout", current, campaign_id, "", "checkout_start", 0.0
                ))

                # sometimes purchase
                if random.random() < 0.35:
                    current += timedelta(seconds=random.randint(5, 30))
                    revenue = round(random.uniform(15, 650), 2)
                    events.append(_event(
                        faker.bothify("EVT-########"), user_id, session_id, "purchase",
                        "/checkout", current, campaign_id, "", "purchase_confirm", revenue
                    ))

    return events


# Database insertion functions
def insert_user(conn, user: Dict) -> bool:
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO users (user_id, full_name, email, country, signup_time, marketing_opt_in, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (user["user_id"], user["full_name"], user["email"], user["country"],
              user["signup_time"], user["marketing_opt_in"], user["ingested_at"]))
        return True
    except psycopg2.Error as e:
        # Ignore duplicate key errors (RisingWave doesn't support ON CONFLICT)
        if "duplicate" in str(e).lower():
            return False
        raise
    finally:
        cursor.close()


def insert_device(conn, device: Dict) -> bool:
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO devices (device_id, device_type, os, browser, user_agent, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (device["device_id"], device["device_type"], device["os"],
              device["browser"], device["user_agent"], device["ingested_at"]))
        return True
    except psycopg2.Error as e:
        if "duplicate" in str(e).lower():
            return False
        raise
    finally:
        cursor.close()


def insert_campaign(conn, campaign: Dict) -> bool:
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO campaigns (campaign_id, source, medium, campaign, content, term, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (campaign["campaign_id"], campaign["source"], campaign["medium"],
              campaign["campaign"], campaign["content"], campaign["term"], campaign["ingested_at"]))
        return True
    except psycopg2.Error as e:
        if "duplicate" in str(e).lower():
            return False
        raise
    finally:
        cursor.close()


def insert_session(conn, session: Dict) -> bool:
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO sessions (session_id, user_id, device_id, session_start, ip_address, geo_city, geo_region, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (session["session_id"], session["user_id"], session["device_id"],
              session["session_start"], session["ip_address"], session["geo_city"],
              session["geo_region"], session["ingested_at"]))
        return True
    except psycopg2.Error as e:
        if "duplicate" in str(e).lower():
            return False
        raise
    finally:
        cursor.close()


def insert_event(conn, event: Dict) -> bool:
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO clickstream_events
            (event_id, user_id, session_id, event_type, page_url, element_id, event_time, referrer, campaign_id, revenue_usd, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (event["event_id"], event["user_id"], event["session_id"], event["event_type"],
              event["page_url"], event["element_id"], event["event_time"], event["referrer"],
              event["campaign_id"], event["revenue_usd"], event["ingested_at"]))
        return True
    except psycopg2.Error as e:
        if "duplicate" in str(e).lower():
            return False
        raise
    finally:
        cursor.close()


def insert_page_catalog(conn, page_url: str, page_category: str, product_id: str = None, product_category: str = None) -> bool:
    """Insert a page into the page catalog (dimension table)."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO page_catalog (page_url, page_category, product_id, product_category, ingested_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (page_url, page_category, product_id, product_category, iso(now_utc())))
        return True
    except psycopg2.Error as e:
        if "duplicate" in str(e).lower():
            return False
        raise
    finally:
        cursor.close()


def ensure_pages_in_catalog(conn):
    """Ensure all static pages and product pages are in the catalog."""
    # Insert static pages
    for url, page_category in STATIC_PAGES:
        insert_page_catalog(conn, url, page_category, None, None)
    
    # Insert product pages
    for pid, pcat, url in PRODUCTS:
        insert_page_catalog(conn, url, "product", pid, pcat)


def generate_and_insert_session(conn, user_id_counter: int) -> Tuple[int, int, int, int]:
    """Generate a complete session with all dimension data and events."""
    user_id = user_id_counter
    session_id = faker.uuid4()
    
    # Generate dimension data
    user = gen_user(user_id)
    device_id, device = gen_device()
    campaign_id, campaign = gen_campaign()
    session = gen_session(session_id, user_id, device_id)
    
    # Insert dimension data (ignore conflicts for existing records)
    users_inserted = 1 if insert_user(conn, user) else 0
    devices_inserted = 1 if insert_device(conn, device) else 0
    campaigns_inserted = 1 if insert_campaign(conn, campaign) else 0
    sessions_inserted = 1 if insert_session(conn, session) else 0
    
    # Generate and insert events
    events = gen_session_journey(user_id, session_id, campaign_id)
    events_inserted = 0
    for event in events:
        if insert_event(conn, event):
            events_inserted += 1
    
    return users_inserted, devices_inserted, campaigns_inserted, events_inserted


def main():
    parser = argparse.ArgumentParser(description="Direct Producer for RisingWave with rich clickstream data")
    parser.add_argument("--tps", type=float, default=1.0, help="Transactions (sessions) per second (default: 1.0)")
    args = parser.parse_args()

    print(f"Starting direct producer pre-flight checks...", flush=True)
    
    # Connect to database
    conn = connect_db()
    
    # Check tables exist
    if not check_tables_exist(conn):
        conn.close()
        sys.exit(1)
    
    # Ensure all pages are in the catalog (run once at startup)
    ensure_pages_in_catalog(conn)
    
    print(f"✅ Pre-flight checks passed. RisingWave is reachable and all tables exist.", flush=True)
    print(f"Starting data generation at {args.tps} sessions/second... Press Ctrl+C to stop.", flush=True)
    
    interval = 1.0 / args.tps if args.tps > 0 else 0
    
    # Counters and reporting state
    total_users = 0
    total_devices = 0
    total_campaigns = 0
    total_events = 0
    sessions_count = 0
    last_report_time = time.time()
    user_id_counter = random.randint(1, 1000000)

    try:
        while True:
            current_loop_time = time.time()
            
            # Check if it's time to report (every second)
            if current_loop_time - last_report_time >= 1.0:
                print(f"[{get_timestamp()}] Sessions: {sessions_count}, Users: {total_users}, "
                      f"Devices: {total_devices}, Campaigns: {total_campaigns}, Events: {total_events}", flush=True)
                # Reset counters
                total_users = 0
                total_devices = 0
                total_campaigns = 0
                total_events = 0
                sessions_count = 0
                last_report_time = current_loop_time

            # Produce session if TPS > 0
            if interval > 0:
                u, d, c, e = generate_and_insert_session(conn, user_id_counter)
                total_users += u
                total_devices += d
                total_campaigns += c
                total_events += e
                sessions_count += 1
                user_id_counter += 1

                # Control execution rate
                elapsed = time.time() - current_loop_time
                time_to_next_report = max(0, 1.0 - (time.time() - last_report_time))
                sleep_time = min(max(0, interval - elapsed), time_to_next_report)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
            else:
                # If TPS is 0, just sleep until next report
                time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nStopping data generation.", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
