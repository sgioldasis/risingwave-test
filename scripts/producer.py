import json
import time
import random
import argparse
from datetime import datetime
from kafka import KafkaProducer

# Connect to local Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPICS = ['page_views', 'cart_events', 'purchases']

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def main():
    parser = argparse.ArgumentParser(description="Kafka Event Producer with configurable TPS")
    parser.add_argument("--tps", type=float, default=1.0, help="Transactions per second (default: 1.0)")
    args = parser.parse_args()

    # Connect to local Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:19092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    TOPICS = ['page_views', 'cart_events', 'purchases']

    print(f"Starting data generation at {args.tps} TPS... Press Ctrl+C to stop.", flush=True)

    interval = 1.0 / args.tps if args.tps > 0 else 0
    
    # Counters and reporting state
    views_count = 0
    carts_count = 0
    purchases_count = 0
    last_report_time = time.time()

    try:
        while True:
            current_loop_time = time.time()
            
            # Check if it's time to report (every second)
            if current_loop_time - last_report_time >= 1.0:
                total_count = views_count + carts_count + purchases_count
                print(f"[{get_timestamp()}] Views: {views_count}, Carts: {carts_count}, Purchases: {purchases_count}, Total: {total_count}", flush=True)
                # Reset counters
                views_count = 0
                carts_count = 0
                purchases_count = 0
                last_report_time = current_loop_time

            # Produce events if TPS > 0
            if interval > 0:
                user_id = random.randint(1, 10000)
                event_time_str = get_timestamp()

                # 1. Page Views (Always)
                view_data = {
                    "user_id": user_id,
                    "page_id": f"page_{random.randint(1, 100)}",
                    "event_time": event_time_str
                }
                producer.send('page_views', value=view_data)
                views_count += 1

                # 2. Add to Cart (Medium volume)
                if random.random() < 0.3:
                    cart_data = {
                        "user_id": user_id,
                        "item_id": f"item_{random.randint(100, 1000)}",
                        "event_time": event_time_str
                    }
                    producer.send('cart_events', value=cart_data)
                    carts_count += 1

                # 3. Purchases (Low volume)
                if random.random() < 0.1:
                    purchase_data = {
                        "user_id": user_id,
                        "amount": round(random.uniform(10, 500), 2),
                        "event_time": event_time_str
                    }
                    producer.send('purchases', value=purchase_data)
                    purchases_count += 1

                # Control execution rate for events
                elapsed = time.time() - current_loop_time
                # We want to sleep for 'interval' but not longer than the time until the next report
                time_to_next_report = max(0, 1.0 - (time.time() - last_report_time))
                sleep_time = min(max(0, interval - elapsed), time_to_next_report)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
            else:
                # If TPS is 0, just sleep until next report
                time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nStopping data generation.", flush=True)
        producer.close()

if __name__ == "__main__":
    main()