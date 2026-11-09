"""
Mock Event Generator for GCP Pub/Sub Ingestion Testing.
Publishes synthetic valid e-commerce transactions to verify streaming dataflow.
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone
import uuid
from google.cloud import pubsub_v1


def generate_mock_order() -> dict:
    item_catalog = [
        {"item_id": "SKU-LAPTOP-01", "price": 999.99},
        {"item_id": "SKU-MOUSE-02", "price": 29.50},
        {"item_id": "SKU-KEYBOARD-03", "price": 75.00},
        {"item_id": "SKU-MONITOR-04", "price": 289.00},
    ]

    num_items = random.randint(1, 3)
    selected_items = random.sample(item_catalog, num_items)
    items = []
    total_amount = 0.0

    for item in selected_items:
        qty = random.randint(1, 2)
        price = item["price"]
        items.append({"item_id": item["item_id"], "qty": qty, "price": price})
        total_amount += qty * price

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"cust_{random.randint(100, 999)}",
        "amount": round(total_amount, 2),
        "items": items,
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
        "status": random.choice(["COMPLETED", "COMPLETED", "PENDING"])
    }


def main():
    parser = argparse.ArgumentParser(description="Generate and publish mock orders to Pub/Sub.")
    parser.add_argument("--project", required=True, help="GCP Project ID")
    parser.add_argument("--topic", default="orders-stream-topic", help="Pub/Sub Topic name")
    parser.add_argument("--count", type=int, default=20, help="Number of messages to publish")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between messages in seconds")

    args = parser.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)

    print(f"🚀 Publishing {args.count} events to {topic_path}...")

    for i in range(args.count):
        order = generate_mock_order()
        payload_bytes = json.dumps(order).encode("utf-8")
        future = publisher.publish(topic_path, data=payload_bytes)
        print(f"[{i+1}/{args.count}] Published Order ID: {order['order_id']} (${order['amount']}) -> Msg ID: {future.result()}")
        time.sleep(args.delay)

    print("✅ All events published successfully.")


if __name__ == "__main__":
    main()
