"""
Poison Pill Simulator for GCP Dataflow & DLQ Verification.
Sends deliberate schema violations and malformed data to ensure the pipeline
doesn't crash and correctly routes bad elements into the Dead-Letter Queue.
"""

import argparse
import time
from google.cloud import pubsub_v1


POISON_PILLS = [
    # 1. Totally invalid JSON
    b"MALFORMED_JSON_{{{not_valid_json",
    # 2. Missing mandatory field 'order_id'
    b'{"customer_id": "cust_999", "amount": 50.0, "order_timestamp": "2026-11-09T10:00:00Z", "status": "COMPLETED"}',
    # 3. Missing mandatory field 'amount'
    b'{"order_id": "bad-uuid-1", "customer_id": "cust_888", "order_timestamp": "2026-11-09T10:00:00Z", "status": "COMPLETED"}',
    # 4. Type mismatch: amount is a string that cannot be cast to float
    b'{"order_id": "bad-uuid-2", "customer_id": "cust_777", "amount": "ONE_MILLION_DOLLARS", "order_timestamp": "2026-11-09T10:00:00Z", "status": "COMPLETED"}',
    # 5. Null payload
    b'{"order_id": null, "customer_id": null, "amount": null, "order_timestamp": null, "status": null}'
]


def main():
    parser = argparse.ArgumentParser(description="Publish poison pills to trigger DLQ.")
    parser.add_argument("--project", required=True, help="GCP Project ID")
    parser.add_argument("--topic", default="orders-stream-topic", help="Pub/Sub Topic name")

    args = parser.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)

    print(f"⚠️ Injecting {len(POISON_PILLS)} poison pills into {topic_path}...")

    for i, pill in enumerate(POISON_PILLS, 1):
        future = publisher.publish(topic_path, data=pill)
        print(f"[{i}/{len(POISON_PILLS)}] Published Poison Pill -> Msg ID: {future.result()}")
        time.sleep(0.3)

    print("✅ Poison pills sent. Verify in Dataflow metrics (invalid_orders counter) and BigQuery DLQ table.")


if __name__ == "__main__":
    main()
