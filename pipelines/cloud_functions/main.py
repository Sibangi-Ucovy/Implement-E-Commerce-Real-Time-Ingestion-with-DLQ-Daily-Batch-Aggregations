import os
import json
import logging
from typing import Tuple
import functions_framework
from google.cloud import pubsub_v1

# Initialize Pub/Sub Publisher Client outside request handler for connection pooling
PROJECT_ID = os.environ.get("GCP_PROJECT", "my-gcp-project")
TOPIC_ID = os.environ.get("PUBSUB_TOPIC", "orders-stream-topic")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


@functions_framework.http
def ingest_order(request) -> Tuple[str, int]:
    """
    HTTP Cloud Function endpoint to receive order webhooks.
    Validates payload and forwards to Pub/Sub.
    """
    if request.method != "POST":
        return ("Method Not Allowed. Use POST.", 405)

    request_json = request.get_json(silent=True)
    if not request_json:
        return ("Invalid JSON payload.", 400)

    # Basic quick validation
    if "order_id" not in request_json or "amount" not in request_json:
        return ("Missing 'order_id' or 'amount'", 422)

    try:
        data_str = json.dumps(request_json)
        data_bytes = data_str.encode("utf-8")

        # Publish message with ordering key or customer attribute
        future = publisher.publish(
            topic_path,
            data=data_bytes,
            customer_id=str(request_json.get("customer_id", "unknown")),
            source="http-webhook"
        )
        message_id = future.result(timeout=10)
        return (json.dumps({"status": "SUCCESS", "message_id": message_id}), 200)

    except Exception as exc:
        logging.exception("Failed to publish message to Pub/Sub: %s", exc)
        return (json.dumps({"status": "ERROR", "error": str(exc)}), 500)
