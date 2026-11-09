"""
Production-Grade Apache Beam Streaming Pipeline for Google Cloud Dataflow.
Features:
- Reads real-time events from Cloud Pub/Sub
- Validates records against strict schema
- Routes invalid records (poison pills) to a Dead-Letter Queue (TaggedOutput)
- Writes clean records into BigQuery using modern Storage Write API
- Handles windowing and metrics
"""

import argparse
import json
import logging
from typing import Dict, Any, Tuple
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.pvalue import TaggedOutput


# Dead-letter tag for side output
TAG_DLQ = "dead_letter_records"
TAG_VALID = "valid_records"


class ParseAndValidateOrderFn(beam.DoFn):
    """
    DoFn that decodes, parses JSON, and validates mandatory schema fields.
    Valid records are emitted normally.
    Failed/malformed records are routed to the DLQ TaggedOutput.
    """
    def __init__(self):
        super().__init__()
        self.valid_counter = beam.metrics.Metrics.counter(self.__class__, "valid_orders")
        self.invalid_counter = beam.metrics.Metrics.counter(self.__class__, "invalid_orders")

    def process(self, element: bytes):
        raw_str = ""
        try:
            # 1. Decode UTF-8
            raw_str = element.decode("utf-8")
            payload: Dict[str, Any] = json.loads(raw_str)

            # 2. Schema Validation (Mandatory Fields & Types)
            required_fields = ["order_id", "customer_id", "amount", "order_timestamp", "status"]
            for field in required_fields:
                if field not in payload or payload[field] is None:
                    raise ValueError(f"Missing required field: '{field}'")

            # Validate amount is numeric
            float(payload["amount"])

            # 3. Clean and normalize record for BigQuery
            clean_record = {
                "order_id": str(payload["order_id"]),
                "customer_id": str(payload["customer_id"]),
                "amount": float(payload["amount"]),
                "items": payload.get("items", []),
                "order_timestamp": str(payload["order_timestamp"]),
                "status": str(payload["status"]).upper()
            }

            self.valid_counter.inc()
            yield TaggedOutput(TAG_VALID, clean_record)

        except Exception as exc:
            self.invalid_counter.inc()
            logging.warning("Routing failed message to DLQ: %s | Error: %s", raw_str, str(exc))
            dlq_record = {
                "raw_payload": raw_str,
                "error_message": str(exc),
                "insertion_timestamp": beam.utils.timestamp.Timestamp.now().to_rfc3339()
            }
            yield TaggedOutput(TAG_DLQ, dlq_record)


def run():
    parser = argparse.ArgumentParser(description="PubSub to BigQuery Streaming Pipeline")
    parser.add_argument(
        "--input_subscription",
        required=True,
        help="Full resource path of the Pub/Sub subscription."
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="Destination BigQuery table: PROJECT_ID:DATASET.TABLE"
    )
    parser.add_argument(
        "--dlq_topic",
        required=False,
        help="Pub/Sub topic for Dead-Letter messages."
    )
    parser.add_argument(
        "--dlq_table",
        required=False,
        help="BigQuery table for Dead-Letter records: PROJECT_ID:DATASET.DLQ_TABLE"
    )

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Ingest Stream from Pub/Sub
        raw_messages = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
        )

        # Step 2: Validate and Split Stream
        validated_results = (
            raw_messages
            | "ParseAndValidate" >> beam.ParDo(ParseAndValidateOrderFn()).with_outputs(
                TAG_VALID, TAG_DLQ
            )
        )

        valid_orders = validated_results[TAG_VALID]
        dlq_records = validated_results[TAG_DLQ]

        # Step 3: Write Valid Records to BigQuery (Storage Write API)
        valid_orders | "WriteValidToBigQuery" >> beam.io.WriteToBigQuery(
            table=known_args.output_table,
            schema=(
                "order_id:STRING, customer_id:STRING, amount:NUMERIC, "
                "items:RECORD, order_timestamp:TIMESTAMP, status:STRING"
            ),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API
        )

        # Step 4: Handle Dead-Letter Queue (DLQ)
        if known_args.dlq_table:
            dlq_records | "WriteDLQToBigQuery" >> beam.io.WriteToBigQuery(
                table=known_args.dlq_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )

        if known_args.dlq_topic:
            (
                dlq_records
                | "EncodeDLQJson" >> beam.Map(lambda x: json.dumps(x).encode("utf-8"))
                | "WriteDLQToPubSub" >> beam.io.WriteToPubSub(topic=known_args.dlq_topic)
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
