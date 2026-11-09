import json
import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipelines.streaming_beam_dataflow.pipeline import ParseAndValidateOrderFn, TAG_VALID, TAG_DLQ


def test_valid_order_processing():
    valid_payload = json.dumps({
        "order_id": "ord-001",
        "customer_id": "cust-10",
        "amount": 99.95,
        "items": [{"item_id": "sku-1", "qty": 1, "price": 99.95}],
        "order_timestamp": "2026-11-09T10:00:00Z",
        "status": "COMPLETED"
    }).encode("utf-8")

    with TestPipeline() as p:
        input_data = p | beam.Create([valid_payload])
        results = input_data | beam.ParDo(ParseAndValidateOrderFn()).with_outputs(TAG_VALID, TAG_DLQ)

        valid_out = results[TAG_VALID]
        assert_that(valid_out, equal_to([{
            "order_id": "ord-001",
            "customer_id": "cust-10",
            "amount": 99.95,
            "items": [{"item_id": "sku-1", "qty": 1, "price": 99.95}],
            "order_timestamp": "2026-11-09T10:00:00Z",
            "status": "COMPLETED"
        }]))


def test_poison_pill_routes_to_dlq():
    malformed_payload = b"NOT_A_VALID_JSON_STRING"

    with TestPipeline() as p:
        input_data = p | beam.Create([malformed_payload])
        results = input_data | beam.ParDo(ParseAndValidateOrderFn()).with_outputs(TAG_VALID, TAG_DLQ)

        dlq_out = results[TAG_DLQ]

        def check_dlq(elements):
            assert len(elements) == 1
            assert elements[0]["raw_payload"] == "NOT_A_VALID_JSON_STRING"
            assert "JSONDecodeError" in elements[0]["error_message"] or "Expecting value" in elements[0]["error_message"]

        assert_that(dlq_out, check_dlq)
