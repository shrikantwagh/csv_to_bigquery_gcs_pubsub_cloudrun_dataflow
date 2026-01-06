"""Cloud Run entrypoint: Pub/Sub push subscription handler.

Expected flow:
  - Cloud Storage publishes OBJECT_FINALIZE notifications to Pub/Sub.
  - Pub/Sub push subscription POSTs events to this Cloud Run service.
  - This handler:
      1) Parses the Pub/Sub message and extracts bucket/object metadata.
      2) Filters to CSV files under a configured prefix (e.g., 'incoming/').
      3) Infers a BigQuery schema from a sample of the CSV.
      4) Creates the destination BigQuery table if it does not exist.
      5) Launches a Dataflow job that reads the entire CSV and writes to BigQuery.

This design keeps "schema inference" in Cloud Run (fast, cheap) and leaves
scalable ingestion to Dataflow.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
from typing import Any, Dict, Optional, Tuple

from .schema_inference import infer_bq_schema_from_gcs_csv
from .bq_utils import ensure_dataset, ensure_table
from .dataflow_launcher import launch_dataflow_csv_to_bq

logging.basicConfig(level=logging.INFO)

# ---- Environment configuration ----
PROJECT_ID = os.getenv("PROJECT_ID", "")
REGION = os.getenv("REGION", "us-central1")
BUCKET = os.getenv("BUCKET", "")
PREFIX = os.getenv("PREFIX", "incoming/")
BQ_DATASET = os.getenv("BQ_DATASET", "csv_ingest")
TABLE_PREFIX = os.getenv("TABLE_PREFIX", "csv_")
DF_TEMP_LOCATION = os.getenv("DF_TEMP_LOCATION", "")
DF_STAGING_LOCATION = os.getenv("DF_STAGING_LOCATION", "")
DATAFLOW_SA_EMAIL = os.getenv("DATAFLOW_SA_EMAIL")  # optional

# How many lines to sample for schema inference
SCHEMA_SAMPLE_LINES = int(os.getenv("SCHEMA_SAMPLE_LINES", "200"))

# If your filenames include spaces/special chars, sanitize table name more aggressively.
_TABLE_NAME_SAFE = re.compile(r"[^a-zA-Z0-9_]+")


def _decode_pubsub_push(request_json: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Decode Pub/Sub push JSON.

    Pub/Sub push typically posts:
      {
        "message": {
          "data": "base64...",
          "attributes": {...},
          ...
        },
        "subscription": "..."
      }

    Returns:
      data_obj: Dict decoded from message.data if possible, else {}
      attrs: message.attributes (may contain GCS metadata depending on publisher)
    """
    if "message" not in request_json:
        raise ValueError("Missing 'message' in Pub/Sub push payload.")

    msg = request_json["message"]
    attrs = msg.get("attributes") or {}

    data_obj: Dict[str, Any] = {}
    data_b64 = msg.get("data")
    if data_b64:
        raw = base64.b64decode(data_b64).decode("utf-8", errors="replace")
        try:
            data_obj = json.loads(raw)
        except json.JSONDecodeError:
            # Some publishers send non-JSON strings; keep raw for debugging.
            data_obj = {"raw": raw}

    return data_obj, attrs


def _extract_gcs_object(data_obj: Dict[str, Any], attrs: Dict[str, str]) -> Optional[Tuple[str, str]]:
    """Extract (bucket, object_name) from common GCS->Pub/Sub event formats.

    Cloud Storage notifications can include object info in attributes like:
      - bucketId
      - objectId
      - eventType

    Or some pipelines encode bucket/object in message.data JSON.

    We attempt several known shapes.
    """
    # 1) Attribute-based (common for GCS notifications)
    bucket = attrs.get("bucketId") or attrs.get("bucket") or attrs.get("bucket_name")
    obj = attrs.get("objectId") or attrs.get("name") or attrs.get("object") or attrs.get("object_name")
    if bucket and obj:
        return bucket, obj

    # 2) Data-based (some systems publish JSON in data)
    bucket = data_obj.get("bucket") or data_obj.get("bucketId") or data_obj.get("bucket_name")
    obj = data_obj.get("name") or data_obj.get("objectId") or data_obj.get("object") or data_obj.get("object_name")
    if bucket and obj:
        return bucket, obj

    # 3) CloudEvents-ish shapes (if you later swap to Eventarc)
    if "data" in data_obj and isinstance(data_obj["data"], dict):
        d = data_obj["data"]
        bucket = d.get("bucket")
        obj = d.get("name")
        if bucket and obj:
            return bucket, obj

    return None


def _is_target_csv(bucket: str, object_name: str) -> bool:
    """Return True if this event should be processed."""
    if BUCKET and bucket != BUCKET:
        logging.info("Ignoring event for bucket=%s (expected BUCKET=%s).", bucket, BUCKET)
        return False

    if PREFIX and not object_name.startswith(PREFIX):
        logging.info("Ignoring object outside PREFIX. object=%s prefix=%s", object_name, PREFIX)
        return False

    if not object_name.lower().endswith(".csv"):
        logging.info("Ignoring non-CSV object: %s", object_name)
        return False

    return True


def _make_table_id_from_object(object_name: str) -> str:
    """Derive a BigQuery table name from the object path.

    Example:
      incoming/sales_2026-01-01.csv -> csv_sales_2026_01_01

    BigQuery table rules:
      - Letters, numbers, and underscores
      - Must start with a letter or underscore
    """
    file_stem = object_name.split("/")[-1].rsplit(".", 1)[0]
    cleaned = _TABLE_NAME_SAFE.sub("_", file_stem).strip("_")
    if not cleaned:
        cleaned = "data"
    if not re.match(r"^[A-Za-z_]", cleaned):
        cleaned = "_" + cleaned
    return f"{TABLE_PREFIX}{cleaned}"


def pubsub_handler(request):  # Functions Framework entry point
    """HTTP handler for Pub/Sub push subscription.

    Returns an HTTP 2xx code on success so Pub/Sub acknowledges the message.
    """
    try:
        request_json = request.get_json(silent=True) or {}
        data_obj, attrs = _decode_pubsub_push(request_json)

        extracted = _extract_gcs_object(data_obj, attrs)
        if not extracted:
            logging.warning("Could not extract GCS bucket/object from event. data=%s attrs=%s", data_obj, attrs)
            return ("Bad Request: missing bucket/object", 400)

        bucket, object_name = extracted
        logging.info("Received event for gs://%s/%s", bucket, object_name)

        if not _is_target_csv(bucket, object_name):
            # Return 204 (No Content) so Pub/Sub considers it delivered.
            return ("", 204)

        if not PROJECT_ID:
            raise ValueError("PROJECT_ID env var is required.")

        # Infer schema from a sample of the CSV (fast).
        schema_fields, header_fields = infer_bq_schema_from_gcs_csv(
            project_id=PROJECT_ID,
            bucket=bucket,
            object_name=object_name,
            sample_lines=SCHEMA_SAMPLE_LINES,
        )

        # Ensure dataset/table exist.
        ensure_dataset(project_id=PROJECT_ID, dataset_id=BQ_DATASET, location=REGION)
        table_id = _make_table_id_from_object(object_name)
        ensure_table(
            project_id=PROJECT_ID,
            dataset_id=BQ_DATASET,
            table_id=table_id,
            schema_fields=schema_fields,
        )

        # Launch Dataflow to ingest full CSV to BigQuery.
        gcs_uri = f"gs://{bucket}/{object_name}"
        bq_table_spec = f"{PROJECT_ID}:{BQ_DATASET}.{table_id}"

        job_id = launch_dataflow_csv_to_bq(
            project_id=PROJECT_ID,
            region=REGION,
            gcs_input_path=gcs_uri,
            bq_table_spec=bq_table_spec,
            schema_fields=schema_fields,
            header_fields=header_fields,
            temp_location=DF_TEMP_LOCATION,
            staging_location=DF_STAGING_LOCATION,
            dataflow_service_account_email=DATAFLOW_SA_EMAIL,
        )

        logging.info("Launched Dataflow job_id=%s for %s -> %s", job_id, gcs_uri, bq_table_spec)
        return ("OK", 200)

    except Exception as e:
        # Returning non-2xx causes Pub/Sub to retry delivery.
        logging.exception("Handler failed: %s", e)
        return (f"Internal error: {e}", 500)
