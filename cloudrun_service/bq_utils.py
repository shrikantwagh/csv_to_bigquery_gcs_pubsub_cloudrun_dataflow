"""BigQuery helpers: dataset/table creation and schema conversion."""

from __future__ import annotations

import logging
from typing import Iterable, List

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from .schema_inference import BQField


def ensure_dataset(project_id: str, dataset_id: str, location: str) -> None:
    """Create dataset if it doesn't exist."""
    client = bigquery.Client(project=project_id)
    ds = bigquery.Dataset(f"{project_id}.{dataset_id}")
    ds.location = location

    try:
        client.get_dataset(ds)
        return
    except NotFound:
        logging.info("Creating BigQuery dataset: %s.%s", project_id, dataset_id)
        client.create_dataset(ds, exists_ok=True)


def ensure_table(project_id: str, dataset_id: str, table_id: str, schema_fields: List[BQField]) -> None:
    """Create table if it doesn't exist (schema used on first create)."""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        client.get_table(table_ref)
        return
    except NotFound:
        logging.info("Creating BigQuery table: %s", table_ref)

    schema = [bigquery.SchemaField(f.name, f.field_type, mode=f.mode, description=f.description) for f in schema_fields]
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)


def bq_schema_string(schema_fields: Iterable[BQField]) -> str:
    """Convert schema fields into Beam/BigQuery schema string: 'a:STRING,b:INT64'."""
    return ",".join([f"{f.name}:{f.field_type}" for f in schema_fields])
