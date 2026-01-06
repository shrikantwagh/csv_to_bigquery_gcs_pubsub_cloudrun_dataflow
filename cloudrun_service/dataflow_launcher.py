"""Launch the Apache Beam pipeline on Dataflow.

This repo uses a programmatic Dataflow submission:
  - Cloud Run receives the Pub/Sub event
  - Cloud Run submits a Dataflow job using Beam's DataflowRunner
  - Dataflow workers run the actual CSV->BigQuery pipeline

Important:
  - Do NOT wait for job completion inside Cloud Run.
  - Beam dependencies inside Cloud Run make the image heavier; for production,
    consider a Dataflow Flex Template.
"""

from __future__ import annotations

import logging
import time
from typing import List, Optional

from apache_beam.options.pipeline_options import PipelineOptions

from .schema_inference import BQField
from .bq_utils import bq_schema_string

# We vendor the beam pipeline code into the Cloud Run build context so it can be imported.
from beam_pipeline.pipeline import build_pipeline


def _job_name(prefix: str = "csv-to-bq") -> str:
    # Dataflow job names: lowercase letters, numbers, hyphens; must start with a letter.
    ts = time.strftime("%Y%m%d-%H%M%S")
    return f"{prefix}-{ts}".lower()


def launch_dataflow_csv_to_bq(
    project_id: str,
    region: str,
    gcs_input_path: str,
    bq_table_spec: str,
    schema_fields: List[BQField],
    header_fields: List[str],
    temp_location: str,
    staging_location: str,
    dataflow_service_account_email: Optional[str] = None,
) -> str:
    """Launch a Dataflow job to ingest ONE CSV file into BigQuery.

    Args:
      project_id: GCP project id
      region: Dataflow region (e.g. us-central1)
      gcs_input_path: Single GCS file URI, e.g. gs://bucket/incoming/file.csv
      bq_table_spec: BigQuery table spec: project:dataset.table
      schema_fields: Inferred BigQuery schema fields
      header_fields: Sanitized header field names (same order as CSV columns)
      temp_location: GCS temp location for Dataflow, e.g. gs://bucket/dataflow/temp
      staging_location: GCS staging location for Dataflow, e.g. gs://bucket/dataflow/staging
      dataflow_service_account_email: Optional service account for Dataflow workers

    Returns:
      Dataflow job id (string) if available.
    """
    if not temp_location or not staging_location:
        raise ValueError(
            "DF_TEMP_LOCATION and DF_STAGING_LOCATION must be set to valid gs:// paths."
        )

    schema_str = bq_schema_string(schema_fields)

    opts = [
        "--runner=DataflowRunner",
        f"--project={project_id}",
        f"--region={region}",
        f"--job_name={_job_name()}",
        f"--temp_location={temp_location}",
        f"--staging_location={staging_location}",
        "--experiments=use_runner_v2",
        "--setup_file=/app/setup.py",
    ]
    if dataflow_service_account_email:
        opts.append(f"--service_account_email={dataflow_service_account_email}")

    pipeline_options = PipelineOptions(opts)

    # IMPORTANT:
    # Our updated Beam pipeline does NOT try to read the header on Dataflow.
    # Cloud Run already sampled the file for schema inference; we reuse the same
    # sanitized header_fields here to map each CSV line -> dict reliably.
    p = build_pipeline(
        pipeline_options=pipeline_options,
        input_path=gcs_input_path,
        bq_table_spec=bq_table_spec,
        bq_schema=schema_str,
        header_fields=header_fields,
    )

    result = p.run()

    # For DataflowRunner, the job is submitted asynchronously.
    job_id = getattr(result, "job_id", lambda: None)()
    logging.info("Submitted Dataflow pipeline. job_id=%s state=%s", job_id, result.state)

    return job_id or "unknown"
