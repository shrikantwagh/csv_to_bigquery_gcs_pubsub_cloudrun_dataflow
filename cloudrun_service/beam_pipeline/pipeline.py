"""Apache Beam pipeline: Read a single CSV from GCS and write to BigQuery.

This version is intentionally *simple and version-stable* for Dataflow:

- Cloud Run already sampled the CSV to infer schema, so Cloud Run can also provide the
  sanitized header field names (matching BigQuery column names).
- Dataflow reads the CSV with ReadFromText(..., skip_header_lines=1) and parses each
  line into a dict using the provided header fields.
- Then it appends rows into an existing BigQuery table.

Why this design?
- Avoids Beam transforms that vary by version (e.g., filename-aware text reads).
- Works well with the "one file upload -> one Dataflow job" trigger model.

Inputs:
  - input_path: gs://bucket/prefix/file.csv
  - bq_table_spec: PROJECT:DATASET.TABLE
  - bq_schema: 'col1:STRING,col2:INT64,...' (Beam/BigQuery schema string)
  - header_fields: list of column names that match the BigQuery schema
"""

from __future__ import annotations

import csv
from typing import Dict, List, Optional

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


def _row_to_dict(line: str, header_fields: List[str]) -> Dict[str, str]:
    """Parse one CSV line into a dict keyed by header_fields.

    Notes:
      - Uses csv.reader to properly handle quotes and commas inside quoted strings.
      - Pads/truncates row length to match header length.
      - Produces STRING values; BigQuery will coerce types based on the destination schema.
    """
    row = next(csv.reader([line]), [])
    if len(row) < len(header_fields):
        row += [""] * (len(header_fields) - len(row))
    elif len(row) > len(header_fields):
        row = row[: len(header_fields)]
    return {k: v for k, v in zip(header_fields, row)}


def build_pipeline(
    pipeline_options: PipelineOptions,
    input_path: str,
    bq_table_spec: str,
    bq_schema: str,
    header_fields: List[str],
) -> beam.Pipeline:
    """Build (do not run) the Beam pipeline."""
    if not header_fields:
        raise ValueError("header_fields is empty; cannot map CSV rows to columns.")

    p = beam.Pipeline(options=pipeline_options)

    rows = (
        p
        | "ReadCsvLines" >> ReadFromText(input_path, skip_header_lines=1)
        | "ParseCsv" >> beam.Map(lambda line, hf=header_fields: _row_to_dict(line, hf))
    )

    _ = rows | "WriteToBigQuery" >> WriteToBigQuery(
        table=bq_table_spec,
        schema=bq_schema,
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        method=WriteToBigQuery.Method.FILE_LOADS,
    )

    return p


# Optional local runner entrypoint (useful for debugging with DirectRunner).
def main(argv: Optional[List[str]] = None) -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--bq_table_spec", required=True)
    parser.add_argument("--bq_schema", required=True)
    parser.add_argument("--header_fields", required=True, help="Comma-separated header fields.")
    known, beam_args = parser.parse_known_args(argv)

    header_fields = [h.strip() for h in known.header_fields.split(",") if h.strip()]
    opts = PipelineOptions(beam_args)

    p = build_pipeline(
        pipeline_options=opts,
        input_path=known.input_path,
        bq_table_spec=known.bq_table_spec,
        bq_schema=known.bq_schema,
        header_fields=header_fields,
    )
    p.run().wait_until_finish()


if __name__ == "__main__":
    main()
