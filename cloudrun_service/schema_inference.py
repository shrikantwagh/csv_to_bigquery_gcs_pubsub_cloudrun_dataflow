"""CSV schema inference for BigQuery.

We infer a BigQuery schema from a small sample of the CSV file stored in GCS.

Approach:
  - Download only the first N lines (not whole file) from GCS.
  - Parse with Python's csv module (handles quoted fields).
  - Assume first row is header.
  - For each column, infer the "best" BigQuery type using heuristics:
      INT64, FLOAT64, BOOL, DATE, TIMESTAMP, STRING
    (STRING is the safe fallback.)

This is intentionally conservative:
  - If values conflict (e.g., some ints and some strings), we pick STRING.
  - We treat empty strings / null-like values as missing (do not affect type).
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dateutil import parser as date_parser
from google.cloud import storage


NULL_LIKE = {"", "null", "none", "na", "n/a", "nan", "nil", "-"}

_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?(\d+\.\d*|\d*\.\d+|\d+)([eE][+-]?\d+)?$")


@dataclass(frozen=True)
class BQField:
    """A minimal schema field representation."""
    name: str
    field_type: str
    mode: str = "NULLABLE"
    description: str = ""


def _download_head_lines(bucket: str, object_name: str, sample_lines: int) -> str:
    """Download enough bytes from the start of a blob to cover ~sample_lines lines.

    GCS supports range reads; we can fetch just the head.
    We grow the range until we capture at least sample_lines newlines or hit a ceiling.
    """
    client = storage.Client()
    blob = client.bucket(bucket).blob(object_name)

    # Heuristic: start with 256KB and grow up to 4MB max.
    start = 0
    end = 256 * 1024 - 1
    max_end = 4 * 1024 * 1024 - 1

    while True:
        data = blob.download_as_bytes(start=start, end=end)
        text = data.decode("utf-8", errors="replace")
        if text.count("\n") >= sample_lines or end >= max_end:
            return text
        end = min(max_end, end * 2 + 1)


def _detect_dialect(sample_text: str) -> csv.Dialect:
    """Try to detect delimiter/quote style."""
    sniffer = csv.Sniffer()
    try:
        return sniffer.sniff(sample_text)
    except Exception:
        return csv.get_dialect("excel")


def _is_null_like(v: str) -> bool:
    return v.strip().lower() in NULL_LIKE


def _is_bool(v: str) -> bool:
    x = v.strip().lower()
    return x in {"true", "false", "t", "f", "1", "0", "yes", "no", "y", "n"}


def _is_int(v: str) -> bool:
    return bool(_INT_RE.match(v.strip()))


def _is_float(v: str) -> bool:
    s = v.strip()
    return bool(_FLOAT_RE.match(s)) and not _is_int(s)


def _parse_datetime(v: str) -> Optional[datetime]:
    try:
        return date_parser.parse(v)
    except Exception:
        return None


def _merge_types(current: str, new_type: str) -> str:
    """Merge column types conservatively."""
    if current == new_type:
        return current

    if current == "STRING" or new_type == "STRING":
        return "STRING"

    if {current, new_type} == {"INT64", "FLOAT64"}:
        return "FLOAT64"

    if {current, new_type} == {"DATE", "TIMESTAMP"}:
        return "TIMESTAMP"

    return "STRING"


def _infer_type_for_value(v: str) -> Optional[str]:
    if v is None or _is_null_like(v):
        return None

    if _is_bool(v):
        return "BOOL"
    if _is_int(v):
        return "INT64"
    if _is_float(v):
        return "FLOAT64"

    dt = _parse_datetime(v)
    if dt:
        s = v.strip()
        if re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$", s) and (dt.hour, dt.minute, dt.second) == (0, 0, 0):
            return "DATE"
        return "TIMESTAMP"

    return "STRING"


def infer_bq_schema_from_gcs_csv(
    project_id: str,
    bucket: str,
    object_name: str,
    sample_lines: int = 200,
) -> Tuple[List[BQField], List[str]]:
    """Infer BigQuery schema from the head of a CSV stored in GCS."""
    _ = project_id  # reserved for future improvements
    head_text = _download_head_lines(bucket=bucket, object_name=object_name, sample_lines=sample_lines)

    lines = head_text.splitlines()[:sample_lines]
    sample_text = "\n".join(lines)

    dialect = _detect_dialect(sample_text)
    reader = csv.reader(io.StringIO(sample_text), dialect=dialect)

    rows = list(reader)
    if not rows:
        raise ValueError("CSV sample is empty; cannot infer schema.")

    header = [h.strip() for h in rows[0]]
    if not header or any(h == "" for h in header):
        raise ValueError("CSV header is missing/blank; expected first row to be header.")

    col_types: Dict[str, str] = {name: "" for name in header}

    for row in rows[1:]:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        elif len(row) > len(header):
            row = row[: len(header)]

        for name, value in zip(header, row):
            t = _infer_type_for_value(value)
            if not t:
                continue
            if not col_types[name]:
                col_types[name] = t
            else:
                col_types[name] = _merge_types(col_types[name], t)

    fields: List[BQField] = []
    for name in header:
        inferred = col_types.get(name) or "STRING"
        safe = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")
        if not safe:
            safe = "col"
        if not re.match(r"^[A-Za-z_]", safe):
            safe = "_" + safe
        fields.append(BQField(name=safe, field_type=inferred, mode="NULLABLE", description=f"Inferred from {object_name}"))
    return fields, [f.name for f in fields]
