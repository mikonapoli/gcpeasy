"""File utilities for BigQuery file loading."""

from pathlib import Path
from typing import Optional
from google.cloud import bigquery


def detect_source_format(file_path: Path | str) -> str:
    """Detect BigQuery source format from file extension."""
    format_map = {
        ".csv": "CSV",
        ".json": "NEWLINE_DELIMITED_JSON",
        ".jsonl": "NEWLINE_DELIMITED_JSON",
        ".ndjson": "NEWLINE_DELIMITED_JSON",
        ".parquet": "PARQUET",
        ".avro": "AVRO",
        ".orc": "ORC",
    }
    ext = file_path.suffix.lower() if isinstance(file_path, Path) else f".{str(file_path).lower().rsplit('.', 1)[-1]}"
    if ext not in format_map:
        raise ValueError(f"Unsupported or undetected file format: Detected {ext}. Supported formats: {', '.join(format_map.keys())}")
    return format_map[ext]


def create_load_job_config(
    source_format: str,
    schema: Optional[list[bigquery.SchemaField]] = None,
    write_disposition: str = "WRITE_APPEND",
    skip_leading_rows: Optional[int] = None,
    field_delimiter: Optional[str] = None,
    autodetect: bool = False,
) -> bigquery.LoadJobConfig:
    """Create a LoadJobConfig for file loading."""
    config = bigquery.LoadJobConfig(source_format=source_format, write_disposition=write_disposition)
    if schema: config.schema = schema
    elif autodetect: config.autodetect = True
    if source_format == "CSV":
        config.skip_leading_rows = skip_leading_rows if skip_leading_rows is not None else 1
        if field_delimiter: config.field_delimiter = field_delimiter
    return config
