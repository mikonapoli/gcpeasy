"""File utilities for BigQuery file loading."""

from pathlib import Path
from typing import Optional
from google.cloud import bigquery


def detect_source_format(file_path: Path) -> str:
    """Detect BigQuery source format from file extension.

    Args:
        file_path: Path to file.

    Returns:
        BigQuery source format string.
    """
    ext = file_path.suffix.lower()
    format_map = {
        ".csv": "CSV",
        ".json": "NEWLINE_DELIMITED_JSON",
        ".jsonl": "NEWLINE_DELIMITED_JSON",
        ".ndjson": "NEWLINE_DELIMITED_JSON",
        ".parquet": "PARQUET",
        ".avro": "AVRO",
        ".orc": "ORC",
    }

    if ext not in format_map:
        raise ValueError(
            f"Unsupported file format: {ext}. "
            f"Supported formats: {', '.join(format_map.keys())}"
        )

    return format_map[ext]


def create_load_job_config(
    source_format: str,
    schema: Optional[list[bigquery.SchemaField]] = None,
    write_disposition: str = "WRITE_APPEND",
    skip_leading_rows: Optional[int] = None,
    field_delimiter: Optional[str] = None,
    autodetect: bool = False,
) -> bigquery.LoadJobConfig:
    """Create a LoadJobConfig for file loading.

    Args:
        source_format: BigQuery source format.
        schema: Schema fields. Auto-detected if None and autodetect=True.
        write_disposition: How to handle existing data.
        skip_leading_rows: Header rows to skip (CSV only).
        field_delimiter: Field delimiter (CSV only).
        autodetect: Whether to auto-detect schema.

    Returns:
        Configured LoadJobConfig.
    """
    job_config = bigquery.LoadJobConfig(
        source_format=source_format,
        write_disposition=write_disposition,
    )

    if schema:
        job_config.schema = schema
    elif autodetect:
        job_config.autodetect = True

    if source_format == "CSV":
        job_config.skip_leading_rows = skip_leading_rows if skip_leading_rows is not None else 1
        if field_delimiter is not None:
            job_config.field_delimiter = field_delimiter

    return job_config
