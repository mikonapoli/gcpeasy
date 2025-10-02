"""File utilities for BigQuery file loading."""

from pathlib import Path
from typing import Optional
from google.cloud import bigquery


def detect_source_format(file_path: Path) -> str:
    """Detect BigQuery source format from file extension.

    Args:
        file_path: Path to the file.

    Returns:
        BigQuery source format string (e.g., "CSV", "NEWLINE_DELIMITED_JSON").

    Raises:
        ValueError: If file extension is not supported.
    """
    extension = file_path.suffix.lower()

    format_map = {
        ".csv": "CSV",
        ".json": "NEWLINE_DELIMITED_JSON",
        ".jsonl": "NEWLINE_DELIMITED_JSON",
        ".ndjson": "NEWLINE_DELIMITED_JSON",
        ".parquet": "PARQUET",
        ".avro": "AVRO",
        ".orc": "ORC",
    }

    if extension not in format_map:
        raise ValueError(
            f"Unsupported file format: {extension}. "
            f"Supported formats: {', '.join(format_map.keys())}"
        )

    return format_map[extension]


def auto_detect_schema_from_file(
    client: bigquery.Client, file_path: Path, source_format: str
) -> list[bigquery.SchemaField]:
    """Auto-detect schema from a file.

    Args:
        client: BigQuery client.
        file_path: Path to the file.
        source_format: BigQuery source format (e.g., "CSV", "PARQUET").

    Returns:
        List of SchemaField objects representing the detected schema.

    Raises:
        ValueError: If auto-detection is not supported for the format.
    """
    # Auto-detection is supported for CSV, JSON, and Parquet
    if source_format not in ["CSV", "NEWLINE_DELIMITED_JSON", "PARQUET"]:
        raise ValueError(
            f"Schema auto-detection not supported for format: {source_format}"
        )

    # For now, we'll return None to let BigQuery handle auto-detection
    # This will be used in the job config with autodetect=True
    return []


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
        source_format: BigQuery source format (e.g., "CSV", "PARQUET").
        schema: Optional schema. If None and autodetect is True, schema will be auto-detected.
        write_disposition: How to handle existing data.
        skip_leading_rows: Number of header rows to skip (CSV only).
        field_delimiter: Field delimiter (CSV only).
        autodetect: Whether to auto-detect schema.

    Returns:
        Configured LoadJobConfig.
    """
    job_config = bigquery.LoadJobConfig(
        source_format=source_format,
        write_disposition=write_disposition,
    )

    # Set schema or autodetect
    if schema:
        job_config.schema = schema
    elif autodetect:
        job_config.autodetect = True

    # CSV-specific options
    if source_format == "CSV":
        # Default to skip 1 row for CSV if not specified
        if skip_leading_rows is not None:
            job_config.skip_leading_rows = skip_leading_rows
        else:
            job_config.skip_leading_rows = 1

        if field_delimiter is not None:
            job_config.field_delimiter = field_delimiter

    return job_config
