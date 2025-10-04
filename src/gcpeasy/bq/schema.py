"""Schema utilities for BigQuery."""

from typing import Any
import pandas as pd
from google.cloud import bigquery


def dict_to_schema_fields(schema_dict: dict[str, str]) -> list[bigquery.SchemaField]:
    """Convert a dictionary to BigQuery SchemaField objects."""
    return [bigquery.SchemaField(name, _normalize_type_name(t)) for name, t in schema_dict.items()]


def df_to_schema_fields(df: pd.DataFrame) -> list[bigquery.SchemaField]:
    """Infer BigQuery schema from a pandas DataFrame."""
    return [bigquery.SchemaField(str(col), _pandas_dtype_to_bigquery_type(dtype)) for col, dtype in df.dtypes.items()]


def _normalize_type_name(type_str: str) -> str:
    """Normalize BigQuery type names."""
    type_upper = type_str.upper()
    type_map = {
        "INTEGER": "INT64",
        "INT": "INT64",
        "BIGINT": "INT64",
        "FLOAT": "FLOAT64",
        "DOUBLE": "FLOAT64",
        "BOOL": "BOOLEAN",
        "BYTES": "BYTES",
        "STRING": "STRING",
        "TEXT": "STRING",
        "VARCHAR": "STRING",
        "DATE": "DATE",
        "DATETIME": "DATETIME",
        "TIME": "TIME",
        "TIMESTAMP": "TIMESTAMP",
        "NUMERIC": "NUMERIC",
        "BIGNUMERIC": "BIGNUMERIC",
        "GEOGRAPHY": "GEOGRAPHY",
        "JSON": "JSON",
        "ARRAY": "ARRAY",
        "STRUCT": "STRUCT",
    }
    return type_map.get(type_upper, type_upper)


def _pandas_dtype_to_bigquery_type(dtype: Any) -> str:
    """Convert pandas dtype to BigQuery type string."""
    d = str(dtype)
    if d.startswith("int"): return "INT64"
    if d.startswith("float"): return "FLOAT64"
    if d == "bool": return "BOOLEAN"
    if d == "object": return "STRING"
    if d.startswith("datetime64"): return "TIMESTAMP"
    if d == "date": return "DATE"
    if d.startswith("timedelta"): return "TIME"
    return "STRING"
