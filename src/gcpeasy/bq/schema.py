"""Schema utilities for BigQuery."""

from typing import Any
import pandas as pd
from google.cloud import bigquery


def dict_to_schema_fields(schema_dict: dict[str, str]) -> list[bigquery.SchemaField]:
    """Convert a dictionary to BigQuery SchemaField objects.

    Args:
        schema_dict: Dictionary mapping column names to BigQuery type strings.
            Example: {"name": "STRING", "age": "INTEGER", "score": "FLOAT"}

    Returns:
        List of SchemaField objects.

    Example:
        >>> schema = dict_to_schema_fields({"name": "STRING", "age": "INT64"})
        >>> print(schema)
        [SchemaField('name', 'STRING'), SchemaField('age', 'INT64')]
    """
    return [
        bigquery.SchemaField(name, _normalize_type_name(type_str))
        for name, type_str in schema_dict.items()
    ]


def dataframe_to_schema_fields(df: pd.DataFrame) -> list[bigquery.SchemaField]:
    """Infer BigQuery schema from a pandas DataFrame.

    Args:
        df: The DataFrame to infer schema from.

    Returns:
        List of SchemaField objects.

    Example:
        >>> df = pd.DataFrame({"name": ["Alice"], "age": [30]})
        >>> schema = dataframe_to_schema_fields(df)
        >>> print(schema)
        [SchemaField('name', 'STRING'), SchemaField('age', 'INT64')]
    """
    schema_fields = []
    for col_name, dtype in df.dtypes.items():
        bq_type = _pandas_dtype_to_bigquery_type(dtype)
        schema_fields.append(bigquery.SchemaField(str(col_name), bq_type))
    return schema_fields


def _normalize_type_name(type_str: str) -> str:
    """Normalize BigQuery type names.

    Converts common type name variations to standard BigQuery type names.

    Args:
        type_str: Type string (case-insensitive).

    Returns:
        Normalized BigQuery type string.
    """
    type_upper = type_str.upper()

    # Map common aliases to standard BigQuery types
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
    """Convert pandas dtype to BigQuery type string.

    Args:
        dtype: Pandas dtype object.

    Returns:
        BigQuery type string.
    """
    dtype_str = str(dtype)

    if dtype_str.startswith("int"):
        return "INT64"
    elif dtype_str.startswith("float"):
        return "FLOAT64"
    elif dtype_str == "bool":
        return "BOOLEAN"
    elif dtype_str == "object":
        # Object could be string or other types
        return "STRING"
    elif dtype_str.startswith("datetime64"):
        return "TIMESTAMP"
    elif dtype_str == "date":
        return "DATE"
    elif dtype_str.startswith("timedelta"):
        return "TIME"
    else:
        # Default to STRING for unknown types
        return "STRING"
