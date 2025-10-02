"""BigQuery client for gcpeasy."""

from typing import Any, Optional, Union
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from google.cloud.bigquery.table import RowIterator


class Client:
    """BigQuery client wrapper.

    Provides ergonomic access to BigQuery operations with sensible defaults.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        location: str = "EU",
        default_query_job_config: Optional[QueryJobConfig] = None,
    ):
        """Initialize BigQuery client.

        Args:
            project_id: GCP project ID. If None, uses default credentials project.
            location: Default location for BigQuery operations. Defaults to 'EU'.
            default_query_job_config: Default configuration for query jobs.
        """
        self._gcp = bigquery.Client(project=project_id, location=location)
        self._default_query_job_config = default_query_job_config
        self.project_id = self._gcp.project
        self.location = location

    def __call__(
        self, query: str, to_dataframe: bool = True, **kwargs
    ) -> Union[pd.DataFrame, RowIterator]:
        """Execute a SQL query and return results as a DataFrame.

        This is the primary interface for running queries.

        Args:
            query: SQL query string to execute.
            to_dataframe: Return pandas DataFrame (True) or raw iterator (False).
            **kwargs: Additional parameters passed to query().

        Returns:
            DataFrame containing query results (if to_dataframe=True) or
            raw query result iterator (if to_dataframe=False).

        Example:
            >>> client = init()
            >>> df = client("SELECT * FROM `project.dataset.table` LIMIT 10")
            >>>
            >>> # Get raw iterator for large results
            >>> results = client("SELECT * FROM large_table", to_dataframe=False)
            >>> for row in results:
            ...     print(row)
        """
        return self.query(query, to_dataframe=to_dataframe, **kwargs)

    def query(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        job_config: Optional[QueryJobConfig] = None,
        to_dataframe: bool = True,
    ) -> Union[pd.DataFrame, RowIterator]:
        """Execute a SQL query with optional parameters.

        Args:
            query: SQL query string to execute.
            params: Dictionary of query parameters for parameterized queries.
            job_config: Optional QueryJobConfig to override defaults.
            to_dataframe: Return pandas DataFrame (True) or raw iterator (False).

        Returns:
            DataFrame containing query results (if to_dataframe=True) or
            raw query result iterator (if to_dataframe=False).

        Example:
            >>> df = client.query(
            ...     "SELECT * FROM `dataset.table` WHERE id = @id",
            ...     params={"id": 123}
            ... )
            >>>
            >>> # Get raw iterator for large results
            >>> results = client.query("SELECT * FROM large_table", to_dataframe=False)
            >>> for row in results:
            ...     print(row)
        """
        # Build job config
        config = job_config or self._default_query_job_config

        # Handle parameterized queries
        if params:
            if config is None:
                config = QueryJobConfig()
            config.query_parameters = [
                bigquery.ScalarQueryParameter(key, _infer_param_type(value), value)
                for key, value in params.items()
            ]

        # Execute query
        query_job = self._gcp.query(query, job_config=config)

        # Convert results to DataFrame or return raw iterator
        if to_dataframe:
            return query_job.to_dataframe()
        else:
            return query_job.result()

    def datasets(self, max_results: Optional[int] = None) -> list[str]:
        """List all datasets in the project.

        Args:
            max_results: Maximum number of datasets to return. If None, returns all datasets.

        Returns:
            List of dataset IDs (without project prefix).

        Example:
            >>> client = init()
            >>> datasets = client.datasets()
            >>> print(datasets)
            ['dataset1', 'dataset2']
            >>> # Limit results
            >>> datasets = client.datasets(max_results=10)
        """
        datasets = self._gcp.list_datasets(max_results=max_results)
        return [dataset.dataset_id for dataset in datasets]

    def dataset(self, dataset_id: str) -> "Dataset":
        """Get a Dataset object for a specific dataset.

        Args:
            dataset_id: The dataset ID (without project prefix).

        Returns:
            Dataset object for the specified dataset.

        Example:
            >>> client = init()
            >>> dataset = client.dataset("my_dataset")
            >>> tables = dataset.tables()
        """
        from .dataset import Dataset

        return Dataset(self._gcp, dataset_id, self.project_id)

    def tables(self, dataset_id: str, max_results: Optional[int] = None) -> list[str]:
        """List all tables in a dataset (convenience method).

        Args:
            dataset_id: The dataset ID (without project prefix).
            max_results: Maximum number of tables to return. If None, returns all tables.

        Returns:
            List of table IDs (without project or dataset prefix).

        Example:
            >>> client = init()
            >>> tables = client.tables("my_dataset")
            >>> print(tables)
            ['table1', 'table2']
            >>> # Limit results
            >>> tables = client.tables("my_dataset", max_results=10)
        """
        return self.dataset(dataset_id).tables(max_results=max_results)

    def load_data(
        self,
        data: Union[pd.DataFrame, str, Path, None],
        table_id: str,
        schema: Optional[dict[str, str]] = None,
        source_format: Optional[str] = None,
        write_disposition: str = "WRITE_APPEND",
        skip_leading_rows: Optional[int] = None,
        field_delimiter: Optional[str] = None,
    ) -> None:
        """Load data into a BigQuery table.

        Args:
            data: One of the following:
                - Path to file (CSV, JSON, Parquet, Avro, ORC)
                - String that can be converted to a Path to a file
                - A Pandas DataFrame
                - None (To just create a new table with a schema)
            table_id: Target table ID (format: `dataset.table` or `project.dataset.table`)
            schema: Table schema as dict where keys are field names and values are type strings
                (e.g., `{"name": "STRING", "age": "INTEGER"}`). Auto-detected if None and possible.
            source_format: File format (auto-detected from extension if None)
            write_disposition: How to handle existing data:
                - "WRITE_APPEND": Append to existing data (default)
                - "WRITE_TRUNCATE": Overwrite existing data
                - "WRITE_EMPTY": Only write if table is empty
            skip_leading_rows: Number of header rows to skip (CSV only, default: 1 for CSV)
            field_delimiter: Field delimiter (CSV only, default: ',')

        Example:
            >>> # Load from DataFrame
            >>> df = pd.DataFrame({"name": ["Alice"], "age": [30]})
            >>> client.load_data(df, "my_dataset.my_table")
            >>>
            >>> # Load from file
            >>> client.load_data("data.csv", "my_dataset.my_table")
            >>>
            >>> # Create empty table with schema
            >>> schema = {"name": "STRING", "age": "INTEGER"}
            >>> client.load_data(None, "my_dataset.my_table", schema=schema)
        """
        # Parse table_id
        parts = table_id.split(".")
        if len(parts) == 2:
            dataset_id, table_name = parts
        elif len(parts) == 3:
            _, dataset_id, table_name = parts
        else:
            raise ValueError(
                f"Invalid table_id format: {table_id}. "
                "Expected 'dataset.table' or 'project.dataset.table'"
            )

        table = self.dataset(dataset_id).table(table_name)
        table.write(
            data,  # type: ignore
            schema=schema,
            write_disposition=write_disposition,
            source_format=source_format,
            skip_leading_rows=skip_leading_rows,
            field_delimiter=field_delimiter,
        )


def init(
    project_id: Optional[str] = None,
    location: str = "EU",
    default_query_job_config: Optional[QueryJobConfig] = None,
) -> Client:
    """Initialize and return a BigQuery client.

    Args:
        project_id: GCP project ID. If None, uses default credentials project.
        location: Default location for BigQuery operations. Defaults to 'EU'.
        default_query_job_config: Default configuration for query jobs.

    Returns:
        Initialized BigQuery Client.

    Example:
        >>> from gcpeasy import bq
        >>> client = bq.init()
        >>> df = client("SELECT 1 as value")
    """
    return Client(
        project_id=project_id,
        location=location,
        default_query_job_config=default_query_job_config,
    )


def _infer_param_type(value: Any) -> str:
    """Infer BigQuery parameter type from Python value.

    Args:
        value: Python value to infer type from.

    Returns:
        BigQuery type string.
    """
    if isinstance(value, bool):
        return "BOOL"
    elif isinstance(value, int):
        return "INT64"
    elif isinstance(value, float):
        return "FLOAT64"
    elif isinstance(value, str):
        return "STRING"
    elif isinstance(value, bytes):
        return "BYTES"
    else:
        # Default to STRING and let BigQuery handle conversion
        return "STRING"
