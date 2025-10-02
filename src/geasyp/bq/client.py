"""BigQuery client for geasyp."""

from typing import Any, Optional
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig


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

    def __call__(self, query: str, **kwargs) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame.

        This is the primary interface for running queries.

        Args:
            query: SQL query string to execute.
            **kwargs: Additional parameters passed to query().

        Returns:
            DataFrame containing query results.

        Example:
            >>> client = init()
            >>> df = client("SELECT * FROM `project.dataset.table` LIMIT 10")
        """
        return self.query(query, **kwargs)

    def query(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        job_config: Optional[QueryJobConfig] = None,
    ) -> pd.DataFrame:
        """Execute a SQL query with optional parameters.

        Args:
            query: SQL query string to execute.
            params: Dictionary of query parameters for parameterized queries.
            job_config: Optional QueryJobConfig to override defaults.

        Returns:
            DataFrame containing query results.

        Example:
            >>> df = client.query(
            ...     "SELECT * FROM `dataset.table` WHERE id = @id",
            ...     params={"id": 123}
            ... )
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

        # Convert results to DataFrame
        return query_job.to_dataframe()

    def datasets(self) -> list[str]:
        """List all datasets in the project.

        Returns:
            List of dataset IDs (without project prefix).

        Example:
            >>> client = init()
            >>> datasets = client.datasets()
            >>> print(datasets)
            ['dataset1', 'dataset2']
        """
        datasets = self._gcp.list_datasets()
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

    def tables(self, dataset_id: str) -> list[str]:
        """List all tables in a dataset (convenience method).

        Args:
            dataset_id: The dataset ID (without project prefix).

        Returns:
            List of table IDs (without project or dataset prefix).

        Example:
            >>> client = init()
            >>> tables = client.tables("my_dataset")
            >>> print(tables)
            ['table1', 'table2']
        """
        return self.dataset(dataset_id).tables()

    def load_data(
        self,
        table_id: str,
        data: pd.DataFrame,
        schema: Optional[dict[str, str]] = None,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> None:
        """Load data from a DataFrame to a table (convenience method).

        Args:
            table_id: Fully qualified table ID or "dataset.table" format.
                If not fully qualified, uses the client's project.
            data: DataFrame to load.
            schema: Optional schema dictionary. If None, auto-detected from DataFrame.
            write_disposition: How to handle existing data:
                - "WRITE_TRUNCATE": Overwrite existing data (default)
                - "WRITE_APPEND": Append to existing data
                - "WRITE_EMPTY": Only write if table is empty

        Example:
            >>> df = pd.DataFrame({"name": ["Alice"], "age": [30]})
            >>> client.load_data("my_dataset.my_table", df)
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
        table.write(data, schema=schema, write_disposition=write_disposition)  # type: ignore


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
        >>> from geasyp import bq
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
