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
