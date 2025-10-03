"""BigQuery client for gcpeasy."""

from typing import TYPE_CHECKING, Any, Optional, Union
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig

if TYPE_CHECKING:
    from google.cloud.bigquery.table import RowIterator


class Client:
    """BigQuery client wrapper.

    Provides ergonomic access to BigQuery operations with sensible defaults.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        location: str = "EU",
        default_job_config: Optional[QueryJobConfig] = None,
    ):
        """Initialize BigQuery client.

        Args:
            project_id: GCP project ID. Defaults to ADC project.
            location: Default location for BigQuery operations.
            default_job_config: Default configuration for query jobs.
        """
        self._gcp = bigquery.Client(project=project_id, location=location)
        self._default_job_config = default_job_config
        self.project_id = self._gcp.project
        self.location = location

    def __call__(self, query: str, to_dataframe: bool = True, **kwargs):
        """Execute a SQL query.

        Args:
            query: SQL query string.
            to_dataframe: Return DataFrame or raw iterator.
            **kwargs: Additional parameters for query().

        Returns:
            DataFrame or RowIterator based on to_dataframe.
        """
        return self.query(query, to_dataframe=to_dataframe, **kwargs)

    def query(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        job_config: Optional[QueryJobConfig] = None,
        to_dataframe: bool = True,
    ):
        """Execute a SQL query.

        Args:
            query: SQL query string.
            params: Query parameters for parameterized queries.
            job_config: QueryJobConfig to override defaults.
            to_dataframe: Return DataFrame or raw iterator.

        Returns:
            DataFrame or RowIterator based on to_dataframe.
        """
        config = job_config or self._default_job_config

        if params:
            if config is None:
                config = QueryJobConfig()
            config.query_parameters = [
                bigquery.ScalarQueryParameter(key, _infer_param_type(value), value)
                for key, value in params.items()
            ]

        result = self._gcp.query(query, job_config=config)
        return result.to_dataframe() if to_dataframe else result.result()

    def datasets(self, max_results: Optional[int] = None) -> list[str]:
        """List all datasets in the project.

        Args:
            max_results: Maximum number to return.

        Returns:
            List of dataset IDs.
        """
        return [d.dataset_id for d in self._gcp.list_datasets(max_results=max_results)]

    def dataset(self, dataset_id: str) -> "Dataset":
        """Get a Dataset object.

        Args:
            dataset_id: Dataset ID.

        Returns:
            Dataset object.
        """
        from .dataset import Dataset

        return Dataset(self._gcp, dataset_id, self.project_id)

    def tables(self, dataset_id: str, max_results: Optional[int] = None) -> list[str]:
        """List tables in a dataset.

        Args:
            dataset_id: Dataset ID.
            max_results: Maximum number to return.

        Returns:
            List of table IDs.
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
            data: DataFrame, file path, or None.
            table_id: Target table as 'dataset.table' or 'project.dataset.table'.
            schema: Schema dict mapping field names to types.
            source_format: File format (auto-detected if None).
            write_disposition: How to handle existing data.
            skip_leading_rows: Header rows to skip (CSV only).
            field_delimiter: Field delimiter (CSV only).
        """
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
    default_job_config: Optional[QueryJobConfig] = None,
) -> Client:
    """Initialize and return a BigQuery client.

    Args:
        project_id: GCP project ID. Defaults to ADC project.
        location: Default location for BigQuery operations.
        default_job_config: Default configuration for query jobs.

    Returns:
        Initialized BigQuery Client.
    """
    return Client(
        project_id=project_id,
        location=location,
        default_job_config=default_job_config,
    )


def _infer_param_type(value: Any) -> str:
    """Infer BigQuery parameter type from Python value."""
    type_map = {
        bool: "BOOL",
        int: "INT64",
        float: "FLOAT64",
        str: "STRING",
        bytes: "BYTES",
    }
    return type_map.get(type(value), "STRING")
