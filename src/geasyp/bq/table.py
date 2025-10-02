"""BigQuery Table class for geasyp."""

from typing import TYPE_CHECKING, Optional, Literal
import pandas as pd

if TYPE_CHECKING:
    from google.cloud import bigquery

WriteDisposition = Literal["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"]


class Table:
    """Represents a BigQuery table.

    Provides methods to interact with and manage BigQuery tables.
    """

    def __init__(
        self,
        client: "bigquery.Client",
        table_id: str,
        dataset_id: str,
        project_id: str,
    ):
        """Initialize a Table.

        Args:
            client: The underlying BigQuery client.
            table_id: The table ID (without project or dataset).
            dataset_id: The dataset ID containing the table.
            project_id: The project ID containing the dataset.
        """
        self._client = client
        self._table_id = table_id
        self._dataset_id = dataset_id
        self._project_id = project_id

    @property
    def id(self) -> str:
        """Get the fully qualified table ID.

        Returns:
            Fully qualified table ID in format 'project.dataset.table'.
        """
        return f"{self._project_id}.{self._dataset_id}.{self._table_id}"

    def exists(self) -> bool:
        """Check if the table exists.

        Returns:
            True if the table exists, False otherwise.

        Example:
            >>> table = client.dataset("my_dataset").table("my_table")
            >>> if table.exists():
            ...     print("Table exists")
        """
        from google.api_core import exceptions

        try:
            self._client.get_table(self.id)
            return True
        except exceptions.NotFound:
            return False

    def read(self, max_results: Optional[int] = None) -> pd.DataFrame:
        """Read table data into a DataFrame.

        Args:
            max_results: Maximum number of rows to return. If None, returns all rows.

        Returns:
            DataFrame containing the table data.

        Example:
            >>> table = client.dataset("my_dataset").table("my_table")
            >>> df = table.read()
            >>> print(df.head())
        """
        query = f"SELECT * FROM `{self.id}`"
        if max_results is not None:
            query += f" LIMIT {max_results}"

        query_job = self._client.query(query)
        return query_job.to_dataframe()

    def write(
        self,
        data: pd.DataFrame,
        schema: Optional[dict[str, str]] = None,
        write_disposition: WriteDisposition = "WRITE_TRUNCATE",
    ) -> None:
        """Write a DataFrame to the table.

        Args:
            data: DataFrame to write to the table.
            schema: Optional schema dictionary mapping column names to BigQuery types.
                If None, schema is auto-detected from the DataFrame.
                Example: {"name": "STRING", "age": "INTEGER"}
            write_disposition: How to handle existing data:
                - "WRITE_TRUNCATE": Overwrite existing data (default)
                - "WRITE_APPEND": Append to existing data
                - "WRITE_EMPTY": Only write if table is empty

        Raises:
            ValueError: If write_disposition is "WRITE_EMPTY" and table is not empty.

        Example:
            >>> df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
            >>> table = client.dataset("my_dataset").table("my_table")
            >>> table.write(df)  # Overwrites existing data
            >>> table.write(df, write_disposition="WRITE_APPEND")  # Appends
        """
        from google.cloud import bigquery
        from .schema import dict_to_schema_fields, dataframe_to_schema_fields

        # Determine schema
        if schema is not None:
            schema_fields = dict_to_schema_fields(schema)
        else:
            schema_fields = dataframe_to_schema_fields(data)

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            schema=schema_fields,
            write_disposition=write_disposition,
        )

        # Load data
        load_job = self._client.load_table_from_dataframe(
            data, self.id, job_config=job_config
        )

        # Wait for job to complete
        load_job.result()
