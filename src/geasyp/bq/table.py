"""BigQuery Table class for geasyp."""

from typing import TYPE_CHECKING, Optional, Literal, Union
from pathlib import Path
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
        data: Union[pd.DataFrame, str, Path, None],
        schema: Optional[dict[str, str]] = None,
        write_disposition: WriteDisposition = "WRITE_TRUNCATE",
        source_format: Optional[str] = None,
        skip_leading_rows: Optional[int] = None,
        field_delimiter: Optional[str] = None,
    ) -> None:
        """Write data to this table.

        Args:
            data: One of the following:
                - Pandas DataFrame
                - File path (either a Path object or a string)
                - None (to create an empty table with a schema)
            schema: Optional schema dictionary mapping column names to BigQuery types.
                If None, schema is auto-detected.
                Example: {"name": "STRING", "age": "INTEGER"}
            write_disposition: How to handle existing data:
                - "WRITE_TRUNCATE": Overwrite existing data (default)
                - "WRITE_APPEND": Append to existing data
                - "WRITE_EMPTY": Only write if table is empty
            source_format: File format (auto-detected from extension if None).
                Only used when data is a file path.
            skip_leading_rows: Number of header rows to skip (CSV only, default: 1 for CSV).
                Only used when data is a file path.
            field_delimiter: Field delimiter (CSV only, default: ',').
                Only used when data is a file path.

        Raises:
            ValueError: If write_disposition is "WRITE_EMPTY" and table is not empty.

        Example:
            >>> # Write DataFrame
            >>> df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
            >>> table = client.dataset("my_dataset").table("my_table")
            >>> table.write(df)
            >>>
            >>> # Write from file
            >>> table.write("data.csv")
            >>>
            >>> # Create empty table with schema
            >>> table.write(None, schema={"name": "STRING", "age": "INTEGER"})
        """
        from google.cloud import bigquery
        from .schema import dict_to_schema_fields, dataframe_to_schema_fields
        from .file_utils import detect_source_format, create_load_job_config

        # Handle None case (create empty table)
        if data is None:
            if schema is None:
                raise ValueError("schema must be provided when data is None")
            schema_fields = dict_to_schema_fields(schema)
            table_ref = bigquery.Table(self.id, schema=schema_fields)
            self._client.create_table(table_ref, exists_ok=True)
            return

        # Handle file path case
        if isinstance(data, (str, Path)):
            file_path = Path(data) if isinstance(data, str) else data

            # Detect source format if not provided
            if source_format is None:
                source_format = detect_source_format(file_path)

            # Determine schema
            if schema is not None:
                schema_fields = dict_to_schema_fields(schema)
                autodetect = False
            else:
                schema_fields = None
                autodetect = True

            # Create job config
            job_config = create_load_job_config(
                source_format=source_format,
                schema=schema_fields,
                write_disposition=write_disposition,
                skip_leading_rows=skip_leading_rows,
                field_delimiter=field_delimiter,
                autodetect=autodetect,
            )

            # Load from file
            with open(file_path, "rb") as source_file:
                load_job = self._client.load_table_from_file(
                    source_file, self.id, job_config=job_config
                )
                load_job.result()
            return

        # Handle DataFrame case
        if isinstance(data, pd.DataFrame):
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
            return

        raise TypeError(
            f"Unsupported data type: {type(data)}. "
            "Expected DataFrame, file path (str/Path), or None."
        )

    def create(
        self,
        schema: dict[str, str],
        partitioning_field: Optional[str] = None,
        clustering_fields: Optional[list[str]] = None,
        exists_ok: bool = False,
    ) -> "Table":
        """Create the table.

        Args:
            schema: Schema dictionary mapping column names to BigQuery types.
                Example: {"name": "STRING", "age": "INTEGER", "created": "TIMESTAMP"}
            partitioning_field: Optional field name to partition by (must be DATE, TIMESTAMP, or DATETIME).
            clustering_fields: Optional list of field names to cluster by (max 4).
            exists_ok: If True, don't raise error if table already exists.

        Returns:
            Self for method chaining.

        Raises:
            google.api_core.exceptions.Conflict: If table exists and exists_ok is False.

        Example:
            >>> schema = {"name": "STRING", "age": "INT64"}
            >>> table = client.dataset("my_dataset").table("my_table")
            >>> table.create(schema, exists_ok=True)
        """
        from google.cloud import bigquery
        from google.api_core import exceptions
        from .schema import dict_to_schema_fields

        # Create table reference
        table_ref = bigquery.Table(self.id)
        table_ref.schema = dict_to_schema_fields(schema)

        # Set partitioning
        if partitioning_field:
            table_ref.time_partitioning = bigquery.TimePartitioning(
                field=partitioning_field
            )

        # Set clustering
        if clustering_fields:
            table_ref.clustering_fields = clustering_fields

        # Create table
        try:
            self._client.create_table(table_ref)
        except exceptions.Conflict:
            if not exists_ok:
                raise

        return self

    def delete(self, not_found_ok: bool = False) -> None:
        """Delete the table.

        Args:
            not_found_ok: If True, don't raise error if table doesn't exist.

        Raises:
            google.api_core.exceptions.NotFound: If table doesn't exist and not_found_ok is False.

        Example:
            >>> table = client.dataset("my_dataset").table("my_table")
            >>> table.delete()
        """
        from google.api_core import exceptions

        try:
            self._client.delete_table(self.id, not_found_ok=False)
        except exceptions.NotFound:
            if not not_found_ok:
                raise

    def get_metadata(self) -> "bigquery.Table":
        """Get table metadata and properties.

        Returns:
            BigQuery Table object with full metadata including schema,
            partitioning, clustering, creation time, etc.

        Example:
            >>> table = client.dataset("my_dataset").table("my_table")
            >>> metadata = table.get_metadata()
            >>> print(metadata.num_rows)
            >>> print(metadata.created)
        """
        return self._client.get_table(self.id)

    def get_schema(self) -> list["bigquery.SchemaField"]:
        """Get table schema.

        Returns:
            List of SchemaField objects representing the table schema.

        Example:
            >>> table = client.dataset("my_dataset").table("my_table")
            >>> schema = table.get_schema()
            >>> for field in schema:
            ...     print(f"{field.name}: {field.field_type}")
        """
        table_ref = self._client.get_table(self.id)
        return table_ref.schema

    def update(
        self,
        schema: Optional[dict[str, str]] = None,
        description: Optional[str] = None,
        labels: Optional[dict[str, str]] = None,
    ) -> "Table":
        """Update table properties.

        Args:
            schema: Dict where keys are field names and values are type strings.
                Can only add fields, not remove them.
                Example: {"new_field": "STRING"}
            description: New description for the table.
            labels: Labels dict to set on the table.

        Returns:
            Self for method chaining.

        Example:
            >>> table = client.dataset("my_dataset").table("my_table")
            >>> table.update(
            ...     description="Updated table description",
            ...     labels={"env": "prod"}
            ... )
        """
        from google.cloud import bigquery
        from .schema import dict_to_schema_fields

        # Get current table
        table_ref = self._client.get_table(self.id)

        # Update fields
        if schema is not None:
            # Add new fields to existing schema
            new_fields = dict_to_schema_fields(schema)
            table_ref.schema = list(table_ref.schema) + new_fields
        if description is not None:
            table_ref.description = description
        if labels is not None:
            table_ref.labels = labels

        # Determine which fields to update
        fields_to_update = []
        if schema is not None:
            fields_to_update.append("schema")
        if description is not None:
            fields_to_update.append("description")
        if labels is not None:
            fields_to_update.append("labels")

        # Update table
        if fields_to_update:
            self._client.update_table(table_ref, fields_to_update)

        return self
