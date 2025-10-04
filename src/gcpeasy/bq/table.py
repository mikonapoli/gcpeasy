"""BigQuery Table class for gcpeasy."""

from typing import TYPE_CHECKING, Optional, Literal, Union
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
from .validation import validate_identifier
from .schema import dict_to_schema_fields, df_to_schema_fields

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
        self._table_id = validate_identifier(table_id, "table_id")
        self._dataset_id = validate_identifier(dataset_id, "dataset_id")
        self._project_id = validate_identifier(project_id, "project_id")

    @property
    def id(self) -> str:
        """The fully qualified table ID in format 'project.dataset.table'."""
        return f"{self._project_id}.{self._dataset_id}.{self._table_id}"

    def exists(self) -> bool:
        """Check if the table exists."""
        from google.api_core import exceptions

        try:
            self._client.get_table(self.id)
            return True
        except exceptions.NotFound:
            return False

    def read(self, max_results: Optional[int] = None) -> pd.DataFrame:
        """Read table data into a DataFrame.

        Args:
            max_results: Max rows to return. None returns all.

        Returns:
            DataFrame with table data.
        """
        query = f"SELECT * FROM `{self.id}`"
        if max_results is not None:
            query += " LIMIT @max_results"
            try:
                params = {"max_results": int(max_results)}
            except (ValueError, TypeError):
                raise ValueError(f"max_results must be an integer: {max_results}")
            return self._client.query(query, params=params).to_dataframe()
        return self._client.query(query).to_dataframe()

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
            data: DataFrame, file path, GCS URI, or None for empty table.
            schema: Column name to BigQuery type mapping. Auto-detected if None.
            write_disposition: How to handle existing data.
            source_format: File format. Auto-detected if None.
            skip_leading_rows: Header rows to skip (CSV only).
            field_delimiter: Field delimiter (CSV only).
        """
        from .file_utils import detect_source_format, create_load_job_config

        if data is None:
            if schema is None:
                raise ValueError("schema must be provided when data is None")
            schema_fields = dict_to_schema_fields(schema)
            table_ref = bigquery.Table(self.id, schema=schema_fields)
            self._client.create_table(table_ref, exists_ok=True)
            return

        if isinstance(data, (str, Path)):
            source_format = source_format or detect_source_format(data)
            
            job_config = create_load_job_config(
                    source_format=source_format,
                    schema=dict_to_schema_fields(schema) if schema is not None else None,
                    write_disposition=write_disposition,
                    skip_leading_rows=skip_leading_rows,
                    field_delimiter=field_delimiter,
                    autodetect=schema is None,
                )

            if str(data).startswith("gs://"):

                load_job = self._client.load_table_from_uri(
                    data, self.id, job_config=job_config
                )
                load_job.result()
                return

            with open(Path(data), "rb") as source_file:
                load_job = self._client.load_table_from_file(
                    source_file, self.id, job_config=job_config
                )
                load_job.result()
            return

        if isinstance(data, pd.DataFrame):
            schema_fields = dict_to_schema_fields(schema) if schema is not None else df_to_schema_fields(data)
            
            job_config = bigquery.LoadJobConfig(
                schema=schema_fields,
                write_disposition=write_disposition,
            )

            load_job = self._client.load_table_from_dataframe(
                data, self.id, job_config=job_config
            )
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
        description: Optional[str] = None,
        exists_ok: bool = False,
    ) -> "Table":
        """Create the table.

        Args:
            schema: Column name to BigQuery type mapping.
            partitioning_field: Field to partition by (DATE, TIMESTAMP, or DATETIME).
            clustering_fields: Fields to cluster by (max 4).
            description: Table description.
            exists_ok: Don't raise error if table exists.

        Returns:
            Self for chaining.
        """
        from google.api_core import exceptions

        table_ref = bigquery.Table(self.id)
        table_ref.schema = dict_to_schema_fields(schema)

        if description: table_ref.description = description
        if partitioning_field: table_ref.time_partitioning = bigquery.TimePartitioning(field=partitioning_field)
        if clustering_fields: table_ref.clustering_fields = clustering_fields

        try:
            self._client.create_table(table_ref)
        except exceptions.Conflict:
            if not exists_ok: raise

        return self

    def delete(self, not_found_ok: bool = False) -> None:
        """Delete the table.

        Args:
            not_found_ok: Don't raise error if table doesn't exist.
        """
        from google.api_core import exceptions

        try:
            self._client.delete_table(self.id, not_found_ok=False)
        except exceptions.NotFound:
            if not not_found_ok: raise

    def get_metadata(self) -> "bigquery.Table":
        """Get table metadata and properties."""
        return self._client.get_table(self.id)

    def get_schema(self) -> list["bigquery.SchemaField"]:
        """Get table schema."""
        return self._client.get_table(self.id).schema

    def update(self, schema: Optional[dict[str, str]] = None, description: Optional[str] = None, labels: Optional[dict[str, str]] = None) -> "Table":
        """Update table properties."""
        t = self._client.get_table(self.id)
        if schema is not None: t.schema = list(t.schema) + dict_to_schema_fields(schema)
        if description is not None: t.description = description
        if labels is not None: t.labels = labels

        fields = [f for f, v in [("schema", schema), ("description", description), ("labels", labels)] if v is not None]
        if fields: self._client.update_table(t, fields)
        return self

    def insert(self, rows: list[dict], ignore_unknown_values: bool = True, skip_invalid_rows: bool = False) -> list:
        """Stream insert rows into this table (for real-time ingestion)."""
        return self._client.insert_rows_json(self.id, rows, ignore_unknown_values=ignore_unknown_values, skip_invalid_rows=skip_invalid_rows)

    def to_gcs(self, destination_uri: str, export_format: str = "CSV", compression: str = "GZIP", print_header: bool = True, field_delimiter: str = ",") -> "bigquery.ExtractJob":
        """Export this table to Google Cloud Storage."""
        config = bigquery.ExtractJobConfig()
        config.destination_format = export_format
        config.compression = compression
        if export_format == "CSV":
            config.print_header = print_header
            config.field_delimiter = field_delimiter
        return self._client.extract_table(self.id, destination_uri, job_config=config)

    def copy(self, destination_table: Union[str, "Table"], write_disposition: WriteDisposition = "WRITE_TRUNCATE") -> "bigquery.CopyJob":
        """Copy this table to another location."""
        dest_id = destination_table if isinstance(destination_table, str) else destination_table.id
        config = bigquery.CopyJobConfig()
        config.write_disposition = write_disposition
        return self._client.copy_table(self.id, dest_id, job_config=config)
