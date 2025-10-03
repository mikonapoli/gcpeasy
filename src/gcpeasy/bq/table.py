"""BigQuery Table class for gcpeasy."""

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
            True if exists, False otherwise.
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
            max_results: Max rows to return. None returns all.

        Returns:
            DataFrame with table data.
        """
        query = f"SELECT * FROM `{self.id}`{f' LIMIT {max_results}' if max_results else ''}"
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
        from google.cloud import bigquery
        from .schema import dict_to_schema_fields, dataframe_to_schema_fields
        from .file_utils import detect_source_format, create_load_job_config

        if data is None:
            if schema is None:
                raise ValueError("schema must be provided when data is None")
            schema_fields = dict_to_schema_fields(schema)
            table_ref = bigquery.Table(self.id, schema=schema_fields)
            self._client.create_table(table_ref, exists_ok=True)
            return

        if isinstance(data, (str, Path)):
            data_str = str(data)

            if data_str.startswith("gs://"):
                if source_format is None:
                    gcs_formats = {
                        ".csv": "CSV", ".json": "NEWLINE_DELIMITED_JSON",
                        ".jsonl": "NEWLINE_DELIMITED_JSON", ".ndjson": "NEWLINE_DELIMITED_JSON",
                        ".parquet": "PARQUET", ".avro": "AVRO", ".orc": "ORC",
                    }
                    ext = next((e for e in gcs_formats if data_str.endswith(e)), None)
                    if not ext:
                        raise ValueError(
                            f"Cannot detect format from GCS URI: {data_str}. "
                            "Please provide source_format parameter."
                        )
                    source_format = gcs_formats[ext]

                if schema is not None:
                    schema_fields = dict_to_schema_fields(schema)
                    autodetect = False
                else:
                    schema_fields = None
                    autodetect = True

                job_config = create_load_job_config(
                    source_format=source_format,
                    schema=schema_fields,
                    write_disposition=write_disposition,
                    skip_leading_rows=skip_leading_rows,
                    field_delimiter=field_delimiter,
                    autodetect=autodetect,
                )

                load_job = self._client.load_table_from_uri(
                    data_str, self.id, job_config=job_config
                )
                load_job.result()
                return

            file_path = Path(data) if isinstance(data, str) else data

            if source_format is None:
                source_format = detect_source_format(file_path)

            if schema is not None:
                schema_fields = dict_to_schema_fields(schema)
                autodetect = False
            else:
                schema_fields = None
                autodetect = True

            job_config = create_load_job_config(
                source_format=source_format,
                schema=schema_fields,
                write_disposition=write_disposition,
                skip_leading_rows=skip_leading_rows,
                field_delimiter=field_delimiter,
                autodetect=autodetect,
            )

            with open(file_path, "rb") as source_file:
                load_job = self._client.load_table_from_file(
                    source_file, self.id, job_config=job_config
                )
                load_job.result()
            return

        if isinstance(data, pd.DataFrame):
            if schema is not None:
                schema_fields = dict_to_schema_fields(schema)
            else:
                schema_fields = dataframe_to_schema_fields(data)

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
        from google.cloud import bigquery
        from google.api_core import exceptions
        from .schema import dict_to_schema_fields

        table_ref = bigquery.Table(self.id)
        table_ref.schema = dict_to_schema_fields(schema)

        if description:
            table_ref.description = description

        if partitioning_field:
            table_ref.time_partitioning = bigquery.TimePartitioning(
                field=partitioning_field
            )

        if clustering_fields:
            table_ref.clustering_fields = clustering_fields

        try:
            self._client.create_table(table_ref)
        except exceptions.Conflict:
            if not exists_ok:
                raise

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
            if not not_found_ok:
                raise

    def get_metadata(self) -> "bigquery.Table":
        """Get table metadata and properties.

        Returns:
            BigQuery Table object with full metadata.
        """
        return self._client.get_table(self.id)

    def get_schema(self) -> list["bigquery.SchemaField"]:
        """Get table schema.

        Returns:
            List of SchemaField objects.
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
            schema: Fields to add (cannot remove fields).
            description: New description.
            labels: Labels to set.

        Returns:
            Self for chaining.
        """
        from google.cloud import bigquery
        from .schema import dict_to_schema_fields

        table_ref = self._client.get_table(self.id)

        if schema is not None:
            new_fields = dict_to_schema_fields(schema)
            table_ref.schema = list(table_ref.schema) + new_fields
        if description is not None:
            table_ref.description = description
        if labels is not None:
            table_ref.labels = labels

        fields_to_update = [
            field for field, value in [
                ("schema", schema),
                ("description", description),
                ("labels", labels),
            ] if value is not None
        ]

        if fields_to_update:
            self._client.update_table(table_ref, fields_to_update)

        return self

    def insert(
        self,
        rows: list[dict],
        ignore_unknown_values: bool = True,
        skip_invalid_rows: bool = False,
    ) -> list:
        """Stream insert rows into this table (for real-time ingestion).

        Args:
            rows: List of dicts representing rows.
            ignore_unknown_values: Ignore row values not in schema.
            skip_invalid_rows: Skip rows with invalid data.

        Returns:
            List of errors (empty if successful).
        """
        errors = self._client.insert_rows_json(
            self.id,
            rows,
            ignore_unknown_values=ignore_unknown_values,
            skip_invalid_rows=skip_invalid_rows,
        )
        return errors

    def to_gcs(
        self,
        destination_uri: str,
        export_format: str = "CSV",
        compression: str = "GZIP",
        print_header: bool = True,
        field_delimiter: str = ",",
    ) -> "bigquery.ExtractJob":
        """Export this table to Google Cloud Storage.

        Args:
            destination_uri: GCS destination URI.
            export_format: CSV, NEWLINE_DELIMITED_JSON, AVRO, or PARQUET.
            compression: NONE, GZIP, or SNAPPY.
            print_header: Include header row (CSV only).
            field_delimiter: Field delimiter (CSV only).

        Returns:
            Extract job.
        """
        from google.cloud import bigquery

        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = export_format
        job_config.compression = compression

        if export_format == "CSV":
            job_config.print_header = print_header
            job_config.field_delimiter = field_delimiter

        extract_job = self._client.extract_table(
            self.id, destination_uri, job_config=job_config
        )

        return extract_job

    def copy(
        self,
        destination_table: Union[str, "Table"],
        write_disposition: WriteDisposition = "WRITE_TRUNCATE",
    ) -> "bigquery.CopyJob":
        """Copy this table to another location.

        Args:
            destination_table: Destination Table or table ID string.
            write_disposition: How to handle existing destination.

        Returns:
            Copy job.
        """
        from google.cloud import bigquery

        dest_id = destination_table if isinstance(destination_table, str) else destination_table.id

        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = write_disposition

        copy_job = self._client.copy_table(self.id, dest_id, job_config=job_config)

        return copy_job
