"""BigQuery Dataset class for gcpeasy."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from google.cloud import bigquery


class Dataset:
    """Represents a BigQuery dataset.

    Provides methods to interact with and manage BigQuery datasets.
    """

    def __init__(self, client: "bigquery.Client", dataset_id: str, project_id: str):
        """Initialize a Dataset.

        Args:
            client: The underlying BigQuery client.
            dataset_id: Dataset ID.
            project_id: Project ID.
        """
        self._client = client
        self._dataset_id = dataset_id
        self._project_id = project_id

    @property
    def id(self) -> str:
        """Get the fully qualified dataset ID."""
        return f"{self._project_id}.{self._dataset_id}"

    def exists(self) -> bool:
        """Check if the dataset exists."""
        from google.api_core import exceptions

        try:
            self._client.get_dataset(self.id)
            return True
        except exceptions.NotFound:
            return False

    def tables(self, max_results: Optional[int] = None) -> list[str]:
        """List all tables in the dataset.

        Args:
            max_results: Maximum number to return.

        Returns:
            List of table IDs.
        """
        return [t.table_id for t in self._client.list_tables(self.id, max_results=max_results)]

    def table(self, table_id: str) -> "Table":
        """Get a Table object.

        Args:
            table_id: Table ID.

        Returns:
            Table object.
        """
        from .table import Table

        return Table(self._client, table_id, self._dataset_id, self._project_id)

    def create(
        self,
        location: Optional[str] = None,
        description: Optional[str] = None,
        default_table_expiration_ms: Optional[int] = None,
        exists_ok: bool = False,
    ) -> "Dataset":
        """Create the dataset.

        Args:
            location: Geographic location.
            description: Dataset description.
            default_table_expiration_ms: Default expiration for tables.
            exists_ok: Don't raise error if already exists.

        Returns:
            Self for chaining.
        """
        from google.cloud import bigquery
        from google.api_core import exceptions

        dataset_ref = bigquery.Dataset(self.id)
        if location:
            dataset_ref.location = location
        if description:
            dataset_ref.description = description
        if default_table_expiration_ms:
            dataset_ref.default_table_expiration_ms = default_table_expiration_ms

        try:
            self._client.create_dataset(dataset_ref)
        except exceptions.Conflict:
            if not exists_ok:
                raise

        return self

    def delete(self, delete_contents: bool = False, not_found_ok: bool = False) -> None:
        """Delete the dataset.

        Args:
            delete_contents: Delete all tables first.
            not_found_ok: Don't raise error if doesn't exist.
        """
        from google.api_core import exceptions

        try:
            self._client.delete_dataset(
                self.id, delete_contents=delete_contents, not_found_ok=False
            )
        except exceptions.NotFound:
            if not not_found_ok:
                raise

    def get_metadata(self) -> "bigquery.Dataset":
        """Get dataset metadata.

        Returns:
            BigQuery Dataset object with metadata.
        """
        return self._client.get_dataset(self.id)

    def update(
        self,
        description: Optional[str] = None,
        labels: Optional[dict[str, str]] = None,
        default_table_expiration_ms: Optional[int] = None,
    ) -> "Dataset":
        """Update dataset properties.

        Args:
            description: New description.
            labels: Labels to set.
            default_table_expiration_ms: Default expiration for tables.

        Returns:
            Self for chaining.
        """
        dataset_ref = self._client.get_dataset(self.id)

        if description is not None:
            dataset_ref.description = description
        if labels is not None:
            dataset_ref.labels = labels
        if default_table_expiration_ms is not None:
            dataset_ref.default_table_expiration_ms = default_table_expiration_ms

        fields_to_update = [
            field
            for field, value in [
                ("description", description),
                ("labels", labels),
                ("default_table_expiration_ms", default_table_expiration_ms),
            ]
            if value is not None
        ]

        if fields_to_update:
            self._client.update_dataset(dataset_ref, fields_to_update)

        return self
