"""BigQuery Dataset class for geasyp."""

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
            dataset_id: The dataset ID (without project).
            project_id: The project ID containing the dataset.
        """
        self._client = client
        self._dataset_id = dataset_id
        self._project_id = project_id

    @property
    def id(self) -> str:
        """Get the fully qualified dataset ID.

        Returns:
            Fully qualified dataset ID in format 'project.dataset'.
        """
        return f"{self._project_id}.{self._dataset_id}"

    def exists(self) -> bool:
        """Check if the dataset exists.

        Returns:
            True if the dataset exists, False otherwise.
        """
        from google.api_core import exceptions

        try:
            self._client.get_dataset(self.id)
            return True
        except exceptions.NotFound:
            return False

    def tables(self) -> list[str]:
        """List all tables in the dataset.

        Returns:
            List of table IDs (without project or dataset prefix).
        """
        tables = self._client.list_tables(self.id)
        return [table.table_id for table in tables]

    def table(self, table_id: str) -> "Table":
        """Get a Table object for a specific table.

        Args:
            table_id: The table ID (without project or dataset prefix).

        Returns:
            Table object for the specified table.
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
            location: Geographic location for the dataset. If None, uses client's location.
            description: Optional description for the dataset.
            default_table_expiration_ms: Default expiration time for tables in milliseconds.
            exists_ok: If True, don't raise error if dataset already exists.

        Returns:
            Self for method chaining.

        Raises:
            google.api_core.exceptions.Conflict: If dataset exists and exists_ok is False.

        Example:
            >>> dataset = client.dataset("my_dataset")
            >>> dataset.create(description="My dataset", exists_ok=True)
        """
        from google.cloud import bigquery
        from google.api_core import exceptions

        # Create dataset object
        dataset_ref = bigquery.Dataset(self.id)
        if location:
            dataset_ref.location = location
        if description:
            dataset_ref.description = description
        if default_table_expiration_ms:
            dataset_ref.default_table_expiration_ms = default_table_expiration_ms

        # Create dataset
        try:
            self._client.create_dataset(dataset_ref)
        except exceptions.Conflict:
            if not exists_ok:
                raise

        return self

    def delete(self, delete_contents: bool = False, not_found_ok: bool = False) -> None:
        """Delete the dataset.

        Args:
            delete_contents: If True, delete all tables in the dataset before deleting.
            not_found_ok: If True, don't raise error if dataset doesn't exist.

        Raises:
            google.api_core.exceptions.NotFound: If dataset doesn't exist and not_found_ok is False.

        Example:
            >>> dataset = client.dataset("my_dataset")
            >>> dataset.delete(delete_contents=True)
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
        """Get dataset metadata and properties.

        Returns:
            BigQuery Dataset object with full metadata including location,
            description, labels, creation time, etc.

        Example:
            >>> dataset = client.dataset("my_dataset")
            >>> metadata = dataset.get_metadata()
            >>> print(metadata.location)
            >>> print(metadata.description)
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
            description: New description for the dataset.
            labels: Labels dict to set on the dataset.
            default_table_expiration_ms: Default expiration time for tables in milliseconds.

        Returns:
            Self for method chaining.

        Example:
            >>> dataset = client.dataset("my_dataset")
            >>> dataset.update(
            ...     description="Updated description",
            ...     labels={"env": "prod"}
            ... )
        """
        from google.cloud import bigquery

        # Get current dataset
        dataset_ref = self._client.get_dataset(self.id)

        # Update fields
        if description is not None:
            dataset_ref.description = description
        if labels is not None:
            dataset_ref.labels = labels
        if default_table_expiration_ms is not None:
            dataset_ref.default_table_expiration_ms = default_table_expiration_ms

        # Determine which fields to update
        fields_to_update = []
        if description is not None:
            fields_to_update.append("description")
        if labels is not None:
            fields_to_update.append("labels")
        if default_table_expiration_ms is not None:
            fields_to_update.append("default_table_expiration_ms")

        # Update dataset
        if fields_to_update:
            self._client.update_dataset(dataset_ref, fields_to_update)

        return self
