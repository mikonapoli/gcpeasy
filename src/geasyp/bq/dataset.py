"""BigQuery Dataset class for geasyp."""

from typing import TYPE_CHECKING

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
