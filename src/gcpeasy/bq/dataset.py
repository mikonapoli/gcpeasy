"""BigQuery Dataset class for gcpeasy."""

from typing import TYPE_CHECKING, Optional
from .validation import validate_identifier

if TYPE_CHECKING:
    from google.cloud import bigquery


class Dataset:
    """Represents a BigQuery dataset."""

    def __init__(self, client: "bigquery.Client", dataset_id: str, project_id: str):
        self._client = client
        self._dataset_id = validate_identifier(dataset_id, "dataset_id")
        self._project_id = validate_identifier(project_id, "project_id")

    @property
    def id(self) -> str:
        """The fully qualified dataset ID."""
        return f"{self._project_id}.{self._dataset_id}"

    def exists(self) -> bool:
        """Whether the dataset exists."""
        from google.api_core import exceptions
        try:
            self._client.get_dataset(self.id)
            return True
        except exceptions.NotFound:
            return False

    def tables(self, max_results: Optional[int] = None) -> list[str]:
        """Table IDs in this dataset."""
        return [t.table_id for t in self._client.list_tables(self.id, max_results=max_results)]

    def table(self, table_id: str) -> "Table":
        """Get a Table object for the given table_id."""
        from .table import Table
        return Table(self._client, table_id, self._dataset_id, self._project_id)

    def create(self, location: Optional[str] = None, description: Optional[str] = None, default_table_expiration_ms: Optional[int] = None, exists_ok: bool = False) -> "Dataset":
        """Create the dataset."""
        from google.cloud import bigquery
        from google.api_core import exceptions

        ds = bigquery.Dataset(self.id)
        if location: ds.location = location
        if description: ds.description = description
        if default_table_expiration_ms: ds.default_table_expiration_ms = default_table_expiration_ms

        try:
            self._client.create_dataset(ds)
        except exceptions.Conflict:
            if not exists_ok: raise

        return self

    def delete(self, delete_contents: bool = False, not_found_ok: bool = False) -> None:
        """Delete the dataset."""
        from google.api_core import exceptions
        try:
            self._client.delete_dataset(self.id, delete_contents=delete_contents, not_found_ok=False)
        except exceptions.NotFound:
            if not not_found_ok: raise

    def get_metadata(self) -> "bigquery.Dataset":
        """The dataset metadata."""
        return self._client.get_dataset(self.id)

    def update(self, description: Optional[str] = None, labels: Optional[dict[str, str]] = None, default_table_expiration_ms: Optional[int] = None) -> "Dataset":
        """Update dataset properties."""
        ds = self._client.get_dataset(self.id)
        if description is not None: ds.description = description
        if labels is not None: ds.labels = labels
        if default_table_expiration_ms is not None: ds.default_table_expiration_ms = default_table_expiration_ms

        fields = [f for f, v in [("description", description), ("labels", labels), ("default_table_expiration_ms", default_table_expiration_ms)] if v is not None]
        if fields: self._client.update_dataset(ds, fields)
        return self
