"""Tests for BigQuery table and dataset creation and management."""

import pytest
from unittest.mock import Mock, patch
from google.api_core import exceptions
from google.cloud import bigquery

from geasyp.bq import init


class TestDatasetCreate:
    """Tests for Dataset.create() method."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_create_basic(self, mock_bq_client):
        """Test basic dataset creation."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")
        result = dataset.create()

        # Verify create_dataset was called
        mock_bq_client.return_value.create_dataset.assert_called_once()
        call_args = mock_bq_client.return_value.create_dataset.call_args
        dataset_obj = call_args[0][0]

        # BigQuery Dataset object has reference properties
        assert dataset_obj.dataset_id == "my_dataset"
        assert dataset_obj.project == "test-project"
        assert result is dataset  # Returns self for chaining

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_create_with_options(self, mock_bq_client):
        """Test dataset creation with description and expiration."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.create(
            location="US",
            description="Test dataset",
            default_table_expiration_ms=86400000,  # 1 day
        )

        call_args = mock_bq_client.return_value.create_dataset.call_args
        dataset_obj = call_args[0][0]

        assert dataset_obj.location == "US"
        assert dataset_obj.description == "Test dataset"
        assert dataset_obj.default_table_expiration_ms == 86400000

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_create_exists_ok_true(self, mock_bq_client):
        """Test dataset creation with exists_ok=True doesn't raise on conflict."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.side_effect = exceptions.Conflict(
            "Dataset already exists"
        )

        client = init()
        dataset = client.dataset("my_dataset")

        # Should not raise
        dataset.create(exists_ok=True)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_create_exists_ok_false(self, mock_bq_client):
        """Test dataset creation with exists_ok=False raises on conflict."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.side_effect = exceptions.Conflict(
            "Dataset already exists"
        )

        client = init()
        dataset = client.dataset("my_dataset")

        with pytest.raises(exceptions.Conflict):
            dataset.create(exists_ok=False)


class TestDatasetDelete:
    """Tests for Dataset.delete() method."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_delete_basic(self, mock_bq_client):
        """Test basic dataset deletion."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.delete_dataset.return_value = None

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.delete()

        mock_bq_client.return_value.delete_dataset.assert_called_once_with(
            "test-project.my_dataset", delete_contents=False, not_found_ok=False
        )

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_delete_with_contents(self, mock_bq_client):
        """Test dataset deletion with delete_contents=True."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.delete_dataset.return_value = None

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.delete(delete_contents=True)

        mock_bq_client.return_value.delete_dataset.assert_called_once_with(
            "test-project.my_dataset", delete_contents=True, not_found_ok=False
        )

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_delete_not_found_ok_true(self, mock_bq_client):
        """Test dataset deletion with not_found_ok=True doesn't raise."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.delete_dataset.side_effect = exceptions.NotFound(
            "Dataset not found"
        )

        client = init()
        dataset = client.dataset("my_dataset")

        # Should not raise
        dataset.delete(not_found_ok=True)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_delete_not_found_ok_false(self, mock_bq_client):
        """Test dataset deletion with not_found_ok=False raises."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.delete_dataset.side_effect = exceptions.NotFound(
            "Dataset not found"
        )

        client = init()
        dataset = client.dataset("my_dataset")

        with pytest.raises(exceptions.NotFound):
            dataset.delete(not_found_ok=False)


class TestTableCreate:
    """Tests for Table.create() method."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_create_basic(self, mock_bq_client):
        """Test basic table creation."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.return_value = Mock()

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING", "age": "INT64"}
        result = table.create(schema)

        # Verify create_table was called
        mock_bq_client.return_value.create_table.assert_called_once()
        call_args = mock_bq_client.return_value.create_table.call_args
        table_obj = call_args[0][0]

        # BigQuery Table object has reference properties
        assert table_obj.table_id == "my_table"
        assert table_obj.dataset_id == "my_dataset"
        assert table_obj.project == "test-project"
        assert len(table_obj.schema) == 2
        assert result is table  # Returns self for chaining

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_create_with_partitioning(self, mock_bq_client):
        """Test table creation with partitioning."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.return_value = Mock()

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING", "created": "TIMESTAMP"}
        table.create(schema, partitioning_field="created")

        call_args = mock_bq_client.return_value.create_table.call_args
        table_obj = call_args[0][0]

        assert table_obj.time_partitioning is not None
        assert table_obj.time_partitioning.field == "created"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_create_with_clustering(self, mock_bq_client):
        """Test table creation with clustering."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.return_value = Mock()

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING", "age": "INT64", "city": "STRING"}
        table.create(schema, clustering_fields=["city", "age"])

        call_args = mock_bq_client.return_value.create_table.call_args
        table_obj = call_args[0][0]

        assert table_obj.clustering_fields == ["city", "age"]

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_create_exists_ok_true(self, mock_bq_client):
        """Test table creation with exists_ok=True doesn't raise on conflict."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.side_effect = exceptions.Conflict(
            "Table already exists"
        )

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING"}

        # Should not raise
        table.create(schema, exists_ok=True)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_create_exists_ok_false(self, mock_bq_client):
        """Test table creation with exists_ok=False raises on conflict."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.side_effect = exceptions.Conflict(
            "Table already exists"
        )

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING"}

        with pytest.raises(exceptions.Conflict):
            table.create(schema, exists_ok=False)


class TestTableDelete:
    """Tests for Table.delete() method."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_delete_basic(self, mock_bq_client):
        """Test basic table deletion."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.delete_table.return_value = None

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        table.delete()

        mock_bq_client.return_value.delete_table.assert_called_once_with(
            "test-project.my_dataset.my_table", not_found_ok=False
        )

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_delete_not_found_ok_true(self, mock_bq_client):
        """Test table deletion with not_found_ok=True doesn't raise."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.delete_table.side_effect = exceptions.NotFound(
            "Table not found"
        )

        client = init()
        table = client.dataset("my_dataset").table("my_table")

        # Should not raise
        table.delete(not_found_ok=True)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_delete_not_found_ok_false(self, mock_bq_client):
        """Test table deletion with not_found_ok=False raises."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.delete_table.side_effect = exceptions.NotFound(
            "Table not found"
        )

        client = init()
        table = client.dataset("my_dataset").table("my_table")

        with pytest.raises(exceptions.NotFound):
            table.delete(not_found_ok=False)
