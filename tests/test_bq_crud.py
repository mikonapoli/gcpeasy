"""Tests for BigQuery table and dataset creation and management."""

import pytest
from unittest.mock import Mock, patch
from google.api_core import exceptions
from google.cloud import bigquery

from gcpeasy.bq import init


class TestDatasetCreate:
    """Tests for Dataset.create() method."""

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_dataset_create_should_call_gcp_create_dataset(self, mock_bq_client):
        """Test that create() calls GCP client's create_dataset."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.create()

        mock_bq_client.return_value.create_dataset.assert_called_once()

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_dataset_create_should_use_correct_dataset_id(self, mock_bq_client):
        """Test that create() uses correct dataset_id."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.create()

        call_args = mock_bq_client.return_value.create_dataset.call_args
        dataset_obj = call_args[0][0]
        assert dataset_obj.dataset_id == "my_dataset"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_dataset_create_should_use_client_project(self, mock_bq_client):
        """Test that create() uses client's project."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.create()

        call_args = mock_bq_client.return_value.create_dataset.call_args
        dataset_obj = call_args[0][0]
        assert dataset_obj.project == "test-project"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_dataset_create_should_return_self_for_chaining(self, mock_bq_client):
        """Test that create() returns self to allow method chaining."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")
        result = dataset.create()

        assert result is dataset

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_dataset_create_with_location_should_set_location(self, mock_bq_client):
        """Test that create() with location parameter sets location."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.create(location="US")

        call_args = mock_bq_client.return_value.create_dataset.call_args
        dataset_obj = call_args[0][0]
        assert dataset_obj.location == "US"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_dataset_create_with_description_should_set_description(self, mock_bq_client):
        """Test that create() with description sets description."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.create(description="Test dataset")

        call_args = mock_bq_client.return_value.create_dataset.call_args
        dataset_obj = call_args[0][0]
        assert dataset_obj.description == "Test dataset"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_dataset_create_with_expiration_should_set_table_expiration(self, mock_bq_client):
        """Test that create() with expiration sets default table expiration."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.create(default_table_expiration_ms=86400000)

        call_args = mock_bq_client.return_value.create_dataset.call_args
        dataset_obj = call_args[0][0]
        assert dataset_obj.default_table_expiration_ms == 86400000

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_dataset_delete_should_call_gcp_with_default_parameters(self, mock_bq_client):
        """Test basic dataset deletion with default parameters."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.delete_dataset.return_value = None

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.delete()

        mock_bq_client.return_value.delete_dataset.assert_called_once_with(
            "test-project.my_dataset", delete_contents=False, not_found_ok=False
        )

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_table_create_should_call_gcp_create_table(self, mock_bq_client):
        """Test that create() calls GCP client's create_table."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.return_value = Mock()

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING", "age": "INT64"}
        table.create(schema)

        mock_bq_client.return_value.create_table.assert_called_once()

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_table_create_should_set_correct_table_id(self, mock_bq_client):
        """Test that create() sets correct table_id."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.return_value = Mock()

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING", "age": "INT64"}
        table.create(schema)

        call_args = mock_bq_client.return_value.create_table.call_args
        table_obj = call_args[0][0]
        assert table_obj.table_id == "my_table"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_table_create_should_set_correct_dataset_id(self, mock_bq_client):
        """Test that create() sets correct dataset_id."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.return_value = Mock()

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING", "age": "INT64"}
        table.create(schema)

        call_args = mock_bq_client.return_value.create_table.call_args
        table_obj = call_args[0][0]
        assert table_obj.dataset_id == "my_dataset"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_table_create_should_use_client_project(self, mock_bq_client):
        """Test that create() sets correct project."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.return_value = Mock()

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING", "age": "INT64"}
        table.create(schema)

        call_args = mock_bq_client.return_value.create_table.call_args
        table_obj = call_args[0][0]
        assert table_obj.project == "test-project"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_table_create_should_convert_schema_dict_to_fields(self, mock_bq_client):
        """Test that create() converts schema dict to SchemaField objects."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.return_value = Mock()

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING", "age": "INT64"}
        table.create(schema)

        call_args = mock_bq_client.return_value.create_table.call_args
        table_obj = call_args[0][0]
        assert len(table_obj.schema) == 2

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_table_create_should_return_self_for_chaining(self, mock_bq_client):
        """Test that create() returns self to allow method chaining."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.create_table.return_value = Mock()

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        schema = {"name": "STRING", "age": "INT64"}
        result = table.create(schema)

        assert result is table

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
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

    @patch("gcpeasy.bq.client.bigquery.Client")
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
