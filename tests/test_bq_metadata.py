"""Tests for BigQuery metadata operations."""

import pytest
from unittest.mock import Mock, MagicMock
from google.cloud import bigquery

from gcpeasy.bq.dataset import Dataset
from gcpeasy.bq.table import Table


class TestDatasetMetadata:
    """Tests for Dataset metadata methods."""

    def test_getting_metadata_should_call_gcp_get_dataset(self):
        mock_client = Mock()
        mock_dataset = Mock(spec=bigquery.Dataset)
        mock_client.get_dataset.return_value = mock_dataset

        dataset = Dataset(mock_client, "test_dataset", "test_project")
        result = dataset.get_metadata()

        mock_client.get_dataset.assert_called_once_with("test_project.test_dataset")
        assert result == mock_dataset

    def test_getting_metadata_should_return_bigquery_dataset(self):
        mock_client = Mock()
        mock_dataset = Mock(spec=bigquery.Dataset)
        mock_dataset.location = "EU"
        mock_dataset.description = "Test dataset"
        mock_client.get_dataset.return_value = mock_dataset

        dataset = Dataset(mock_client, "test_dataset", "test_project")
        result = dataset.get_metadata()

        assert result.location == "EU"
        assert result.description == "Test dataset"


class TestDatasetUpdate:
    """Tests for Dataset.update() method."""

    def test_updating_description_should_call_update_dataset(self):
        mock_client = Mock()
        mock_dataset = Mock(spec=bigquery.Dataset)
        mock_client.get_dataset.return_value = mock_dataset

        dataset = Dataset(mock_client, "test_dataset", "test_project")
        dataset.update(description="New description")

        mock_client.update_dataset.assert_called_once()
        call_args = mock_client.update_dataset.call_args
        assert call_args[0][1] == ["description"]
        assert mock_dataset.description == "New description"

    def test_updating_labels_should_call_update_dataset(self):
        mock_client = Mock()
        mock_dataset = Mock(spec=bigquery.Dataset)
        mock_client.get_dataset.return_value = mock_dataset

        dataset = Dataset(mock_client, "test_dataset", "test_project")
        labels = {"env": "prod", "team": "data"}
        dataset.update(labels=labels)

        mock_client.update_dataset.assert_called_once()
        call_args = mock_client.update_dataset.call_args
        assert call_args[0][1] == ["labels"]
        assert mock_dataset.labels == labels

    def test_updating_expiration_should_call_update_dataset(self):
        mock_client = Mock()
        mock_dataset = Mock(spec=bigquery.Dataset)
        mock_client.get_dataset.return_value = mock_dataset

        dataset = Dataset(mock_client, "test_dataset", "test_project")
        expiration = 86400000  # 1 day in milliseconds
        dataset.update(default_table_expiration_ms=expiration)

        mock_client.update_dataset.assert_called_once()
        call_args = mock_client.update_dataset.call_args
        assert call_args[0][1] == ["default_table_expiration_ms"]
        assert mock_dataset.default_table_expiration_ms == expiration

    def test_updating_multiple_fields_should_include_all_in_update(self):
        mock_client = Mock()
        mock_dataset = Mock(spec=bigquery.Dataset)
        mock_client.get_dataset.return_value = mock_dataset

        dataset = Dataset(mock_client, "test_dataset", "test_project")
        dataset.update(
            description="New description",
            labels={"env": "prod"},
            default_table_expiration_ms=86400000,
        )

        mock_client.update_dataset.assert_called_once()
        call_args = mock_client.update_dataset.call_args
        fields = call_args[0][1]
        assert "description" in fields
        assert "labels" in fields
        assert "default_table_expiration_ms" in fields

    def test_updating_should_return_self_for_chaining(self):
        mock_client = Mock()
        mock_dataset = Mock(spec=bigquery.Dataset)
        mock_client.get_dataset.return_value = mock_dataset

        dataset = Dataset(mock_client, "test_dataset", "test_project")
        result = dataset.update(description="New description")

        assert result is dataset


class TestTableMetadata:
    """Tests for Table metadata methods."""

    def test_getting_metadata_should_call_gcp_get_table(self):
        mock_client = Mock()
        mock_table = Mock(spec=bigquery.Table)
        mock_client.get_table.return_value = mock_table

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        result = table.get_metadata()

        mock_client.get_table.assert_called_once_with("test_project.test_dataset.test_table")
        assert result == mock_table

    def test_getting_metadata_should_return_bigquery_table(self):
        mock_client = Mock()
        mock_table = Mock(spec=bigquery.Table)
        mock_table.num_rows = 1000
        mock_table.description = "Test table"
        mock_client.get_table.return_value = mock_table

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        result = table.get_metadata()

        assert result.num_rows == 1000
        assert result.description == "Test table"

    def test_getting_schema_should_return_schema_fields(self):
        mock_client = Mock()
        mock_table = Mock(spec=bigquery.Table)
        mock_schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
        ]
        mock_table.schema = mock_schema
        mock_client.get_table.return_value = mock_table

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        result = table.get_schema()

        assert result == mock_schema
        assert len(result) == 2
        assert result[0].name == "name"
        assert result[1].name == "age"


class TestTableUpdate:
    """Tests for Table.update() method."""

    def test_updating_description_should_call_update_table(self):
        mock_client = Mock()
        mock_table = Mock(spec=bigquery.Table)
        mock_table.schema = []
        mock_client.get_table.return_value = mock_table

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.update(description="New description")

        mock_client.update_table.assert_called_once()
        call_args = mock_client.update_table.call_args
        assert call_args[0][1] == ["description"]
        assert mock_table.description == "New description"

    def test_updating_labels_should_call_update_table(self):
        mock_client = Mock()
        mock_table = Mock(spec=bigquery.Table)
        mock_table.schema = []
        mock_client.get_table.return_value = mock_table

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        labels = {"env": "prod", "team": "data"}
        table.update(labels=labels)

        mock_client.update_table.assert_called_once()
        call_args = mock_client.update_table.call_args
        assert call_args[0][1] == ["labels"]
        assert mock_table.labels == labels

    def test_updating_schema_should_add_new_fields(self):
        mock_client = Mock()
        mock_table = Mock(spec=bigquery.Table)
        existing_schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
        ]
        mock_table.schema = existing_schema
        mock_client.get_table.return_value = mock_table

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        new_fields = {"email": "STRING"}
        table.update(schema=new_fields)

        mock_client.update_table.assert_called_once()
        call_args = mock_client.update_table.call_args
        assert call_args[0][1] == ["schema"]
        # Schema should have original fields plus new field
        updated_schema = mock_table.schema
        assert len(updated_schema) == 3

    def test_updating_multiple_fields_should_include_all_in_update(self):
        mock_client = Mock()
        mock_table = Mock(spec=bigquery.Table)
        mock_table.schema = []
        mock_client.get_table.return_value = mock_table

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.update(
            description="New description",
            labels={"env": "prod"},
            schema={"new_field": "STRING"},
        )

        mock_client.update_table.assert_called_once()
        call_args = mock_client.update_table.call_args
        fields = call_args[0][1]
        assert "description" in fields
        assert "labels" in fields
        assert "schema" in fields

    def test_updating_should_return_self_for_chaining(self):
        mock_client = Mock()
        mock_table = Mock(spec=bigquery.Table)
        mock_table.schema = []
        mock_client.get_table.return_value = mock_table

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        result = table.update(description="New description")

        assert result is table
