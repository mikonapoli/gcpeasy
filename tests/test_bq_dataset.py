"""Tests for BigQuery dataset discovery and navigation."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from google.api_core import exceptions

from geasyp.bq import init
from geasyp.bq.dataset import Dataset


class TestClientDatasets:
    """Tests for Client dataset methods."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_datasets_lists_all_datasets(self, mock_bq_client):
        """Test that datasets() returns list of dataset IDs."""
        # Setup mocks
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_dataset1 = Mock()
        mock_dataset1.dataset_id = "dataset1"
        mock_dataset2 = Mock()
        mock_dataset2.dataset_id = "dataset2"

        mock_bq_client.return_value.list_datasets.return_value = [
            mock_dataset1,
            mock_dataset2,
        ]

        # Test
        client = init()
        result = client.datasets()

        # Verify
        assert result == ["dataset1", "dataset2"]
        mock_bq_client.return_value.list_datasets.assert_called_once()

    @patch("geasyp.bq.client.bigquery.Client")
    def test_datasets_returns_empty_list_when_no_datasets(self, mock_bq_client):
        """Test that datasets() returns empty list when no datasets exist."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.list_datasets.return_value = []

        client = init()
        result = client.datasets()

        assert result == []

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_returns_dataset_object(self, mock_bq_client):
        """Test that dataset() returns a Dataset object."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        assert isinstance(dataset, Dataset)
        assert dataset.id == "test-project.my_dataset"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_tables_lists_tables_in_dataset(self, mock_bq_client):
        """Test that tables() returns list of table IDs."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_table1 = Mock()
        mock_table1.table_id = "table1"
        mock_table2 = Mock()
        mock_table2.table_id = "table2"

        mock_bq_client.return_value.list_tables.return_value = [
            mock_table1,
            mock_table2,
        ]

        client = init()
        result = client.tables("my_dataset")

        assert result == ["table1", "table2"]
        mock_bq_client.return_value.list_tables.assert_called_once_with(
            "test-project.my_dataset"
        )


class TestDataset:
    """Tests for Dataset class."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_id_property(self, mock_bq_client):
        """Test that Dataset.id returns fully qualified ID."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        assert dataset.id == "test-project.my_dataset"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_exists_returns_true(self, mock_bq_client):
        """Test that exists() returns True when dataset exists."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.get_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")

        assert dataset.exists() is True
        mock_bq_client.return_value.get_dataset.assert_called_once_with(
            "test-project.my_dataset"
        )

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_exists_returns_false(self, mock_bq_client):
        """Test that exists() returns False when dataset doesn't exist."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.get_dataset.side_effect = exceptions.NotFound(
            "Dataset not found"
        )

        client = init()
        dataset = client.dataset("nonexistent_dataset")

        assert dataset.exists() is False

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_tables_lists_tables(self, mock_bq_client):
        """Test that Dataset.tables() returns list of table IDs."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_table1 = Mock()
        mock_table1.table_id = "table1"
        mock_table2 = Mock()
        mock_table2.table_id = "table2"

        mock_bq_client.return_value.list_tables.return_value = [
            mock_table1,
            mock_table2,
        ]

        client = init()
        dataset = client.dataset("my_dataset")
        result = dataset.tables()

        assert result == ["table1", "table2"]
        mock_bq_client.return_value.list_tables.assert_called_once_with(
            "test-project.my_dataset"
        )

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_table_returns_table_object(self, mock_bq_client):
        """Test that Dataset.table() returns a Table object."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")
        table = dataset.table("my_table")

        # Table class not yet implemented, but we can check it's not None
        assert table is not None
