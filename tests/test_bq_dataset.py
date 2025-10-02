"""Tests for BigQuery dataset discovery and navigation."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from google.api_core import exceptions

from geasyp.bq import init
from geasyp.bq.dataset import Dataset
from geasyp.bq.table import Table


class TestClientDatasets:
    """Tests for Client dataset methods."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_datasets_should_return_list_of_dataset_ids(self, mock_bq_client):
        """Test that datasets() returns list of dataset IDs."""
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

        client = init()
        result = client.datasets()

        assert result == ["dataset1", "dataset2"]

    @patch("geasyp.bq.client.bigquery.Client")
    def test_datasets_should_call_gcp_list_datasets(self, mock_bq_client):
        """Test that datasets() calls GCP list_datasets."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_dataset1 = Mock()
        mock_dataset1.dataset_id = "dataset1"

        mock_bq_client.return_value.list_datasets.return_value = [mock_dataset1]

        client = init()
        client.datasets()

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
    def test_dataset_should_return_dataset_instance(self, mock_bq_client):
        """Test that dataset() returns a Dataset instance."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        assert isinstance(dataset, Dataset)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_should_use_fully_qualified_id_with_project(self, mock_bq_client):
        """Test that dataset() creates Dataset with fully qualified ID."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        assert dataset.id == "test-project.my_dataset"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_tables_should_return_list_of_table_ids(self, mock_bq_client):
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

    @patch("geasyp.bq.client.bigquery.Client")
    def test_tables_should_call_gcp_with_fully_qualified_dataset_id(self, mock_bq_client):
        """Test that tables() uses correct dataset ID."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_table1 = Mock()
        mock_table1.table_id = "table1"

        mock_bq_client.return_value.list_tables.return_value = [mock_table1]

        client = init()
        client.tables("my_dataset")

        mock_bq_client.return_value.list_tables.assert_called_once_with(
            "test-project.my_dataset", max_results=None
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
    def test_dataset_exists_should_return_true_when_dataset_found(self, mock_bq_client):
        """Test that exists() returns True when dataset exists."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.get_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")

        assert dataset.exists() is True

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_exists_should_call_gcp_get_dataset_with_correct_id(self, mock_bq_client):
        """Test that exists() calls GCP get_dataset with correct ID."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.get_dataset.return_value = Mock()

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.exists()

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
    def test_dataset_tables_should_return_list_of_table_ids(self, mock_bq_client):
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

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_tables_should_call_gcp_list_tables_with_correct_id(self, mock_bq_client):
        """Test that Dataset.tables() uses correct dataset ID."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_table1 = Mock()
        mock_table1.table_id = "table1"

        mock_bq_client.return_value.list_tables.return_value = [mock_table1]

        client = init()
        dataset = client.dataset("my_dataset")
        dataset.tables()

        mock_bq_client.return_value.list_tables.assert_called_once_with(
            "test-project.my_dataset", max_results=None
        )

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_table_should_return_table_instance(self, mock_bq_client):
        """Test that Dataset.table() returns a Table instance."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")
        table = dataset.table("my_table")

        assert isinstance(table, Table)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_table_should_create_table_with_correct_id(self, mock_bq_client):
        """Test that Dataset.table() creates Table with correct ID."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")
        table = dataset.table("my_table")

        assert table.id == "test-project.my_dataset.my_table"
