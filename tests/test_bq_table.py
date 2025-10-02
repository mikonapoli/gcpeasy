"""Tests for BigQuery table reading."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
from google.api_core import exceptions

from geasyp.bq import init
from geasyp.bq.table import Table


class TestTable:
    """Tests for Table class."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_id_property(self, mock_bq_client):
        """Test that Table.id returns fully qualified ID."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        table = client.dataset("my_dataset").table("my_table")

        assert table.id == "test-project.my_dataset.my_table"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_exists_returns_true(self, mock_bq_client):
        """Test that exists() returns True when table exists."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.get_table.return_value = Mock()

        client = init()
        table = client.dataset("my_dataset").table("my_table")

        assert table.exists() is True
        mock_bq_client.return_value.get_table.assert_called_once_with(
            "test-project.my_dataset.my_table"
        )

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_exists_returns_false(self, mock_bq_client):
        """Test that exists() returns False when table doesn't exist."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.get_table.side_effect = exceptions.NotFound(
            "Table not found"
        )

        client = init()
        table = client.dataset("my_dataset").table("nonexistent_table")

        assert table.exists() is False

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_read_returns_dataframe(self, mock_bq_client):
        """Test that read() returns a DataFrame."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        # Setup mock query result
        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        result = table.read()

        # Verify query was called with SELECT *
        call_args = mock_bq_client.return_value.query.call_args
        query = call_args[0][0]
        assert "SELECT * FROM `test-project.my_dataset.my_table`" in query
        pd.testing.assert_frame_equal(result, mock_df)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_read_with_max_results(self, mock_bq_client):
        """Test that read() with max_results adds LIMIT clause."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        # Setup mock query result
        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        result = table.read(max_results=100)

        # Verify query includes LIMIT
        call_args = mock_bq_client.return_value.query.call_args
        query = call_args[0][0]
        assert "LIMIT 100" in query
        pd.testing.assert_frame_equal(result, mock_df)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_read_without_max_results(self, mock_bq_client):
        """Test that read() without max_results doesn't add LIMIT."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        # Setup mock query result
        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        result = table.read()

        # Verify query doesn't include LIMIT
        call_args = mock_bq_client.return_value.query.call_args
        query = call_args[0][0]
        assert "LIMIT" not in query
        pd.testing.assert_frame_equal(result, mock_df)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_table_returns_table_instance(self, mock_bq_client):
        """Test that Dataset.table() returns a Table instance."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")
        table = dataset.table("my_table")

        assert isinstance(table, Table)
        assert table.id == "test-project.my_dataset.my_table"
