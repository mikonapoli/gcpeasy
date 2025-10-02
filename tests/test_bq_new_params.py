"""Tests for newly added parameters."""

import pytest
from unittest.mock import Mock, patch
from google.cloud import bigquery

from geasyp.bq import Client, init
from geasyp.bq.dataset import Dataset
from geasyp.bq.table import Table


class TestMaxResultsParameter:
    """Tests for max_results parameter on list methods."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_datasets_with_max_results_should_pass_to_gcp(self, mock_bq_client):
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.list_datasets.return_value = []

        client = init()
        client.datasets(max_results=5)

        mock_bq_client.return_value.list_datasets.assert_called_once_with(max_results=5)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_datasets_without_max_results_should_pass_none(self, mock_bq_client):
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.list_datasets.return_value = []

        client = init()
        client.datasets()

        mock_bq_client.return_value.list_datasets.assert_called_once_with(max_results=None)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_client_tables_with_max_results_should_pass_to_dataset(self, mock_bq_client):
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"
        mock_bq_client.return_value.list_tables.return_value = []

        client = init()
        client.tables("my_dataset", max_results=10)

        mock_bq_client.return_value.list_tables.assert_called_once_with(
            "test-project.my_dataset", max_results=10
        )

    @patch("geasyp.bq.client.bigquery.Client")
    def test_dataset_tables_with_max_results_should_pass_to_gcp(self, mock_bq_client):
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.list_tables.return_value = []

        dataset = Dataset(mock_bq_client.return_value, "my_dataset", "test-project")
        dataset.tables(max_results=20)

        mock_bq_client.return_value.list_tables.assert_called_once_with(
            "test-project.my_dataset", max_results=20
        )


class TestToDataframeParameter:
    """Tests for to_dataframe parameter on query methods."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_query_with_to_dataframe_true_should_return_dataframe(self, mock_bq_client):
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = Mock()
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        result = client.query("SELECT 1", to_dataframe=True)

        assert result == mock_df
        mock_job.to_dataframe.assert_called_once()

    @patch("geasyp.bq.client.bigquery.Client")
    def test_query_with_to_dataframe_false_should_return_iterator(self, mock_bq_client):
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_iterator = Mock()
        mock_job.result.return_value = mock_iterator
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        result = client.query("SELECT 1", to_dataframe=False)

        assert result == mock_iterator
        mock_job.result.assert_called_once()

    @patch("geasyp.bq.client.bigquery.Client")
    def test_query_default_should_return_dataframe(self, mock_bq_client):
        """Test that default behavior returns DataFrame."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = Mock()
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        result = client.query("SELECT 1")

        assert result == mock_df
        mock_job.to_dataframe.assert_called_once()

    @patch("geasyp.bq.client.bigquery.Client")
    def test_call_with_to_dataframe_false_should_return_iterator(self, mock_bq_client):
        """Test __call__ method with to_dataframe=False."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_iterator = Mock()
        mock_job.result.return_value = mock_iterator
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        result = client("SELECT 1", to_dataframe=False)

        assert result == mock_iterator
        mock_job.result.assert_called_once()

    @patch("geasyp.bq.client.bigquery.Client")
    def test_call_default_should_return_dataframe(self, mock_bq_client):
        """Test __call__ method default behavior."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = Mock()
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        result = client("SELECT 1")

        assert result == mock_df
        mock_job.to_dataframe.assert_called_once()


class TestTableCreateDescription:
    """Tests for description parameter on Table.create()."""

    def test_create_with_description_should_set_it(self):
        mock_client = Mock()
        mock_client.create_table = Mock()

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        schema = {"name": "STRING", "age": "INTEGER"}

        table.create(schema, description="Test table description")

        # Check that create_table was called
        mock_client.create_table.assert_called_once()

        # Check that the table reference has description set
        call_args = mock_client.create_table.call_args
        table_ref = call_args[0][0]
        assert table_ref.description == "Test table description"

    def test_create_without_description_should_not_set_it(self):
        mock_client = Mock()
        mock_client.create_table = Mock()

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        schema = {"name": "STRING", "age": "INTEGER"}

        table.create(schema)

        # Check that create_table was called
        mock_client.create_table.assert_called_once()

        # Check that description is not set (or is None)
        call_args = mock_client.create_table.call_args
        table_ref = call_args[0][0]
        # Description should not have been set or should be None
        assert not hasattr(table_ref, 'description') or table_ref.description is None

    def test_create_with_description_and_other_params_should_work(self):
        mock_client = Mock()
        mock_client.create_table = Mock()

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        schema = {"name": "STRING", "age": "INTEGER", "created": "TIMESTAMP"}

        table.create(
            schema,
            description="Test table",
            partitioning_field="created",
            clustering_fields=["name"]
        )

        mock_client.create_table.assert_called_once()
        call_args = mock_client.create_table.call_args
        table_ref = call_args[0][0]

        assert table_ref.description == "Test table"
        assert table_ref.time_partitioning is not None
        assert table_ref.clustering_fields == ["name"]
