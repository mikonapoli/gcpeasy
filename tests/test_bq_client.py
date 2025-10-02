"""Tests for BigQuery client basic query execution."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig

from geasyp.bq import Client, init


class TestInit:
    """Tests for the init() function."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_init_returns_client(self, mock_bq_client):
        """Test that init() returns a Client instance."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        assert isinstance(client, Client)
        assert client.project_id == "test-project"
        assert client.location == "EU"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_init_with_project_id(self, mock_bq_client):
        """Test init() with explicit project_id."""
        mock_bq_client.return_value.project = "custom-project"
        mock_bq_client.return_value.location = "EU"

        client = init(project_id="custom-project")

        mock_bq_client.assert_called_once_with(project="custom-project", location="EU")
        assert client.project_id == "custom-project"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_init_with_location(self, mock_bq_client):
        """Test init() with explicit location."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "US"

        client = init(location="US")

        mock_bq_client.assert_called_once_with(project=None, location="US")
        assert client.location == "US"


class TestClientQuery:
    """Tests for Client query methods."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_call_executes_query(self, mock_bq_client):
        """Test that calling client executes a query."""
        # Setup mock
        mock_job = Mock()
        mock_df = pd.DataFrame({"value": [1, 2, 3]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        result = client("SELECT 1 as value")

        # Verify query was executed
        mock_bq_client.return_value.query.assert_called_once()
        pd.testing.assert_frame_equal(result, mock_df)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_query_method(self, mock_bq_client):
        """Test query() method."""
        mock_job = Mock()
        mock_df = pd.DataFrame({"col": ["a", "b"]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        result = client.query("SELECT col FROM table")

        mock_bq_client.return_value.query.assert_called_once()
        pd.testing.assert_frame_equal(result, mock_df)

    @patch("geasyp.bq.client.bigquery.Client")
    @patch("geasyp.bq.client.bigquery.ScalarQueryParameter")
    def test_query_with_params(self, mock_param_class, mock_bq_client):
        """Test query() with parameters."""
        mock_job = Mock()
        mock_df = pd.DataFrame({"id": [123]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        # Setup parameter mock
        mock_param = Mock()
        mock_param_class.return_value = mock_param

        client = init()
        result = client.query(
            "SELECT * FROM table WHERE id = @id",
            params={"id": 123}
        )

        # Verify parameter was created
        mock_param_class.assert_called_once_with("id", "INT64", 123)
        pd.testing.assert_frame_equal(result, mock_df)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_query_with_custom_job_config(self, mock_bq_client):
        """Test query() with custom job config."""
        mock_job = Mock()
        mock_df = pd.DataFrame({"value": [1]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        custom_config = QueryJobConfig(use_query_cache=False)

        client = init()
        result = client.query("SELECT 1", job_config=custom_config)

        # Verify custom config was used
        call_args = mock_bq_client.return_value.query.call_args
        assert call_args[1]["job_config"] == custom_config


class TestClientProperties:
    """Tests for Client properties."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_client_has_gcp_attribute(self, mock_bq_client):
        """Test that Client exposes _gcp attribute."""
        mock_instance = Mock()
        mock_bq_client.return_value = mock_instance
        mock_instance.project = "test-project"
        mock_instance.location = "EU"

        client = init()

        assert client._gcp == mock_instance

    @patch("geasyp.bq.client.bigquery.Client")
    def test_client_stores_project_id(self, mock_bq_client):
        """Test that Client stores project_id."""
        mock_bq_client.return_value.project = "my-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        assert client.project_id == "my-project"
