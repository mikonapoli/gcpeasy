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
    def test_init_should_return_client_instance(self, mock_bq_client):
        """Test that init() returns a Client instance."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        assert isinstance(client, Client)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_init_should_set_project_id_from_gcp_client(self, mock_bq_client):
        """Test that init() sets project_id from GCP client."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        assert client.project_id == "test-project"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_init_should_set_location_from_gcp_client(self, mock_bq_client):
        """Test that init() sets location from GCP client."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        assert client.location == "EU"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_init_with_project_id_should_pass_project_to_bigquery_client(self, mock_bq_client):
        """Test that init() with project_id passes it to BigQuery client."""
        mock_bq_client.return_value.project = "custom-project"
        mock_bq_client.return_value.location = "EU"

        init(project_id="custom-project")

        mock_bq_client.assert_called_once_with(project="custom-project", location="EU")

    @patch("geasyp.bq.client.bigquery.Client")
    def test_init_with_project_id_should_store_project_id_on_client(self, mock_bq_client):
        """Test that init() with project_id stores it on client."""
        mock_bq_client.return_value.project = "custom-project"
        mock_bq_client.return_value.location = "EU"

        client = init(project_id="custom-project")

        assert client.project_id == "custom-project"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_init_with_location_should_pass_location_to_bigquery_client(self, mock_bq_client):
        """Test that init() with location passes it to BigQuery client."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "US"

        init(location="US")

        mock_bq_client.assert_called_once_with(project=None, location="US")

    @patch("geasyp.bq.client.bigquery.Client")
    def test_init_with_location_should_store_location_on_client(self, mock_bq_client):
        """Test that init() with location stores it on client."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "US"

        client = init(location="US")

        assert client.location == "US"


class TestClientQuery:
    """Tests for Client query methods."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_calling_client_should_execute_query_on_gcp_client(self, mock_bq_client):
        """Test that calling client executes query on GCP client."""
        mock_job = Mock()
        mock_df = pd.DataFrame({"value": [1, 2, 3]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        client("SELECT 1 as value")

        mock_bq_client.return_value.query.assert_called_once()

    @patch("geasyp.bq.client.bigquery.Client")
    def test_calling_client_should_return_query_result_as_dataframe(self, mock_bq_client):
        """Test that calling client returns query result as DataFrame."""
        mock_job = Mock()
        mock_df = pd.DataFrame({"value": [1, 2, 3]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        result = client("SELECT 1 as value")

        pd.testing.assert_frame_equal(result, mock_df)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_query_should_execute_query_on_gcp_client(self, mock_bq_client):
        """Test that query() executes query on GCP client."""
        mock_job = Mock()
        mock_df = pd.DataFrame({"col": ["a", "b"]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        client.query("SELECT col FROM table")

        mock_bq_client.return_value.query.assert_called_once()

    @patch("geasyp.bq.client.bigquery.Client")
    def test_query_should_return_query_result_as_dataframe(self, mock_bq_client):
        """Test that query() returns query result as DataFrame."""
        mock_job = Mock()
        mock_df = pd.DataFrame({"col": ["a", "b"]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        result = client.query("SELECT col FROM table")

        pd.testing.assert_frame_equal(result, mock_df)

    @patch("geasyp.bq.client.bigquery.Client")
    @patch("geasyp.bq.client.bigquery.ScalarQueryParameter")
    def test_query_with_params_should_create_scalar_query_parameters(self, mock_param_class, mock_bq_client):
        """Test that query() with params creates ScalarQueryParameter."""
        mock_job = Mock()
        mock_df = pd.DataFrame({"id": [123]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_param = Mock()
        mock_param_class.return_value = mock_param

        client = init()
        client.query(
            "SELECT * FROM table WHERE id = @id",
            params={"id": 123}
        )

        mock_param_class.assert_called_once_with("id", "INT64", 123)

    @patch("geasyp.bq.client.bigquery.Client")
    @patch("geasyp.bq.client.bigquery.ScalarQueryParameter")
    def test_query_with_params_should_return_query_result_as_dataframe(self, mock_param_class, mock_bq_client):
        """Test that query() with params returns query result as DataFrame."""
        mock_job = Mock()
        mock_df = pd.DataFrame({"id": [123]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_param = Mock()
        mock_param_class.return_value = mock_param

        client = init()
        result = client.query(
            "SELECT * FROM table WHERE id = @id",
            params={"id": 123}
        )

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
