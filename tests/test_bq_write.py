"""Tests for BigQuery DataFrame writing."""

import pytest
from unittest.mock import Mock, patch, call
import pandas as pd
from google.cloud import bigquery

from geasyp.bq import init
from geasyp.bq.schema import dict_to_schema_fields, dataframe_to_schema_fields


class TestSchemaConversion:
    """Tests for schema conversion utilities."""

    def test_dict_to_schema_fields(self):
        """Test converting dict to SchemaField objects."""
        schema_dict = {"name": "STRING", "age": "INTEGER", "score": "FLOAT"}
        fields = dict_to_schema_fields(schema_dict)

        assert len(fields) == 3
        assert all(isinstance(f, bigquery.SchemaField) for f in fields)
        assert fields[0].name == "name"
        assert fields[0].field_type == "STRING"
        assert fields[1].name == "age"
        assert fields[1].field_type == "INT64"  # INTEGER normalized to INT64
        assert fields[2].name == "score"
        assert fields[2].field_type == "FLOAT64"  # FLOAT normalized to FLOAT64

    def test_dataframe_to_schema_fields(self):
        """Test inferring schema from DataFrame."""
        df = pd.DataFrame({
            "name": ["Alice", "Bob"],
            "age": [30, 25],
            "score": [95.5, 87.3],
            "active": [True, False],
        })
        fields = dataframe_to_schema_fields(df)

        assert len(fields) == 4
        assert fields[0].name == "name"
        assert fields[0].field_type == "STRING"
        assert fields[1].name == "age"
        assert fields[1].field_type == "INT64"
        assert fields[2].name == "score"
        assert fields[2].field_type == "FLOAT64"
        assert fields[3].name == "active"
        assert fields[3].field_type == "BOOLEAN"


class TestTableWrite:
    """Tests for Table.write() method."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_write_should_call_gcp_load_table_from_dataframe(self, mock_bq_client):
        """Test that write() calls GCP client's load method."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        df = pd.DataFrame({"name": ["Alice"], "age": [30]})

        table.write(df)

        mock_bq_client.return_value.load_table_from_dataframe.assert_called_once()

    @patch("geasyp.bq.client.bigquery.Client")
    def test_write_should_pass_dataframe_to_gcp_client(self, mock_bq_client):
        """Test that write() passes DataFrame to GCP client."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        df = pd.DataFrame({"name": ["Alice"], "age": [30]})

        table.write(df)

        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        pd.testing.assert_frame_equal(call_args[0][0], df)

    @patch("geasyp.bq.client.bigquery.Client")
    def test_write_should_use_fully_qualified_table_id(self, mock_bq_client):
        """Test that write() uses correct table ID."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        df = pd.DataFrame({"name": ["Alice"], "age": [30]})

        table.write(df)

        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        assert call_args[0][1] == "test-project.my_dataset.my_table"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_write_should_default_to_write_truncate_disposition(self, mock_bq_client):
        """Test that write() uses WRITE_TRUNCATE by default."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        df = pd.DataFrame({"name": ["Alice"], "age": [30]})

        table.write(df)

        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.write_disposition == "WRITE_TRUNCATE"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_write_with_auto_schema_should_infer_schema_from_dataframe(self, mock_bq_client):
        """Test that write() without schema infers it from DataFrame."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        df = pd.DataFrame({"name": ["Alice"], "age": [30]})

        table.write(df)

        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        job_config = call_args[1]["job_config"]
        assert len(job_config.schema) == 2

    @patch("geasyp.bq.client.bigquery.Client")
    def test_write_should_wait_for_job_completion(self, mock_bq_client):
        """Test that write() waits for load job to complete."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        df = pd.DataFrame({"name": ["Alice"], "age": [30]})

        table.write(df)

        mock_job.result.assert_called_once()

    @patch("geasyp.bq.client.bigquery.Client")
    def test_write_with_schema_should_use_provided_schema(self, mock_bq_client):
        """Test that write() with schema parameter uses it."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        df = pd.DataFrame({"name": ["Alice"], "age": [30]})
        schema = {"name": "STRING", "age": "INT64"}

        table.write(df, schema=schema)

        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        job_config = call_args[1]["job_config"]
        assert len(job_config.schema) == 2

    @patch("geasyp.bq.client.bigquery.Client")
    def test_write_with_schema_should_convert_dict_to_schema_fields(self, mock_bq_client):
        """Test that write() converts schema dict to SchemaField objects."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        df = pd.DataFrame({"name": ["Alice"], "age": [30]})
        schema = {"name": "STRING", "age": "INT64"}

        table.write(df, schema=schema)

        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.schema[0].name == "name"
        assert job_config.schema[0].field_type == "STRING"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_write_with_append_disposition(self, mock_bq_client):
        """Test writing with WRITE_APPEND disposition."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        df = pd.DataFrame({"name": ["Alice"]})

        table.write(df, write_disposition="WRITE_APPEND")

        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.write_disposition == "WRITE_APPEND"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_table_write_with_empty_disposition(self, mock_bq_client):
        """Test writing with WRITE_EMPTY disposition."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        df = pd.DataFrame({"name": ["Alice"]})

        table.write(df, write_disposition="WRITE_EMPTY")

        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.write_disposition == "WRITE_EMPTY"


class TestClientLoadData:
    """Tests for Client.load_data() convenience method."""

    @patch("geasyp.bq.client.bigquery.Client")
    def test_load_data_with_dataset_table_format(self, mock_bq_client):
        """Test load_data with 'dataset.table' format."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        df = pd.DataFrame({"name": ["Alice"], "age": [30]})

        client.load_data(df, "my_dataset.my_table")

        # Verify load was called with correct table ID
        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        assert call_args[0][1] == "test-project.my_dataset.my_table"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_load_data_with_full_table_id_should_override_with_client_project(self, mock_bq_client):
        """Test load_data uses client's project even when full ID provided."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        df = pd.DataFrame({"name": ["Alice"]})

        client.load_data(df, "other-project.my_dataset.my_table")

        # Verify correct table ID (should use "test-project")
        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        assert call_args[0][1] == "test-project.my_dataset.my_table"  # Uses client's project

    @patch("geasyp.bq.client.bigquery.Client")
    def test_load_data_with_write_disposition_should_pass_it_to_job_config(self, mock_bq_client):
        """Test that load_data passes write_disposition to job config."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        df = pd.DataFrame({"name": ["Alice"]})

        client.load_data(df, "my_dataset.my_table", write_disposition="WRITE_APPEND")

        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.write_disposition == "WRITE_APPEND"

    @patch("geasyp.bq.client.bigquery.Client")
    def test_load_data_with_schema_should_pass_it_to_job_config(self, mock_bq_client):
        """Test that load_data passes schema to job config."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_job.result.return_value = None
        mock_bq_client.return_value.load_table_from_dataframe.return_value = mock_job

        client = init()
        df = pd.DataFrame({"name": ["Alice"]})
        schema = {"name": "STRING"}

        client.load_data(df, "my_dataset.my_table", schema=schema)

        call_args = mock_bq_client.return_value.load_table_from_dataframe.call_args
        job_config = call_args[1]["job_config"]
        assert len(job_config.schema) == 1

    @patch("geasyp.bq.client.bigquery.Client")
    def test_load_data_with_invalid_table_id(self, mock_bq_client):
        """Test load_data with invalid table ID format."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        df = pd.DataFrame({"name": ["Alice"]})

        with pytest.raises(ValueError, match="Invalid table_id format"):
            client.load_data(df, "invalid_format")
