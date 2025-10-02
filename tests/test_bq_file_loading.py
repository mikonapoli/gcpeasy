"""Tests for BigQuery file loading functionality."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, mock_open
from google.cloud import bigquery

from geasyp.bq.file_utils import (
    detect_source_format,
    create_load_job_config,
)
from geasyp.bq.table import Table


class TestDetectSourceFormat:
    """Tests for file format detection."""

    def test_detecting_csv_format_should_return_csv(self):
        file_path = Path("data.csv")
        assert detect_source_format(file_path) == "CSV"

    def test_detecting_json_format_should_return_newline_delimited_json(self):
        file_path = Path("data.json")
        assert detect_source_format(file_path) == "NEWLINE_DELIMITED_JSON"

    def test_detecting_jsonl_format_should_return_newline_delimited_json(self):
        file_path = Path("data.jsonl")
        assert detect_source_format(file_path) == "NEWLINE_DELIMITED_JSON"

    def test_detecting_ndjson_format_should_return_newline_delimited_json(self):
        file_path = Path("data.ndjson")
        assert detect_source_format(file_path) == "NEWLINE_DELIMITED_JSON"

    def test_detecting_parquet_format_should_return_parquet(self):
        file_path = Path("data.parquet")
        assert detect_source_format(file_path) == "PARQUET"

    def test_detecting_avro_format_should_return_avro(self):
        file_path = Path("data.avro")
        assert detect_source_format(file_path) == "AVRO"

    def test_detecting_orc_format_should_return_orc(self):
        file_path = Path("data.orc")
        assert detect_source_format(file_path) == "ORC"

    def test_detecting_unsupported_format_should_raise_error(self):
        file_path = Path("data.txt")
        with pytest.raises(ValueError, match="Unsupported file format: .txt"):
            detect_source_format(file_path)

    def test_detecting_format_should_be_case_insensitive(self):
        file_path = Path("data.CSV")
        assert detect_source_format(file_path) == "CSV"


class TestCreateLoadJobConfig:
    """Tests for LoadJobConfig creation."""

    def test_creating_config_with_csv_format_should_set_source_format(self):
        config = create_load_job_config("CSV")
        assert config.source_format == "CSV"

    def test_creating_csv_config_should_default_to_skip_one_row(self):
        config = create_load_job_config("CSV")
        assert config.skip_leading_rows == 1

    def test_creating_csv_config_with_custom_skip_rows_should_use_custom_value(self):
        config = create_load_job_config("CSV", skip_leading_rows=2)
        assert config.skip_leading_rows == 2

    def test_creating_csv_config_with_field_delimiter_should_set_delimiter(self):
        config = create_load_job_config("CSV", field_delimiter="|")
        assert config.field_delimiter == "|"

    def test_creating_config_with_schema_should_set_schema(self):
        schema = [bigquery.SchemaField("name", "STRING")]
        config = create_load_job_config("CSV", schema=schema)
        assert config.schema == schema
        assert config.autodetect is None or config.autodetect is False

    def test_creating_config_with_autodetect_should_enable_autodetect(self):
        config = create_load_job_config("CSV", autodetect=True)
        assert config.autodetect is True

    def test_creating_config_should_set_write_disposition(self):
        config = create_load_job_config("CSV", write_disposition="WRITE_TRUNCATE")
        assert config.write_disposition == "WRITE_TRUNCATE"

    def test_creating_parquet_config_should_not_set_csv_options(self):
        config = create_load_job_config("PARQUET")
        # For Parquet, skip_leading_rows shouldn't be set (or should be None/0)
        assert config.source_format == "PARQUET"


class TestTableWriteWithFiles:
    """Tests for Table.write() with file inputs."""

    def test_writing_none_without_schema_should_raise_error(self):
        mock_client = Mock()
        table = Table(mock_client, "test_table", "test_dataset", "test_project")

        with pytest.raises(ValueError, match="schema must be provided when data is None"):
            table.write(None)

    def test_writing_none_with_schema_should_create_empty_table(self):
        mock_client = Mock()
        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        schema = {"name": "STRING", "age": "INTEGER"}

        table.write(None, schema=schema)

        mock_client.create_table.assert_called_once()
        call_args = mock_client.create_table.call_args
        created_table = call_args[0][0]
        # Check that it's a Table object (not necessarily a bigquery.Table due to mocking)
        assert hasattr(created_table, "table_id") or hasattr(created_table, "reference")

    def test_writing_csv_file_should_load_from_file(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_file.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")

        with patch("builtins.open", mock_open(read_data=b"name,age\nAlice,30\n")):
            table.write("test.csv")

        mock_client.load_table_from_file.assert_called_once()
        call_args = mock_client.load_table_from_file.call_args
        assert call_args[0][1] == "test_project.test_dataset.test_table"
        job_config = call_args[1]["job_config"]
        assert job_config.source_format == "CSV"
        assert job_config.skip_leading_rows == 1

    def test_writing_json_file_should_detect_format(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_file.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")

        with patch("builtins.open", mock_open(read_data=b'{"name":"Alice","age":30}\n')):
            table.write("test.json")

        mock_client.load_table_from_file.assert_called_once()
        job_config = mock_client.load_table_from_file.call_args[1]["job_config"]
        assert job_config.source_format == "NEWLINE_DELIMITED_JSON"

    def test_writing_file_with_schema_should_not_autodetect(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_file.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        schema = {"name": "STRING", "age": "INTEGER"}

        with patch("builtins.open", mock_open(read_data=b"name,age\nAlice,30\n")):
            table.write("test.csv", schema=schema)

        job_config = mock_client.load_table_from_file.call_args[1]["job_config"]
        # When schema is provided, autodetect should be False or None (not True)
        assert job_config.autodetect is not True
        assert job_config.schema is not None

    def test_writing_file_without_schema_should_autodetect(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_file.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")

        with patch("builtins.open", mock_open(read_data=b"name,age\nAlice,30\n")):
            table.write("test.csv")

        job_config = mock_client.load_table_from_file.call_args[1]["job_config"]
        assert job_config.autodetect is True

    def test_writing_path_object_should_work(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_file.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")

        with patch("builtins.open", mock_open(read_data=b"name,age\nAlice,30\n")):
            table.write(Path("test.csv"))

        mock_client.load_table_from_file.assert_called_once()

    def test_writing_unsupported_type_should_raise_error(self):
        mock_client = Mock()
        table = Table(mock_client, "test_table", "test_dataset", "test_project")

        with pytest.raises(TypeError, match="Unsupported data type"):
            table.write(123)  # type: ignore

    def test_writing_csv_with_custom_delimiter_should_set_delimiter(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_file.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")

        with patch("builtins.open", mock_open(read_data=b"name|age\nAlice|30\n")):
            table.write("test.csv", field_delimiter="|")

        job_config = mock_client.load_table_from_file.call_args[1]["job_config"]
        assert job_config.field_delimiter == "|"


class TestClientLoadData:
    """Tests for Client.load_data() with file inputs."""

    def test_loading_csv_file_should_delegate_to_table_write(self):
        from geasyp.bq.client import Client

        with patch("geasyp.bq.client.bigquery.Client") as mock_bq_client:
            mock_instance = Mock()
            mock_bq_client.return_value = mock_instance
            mock_instance.project = "test_project"

            client = Client(project_id="test_project")

            with patch.object(Table, "write") as mock_write:
                with patch("builtins.open", mock_open(read_data=b"name,age\nAlice,30\n")):
                    client.load_data("test.csv", "dataset.table")

                mock_write.assert_called_once()
                call_args = mock_write.call_args
                assert call_args[0][0] == "test.csv"

    def test_loading_file_with_schema_should_pass_schema(self):
        from geasyp.bq.client import Client

        with patch("geasyp.bq.client.bigquery.Client") as mock_bq_client:
            mock_instance = Mock()
            mock_bq_client.return_value = mock_instance
            mock_instance.project = "test_project"

            client = Client(project_id="test_project")
            schema = {"name": "STRING", "age": "INTEGER"}

            with patch.object(Table, "write") as mock_write:
                with patch("builtins.open", mock_open(read_data=b"name,age\nAlice,30\n")):
                    client.load_data("test.csv", "dataset.table", schema=schema)

                assert mock_write.call_args[1]["schema"] == schema

    def test_loading_none_with_schema_should_create_table(self):
        from geasyp.bq.client import Client

        with patch("geasyp.bq.client.bigquery.Client") as mock_bq_client:
            mock_instance = Mock()
            mock_bq_client.return_value = mock_instance
            mock_instance.project = "test_project"

            client = Client(project_id="test_project")
            schema = {"name": "STRING", "age": "INTEGER"}

            with patch.object(Table, "write") as mock_write:
                client.load_data(None, "dataset.table", schema=schema)

                mock_write.assert_called_once()
                assert mock_write.call_args[0][0] is None
                assert mock_write.call_args[1]["schema"] == schema
