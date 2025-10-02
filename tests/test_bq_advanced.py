"""Tests for BigQuery advanced operations."""

import pytest
from unittest.mock import Mock, MagicMock, patch, mock_open
from google.cloud import bigquery

from geasyp.bq.table import Table


class TestTableInsert:
    """Tests for Table.insert() streaming inserts."""

    def test_inserting_rows_should_call_insert_rows_json(self):
        mock_client = Mock()
        mock_client.insert_rows_json.return_value = []

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        rows = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

        errors = table.insert(rows)

        mock_client.insert_rows_json.assert_called_once_with(
            "test_project.test_dataset.test_table",
            rows,
            ignore_unknown_values=True,
            skip_invalid_rows=False,
        )
        assert errors == []

    def test_inserting_with_custom_options_should_pass_them(self):
        mock_client = Mock()
        mock_client.insert_rows_json.return_value = []

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        rows = [{"name": "Alice"}]

        table.insert(rows, ignore_unknown_values=False, skip_invalid_rows=True)

        call_args = mock_client.insert_rows_json.call_args
        assert call_args[1]["ignore_unknown_values"] is False
        assert call_args[1]["skip_invalid_rows"] is True

    def test_inserting_should_return_errors_list(self):
        mock_client = Mock()
        errors_list = [{"index": 0, "errors": [{"reason": "invalid"}]}]
        mock_client.insert_rows_json.return_value = errors_list

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        rows = [{"name": "Alice"}]

        errors = table.insert(rows)

        assert errors == errors_list


class TestTableToGCS:
    """Tests for Table.to_gcs() export."""

    def test_exporting_to_gcs_should_call_extract_table(self):
        mock_client = Mock()
        mock_job = Mock(spec=bigquery.ExtractJob)
        mock_client.extract_table.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        result = table.to_gcs("gs://bucket/path/*.csv")

        mock_client.extract_table.assert_called_once()
        call_args = mock_client.extract_table.call_args
        assert call_args[0][0] == "test_project.test_dataset.test_table"
        assert call_args[0][1] == "gs://bucket/path/*.csv"
        assert result == mock_job

    def test_exporting_should_use_default_csv_format(self):
        mock_client = Mock()
        mock_job = Mock(spec=bigquery.ExtractJob)
        mock_client.extract_table.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.to_gcs("gs://bucket/path/*.csv")

        job_config = mock_client.extract_table.call_args[1]["job_config"]
        assert job_config.destination_format == "CSV"
        assert job_config.compression == "GZIP"
        assert job_config.print_header is True
        assert job_config.field_delimiter == ","

    def test_exporting_with_custom_format_should_use_it(self):
        mock_client = Mock()
        mock_job = Mock(spec=bigquery.ExtractJob)
        mock_client.extract_table.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.to_gcs(
            "gs://bucket/path/*.json",
            export_format="NEWLINE_DELIMITED_JSON",
            compression="NONE",
        )

        job_config = mock_client.extract_table.call_args[1]["job_config"]
        assert job_config.destination_format == "NEWLINE_DELIMITED_JSON"
        assert job_config.compression == "NONE"

    def test_exporting_with_csv_options_should_set_them(self):
        mock_client = Mock()
        mock_job = Mock(spec=bigquery.ExtractJob)
        mock_client.extract_table.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.to_gcs(
            "gs://bucket/path/*.csv", print_header=False, field_delimiter="|"
        )

        job_config = mock_client.extract_table.call_args[1]["job_config"]
        assert job_config.print_header is False
        assert job_config.field_delimiter == "|"


class TestTableCopy:
    """Tests for Table.copy() table copying."""

    def test_copying_table_should_call_copy_table(self):
        mock_client = Mock()
        mock_job = Mock(spec=bigquery.CopyJob)
        mock_client.copy_table.return_value = mock_job

        source = Table(mock_client, "source_table", "test_dataset", "test_project")
        dest = Table(mock_client, "dest_table", "test_dataset", "test_project")

        result = source.copy(dest)

        mock_client.copy_table.assert_called_once()
        call_args = mock_client.copy_table.call_args
        assert call_args[0][0] == "test_project.test_dataset.source_table"
        assert call_args[0][1] == "test_project.test_dataset.dest_table"
        assert result == mock_job

    def test_copying_with_string_destination_should_work(self):
        mock_client = Mock()
        mock_job = Mock(spec=bigquery.CopyJob)
        mock_client.copy_table.return_value = mock_job

        source = Table(mock_client, "source_table", "test_dataset", "test_project")
        source.copy("other_project.other_dataset.dest_table")

        call_args = mock_client.copy_table.call_args
        assert call_args[0][1] == "other_project.other_dataset.dest_table"

    def test_copying_should_default_to_write_truncate(self):
        mock_client = Mock()
        mock_job = Mock(spec=bigquery.CopyJob)
        mock_client.copy_table.return_value = mock_job

        source = Table(mock_client, "source_table", "test_dataset", "test_project")
        dest = Table(mock_client, "dest_table", "test_dataset", "test_project")

        source.copy(dest)

        job_config = mock_client.copy_table.call_args[1]["job_config"]
        assert job_config.write_disposition == "WRITE_TRUNCATE"

    def test_copying_with_custom_disposition_should_use_it(self):
        mock_client = Mock()
        mock_job = Mock(spec=bigquery.CopyJob)
        mock_client.copy_table.return_value = mock_job

        source = Table(mock_client, "source_table", "test_dataset", "test_project")
        dest = Table(mock_client, "dest_table", "test_dataset", "test_project")

        source.copy(dest, write_disposition="WRITE_APPEND")

        job_config = mock_client.copy_table.call_args[1]["job_config"]
        assert job_config.write_disposition == "WRITE_APPEND"


class TestTableWriteGCS:
    """Tests for Table.write() with GCS URIs."""

    def test_writing_gcs_uri_should_call_load_table_from_uri(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_uri.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.write("gs://bucket/data.csv")

        mock_client.load_table_from_uri.assert_called_once()
        call_args = mock_client.load_table_from_uri.call_args
        assert call_args[0][0] == "gs://bucket/data.csv"
        assert call_args[0][1] == "test_project.test_dataset.test_table"

    def test_writing_gcs_csv_should_detect_format(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_uri.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.write("gs://bucket/data.csv")

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.source_format == "CSV"

    def test_writing_gcs_json_should_detect_format(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_uri.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.write("gs://bucket/data.json")

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.source_format == "NEWLINE_DELIMITED_JSON"

    def test_writing_gcs_parquet_should_detect_format(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_uri.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.write("gs://bucket/data.parquet")

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.source_format == "PARQUET"

    def test_writing_gcs_without_extension_should_require_format(self):
        mock_client = Mock()
        table = Table(mock_client, "test_table", "test_dataset", "test_project")

        with pytest.raises(ValueError, match="Cannot detect format from GCS URI"):
            table.write("gs://bucket/data")

    def test_writing_gcs_with_explicit_format_should_use_it(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_uri.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.write("gs://bucket/data", source_format="CSV")

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.source_format == "CSV"

    def test_writing_gcs_with_schema_should_not_autodetect(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_uri.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        schema = {"name": "STRING", "age": "INTEGER"}
        table.write("gs://bucket/data.csv", schema=schema)

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.autodetect is not True
        assert job_config.schema is not None

    def test_writing_gcs_without_schema_should_autodetect(self):
        mock_client = Mock()
        mock_job = Mock()
        mock_job.result = Mock()
        mock_client.load_table_from_uri.return_value = mock_job

        table = Table(mock_client, "test_table", "test_dataset", "test_project")
        table.write("gs://bucket/data.csv")

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.autodetect is True
