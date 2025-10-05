"""Security tests for BigQuery module to prevent SQL injection and other attacks."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
from gcpeasy.bq import init


class TestSQLInjectionPrevention:
    """Test that SQL injection attempts are prevented through public API."""

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_backtick_in_id_should_fail(self, mock_bq_client):
        """Test that backtick injection in table_id is blocked."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        with pytest.raises(ValueError, match="Invalid table_id"):
            dataset.table("users` WHERE 1=1; DROP TABLE important; --")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_semicolon_in_id_should_fail(self, mock_bq_client):
        """Test that semicolon injection in table_id is blocked."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        with pytest.raises(ValueError, match="Invalid table_id"):
            dataset.table("users; DELETE FROM secrets")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_comment_syntax_in_id_should_fail(self, mock_bq_client):
        """Test that SQL comment injection in table_id is blocked."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        with pytest.raises(ValueError, match="Invalid table_id"):
            dataset.table("users-- comment")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_dataset_with_union_injection_should_fail(self, mock_bq_client):
        """Test that UNION injection via dataset_id is blocked."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        with pytest.raises(ValueError, match="Invalid dataset_id"):
            client.dataset("dataset` UNION SELECT * FROM secrets WHERE `x")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_dataset_with_drop_table_injection_should_fail(self, mock_bq_client):
        """Test that DROP TABLE injection via dataset_id is blocked."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        with pytest.raises(ValueError, match="Invalid dataset_id"):
            client.dataset("dataset'; DROP TABLE users; --")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_reading_with_sql_injection_in_max_results_should_fail(self, mock_bq_client):
        """Test that SQL injection via max_results is blocked."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")

        with pytest.raises(ValueError, match="max_results must be an integer"):
            table.read(max_results="100; DROP TABLE users")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_reading_with_union_in_max_results_should_fail(self, mock_bq_client):
        """Test that UNION injection via max_results is blocked."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")

        with pytest.raises(ValueError, match="max_results must be an integer"):
            table.read(max_results="100 OR 1=1")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_reading_should_use_parameterized_query_for_limit(self, mock_bq_client):
        """Test that table.read() uses parameterized queries, not string interpolation."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        table.read(max_results=100)

        call_args = mock_bq_client.return_value.query.call_args
        query = call_args[0][0]

        assert "@max_results" in query

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_reading_should_not_interpolate_limit_value_in_query(self, mock_bq_client):
        """Test that LIMIT value is not directly in query string."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        table.read(max_results=100)

        call_args = mock_bq_client.return_value.query.call_args
        query = call_args[0][0]

        assert "LIMIT 100" not in query

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_reading_should_pass_params_dict_to_query(self, mock_bq_client):
        """Test that query parameters are passed as a dict, not interpolated."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        table.read(max_results=100)

        call_args = mock_bq_client.return_value.query.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.query_parameters[0].name == "max_results"
        assert job_config.query_parameters[0].value == 100


class TestIdentifierValidation:
    """Test that invalid identifiers are rejected through public API."""

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_empty_id_should_fail(self, mock_bq_client):
        """Test that empty table_id is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        with pytest.raises(ValueError, match="cannot be empty"):
            dataset.table("")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_oversized_id_should_fail(self, mock_bq_client):
        """Test that table_id over 1024 characters is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        long_id = "a" * 1025
        with pytest.raises(ValueError, match="cannot exceed 1024 characters"):
            dataset.table(long_id)

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_space_in_id_should_fail(self, mock_bq_client):
        """Test that table_id with spaces is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        with pytest.raises(ValueError, match="Invalid table_id"):
            dataset.table("my table")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_dot_in_id_should_fail(self, mock_bq_client):
        """Test that table_id with dots is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        with pytest.raises(ValueError, match="Invalid table_id"):
            dataset.table("my.table")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_starting_with_number_should_fail(self, mock_bq_client):
        """Test that table_id starting with number is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        with pytest.raises(ValueError, match="Invalid table_id"):
            dataset.table("123table")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_hyphen_in_id_should_fail(self, mock_bq_client):
        """Test that table_id with hyphens is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        with pytest.raises(ValueError, match="Invalid table_id"):
            dataset.table("my-table")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_dataset_with_special_chars_should_fail(self, mock_bq_client):
        """Test that dataset_id with special characters is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        with pytest.raises(ValueError, match="Invalid dataset_id"):
            client.dataset("dataset!")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_dataset_with_at_sign_should_fail(self, mock_bq_client):
        """Test that dataset_id with @ sign is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        with pytest.raises(ValueError, match="Invalid dataset_id"):
            client.dataset("dataset@home")


class TestUnicodeAndEncodingAttacks:
    """Test that Unicode and encoding-based attacks are blocked."""

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_dataset_with_null_byte_should_fail(self, mock_bq_client):
        """Test that dataset_id with null byte is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        with pytest.raises(ValueError):
            client.dataset("dataset\u0000injection")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_dataset_with_unicode_apostrophe_should_fail(self, mock_bq_client):
        """Test that dataset_id with Unicode apostrophe is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        with pytest.raises(ValueError):
            client.dataset("dataset\u2019injection")

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_dataset_with_url_encoded_backtick_should_fail(self, mock_bq_client):
        """Test that dataset_id with URL-encoded backtick is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()

        with pytest.raises(ValueError):
            client.dataset("dataset%60injection")


class TestValidIdentifiers:
    """Test that valid identifiers are accepted."""

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_valid_id_should_succeed(self, mock_bq_client):
        """Test that valid table_id is accepted."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        table = client.dataset("my_dataset").table("my_table")

        assert table.id == "test-project.my_dataset.my_table"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_underscore_prefix_should_succeed(self, mock_bq_client):
        """Test that table_id starting with underscore is accepted."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        table = client.dataset("my_dataset").table("_private")

        assert table.id == "test-project.my_dataset._private"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_table_with_numbers_in_id_should_succeed(self, mock_bq_client):
        """Test that table_id with numbers (not at start) is accepted."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        table = client.dataset("my_dataset").table("table_123")

        assert table.id == "test-project.my_dataset.table_123"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_dataset_with_valid_id_should_succeed(self, mock_bq_client):
        """Test that valid dataset_id is accepted."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        assert dataset.id == "test-project.my_dataset"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_project_with_hyphens_should_succeed(self, mock_bq_client):
        """Test that project_id with hyphens is accepted."""
        mock_bq_client.return_value.project = "my-test-project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        assert dataset.id == "my-test-project.my_dataset"

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_creating_project_with_underscores_should_succeed(self, mock_bq_client):
        """Test that project_id with underscores is accepted."""
        mock_bq_client.return_value.project = "my_test_project"
        mock_bq_client.return_value.location = "EU"

        client = init()
        dataset = client.dataset("my_dataset")

        assert dataset.id == "my_test_project.my_dataset"


class TestMaxResultsTypeHandling:
    """Test that max_results parameter handles different types correctly."""

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_reading_with_integer_max_results_should_succeed(self, mock_bq_client):
        """Test that integer max_results is accepted."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        table.read(max_results=100)

        call_args = mock_bq_client.return_value.query.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.query_parameters[0].value == 100

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_reading_with_float_max_results_should_convert_to_int(self, mock_bq_client):
        """Test that float max_results is converted to integer."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")
        table.read(max_results=100.5)

        call_args = mock_bq_client.return_value.query.call_args
        job_config = call_args[1]["job_config"]
        assert job_config.query_parameters[0].value == 100

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_reading_with_dict_max_results_should_fail(self, mock_bq_client):
        """Test that dict max_results is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")

        with pytest.raises(ValueError, match="max_results must be an integer"):
            table.read(max_results={"limit": 100})

    @patch("gcpeasy.bq.client.bigquery.Client")
    def test_reading_with_list_max_results_should_fail(self, mock_bq_client):
        """Test that list max_results is rejected."""
        mock_bq_client.return_value.project = "test-project"
        mock_bq_client.return_value.location = "EU"

        mock_job = Mock()
        mock_df = pd.DataFrame({"col1": [1]})
        mock_job.to_dataframe.return_value = mock_df
        mock_bq_client.return_value.query.return_value = mock_job

        client = init()
        table = client.dataset("my_dataset").table("my_table")

        with pytest.raises(ValueError, match="max_results must be an integer"):
            table.read(max_results=[100])
