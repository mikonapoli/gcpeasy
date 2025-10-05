"""Tests for Secret Manager client core functionality (Slice 1)."""

import json
from unittest.mock import Mock

import pytest
from google.api_core.exceptions import NotFound
from google.cloud.secretmanager_v1.types import AccessSecretVersionResponse, SecretPayload

from gcpeasy import secretmanager as sm


@pytest.fixture
def mock_gcp_client():
    """Create a mock GCP Secret Manager client."""
    return Mock()


@pytest.fixture
def client(mock_gcp_client):
    """Create a Secret Manager client with mocked GCP client."""
    return sm.init(project_id="test-project", _gcp=mock_gcp_client)


def _mock_response(data: bytes) -> AccessSecretVersionResponse:
    """Create a mock AccessSecretVersionResponse."""
    return AccessSecretVersionResponse(payload=SecretPayload(data=data))


def test_init_uses_provided_project_id(mock_gcp_client):
    """Test init() should use explicitly provided project_id."""
    c = sm.init(project_id="custom-project", _gcp=mock_gcp_client)
    assert c.project_id == "custom-project"


def test_init_uses_project_number_when_no_id(mock_gcp_client):
    """Test init() should convert project_number to string when project_id not provided."""
    c = sm.init(project_number=123456, _gcp=mock_gcp_client)
    assert c.project_id == "123456"


def test_init_with_project_number_allows_get(mock_gcp_client):
    """Test client initialized with project_number should work with get()."""
    c = sm.init(project_number=123456, _gcp=mock_gcp_client)
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"secret-value")
    result = c.get("my-secret")
    assert result == "secret-value"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/123456/secrets/my-secret/versions/latest"
    )


def test_init_prefers_project_id_over_number(mock_gcp_client):
    """Test init() should prefer project_id when both provided."""
    c = sm.init(project_id="my-project", project_number=999, _gcp=mock_gcp_client)
    assert c.project_id == "my-project"


def test_init_defaults_to_adc_project(mock_gcp_client, monkeypatch):
    """Test init() should fall back to ADC project when neither id nor number provided."""
    mock_credentials = Mock()
    monkeypatch.setattr("google.auth.default", lambda: (mock_credentials, "test-project"))
    c = sm.init(_gcp=mock_gcp_client)
    assert c.project_id == "test-project"


def test_init_raises_when_adc_has_no_project(mock_gcp_client, monkeypatch):
    """Test init() should raise helpful error when ADC provides no project."""
    mock_credentials = Mock()
    monkeypatch.setattr("google.auth.default", lambda: (mock_credentials, None))
    with pytest.raises(ValueError, match="Could not determine project ID"):
        sm.init(_gcp=mock_gcp_client)


def test_call_retrieves_string_with_default_version(client, mock_gcp_client):
    """Test __call__() should retrieve latest version as UTF-8 string by default."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"my-value")
    result = client("test-secret")
    assert result == "my-value"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/test-secret/versions/latest"
    )


def test_call_supports_default_none(client, mock_gcp_client):
    """Test __call__() should support default=None explicitly."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("not there")
    result = client("missing", default=None)
    assert result is None


def test_get_retrieves_string_with_default_version(client, mock_gcp_client):
    """Test get() should retrieve latest version as UTF-8 string by default."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"secret-data")
    result = client.get("my-secret")
    assert result == "secret-data"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/my-secret/versions/latest"
    )


def test_get_accepts_digit_prefixed_secret_id(client, mock_gcp_client):
    """Test get() should accept secret IDs starting with digits."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"1st-value")
    result = client.get("1st-secret")
    assert result == "1st-value"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/1st-secret/versions/latest"
    )


def test_get_with_explicit_version_as_int(client, mock_gcp_client):
    """Test get() should accept integer version and convert to string."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"v3-data")
    result = client.get("my-secret", version=3)
    assert result == "v3-data"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/my-secret/versions/3"
    )


def test_get_with_explicit_version_as_string(client, mock_gcp_client):
    """Test get() should accept string version."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"v5-data")
    result = client.get("my-secret", version="5")
    assert result == "v5-data"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/my-secret/versions/5"
    )


def test_get_with_latest_enabled_keyword(client, mock_gcp_client):
    """Test get() should accept 'latest:enabled' version keyword."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"enabled-data")
    result = client.get("my-secret", version="latest:enabled")
    assert result == "enabled-data"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/my-secret/versions/latest:enabled"
    )


def test_get_with_version_zero_should_raise(client):
    """Test get() should reject version=0 instead of silently using default."""
    with pytest.raises(ValueError, match="must be positive"):
        client.get("my-secret", version=0)


def test_get_as_json_parses_payload(client, mock_gcp_client):
    """Test get() should parse JSON when as_json=True."""
    payload = {"key": "value", "nested": {"num": 42}}
    mock_gcp_client.access_secret_version.return_value = _mock_response(
        json.dumps(payload).encode()
    )
    result = client.get("json-secret", as_json=True)
    assert result == payload


def test_get_as_bytes_returns_raw_payload(client, mock_gcp_client):
    """Test get() should return raw bytes when as_bytes=True."""
    raw = b"\x00\x01\x02\xff"
    mock_gcp_client.access_secret_version.return_value = _mock_response(raw)
    result = client.get("binary-secret", as_bytes=True)
    assert result == raw


def test_get_with_default_returns_default_on_not_found(client, mock_gcp_client):
    """Test get() should return default value when secret not found and default provided."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("not there")
    result = client.get("missing", default="fallback")
    assert result == "fallback"


def test_get_with_default_none_returns_none_on_not_found(client, mock_gcp_client):
    """Test get() should return None when default=None explicitly provided."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("not there")
    result = client.get("missing", default=None)
    assert result is None


def test_get_raises_not_found_when_no_default(client, mock_gcp_client):
    """Test get() should raise NotFound when secret missing and no default provided."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("nope")
    with pytest.raises(NotFound):
        client.get("missing")


def test_get_raises_on_as_json_and_as_bytes(client):
    """Test get() should reject both as_json and as_bytes flags."""
    with pytest.raises(ValueError, match="Cannot specify both"):
        client.get("secret", as_json=True, as_bytes=True)


def test_get_with_simple_project_slash_secret(client, mock_gcp_client):
    """Test get() should parse 'project/secret' identifier."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"cross-project")
    result = client.get("other-project/other-secret")
    assert result == "cross-project"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/other-project/secrets/other-secret/versions/latest"
    )


def test_get_with_full_resource_path(client, mock_gcp_client):
    """Test get() should parse full resource path."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"from-path")
    result = client.get("projects/proj/secrets/sec/versions/7")
    assert result == "from-path"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/proj/secrets/sec/versions/7"
    )


def test_get_path_fetches_full_resource(client, mock_gcp_client):
    """Test get_path() should fetch fully qualified path without inference."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"path-value")
    result = client.get_path("projects/acme/secrets/foo/versions/latest")
    assert result == "path-value"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/acme/secrets/foo/versions/latest"
    )


def test_get_path_requires_full_path(client):
    """Test get_path() should reject non-fully-qualified paths."""
    with pytest.raises(ValueError, match="fully qualified"):
        client.get_path("my-secret")


def test_get_path_supports_as_json(client, mock_gcp_client):
    """Test get_path() should support as_json flag."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b'{"x":1}')
    result = client.get_path("projects/p/secrets/s/versions/1", as_json=True)
    assert result == {"x": 1}


def test_get_path_supports_as_bytes(client, mock_gcp_client):
    """Test get_path() should support as_bytes flag."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"\xde\xad")
    result = client.get_path("projects/p/secrets/s/versions/1", as_bytes=True)
    assert result == b"\xde\xad"


def test_get_path_supports_default(client, mock_gcp_client):
    """Test get_path() should support default fallback."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("gone")
    result = client.get_path("projects/p/secrets/s/versions/1", default="nope")
    assert result == "nope"


def test_get_path_supports_default_none(client, mock_gcp_client):
    """Test get_path() should support default=None explicitly."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("gone")
    result = client.get_path("projects/p/secrets/s/versions/1", default=None)
    assert result is None


def test_get_path_rejects_both_as_json_and_as_bytes(client):
    """Test get_path() should reject conflicting format flags."""
    with pytest.raises(ValueError, match="Cannot specify both"):
        client.get_path("projects/p/secrets/s/versions/1", as_json=True, as_bytes=True)


def test_validate_secret_id_accepts_valid_ids():
    """Test _validate_secret_id() should accept valid identifiers."""
    from gcpeasy.secretmanager.client import _validate_secret_id

    _validate_secret_id("my-secret")
    _validate_secret_id("Secret_123-foo")
    _validate_secret_id("1st-secret")  # Digit prefix allowed
    _validate_secret_id("123")  # All digits allowed
    _validate_secret_id("9-my-secret")  # Digit prefix allowed


def test_validate_secret_id_rejects_invalid_ids():
    """Test _validate_secret_id() should reject invalid identifiers."""
    from gcpeasy.secretmanager.client import _validate_secret_id

    with pytest.raises(ValueError, match="Invalid secret ID"):
        _validate_secret_id("has.dots")
    with pytest.raises(ValueError, match="Invalid secret ID"):
        _validate_secret_id("-starts-with-dash")
    with pytest.raises(ValueError, match="Invalid secret ID"):
        _validate_secret_id("_starts-with-underscore")  # Underscore prefix not allowed per API


def test_validate_project_id_accepts_valid_ids():
    """Test _validate_project_id() should accept valid project IDs."""
    from gcpeasy.secretmanager.client import _validate_project_id

    _validate_project_id("my-project")
    _validate_project_id("test123")
    _validate_project_id("a1-b2-c3")


def test_validate_project_id_accepts_numeric_ids():
    """Test _validate_project_id() should accept numeric project IDs."""
    from gcpeasy.secretmanager.client import _validate_project_id

    _validate_project_id("123456")
    _validate_project_id("999999999999")


def test_validate_project_id_rejects_invalid_ids():
    """Test _validate_project_id() should reject invalid project IDs."""
    from gcpeasy.secretmanager.client import _validate_project_id

    with pytest.raises(ValueError, match="Invalid project ID"):
        _validate_project_id("UPPERCASE")
    with pytest.raises(ValueError, match="Invalid project ID"):
        _validate_project_id("ends-with-dash-")
    with pytest.raises(ValueError, match="Invalid project ID"):
        _validate_project_id("123starts-digit")


def test_validate_version_converts_int():
    """Test _validate_version() should convert positive integers to strings."""
    from gcpeasy.secretmanager.client import _validate_version

    assert _validate_version(1) == "1"
    assert _validate_version(999) == "999"


def test_validate_version_accepts_keywords():
    """Test _validate_version() should accept version keywords."""
    from gcpeasy.secretmanager.client import _validate_version

    assert _validate_version("latest") == "latest"
    assert _validate_version("latest:enabled") == "latest:enabled"


def test_validate_version_accepts_numeric_strings():
    """Test _validate_version() should accept numeric strings."""
    from gcpeasy.secretmanager.client import _validate_version

    assert _validate_version("42") == "42"


def test_validate_version_rejects_zero_and_negative():
    """Test _validate_version() should reject non-positive versions."""
    from gcpeasy.secretmanager.client import _validate_version

    with pytest.raises(ValueError, match="must be positive"):
        _validate_version(0)
    with pytest.raises(ValueError, match="must be positive"):
        _validate_version(-5)
    with pytest.raises(ValueError, match="must be positive"):
        _validate_version("0")


def test_validate_version_rejects_invalid_strings():
    """Test _validate_version() should reject invalid string versions."""
    from gcpeasy.secretmanager.client import _validate_version

    with pytest.raises(ValueError, match="Invalid version"):
        _validate_version("not-a-version")


def test_client_exposes_gcp_client(client, mock_gcp_client):
    """Test Client should expose underlying GCP client via _gcp."""
    assert client._gcp is mock_gcp_client


def test_client_stores_default_encoding():
    """Test Client should store custom default_encoding."""
    c = sm.Client(project_id="p", default_encoding="ascii", _gcp=Mock())
    assert c.default_encoding == "ascii"


def test_client_stores_default_version():
    """Test Client should store custom default_version."""
    c = sm.Client(project_id="p", default_version="latest:enabled", _gcp=Mock())
    assert c.default_version == "latest:enabled"


def test_get_uses_custom_encoding(mock_gcp_client):
    """Test get() should use client's default_encoding for decoding."""
    c = sm.Client(project_id="proj", default_encoding="ascii", _gcp=mock_gcp_client)
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"test")
    result = c.get("secret")
    assert result == "test"


def test_malformed_json_raises_value_error(client, mock_gcp_client):
    """Test get() should raise ValueError when as_json=True and payload is not valid JSON."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"not json")
    with pytest.raises(ValueError):
        client.get("bad-json", as_json=True)


def test_get_bytes_returns_raw_payload(client, mock_gcp_client):
    """Test get_bytes() should return raw bytes."""
    raw = b"\xde\xad\xbe\xef"
    mock_gcp_client.access_secret_version.return_value = _mock_response(raw)
    result = client.get_bytes("binary-secret")
    assert result == raw


def test_get_bytes_with_version(client, mock_gcp_client):
    """Test get_bytes() should accept version parameter."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"v2-data")
    result = client.get_bytes("secret", version=2)
    assert result == b"v2-data"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/secret/versions/2"
    )


def test_get_bytes_with_default(client, mock_gcp_client):
    """Test get_bytes() should support default fallback."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("gone")
    result = client.get_bytes("missing", default=b"fallback")
    assert result == b"fallback"


def test_get_json_parses_payload(client, mock_gcp_client):
    """Test get_json() should parse JSON payload."""
    payload = {"key": "value", "count": 42}
    mock_gcp_client.access_secret_version.return_value = _mock_response(
        json.dumps(payload).encode()
    )
    result = client.get_json("json-secret")
    assert result == payload


def test_get_json_with_version(client, mock_gcp_client):
    """Test get_json() should accept version parameter."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b'{"v": 5}')
    result = client.get_json("secret", version=5)
    assert result == {"v": 5}


def test_get_json_with_default(client, mock_gcp_client):
    """Test get_json() should support default fallback."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("gone")
    result = client.get_json("missing", default={"fallback": True})
    assert result == {"fallback": True}


def test_get_dict_parses_env_format(client, mock_gcp_client):
    """Test get_dict() should parse KEY=VALUE format by default."""
    env_data = b"DB_HOST=localhost\nDB_PORT=5432\nDB_NAME=mydb"
    mock_gcp_client.access_secret_version.return_value = _mock_response(env_data)
    result = client.get_dict("env-file")
    assert result == {"DB_HOST": "localhost", "DB_PORT": "5432", "DB_NAME": "mydb"}


def test_get_dict_strips_whitespace_by_default(client, mock_gcp_client):
    """Test get_dict() should strip keys and values by default."""
    env_data = b"  KEY1  =  value1  \n  KEY2=value2  "
    mock_gcp_client.access_secret_version.return_value = _mock_response(env_data)
    result = client.get_dict("env-file")
    assert result == {"KEY1": "value1", "KEY2": "value2"}


def test_get_dict_with_strip_keys_false(client, mock_gcp_client):
    """Test get_dict() should preserve key whitespace when strip_keys=False."""
    env_data = b"  KEY  =value"
    mock_gcp_client.access_secret_version.return_value = _mock_response(env_data)
    result = client.get_dict("env-file", strip_keys=False)
    assert result == {"  KEY  ": "value"}


def test_get_dict_with_strip_values_false(client, mock_gcp_client):
    """Test get_dict() should preserve value whitespace when strip_values=False."""
    env_data = b"KEY=  value  "
    mock_gcp_client.access_secret_version.return_value = _mock_response(env_data)
    result = client.get_dict("env-file", strip_values=False)
    assert result == {"KEY": "  value  "}


def test_get_dict_with_uppercase_keys(client, mock_gcp_client):
    """Test get_dict() should uppercase keys when uppercase_keys=True."""
    env_data = b"db_host=localhost\napi_key=secret"
    mock_gcp_client.access_secret_version.return_value = _mock_response(env_data)
    result = client.get_dict("env-file", uppercase_keys=True)
    assert result == {"DB_HOST": "localhost", "API_KEY": "secret"}


def test_get_dict_with_custom_separators(client, mock_gcp_client):
    """Test get_dict() should accept custom line and key separators."""
    data = b"KEY1:value1;KEY2:value2"
    mock_gcp_client.access_secret_version.return_value = _mock_response(data)
    result = client.get_dict("custom-file", line_separator=";", key_separator=":")
    assert result == {"KEY1": "value1", "KEY2": "value2"}


def test_get_dict_skips_empty_lines(client, mock_gcp_client):
    """Test get_dict() should skip empty lines."""
    env_data = b"KEY1=value1\n\nKEY2=value2\n"
    mock_gcp_client.access_secret_version.return_value = _mock_response(env_data)
    result = client.get_dict("env-file")
    assert result == {"KEY1": "value1", "KEY2": "value2"}


def test_get_dict_skips_lines_without_separator(client, mock_gcp_client):
    """Test get_dict() should skip lines without key separator."""
    env_data = b"KEY1=value1\ncomment line\nKEY2=value2"
    mock_gcp_client.access_secret_version.return_value = _mock_response(env_data)
    result = client.get_dict("env-file")
    assert result == {"KEY1": "value1", "KEY2": "value2"}


def test_get_dict_with_version(client, mock_gcp_client):
    """Test get_dict() should accept version parameter."""
    env_data = b"KEY=value"
    mock_gcp_client.access_secret_version.return_value = _mock_response(env_data)
    result = client.get_dict("env-file", version=3)
    assert result == {"KEY": "value"}
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/env-file/versions/3"
    )


def test_get_dict_with_default(client, mock_gcp_client):
    """Test get_dict() should support default fallback."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("gone")
    result = client.get_dict("missing", default={"fallback": "value"})
    assert result == {"fallback": "value"}


def test_get_dict_handles_equals_in_value(client, mock_gcp_client):
    """Test get_dict() should handle = signs in values."""
    env_data = b"JWT=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload=data"
    mock_gcp_client.access_secret_version.return_value = _mock_response(env_data)
    result = client.get_dict("env-file")
    assert result == {"JWT": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload=data"}
