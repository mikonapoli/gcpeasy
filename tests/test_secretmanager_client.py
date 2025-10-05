"""Tests for Secret Manager client core functionality (Slice 1)."""

import json
from collections import UserDict
from types import MappingProxyType
from unittest.mock import Mock

import pytest
from google.api_core.exceptions import NotFound
from google.cloud.secretmanager_v1.types import AccessSecretVersionResponse, SecretPayload

from gcpeasy import secretmanager as sm


@pytest.fixture
def mock_gcp_client(monkeypatch):
    """Create a mock GCP Secret Manager client and patch the SDK factory."""
    client = Mock()
    monkeypatch.setattr(
        "google.cloud.secretmanager.SecretManagerServiceClient",
        lambda: client,
    )
    return client


@pytest.fixture
def client(mock_gcp_client):
    """Create a Secret Manager client with mocked GCP client."""
    return sm.init(project_id="test-project")


def _mock_response(data: bytes) -> AccessSecretVersionResponse:
    """Create a mock AccessSecretVersionResponse."""
    return AccessSecretVersionResponse(payload=SecretPayload(data=data))


def test_init_uses_provided_project_id(mock_gcp_client):
    """Test init() should use explicitly provided project_id."""
    c = sm.init(project_id="custom-project")
    assert c.project_id == "custom-project"


def test_init_uses_project_number_when_no_id(mock_gcp_client):
    """Test init() should convert project_number to string when project_id not provided."""
    c = sm.init(project_number=123456)
    assert c.project_id == "123456"


def test_init_with_project_number_allows_get(mock_gcp_client):
    """Test client initialized with project_number should work with get()."""
    c = sm.init(project_number=123456)
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"secret-value")
    result = c.get("my-secret")
    assert result == "secret-value"


def test_init_prefers_project_id_over_number(mock_gcp_client):
    """Test init() should prefer project_id when both provided."""
    c = sm.init(project_id="my-project", project_number=999)
    assert c.project_id == "my-project"


def test_init_defaults_to_adc_project(mock_gcp_client, monkeypatch):
    """Test init() should fall back to ADC project when neither id nor number provided."""
    mock_credentials = Mock()
    monkeypatch.setattr("google.auth.default", lambda: (mock_credentials, "test-project"))
    c = sm.init()
    assert c.project_id == "test-project"


def test_init_raises_when_adc_has_no_project(mock_gcp_client, monkeypatch):
    """Test init() should raise helpful error when ADC provides no project."""
    mock_credentials = Mock()
    monkeypatch.setattr("google.auth.default", lambda: (mock_credentials, None))
    with pytest.raises(ValueError, match="Could not determine project ID"):
        sm.init()


def test_call_retrieves_string_with_default_version(client, mock_gcp_client):
    """Test __call__() should retrieve latest version as UTF-8 string by default."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"my-value")
    result = client("test-secret")
    assert result == "my-value"


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


def test_get_accepts_digit_prefixed_secret_id(client, mock_gcp_client):
    """Test get() should accept secret IDs starting with digits."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"1st-value")
    result = client.get("1st-secret")
    assert result == "1st-value"


def test_get_with_explicit_version_as_int(client, mock_gcp_client):
    """Test get() should accept integer version and convert to string."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"v3-data")
    result = client.get("my-secret", version=3)
    assert result == "v3-data"


def test_get_with_explicit_version_as_string(client, mock_gcp_client):
    """Test get() should accept string version."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"v5-data")
    result = client.get("my-secret", version="5")
    assert result == "v5-data"


def test_get_with_latest_enabled_keyword(client, mock_gcp_client):
    """Test get() should accept 'latest:enabled' version keyword."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"enabled-data")
    result = client.get("my-secret", version="latest:enabled")
    assert result == "enabled-data"


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


def test_client_stores_default_encoding(mock_gcp_client):
    """Test Client should store custom default_encoding."""
    c = sm.Client(project_id="p", default_encoding="ascii")
    assert c.default_encoding == "ascii"


def test_client_stores_default_version(mock_gcp_client):
    """Test Client should store custom default_version."""
    c = sm.Client(project_id="p", default_version="latest:enabled")
    assert c.default_version == "latest:enabled"


def test_get_uses_custom_encoding(mock_gcp_client):
    """Test get() should use client's default_encoding for decoding."""
    c = sm.Client(project_id="proj", default_encoding="ascii")
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


def test_get_many_with_list_input(client, mock_gcp_client):
    """Test get_many() should accept list of secret IDs."""
    mock_gcp_client.access_secret_version.side_effect = [
        _mock_response(b"value1"),
        _mock_response(b"value2"),
        _mock_response(b"value3"),
    ]
    result = client.get_many(["secret1", "secret2", "secret3"])
    assert result == {"secret1": "value1", "secret2": "value2", "secret3": "value3"}
    assert mock_gcp_client.access_secret_version.call_count == 3


def test_get_many_with_dict_string_values(client, mock_gcp_client):
    """Test get_many() should accept dict with string values for aliasing."""
    mock_gcp_client.access_secret_version.side_effect = [
        _mock_response(b"db-value"),
        _mock_response(b"api-value"),
    ]
    result = client.get_many({"database": "db-secret", "api_key": "api-secret"})
    assert result == {"database": "db-value", "api_key": "api-value"}


def test_get_many_with_tuple_and_options(client, mock_gcp_client):
    """Test get_many() should accept tuple format (secret_id, options)."""
    mock_gcp_client.access_secret_version.side_effect = [
        _mock_response(b"found"),
        NotFound("missing"),
    ]
    result = client.get_many({
        "secret1": ("actual-secret", {}),
        "secret2": ("missing-secret", {"default": "fallback"}),
    })
    assert result == {"secret1": "found", "secret2": "fallback"}


def test_get_many_with_dict_spec(client, mock_gcp_client):
    """Test get_many() should accept dict spec with 'secret' key."""
    mock_gcp_client.access_secret_version.side_effect = [
        _mock_response(b'{"key": "value"}'),
        _mock_response(b"text"),
    ]
    result = client.get_many({
        "config": {"secret": "app-config", "as_json": True},
        "password": {"secret": "db-password"},
    })
    assert result == {"config": {"key": "value"}, "password": "text"}


def test_get_many_with_mixed_formats(client, mock_gcp_client):
    """Test get_many() should handle mixed input formats."""
    mock_gcp_client.access_secret_version.side_effect = [
        _mock_response(b"simple"),
        _mock_response(b"tuple-val"),
        _mock_response(b'{"a": 1}'),
    ]
    result = client.get_many({
        "alias1": "secret1",
        "alias2": ("secret2", {}),
        "alias3": {"secret": "secret3", "as_json": True},
    })
    assert result == {
        "alias1": "simple",
        "alias2": "tuple-val",
        "alias3": {"a": 1},
    }


def test_get_many_with_version_option(client, mock_gcp_client):
    """Test get_many() should pass through version option."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"v5-data")
    result = client.get_many({"alias": {"secret": "my-secret", "version": 5}})
    assert result == {"alias": "v5-data"}
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/my-secret/versions/5"
    )


def test_get_many_propagates_errors_without_default(client, mock_gcp_client):
    """Test get_many() should propagate NotFound when no default provided."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("gone")
    with pytest.raises(NotFound):
        client.get_many(["missing-secret"])


def test_get_many_returns_default_on_error(client, mock_gcp_client):
    """Test get_many() should return default when NotFound and default provided."""
    mock_gcp_client.access_secret_version.side_effect = NotFound("gone")
    result = client.get_many({"alias": ("missing", {"default": "fallback"})})
    assert result == {"alias": "fallback"}


def test_get_many_raises_on_invalid_tuple_length(client):
    """Test get_many() should reject tuples with wrong number of elements."""
    with pytest.raises(ValueError, match="Tuple spec must be"):
        client.get_many({"alias": ("only-one-element",)})
    with pytest.raises(ValueError, match="Tuple spec must be"):
        client.get_many({"alias": ("one", "two", "three")})


def test_get_many_raises_on_dict_without_secret_key(client):
    """Test get_many() should reject dict spec without 'secret' key."""
    with pytest.raises(ValueError, match="must contain 'secret' key"):
        client.get_many({"alias": {"version": 3, "as_json": True}})


def test_get_many_raises_on_non_string_secret_id_in_list(client):
    """Test get_many() should reject non-string secret IDs in list."""
    with pytest.raises(TypeError, match="Secret ID must be string"):
        client.get_many([123, "valid-secret"])


def test_get_many_raises_on_non_string_secret_id_in_tuple(client):
    """Test get_many() should reject non-string secret IDs in tuple."""
    with pytest.raises(TypeError, match="Secret ID must be string"):
        client.get_many({"alias": (123, {})})


def test_get_many_raises_on_non_string_secret_in_dict(client):
    """Test get_many() should reject non-string 'secret' in dict spec."""
    with pytest.raises(TypeError, match="Secret ID must be string"):
        client.get_many({"alias": {"secret": 123}})


def test_get_many_raises_on_non_dict_options_in_tuple(client):
    """Test get_many() should reject non-dict options in tuple."""
    with pytest.raises(TypeError, match="Options must be dict"):
        client.get_many({"alias": ("secret", "not-a-dict")})


def test_get_many_raises_on_invalid_spec_type(client):
    """Test get_many() should reject invalid spec types."""
    with pytest.raises(TypeError, match="Invalid spec type"):
        client.get_many({"alias": 123})
    with pytest.raises(TypeError, match="Invalid spec type"):
        client.get_many({"alias": ["list", "not", "allowed"]})


def test_get_many_accepts_userdict(client, mock_gcp_client):
    """Test get_many() should handle UserDict as mapping."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"value1")

    class MyUserDict(UserDict):
        pass

    secrets = MyUserDict({"alias": "secret-id"})
    result = client.get_many(secrets)

    assert result == {"alias": "value1"}
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/secret-id/versions/latest"
    )


def test_get_many_accepts_mappingproxytype(client, mock_gcp_client):
    """Test get_many() should handle MappingProxyType as mapping."""
    mock_gcp_client.access_secret_version.return_value = _mock_response(b"value1")

    secrets = MappingProxyType({"alias": "secret-id"})
    result = client.get_many(secrets)

    assert result == {"alias": "value1"}
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/secret-id/versions/latest"
    )


# Slice 4: Listing & Metadata


def test_secrets_returns_secret_ids(client, mock_gcp_client):
    """Test secrets() should return simple secret IDs by default."""
    mock_secret1 = Mock()
    mock_secret1.name = "projects/test-project/secrets/secret1"
    mock_secret2 = Mock()
    mock_secret2.name = "projects/test-project/secrets/secret2"
    mock_gcp_client.list_secrets.return_value = [mock_secret1, mock_secret2]

    result = client.secrets()

    assert result == ["secret1", "secret2"]
    mock_gcp_client.list_secrets.assert_called_once_with(request={"parent": "projects/test-project"})


def test_secrets_with_filter(client, mock_gcp_client):
    """Test secrets() should pass filter to API."""
    mock_secret = Mock()
    mock_secret.name = "projects/test-project/secrets/prod-secret"
    mock_gcp_client.list_secrets.return_value = [mock_secret]

    result = client.secrets(filter="labels.env=prod")

    assert result == ["prod-secret"]
    mock_gcp_client.list_secrets.assert_called_once_with(
        request={"parent": "projects/test-project", "filter": "labels.env=prod"}
    )


def test_secrets_with_max_results(client, mock_gcp_client):
    """Test secrets() should pass max_results as page_size to API."""
    mock_secret = Mock()
    mock_secret.name = "projects/test-project/secrets/secret1"
    mock_gcp_client.list_secrets.return_value = [mock_secret]

    result = client.secrets(max_results=10)

    assert result == ["secret1"]
    mock_gcp_client.list_secrets.assert_called_once_with(
        request={"parent": "projects/test-project", "page_size": 10}
    )


def test_secrets_with_fully_qualified(client, mock_gcp_client):
    """Test secrets() should return full paths when fully_qualified=True."""
    mock_secret1 = Mock()
    mock_secret1.name = "projects/test-project/secrets/secret1"
    mock_secret2 = Mock()
    mock_secret2.name = "projects/test-project/secrets/secret2"
    mock_gcp_client.list_secrets.return_value = [mock_secret1, mock_secret2]

    result = client.secrets(fully_qualified=True)

    assert result == [
        "projects/test-project/secrets/secret1",
        "projects/test-project/secrets/secret2"
    ]


def test_secrets_with_all_options(client, mock_gcp_client):
    """Test secrets() should support all options together."""
    mock_secret = Mock()
    mock_secret.name = "projects/test-project/secrets/filtered"
    mock_gcp_client.list_secrets.return_value = [mock_secret]

    result = client.secrets(filter="labels.team=data", max_results=5, fully_qualified=True)

    assert result == ["projects/test-project/secrets/filtered"]
    mock_gcp_client.list_secrets.assert_called_once_with(
        request={
            "parent": "projects/test-project",
            "filter": "labels.team=data",
            "page_size": 5
        }
    )


def test_secrets_returns_empty_list_when_no_secrets(client, mock_gcp_client):
    """Test secrets() should return empty list when no secrets exist."""
    mock_gcp_client.list_secrets.return_value = []

    result = client.secrets()

    assert result == []


def test_metadata_returns_secret_protobuf(client, mock_gcp_client):
    """Test metadata() should return the Secret protobuf."""
    mock_secret = Mock()
    mock_secret.name = "projects/test-project/secrets/my-secret"
    mock_secret.labels = {"env": "prod"}
    mock_gcp_client.get_secret.return_value = mock_secret

    result = client.metadata("my-secret")

    assert result is mock_secret
    assert result.name == "projects/test-project/secrets/my-secret"
    assert result.labels == {"env": "prod"}
    mock_gcp_client.get_secret.assert_called_once_with(name="projects/test-project/secrets/my-secret")


def test_metadata_with_project_prefix(client, mock_gcp_client):
    """Test metadata() should handle project/secret format."""
    mock_secret = Mock()
    mock_secret.name = "projects/other-project/secrets/secret"
    mock_gcp_client.get_secret.return_value = mock_secret

    result = client.metadata("other-project/secret")

    assert result is mock_secret
    mock_gcp_client.get_secret.assert_called_once_with(name="projects/other-project/secrets/secret")


def test_versions_returns_version_info_list(client, mock_gcp_client):
    """Test versions() should return list of VersionInfo objects."""
    mock_state1 = Mock(spec=["name"])
    mock_state1.name = "ENABLED"
    mock_v1 = Mock()
    mock_v1.name = "projects/test-project/secrets/my-secret/versions/1"
    mock_v1.state = mock_state1
    mock_v1.create_time = "2024-01-01T00:00:00Z"

    mock_state2 = Mock(spec=["name"])
    mock_state2.name = "ENABLED"
    mock_v2 = Mock()
    mock_v2.name = "projects/test-project/secrets/my-secret/versions/2"
    mock_v2.state = mock_state2
    mock_v2.create_time = "2024-01-02T00:00:00Z"

    mock_gcp_client.list_secret_versions.return_value = [mock_v1, mock_v2]

    result = client.versions("my-secret")

    assert len(result) == 2
    assert result[0].version == "1"
    assert result[0].state == "ENABLED"
    assert result[0].enabled is True
    assert result[0].create_time == "2024-01-01T00:00:00Z"
    assert result[1].version == "2"
    mock_gcp_client.list_secret_versions.assert_called_once_with(
        parent="projects/test-project/secrets/my-secret"
    )


def test_versions_excludes_disabled_by_default(client, mock_gcp_client):
    """Test versions() should exclude disabled versions by default."""
    mock_state1 = Mock(spec=["name"])
    mock_state1.name = "ENABLED"
    mock_v1 = Mock()
    mock_v1.name = "projects/test-project/secrets/my-secret/versions/1"
    mock_v1.state = mock_state1
    mock_v1.create_time = "2024-01-01T00:00:00Z"

    mock_state2 = Mock(spec=["name"])
    mock_state2.name = "DISABLED"
    mock_v2 = Mock()
    mock_v2.name = "projects/test-project/secrets/my-secret/versions/2"
    mock_v2.state = mock_state2
    mock_v2.create_time = "2024-01-02T00:00:00Z"

    mock_gcp_client.list_secret_versions.return_value = [mock_v1, mock_v2]

    result = client.versions("my-secret")

    assert len(result) == 1
    assert result[0].version == "1"
    assert result[0].enabled is True


def test_versions_includes_disabled_when_requested(client, mock_gcp_client):
    """Test versions() should include disabled versions when include_disabled=True."""
    mock_state1 = Mock(spec=["name"])
    mock_state1.name = "ENABLED"
    mock_v1 = Mock()
    mock_v1.name = "projects/test-project/secrets/my-secret/versions/1"
    mock_v1.state = mock_state1
    mock_v1.create_time = "2024-01-01T00:00:00Z"

    mock_state2 = Mock(spec=["name"])
    mock_state2.name = "DISABLED"
    mock_v2 = Mock()
    mock_v2.name = "projects/test-project/secrets/my-secret/versions/2"
    mock_v2.state = mock_state2
    mock_v2.create_time = "2024-01-02T00:00:00Z"

    mock_gcp_client.list_secret_versions.return_value = [mock_v1, mock_v2]

    result = client.versions("my-secret", include_disabled=True)

    assert len(result) == 2
    assert result[0].enabled is True
    assert result[1].enabled is False


def test_versions_handles_destroy_time(client, mock_gcp_client):
    """Test versions() should include destroy_time when present."""
    mock_state = Mock(spec=["name"])
    mock_state.name = "DESTROYED"
    mock_v1 = Mock()
    mock_v1.name = "projects/test-project/secrets/my-secret/versions/1"
    mock_v1.state = mock_state
    mock_v1.create_time = "2024-01-01T00:00:00Z"
    mock_v1.destroy_time = "2024-01-03T00:00:00Z"

    mock_gcp_client.list_secret_versions.return_value = [mock_v1]

    result = client.versions("my-secret", include_disabled=True)

    assert len(result) == 1
    assert result[0].state == "DESTROYED"
    assert result[0].destroy_time == "2024-01-03T00:00:00Z"


def test_versions_returns_empty_list_when_no_versions(client, mock_gcp_client):
    """Test versions() should return empty list when no versions exist."""
    mock_gcp_client.list_secret_versions.return_value = []

    result = client.versions("my-secret")

    assert result == []


# Slice 5: Secret Handle Abstraction

def test_client_secret_returns_secret_instance(client):
    """Test Client.secret() should return a Secret instance."""
    from gcpeasy.secretmanager.client import Secret

    secret = client.secret("api-key")

    assert isinstance(secret, Secret)
    assert secret.id == "api-key"
    assert secret.project_id == "test-project"


def test_secret_path_property_returns_fully_qualified_path(client):
    """Test Secret.path property should return fully qualified resource path."""
    secret = client.secret("api-key")

    assert secret.path == "projects/test-project/secrets/api-key"


def test_secret_handles_project_in_identifier(client):
    """Test Secret should extract project from identifier."""
    secret = client.secret("other-project/shared-key")

    assert secret.id == "shared-key"
    assert secret.project_id == "other-project"
    assert secret.path == "projects/other-project/secrets/shared-key"


def test_secret_handles_fully_qualified_path(client):
    """Test Secret should extract project and ID from fully qualified path."""
    secret = client.secret("projects/acme/secrets/legacy-key/versions/3")

    assert secret.id == "legacy-key"
    assert secret.project_id == "acme"
    assert secret.path == "projects/acme/secrets/legacy-key"


def test_secret_call_delegates_to_client(client, mock_gcp_client):
    """Test Secret.__call__() should delegate to client.get()."""
    mock_response = Mock()
    mock_response.payload.data = b"secret-value"
    mock_gcp_client.access_secret_version.return_value = mock_response

    secret = client.secret("api-key")
    result = secret()

    assert result == "secret-value"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/api-key/versions/latest"
    )


def test_secret_get_delegates_to_client(client, mock_gcp_client):
    """Test Secret.get() should delegate to client.get()."""
    mock_response = Mock()
    mock_response.payload.data = b"secret-value"
    mock_gcp_client.access_secret_version.return_value = mock_response

    secret = client.secret("api-key")
    result = secret.get(version=2, as_json=False)

    assert result == "secret-value"
    mock_gcp_client.access_secret_version.assert_called_once_with(
        name="projects/test-project/secrets/api-key/versions/2"
    )


def test_secret_get_bytes_delegates_to_client(client, mock_gcp_client):
    """Test Secret.get_bytes() should delegate to client.get_bytes()."""
    mock_response = Mock()
    mock_response.payload.data = b"binary-data"
    mock_gcp_client.access_secret_version.return_value = mock_response

    secret = client.secret("signing-key")
    result = secret.get_bytes()

    assert result == b"binary-data"


def test_secret_get_json_delegates_to_client(client, mock_gcp_client):
    """Test Secret.get_json() should delegate to client.get_json()."""
    mock_response = Mock()
    mock_response.payload.data = b'{"key": "value"}'
    mock_gcp_client.access_secret_version.return_value = mock_response

    secret = client.secret("config")
    result = secret.get_json()

    assert result == {"key": "value"}


def test_secret_get_dict_delegates_to_client(client, mock_gcp_client):
    """Test Secret.get_dict() should delegate to client.get_dict()."""
    mock_response = Mock()
    mock_response.payload.data = b"KEY1=value1\nKEY2=value2"
    mock_gcp_client.access_secret_version.return_value = mock_response

    secret = client.secret("env-config")
    result = secret.get_dict()

    assert result == {"KEY1": "value1", "KEY2": "value2"}


def test_secret_independent_from_client_mutations(client, mock_gcp_client):
    """Test Secret should be independent from subsequent client mutations."""
    secret = client.secret("api-key")
    original_project = secret.project_id

    # Mutate client
    client.project_id = "different-project"

    # Secret should retain original project
    assert secret.project_id == original_project
    assert secret.path == f"projects/{original_project}/secrets/api-key"
