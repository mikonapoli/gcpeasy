"""Secret Manager client implementation."""

import json
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import NotFound
from google.cloud import secretmanager

if TYPE_CHECKING:
    from google.cloud.secretmanager import SecretManagerServiceClient

_UNSET = object()
_SECRET_ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,254}$")
_PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]*[a-z0-9]$")
_VERSION_KEYWORDS = {"latest", "latest:enabled"}


def _validate_secret_id(secret_id: str) -> None:
    """Validate secret ID format."""
    if _SECRET_ID_PATTERN.match(secret_id):
        return
    raise ValueError(
        f"Invalid secret ID: {secret_id!r}. Must start with letter or number "
        "and contain only letters, numbers, underscores, and hyphens (max 255 chars)"
    )


def _validate_project_id(project_id: str) -> None:
    """Validate project ID format (supports both string IDs and numeric project numbers)."""
    if project_id.isdigit() or _PROJECT_ID_PATTERN.match(project_id):
        return
    raise ValueError(
        f"Invalid project ID: {project_id!r}. Must be numeric or start with letter, "
        "end with letter/number, and contain only lowercase letters, numbers, and hyphens"
    )


def _validate_version(version: str | int) -> str:
    """Validate and normalize version identifier."""
    if isinstance(version, int):
        if version < 1:
            raise ValueError(f"Version must be positive, got {version}")
        return str(version)
    if version in _VERSION_KEYWORDS:
        return version
    if version.isdigit():
        if int(version) < 1:
            raise ValueError(f"Version must be positive, got {version}")
        return version
    raise ValueError(
        f"Invalid version: {version!r}. Must be positive int, 'latest', or 'latest:enabled'"
    )


def _normalize_secret_path(identifier: str, project_id: str) -> tuple[str, str, str | None]:
    """
    Normalize secret identifier to (project, secret_id, version).

    Accepts:
    - simple ID: "my-secret"
    - project/secret: "my-project/my-secret"
    - full path: "projects/my-project/secrets/my-secret[/versions/X]"
    """
    if identifier.startswith("projects/"):
        parts = identifier.split("/")
        if len(parts) < 4 or parts[2] != "secrets":
            raise ValueError(f"Invalid resource path: {identifier!r}")
        ver = parts[5] if len(parts) >= 6 and parts[4] == "versions" else None
        return parts[1], parts[3], ver

    if "/" in identifier:
        proj, sec = identifier.split("/", 1)
        if not proj or not sec:
            raise ValueError(f"Invalid secret identifier: {identifier!r}")
        return proj, sec, None

    return project_id, identifier, None


class Client:
    """Secret Manager client wrapper."""

    def __init__(self, project_id: str, default_version: str = "latest", default_encoding: str = "utf-8"):
        self._gcp = secretmanager.SecretManagerServiceClient()
        self.project_id = project_id
        self.default_version = default_version
        self.default_encoding = default_encoding

    def __call__(
        self,
        secret: str,
        *,
        version: str | int | None = None,
        as_json: bool = False,
        as_bytes: bool = False,
        default: Any = _UNSET,
    ) -> Any:
        """
        Retrieve a secret value (latest version by default).

        Args:
            secret: Secret ID or resource path
            version: Version to fetch (defaults to client's default_version)
            as_json: Parse response as JSON
            as_bytes: Return raw bytes
            default: Fallback value if secret not found
        """
        return self.get(secret, version=version, as_json=as_json, as_bytes=as_bytes, default=default)

    def get(
        self,
        secret: str,
        *,
        version: str | int | None = None,
        as_json: bool = False,
        as_bytes: bool = False,
        default: Any = _UNSET,
    ) -> Any:
        """Retrieve a secret value."""
        if as_json and as_bytes:
            raise ValueError("Cannot specify both as_json and as_bytes")

        proj, sec, path_version = _normalize_secret_path(secret, self.project_id)
        ver = (
            _validate_version(version)
            if version is not None
            else _validate_version(path_version)
            if path_version is not None
            else _validate_version(self.default_version)
        )

        _validate_project_id(proj)
        _validate_secret_id(sec)

        name = f"projects/{proj}/secrets/{sec}/versions/{ver}"

        try:
            response = self._gcp.access_secret_version(name=name)
            payload = response.payload.data
        except NotFound:
            if default is _UNSET: raise
            return default

        if as_bytes: return payload

        text = payload.decode(self.default_encoding)
        if as_json: return json.loads(text)
        return text

    def get_bytes(self, secret: str, *, version: str | int | None = None, default: Any = _UNSET) -> bytes:
        """Retrieve secret as raw bytes."""
        return self.get(secret, version=version, as_bytes=True, default=default)

    def get_json(self, secret: str, *, version: str | int | None = None, default: Any = _UNSET) -> Any:
        """Retrieve secret and parse as JSON."""
        return self.get(secret, version=version, as_json=True, default=default)

    def get_dict(
        self,
        secret: str,
        *,
        version: str | int | None = None,
        line_separator: str = "\n",
        key_separator: str = "=",
        strip_keys: bool = True,
        strip_values: bool = True,
        uppercase_keys: bool = False,
        default: Any = _UNSET,
    ) -> dict[str, str]:
        """
        Retrieve secret as dict by parsing KEY=VALUE pairs.

        Splits by line_separator (default newline), then by key_separator (default =).
        """
        text = self.get(secret, version=version, default=default)
        if text is default: return text

        result: dict[str, str] = {}
        for line in text.split(line_separator):
            if not line or key_separator not in line: continue
            key, value = line.split(key_separator, 1)
            if strip_keys: key = key.strip()
            if strip_values: value = value.strip()
            if uppercase_keys: key = key.upper()
            result[key] = value
        return result

    def get_many(self, secrets) -> dict[str, Any]:
        """
        Retrieve multiple secrets in one call.

        Accepts:
        - Iterable of secret IDs: ["secret1", "secret2"]
        - Mapping with aliases: {"alias": "secret-id"} or {"alias": ("secret-id", options)} or {"alias": {"secret": "id", ...}}
        """
        if isinstance(secrets, Mapping):
            return {alias: self._resolve_many_spec(alias, spec) for alias, spec in secrets.items()}

        result: dict[str, Any] = {}
        for secret in secrets:
            if not isinstance(secret, str):
                raise TypeError(f"Secret ID must be string, got {type(secret).__name__}")
            result[secret] = self.get(secret)
        return result

    def _resolve_many_spec(self, alias: str, spec: Any) -> Any:
        """Resolve a single get_many specification."""
        if isinstance(spec, str): return self.get(spec)
        if isinstance(spec, tuple):
            if len(spec) != 2:
                raise ValueError(
                    f"Tuple spec must be (secret_id, options), got {len(spec)} elements for {alias!r}"
                )
            secret_id, options = spec
            if not isinstance(secret_id, str):
                raise TypeError(f"Secret ID must be string, got {type(secret_id).__name__} for {alias!r}")
            if not isinstance(options, dict):
                raise TypeError(f"Options must be dict, got {type(options).__name__} for {alias!r}")
            return self.get(secret_id, **options)
        if isinstance(spec, dict):
            if "secret" not in spec:
                raise ValueError(f"Dict spec must contain 'secret' key for {alias!r}")
            secret_id = spec["secret"]
            if not isinstance(secret_id, str):
                raise TypeError(f"Secret ID must be string, got {type(secret_id).__name__} for {alias!r}")
            options = {k: v for k, v in spec.items() if k != "secret"}
            return self.get(secret_id, **options)
        raise TypeError(
            f"Invalid spec type for {alias!r}: expected str, tuple, or dict, got {type(spec).__name__}"
        )

    def get_path(self, path: str, *, as_json: bool = False, as_bytes: bool = False, default: Any = _UNSET) -> Any:
        """Fetch a fully qualified resource path without project inference."""
        if not path.startswith("projects/"):
            raise ValueError(f"get_path requires fully qualified path, got {path!r}")

        if as_json and as_bytes:
            raise ValueError("Cannot specify both as_json and as_bytes")

        try:
            response = self._gcp.access_secret_version(name=path)
            payload = response.payload.data
        except NotFound:
            if default is _UNSET:
                raise
            return default

        if as_bytes:
            return payload

        text = payload.decode(self.default_encoding)
        if as_json:
            return json.loads(text)
        return text


def init(project_id: str | None = None, project_number: str | int | None = None, default_version: str = "latest", default_encoding: str = "utf-8") -> Client:
    """
    Initialize a Secret Manager client.

    Args:
        project_id: GCP project ID (defaults to ADC project)
        project_number: GCP project number (project_id takes precedence)
        default_version: Default version for retrieval operations
        default_encoding: Default text encoding
    """
    if project_id: proj = project_id
    elif project_number is not None: proj = str(project_number)
    else:
        import google.auth
        _, proj = google.auth.default()
        if not proj:
            raise ValueError(
                "Could not determine project ID from Application Default Credentials. "
                "Please provide project_id or project_number explicitly."
            )

    return Client(project_id=proj, default_version=default_version, default_encoding=default_encoding)
