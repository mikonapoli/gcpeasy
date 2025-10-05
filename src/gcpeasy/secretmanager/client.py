"""Secret Manager client implementation."""

import json
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from google.cloud.secretmanager import SecretManagerServiceClient

# Sentinel for distinguishing "not provided" from "explicitly None"
_UNSET = object()

# Validation patterns
# Secret IDs: [A-Za-z0-9][A-Za-z0-9_-]{0,254} per API docs (1-255 chars, start with alphanumeric)
_SECRET_ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,254}$")
_PROJECT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]*[a-z0-9]$")
_VERSION_KEYWORDS = {"latest", "latest:enabled"}


def _validate_secret_id(secret_id: str) -> None:
    """Validate secret ID format."""
    if not _SECRET_ID_PATTERN.match(secret_id):
        raise ValueError(
            f"Invalid secret ID: {secret_id!r}. Must start with letter or number "
            "and contain only letters, numbers, underscores, and hyphens (max 255 chars)"
        )


def _validate_project_id(project_id: str) -> None:
    """Validate project ID format (supports both string IDs and numeric project numbers)."""
    # Numeric project IDs are valid
    if project_id.isdigit():
        return
    # String project IDs must follow GCP naming rules
    if not _PROJECT_ID_PATTERN.match(project_id):
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
        v = int(version)
        if v < 1:
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
    # Full resource path
    if identifier.startswith("projects/"):
        parts = identifier.split("/")
        if len(parts) < 4 or parts[2] != "secrets":
            raise ValueError(f"Invalid resource path: {identifier!r}")
        proj, sec = parts[1], parts[3]
        ver = parts[5] if len(parts) >= 6 and parts[4] == "versions" else None
        return proj, sec, ver

    # project/secret or simple ID
    if "/" in identifier:
        parts = identifier.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid secret identifier: {identifier!r}")
        return parts[0], parts[1], None

    # Simple ID
    return project_id, identifier, None


class Client:
    """Secret Manager client wrapper."""

    def __init__(
        self,
        project_id: str,
        default_version: str = "latest",
        default_encoding: str = "utf-8",
        _gcp: "SecretManagerServiceClient | None" = None,
    ):
        if _gcp is None:
            from google.cloud import secretmanager
            _gcp = secretmanager.SecretManagerServiceClient()

        self._gcp = _gcp
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
        # Use explicit None checks to avoid treating falsy values (like 0) as missing
        if version is not None:
            ver = _validate_version(version)
        elif path_version is not None:
            ver = _validate_version(path_version)
        else:
            ver = _validate_version(self.default_version)

        _validate_project_id(proj)
        _validate_secret_id(sec)

        name = f"projects/{proj}/secrets/{sec}/versions/{ver}"

        try:
            from google.api_core.exceptions import NotFound
            response = self._gcp.access_secret_version(name=name)
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
        if text is default:
            return text

        result = {}
        for line in text.split(line_separator):
            if not line or key_separator not in line:
                continue
            key, value = line.split(key_separator, 1)
            if strip_keys:
                key = key.strip()
            if strip_values:
                value = value.strip()
            if uppercase_keys:
                key = key.upper()
            result[key] = value
        return result

    def get_path(self, path: str, *, as_json: bool = False, as_bytes: bool = False, default: Any = _UNSET) -> Any:
        """Fetch a fully qualified resource path without project inference."""
        if not path.startswith("projects/"):
            raise ValueError(f"get_path requires fully qualified path, got {path!r}")

        if as_json and as_bytes:
            raise ValueError("Cannot specify both as_json and as_bytes")

        try:
            from google.api_core.exceptions import NotFound
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


def init(
    project_id: str | None = None,
    project_number: str | int | None = None,
    default_version: str = "latest",
    default_encoding: str = "utf-8",
    _gcp: "SecretManagerServiceClient | None" = None,
) -> Client:
    """
    Initialize a Secret Manager client.

    Args:
        project_id: GCP project ID (defaults to ADC project)
        project_number: GCP project number (project_id takes precedence)
        default_version: Default version for retrieval operations
        default_encoding: Default text encoding
        _gcp: Pre-built SecretManagerServiceClient
    """
    if _gcp is None:
        from google.cloud import secretmanager
        _gcp = secretmanager.SecretManagerServiceClient()

    # Determine project
    if project_id:
        proj = project_id
    elif project_number is not None:
        proj = str(project_number)
    else:
        # Get from ADC
        import google.auth
        _, proj = google.auth.default()
        if not proj:
            raise ValueError(
                "Could not determine project ID from Application Default Credentials. "
                "Please provide project_id or project_number explicitly."
            )

    return Client(
        project_id=proj,
        default_version=default_version,
        default_encoding=default_encoding,
        _gcp=_gcp,
    )
