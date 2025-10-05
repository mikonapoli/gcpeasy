# Secret Manager Module (`gcpeasy.secretmanager`)

Ergonomic wrapper around Google Cloud Secret Manager with the same philosophy as the BigQuery helpers: callable clients, sensible defaults, and immediately usable return values.

## Quick Start

```python
from gcpeasy import secretmanager as sm

# Initialise using Application Default Credentials
client = sm.init()

# Fetch latest secret as UTF-8 text
api_key = client("stripe-api-key")

# Parse JSON secrets on the fly
config = client("app-config", as_json=True)

# Provide safe fallback values
token = client("optional-token", default=None)
```

## Core Concepts

- **Default-friendly client:** `client(secret_id)` fetches the latest version as text. Helper flags switch to bytes/JSON when needed.
- **Flexible identifiers:** Accepts plain IDs, `project/secret` pairs, or fully qualified paths (`projects/.../secrets/.../versions/...`).
- **Deterministic defaults:** Location comes from ADC project; version defaults to `latest`; encoding defaults to UTF-8.
- **Fallbacks without surprises:** Pass `default=` (even `None`) to suppress `NotFound`. All other errors bubble up.
- **Immediate results:** Helpers return Python primitives instead of protobufs. Drop down to the underlying SDK client (stored in the `_gcp` attribute) for advanced usage.

## Initialization

```python
from gcpeasy import secretmanager as sm

# Use ADC project and defaults
client = sm.init()

# Pin operations to a specific project ID
client = sm.init(project_id="analytics-prod")

# Supply a numeric project number
client = sm.init(project_number=1234567890)

# Custom defaults
client = sm.init(
    project_id="acme-sandbox",
    default_version="latest:enabled",
    default_encoding="latin1",
)
```

- `project_id` wins if both `project_id` and `project_number` are provided.
- Without explicit project info, we call `google.auth.default()` and use the discovered project. If the ADC environment lacks a project, `init()` raises a descriptive `ValueError`.
- Numeric project identifiers (e.g., `"123456"`) are accepted anywhere a project ID is expected.
- In tests, monkeypatch `google.cloud.secretmanager.SecretManagerServiceClient` before calling `sm.init()` to inject fakes or emulator-backed clients.

## Retrieving Secrets

```python
# Latest version (string)
value = client("db-password")

# Specific version (int or string)
legacy = client("db-password", version=4)

# Version keyword
enabled_only = client("feature-flag", version="latest:enabled")

# JSON payload
settings = client("service-config", as_json=True)

# Raw bytes
certificate = client("tls-cert", as_bytes=True)

# Cross-project read using project/secret syntax
shared = client("shared-project/shared-secret")

# Fully qualified path
via_path = client("projects/acme/secrets/api-key/versions/12")
```

Key behaviours:
- `version` accepts positive integers, numeric strings, or `"latest"`/`"latest:enabled"`. Invalid values raise before the API call.
- Passing `version=0` or negative values raises `ValueError` rather than silently falling back to `latest`.
- Only one of `as_json` or `as_bytes` can be `True`; setting both raises immediately.
- Payloads are decoded using the client's `default_encoding`. Override per client if your secrets are not UTF-8.
- `default` suppresses only `NotFound`. Any other API error (permission issues, quota, etc.) still surfaces for the caller to handle.
- `default=None` is treated as a real fallback value, not as “argument missing”.

## Convenience Accessors

```python
client.get("api-key")              # identical to client(...)
client.get_bytes("binary-key")     # raw bytes
client.get_json("config")          # JSON dict/list

# Parse KEY=VALUE payloads
env = client.get_dict(
    "env-config",
    line_separator="\n",
    key_separator="=",
    uppercase_keys=True,
)
# {'DB_HOST': 'localhost', 'DB_PORT': '5432'}
```

Notes:
- `get_dict` skips blank lines and any line without the separator. Use `strip_keys=False` or `strip_values=False` to preserve whitespace.
- `uppercase_keys=True` is handy for exporting to environment variables.
- Custom separators allow parsing colon-separated or semicolon-delimited payloads without post-processing.
- When a fallback `default` is provided and the secret is missing, the default is returned untouched (no parsing).

## Batch Retrieval (`get_many`)

```python
# Simple iterable -> alias defaults to the secret ID
secrets = client.get_many(["db-password", "api-token"])

# Mapping with friendly aliases and option tuples
secrets = client.get_many({
    "database": "db-password",
    "api": ("api-token", {"default": "missing"}),
    "config": {"secret": "app-config", "as_json": True},
})
```

Behaviour summary:
- Accepts any `Mapping` (dict, `UserDict`, `MappingProxyType`, etc.) or iterable of secret IDs.
- Tuple specs must be `(secret_id, options_dict)`. Dict specs require a `secret` key; remaining keys are forwarded to `get`.
- When using an iterable, the returned dict keys mirror the secret IDs.
- Each entry is fetched sequentially via `get()`, so all format flags, defaults, and version rules apply consistently.
- `default` handling for individual entries works the same as single-call retrieval: `NotFound` returns the provided fallback, other errors propagate.

## Listing & Metadata Introspection

```python
# List all secret IDs in the current project
all_secrets = client.secrets()
# ['api-key', 'db-password', 'service-account']

# Filter by labels
prod_secrets = client.secrets(filter="labels.env=prod")

# Limit results
recent = client.secrets(max_results=10)

# Get fully qualified resource names
paths = client.secrets(fully_qualified=True)
# ['projects/my-project/secrets/api-key', ...]

# Fetch metadata without retrieving payload
meta = client.metadata("api-key")
print(meta.name, meta.labels, meta.replication)

# List version information
versions = client.versions("api-key")
for v in versions:
    print(f"Version {v.version}: {v.state} (enabled={v.enabled})")
    print(f"  Created: {v.create_time}")

# Include disabled/destroyed versions
all_versions = client.versions("api-key", include_disabled=True)
```

Key behaviours:
- `secrets()` returns simple secret IDs by default. Set `fully_qualified=True` for full resource paths.
- `filter` accepts Secret Manager's filter syntax (e.g., `labels.team=platform`).
- `max_results` translates to `page_size` in the underlying API call.
- `metadata()` returns the raw `Secret` protobuf from the API, giving access to replication config, labels, timestamps, etc.
- `versions()` returns a list of `VersionInfo` dataclasses with `version`, `state`, `enabled`, `create_time`, and optional `destroy_time`.
- By default, `versions()` excludes disabled/destroyed versions. Pass `include_disabled=True` to see everything.

## Working with Fully Qualified Paths

```python
raw = client.get_path("projects/acme/secrets/legacy-key/versions/3")
blob = client.get_path(
    "projects/acme/secrets/legacy-key/versions/3",
    as_bytes=True,
)
missing = client.get_path(
    "projects/acme/secrets/maybe/versions/latest",
    default=None,
)
```

- `get_path` skips identifier inference entirely and requires the path to start with `projects/`.
- Shares the same `as_json`, `as_bytes`, and `default` semantics as `get`.

## Secret Objects

Get a handle for a specific secret that pins the secret ID across operations:

```python
# Get a Secret handle
secret = client.secret("github-token")

# Attributes
secret.id          # "github-token"
secret.path        # "projects/my-project/secrets/github-token"
secret.project_id  # "my-project"

# Retrieve values using the same methods as the client
current = secret()                        # latest version as string
payload = secret(version=2, as_json=True)
raw = secret.get_bytes()
env_vars = secret.get_dict()

# Cross-project secrets
shared = client.secret("shared-project/shared-secret")
shared.id          # "shared-secret"
shared.project_id  # "shared-project"
```

Key behaviours:
- `client.secret()` normalizes the identifier immediately, extracting project and secret ID
- The returned `Secret` object delegates all read operations to the parent client while pinning the secret ID
- Secret handles are independent from subsequent client mutations
- All client-level read methods (`__call__`, `get`, `get_bytes`, `get_json`, `get_dict`) are available on Secret objects with the same signatures (minus the `secret` parameter)

## Identifier & Version Rules

- Secret IDs must be 1–255 characters, start with a letter or digit, and may include letters, digits, `_`, or `-` thereafter. Underscore-prefixed IDs (`_secret`) are rejected to match Google’s API contract.
- Project identifiers may be lowercase strings with hyphens (per GCP constraints) or purely numeric project numbers.
- Versions must be positive integers (or their numeric string form) or one of the keywords `latest` / `latest:enabled`.
- Validation occurs before calling the API so mistakes fail fast.

## Default Encoding & JSON Handling

- Text responses are decoded with `default_encoding`. If decoding fails, the underlying `UnicodeDecodeError` is raised.
- `as_json=True` uses `json.loads` and surfaces the original `ValueError` if the payload is not valid JSON.
- Bytes mode bypasses decoding entirely, so binary secrets remain untouched.

## Underlying Client Access

```python
sdk_client = client._gcp
response = sdk_client.access_secret_version(name="projects/acme/secrets/raw/versions/1")
```

- `_gcp` exposes the original `SecretManagerServiceClient` for operations not yet wrapped by gcpeasy.
- To override it in tests, monkeypatch `SecretManagerServiceClient` before calling `sm.init()`.

## Testing Tips

- Remember that only `NotFound` is caught when a `default` is supplied—other exceptions should be asserted explicitly in your tests.
- When exercising `get_many`, pass sentinel defaults per entry to keep assertions precise.

## Example Workflow

```python
from gcpeasy import secretmanager as sm

client = sm.init(project_id="prod")

# Fetch all configuration secrets at once
config = client.get_many({
    "DATABASE_URL": "db/url",
    "SERVICE_ACCOUNT": {"secret": "sa/json", "as_json": True},
    "REDIS_PASSWORD": ("redis/password", {"default": ""}),
})

# Use defaults when optional secrets are missing
feature_flag = client("flags/new-feature", default="disabled")

# Raw bytes for cryptographic material
signing_key = client.get_bytes("crypto/signing-key", version=2)

# Share a secret path across environments
shared = client.get_path("projects/shared/secrets/central-config/versions/latest")
```

The client keeps the interaction terse while still giving access to low-level primitives when you need them.
