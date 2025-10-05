# Secret Manager Implementation Plan

Thin, value-centric slices to deliver the `gcpeasy.secretmanager` module. Unless stated otherwise, each slice builds on Slice 1; any slice marked **(independent)** can be implemented out of order once its stated prerequisites are satisfied.

---

## Slice 1 – Client Bootstrap & Core Retrieval
- **Goal:** Provide developers an ergonomic way to fetch secret values with sensible defaults.
- **Scope:**
  - Implement `init()` factory and `Client` class storing `_gcp`, `project_id`, `default_version`, `default_encoding`.
  - Support `Client.__call__`, `Client.get`, `Client.get_path` for string retrieval with `version`, `default`, and identifier normalisation.
  - Implement inline decoding logic with `as_json`/`as_bytes` flags (mutually exclusive) and fallback handling for `NotFound` when `default` provided.
  - Add minimal identifier validation utilities used by the client.
  - Document behaviour in module docstrings if needed (README already exists).
- **Deliverables:** Working client returning strings/JSON/bytes per API document, basic validation helpers.
- **Tests:** Unit tests using mocked `SecretManagerServiceClient.access_secret_version`; cover default version, explicit version, JSON parsing, bytes passthrough, default fallback, invalid option combinations, validation errors.
- **✅ STATUS:** COMPLETE - Implemented in `src/gcpeasy/secretmanager/client.py` with 36 passing tests

## Slice 2 – Convenience Accessors & Dict Parsing **(independent)**
- **Goal:** Add ergonomic helpers for common read patterns beyond plain strings.
- **Scope:**
  - Implement `Client.get_bytes`, `Client.get_json` thin wrappers calling `get`.
  - Add `Client.get_dict` handling KEY=VALUE payloads with configurable separator, trimming flags, and optional case transforms per README.
  - Ensure newline default and custom separators behave as documented.
- **Deliverables:** Additional helper methods expanding retrieval ergonomics without touching other features.
- **Tests:** Mocked payload fixtures for env-style secrets verifying trimming, separator handling, uppercase flags, and delegation to `get`.
- **✅ STATUS:** COMPLETE - Implemented `get_bytes()`, `get_json()`, and `get_dict()` with 17 comprehensive tests

## Slice 3 – Batch Retrieval (`get_many`) **(independent)**
- **Goal:** Provide fast configuration hydration by fetching multiple secrets in one call.
- **Scope:**
  - Implement `Client.get_many` accepting iterables or mapping syntax described in the docs, including aliasing and per-entry options (`default`, `as_json`, etc.).
  - Sequential execution using existing `get` so throttling is deterministic.
  - Validate input schema and surface helpful errors on misuse.
- **Deliverables:** Dictionary-returning helper enabling atomic multi-secret fetches.
- **Tests:** Cases covering list input, dict input with option tuples, default fallbacks, and propagation of errors when no default provided.
- **✅ STATUS:** COMPLETE - Implemented `get_many()` with 15 comprehensive tests covering all input formats and error cases

## Slice 4 – Listing & Metadata Introspection **(independent)**
- **Goal:** Allow users to explore and audit secrets without fetching payloads.
- **Scope:**
  - Implement `Client.list` with optional `filter`, `max_results`, and `fully_qualified` flags.
  - Implement `Client.metadata` returning the underlying `Secret` protobuf (or a light wrapper) and `Client.versions` yielding simplified view objects (`version`, `state`, `enabled`, timestamps) with `include_disabled` and pagination support.
  - Add lightweight dataclass/NamedTuple for version summaries.
- **Deliverables:** Metadata surface enabling discovery and governance workflows.
- **Tests:** Mock `list_secrets`, `get_secret`, `list_secret_versions` responses; verify filtering passthrough, fully qualified formatting, and version summarisation.

## Slice 5 – Secret Handle Abstraction (`Secret` class)
- **Goal:** Provide per-secret objects enabling method chaining and clearer call sites.
- **Scope:**
  - Implement `Client.secret` returning a `Secret` instance storing the normalised path and parent client.
  - Add `Secret.__call__`, `get`, `get_bytes`, `get_json`, `get_dict`, `get_path` delegating to client methods while pinning the secret ID.
  - Expose `id`, `path`, and maybe `project_id` attributes for convenience.
- **Deliverables:** Usable `Secret` object supporting read operations with parity to client-level helpers.
- **Tests:** Validate delegation, attribute access, and independence from subsequent client mutations.

## Slice 6 – Secret Creation & Value Writes
- **Goal:** Enable creation of secrets and writing new versions as documented.
- **Scope:**
  - Implement `Secret.create` supporting `labels`, `replication`, `kms_key_name`, `exists_ok`.
  - Implement `Secret.add_version` handling string/bytes/dict/list payloads (JSON serialisation, encoding, json options), and `Secret.set` (create if missing + add version).
  - Internal helpers for payload normalisation and JSON dumping with compact separators.
- **Deliverables:** Ability to create secrets and push versions, fulfilling primary write workflows.
- **Tests:** Mock `create_secret`, `add_secret_version`, error handling for unsupported payload types, JSON serialisation correctness, `exists_ok` behaviour.

## Slice 7 – Version State Management **(independent)**
- **Goal:** Support lifecycle operations on individual versions without requiring Secret creation slice (only needs Secret handle from Slice 5).
- **Scope:**
  - Implement `Secret.disable_version`, `enable_version`, `destroy_version`, and `version_metadata` calling respective API endpoints.
  - Ensure idempotent behaviour (e.g., swallow already-disabled responses) where the API allows.
- **Deliverables:** Controls for disabling, re-enabling, destroying, and inspecting version metadata.
- **Tests:** Mock responses for each call, confirm parameter coercion to strings, verify error propagation.

## Slice 8 – Secret Metadata Updates & Deletion **(independent)**
- **Goal:** Provide maintenance operations for existing secrets.
- **Scope:**
  - Implement `Secret.update` supporting `labels`, `ttl`, and additional fields as described (convert numeric TTL to `timedelta` → `expire_time`).
  - Implement `Secret.delete` with `not_found_ok` (and placeholder for future `force` flag consistent with docs).
- **Deliverables:** Completion of lifecycle coverage (update descriptors, cleanup).
- **Tests:** Mock `update_secret` verifying partial field updates, TTL conversion accuracy, and mocked `delete_secret` w/ `not_found_ok` handling.

## Slice 9 – Hardening & Surface Polish **(independent)**
- **Goal:** Round out the module with validation, error messaging, and documentation wiring.
- **Scope:**
  - Centralise identifier/version validation utilities and reuse across client/secret helpers; add targeted tests.
  - Ensure `_gcp` property is exposed on the client (per README) and check any missing docstring or README cross-links.
  - Add regression tests for edge cases (e.g., conflicting flags, long IDs) not covered earlier.
  - Wire module into package exports (`__all__`, root README updates) if not already done.
- **Deliverables:** Stable, well-tested module ready for release.
- **Tests:** Additional validation-focused tests and documentation linting if applicable.

---

## Notes
- Each slice should update or add README examples/tests only for the behaviours introduced in that slice to keep review scope tight.
- Prefer using dependency injection for the underlying GCP client to keep tests fast and deterministic.
- When touching shared helpers, favour additive changes to minimise cross-slice conflicts.
