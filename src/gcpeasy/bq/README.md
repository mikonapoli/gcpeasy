# BigQuery Module (`gcpeasy.bq`)

Ergonomic and opinionated wrapper for Google Cloud BigQuery operations.

## Quick Start

```python
from gcpeasy import bq

# Initialize client (uses Application Default Credentials)
client = bq.init()

# Query data
df = client("SELECT * FROM `project.dataset.table` LIMIT 10")
```

## Core Concepts

- **Simple auth**: Uses Application Default Credentials
- **Callable client**: The client itself is callable for common operations
- **Immediate usability**: Returns DataFrames by default
- **Method chaining**: Many methods return `self` for chaining

## Client Operations

### Initialization

```python
# Use default settings
client = bq.init()

# Custom project and location
client = bq.init(
    project_id="my-project",
    location="US"
)
```

### Running Queries

```python
# Simple query (returns DataFrame)
df = client("SELECT * FROM `dataset.table` LIMIT 100")

# Parameterized query
df = client.query(
    "SELECT * FROM `dataset.table` WHERE id = @id",
    params={"id": 123}
)

# Get raw iterator for large results
for row in client("SELECT * FROM huge_table", to_dataframe=False):
    print(row)
```

```python
from google.cloud import bigquery

# Attach a default job config (reused by every call)
default_config = bigquery.QueryJobConfig(priority=bigquery.QueryPriority.BATCH)
client = bq.init(default_job_config=default_config)

# Override per-call configuration
custom_config = bigquery.QueryJobConfig(use_legacy_sql=False)
client.query("SELECT 1", job_config=custom_config)
```

- Returns a pandas `DataFrame` by default; set `to_dataframe=False` to work with the underlying `RowIterator` directly.
- Attaching `params` auto-converts scalar Python types (`bool→BOOL`, `int→INT64`, `float→FLOAT64`, `str→STRING`, `bytes→BYTES`). Everything else falls back to `STRING`.
- Parameter binding here is scalar-only. Use your own `QueryJobConfig` if you need arrays or structs.
- Passing a `QueryJobConfig` (either at init or per call) reuses the same object. If you mutate it—for example by adding parameters—those changes persist for the next query unless you pass a fresh config.
- Client location defaults to `EU`; override with `bq.init(project_id="...", location="US")` when necessary.

### Listing Datasets and Tables

```python
# List datasets
datasets = client.datasets()
print(datasets)  # ['dataset1', 'dataset2']

# List tables in a dataset
tables = client.tables("my_dataset")
print(tables)  # ['table1', 'table2']

# Limit results
datasets = client.datasets(max_results=10)
```

## Dataset Operations

```python
# Get dataset object
dataset = client.dataset("my_dataset")

# Check if exists
if dataset.exists():
    print("Dataset exists")

# Create dataset
dataset.create(
    location="EU",
    description="My dataset",
    exists_ok=True
)

# Update dataset
dataset.update(
    description="Updated description",
    labels={"env": "prod"}
)

# Delete dataset
dataset.delete(delete_contents=True)

# Get metadata
metadata = dataset.get_metadata()
print(metadata.location)
print(metadata.created)
```

Notes:
- Dataset IDs are validated up front. Empty strings or IDs with invalid characters raise `ValueError` before any API call is made.
- `dataset.create()` inherits the client's project and location unless you override them explicitly.
- `dataset.delete(delete_contents=False)` mirrors the BigQuery REST API: set `delete_contents=True` to cascade tables, and `not_found_ok=True` to silence missing datasets.
- `dataset.update()` only touches the fields you pass; omitted values are left unchanged.

## Table Operations

### Getting Tables

```python
# Get table object
table = dataset.table("my_table")
# or
table = client.dataset("my_dataset").table("my_table")
```

### Reading Data

```python
# Read entire table
df = table.read()

# Read with limit
df = table.read(max_results=1000)
```

`max_results` is validated and coerced to an integer before being bound as a parameter; passing a non-numeric value raises `ValueError` rather than issuing a malformed query.

### Writing Data

```python
# Write DataFrame (defaults: infers schema, WRITE_TRUNCATE)
import pandas as pd
df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
table.write(df)

# Write from local file (auto-detects format from extension)
table.write("data.csv")
table.write("data.parquet")

# Write from GCS (auto-detects format)
table.write("gs://my-bucket/data.csv")

# Write with explicit schema (disables auto-detection)
schema = {"name": "STRING", "age": "INT64"}
table.write(df, schema=schema)

# Append instead of truncate
table.write(df, write_disposition="WRITE_APPEND")

# Create empty table with schema only
table.write(None, schema={"name": "STRING", "age": "INT64"})

# CSV options: skip_leading_rows (default 1), field_delimiter (default ",")
table.write("data.csv", skip_leading_rows=2, field_delimiter="|")
```

Behaviour summary:
- DataFrames are loaded with `load_table_from_dataframe`. When no schema is supplied we infer one from the pandas dtypes (`int`→`INT64`, `float`→`FLOAT64`, `bool`→`BOOLEAN`, `datetime64`→`TIMESTAMP`, everything else→`STRING`).
- String or `Path` inputs call the appropriate load job. Local paths stream the file, while `gs://` URIs trigger a server-side job (wildcards are supported).
- File formats are auto-detected from the extension; unsupported extensions raise an explicit `ValueError` before submitting a job.
- When you supply a schema for file loads we attach it verbatim; otherwise BigQuery autodetects it. For CSV loads we default to `skip_leading_rows=1` so header rows are ignored unless you override it.
- Passing `data=None` with a schema creates the table if it does not exist. Existing tables are left untouched thanks to `exists_ok=True` under the hood.
- The default write disposition here is `WRITE_TRUNCATE`. Use the client-level helper if you would rather append by default.
- Unsupported input types raise `TypeError` immediately, keeping the behaviour explicit.

### Creating Tables

```python
# Create table with schema
schema = {
    "name": "STRING",
    "age": "INT64",
    "created_at": "TIMESTAMP"
}
table.create(schema, exists_ok=True)

# With partitioning
table.create(
    schema,
    partitioning_field="created_at",
    description="User data"
)

# With clustering
table.create(
    schema,
    clustering_fields=["name", "age"]
)
```

### Updating Tables

```python
# Update description and labels
table.update(
    description="Updated table",
    labels={"version": "v2"}
)

# Add new fields to schema
table.update(schema={"new_field": "STRING"})
```

Schema updates are additive only: new fields are appended to the existing schema. To alter or drop fields, fall back to the raw BigQuery client via `client._gcp`.

### Other Table Operations

```python
# Check if table exists
if table.exists():
    print("Table exists")

# Get schema
schema = table.get_schema()
for field in schema:
    print(f"{field.name}: {field.field_type}")

# Get full metadata
metadata = table.get_metadata()
print(metadata.num_rows)
print(metadata.created)

# Delete table
table.delete()
table.delete(not_found_ok=True)  # Don't error if missing
```

- `table.exists()` and `table.get_metadata()` share the same identifier validation guarantees as dataset operations.
- `table.delete(not_found_ok=True)` mirrors the dataset helper by swallowing `NotFound` so you can clean up idempotently.

## Advanced Operations

### Stream Inserts

```python
# Insert rows for real-time ingestion (defaults: ignore_unknown_values=True, skip_invalid_rows=False)
rows = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
]
errors = table.insert(rows)
if errors:
    print(f"Errors: {errors}")
```

The method returns the raw error payload from `insert_rows_json`. An empty list means the insert succeeded.

### Export to GCS

```python
# Export to CSV (defaults: format=CSV, compression=GZIP, print_header=True, field_delimiter=",")
table.to_gcs("gs://my-bucket/exports/*.csv")

# Export to Parquet with no compression
table.to_gcs(
    "gs://my-bucket/exports/*.parquet",
    export_format="PARQUET",
    compression="NONE"
)

# Wait for completion
job = table.to_gcs("gs://my-bucket/data.csv")
job.result()
```

Extract jobs are asynchronous just like the official client. Use `job.result()` to block until completion or poll manually for long exports.

### Copy Tables

```python
# Copy table (default: WRITE_TRUNCATE)
source = client.dataset("dataset1").table("table1")
dest = client.dataset("dataset2").table("table2")

job = source.copy(dest)
job.result()  # Wait for completion

# Append instead of truncate
source.copy(dest, write_disposition="WRITE_APPEND")
```

You can also pass fully-qualified string IDs (`"project.dataset.table"`) instead of `Table` objects for the destination.

## Loading Data (Client-Level Convenience)

```python
# Load DataFrame (default: WRITE_APPEND)
df = pd.DataFrame({"name": ["Alice"], "age": [30]})
client.load_data(df, "my_dataset.my_table")

# Load from file (auto-detects format)
client.load_data("data.csv", "my_dataset.my_table")

# With explicit format and schema
client.load_data(
    "data.csv",
    "my_dataset.my_table",
    schema={"name": "STRING", "age": "INT64"},
    source_format="CSV",
    write_disposition="WRITE_TRUNCATE"
)
```

- Accepts table IDs in both `dataset.table` and `project.dataset.table` form; anything else raises a `ValueError` before hitting the API.
- Under the hood this simply routes to `dataset(...).table(...).write(...)`, so all the behaviours described in the table section apply here as well—only the default write disposition changes to `WRITE_APPEND`.
- When loading local files, CSV defaults (`skip_leading_rows=1`, comma delimiter) still apply unless you override them in the method call.

## Working with Schemas

Schemas are simple dictionaries. Type aliases are auto-normalized:

```python
schema = {
    "name": "STRING",
    "age": "INTEGER",      # → INT64
    "score": "FLOAT",      # → FLOAT64
    "active": "BOOL",      # → BOOLEAN
    "created": "TIMESTAMP"
}
```

**Type normalizations:**
- `INTEGER`, `INT`, `BIGINT` → `INT64`
- `FLOAT`, `DOUBLE` → `FLOAT64`
- `BOOL` → `BOOLEAN`
- `TEXT`, `VARCHAR` → `STRING`

DataFrame inference mirrors the helper used by `table.write(df)`: integer dtypes map to `INT64`, floats to `FLOAT64`, `bool` to `BOOLEAN`, `datetime64` to `TIMESTAMP`, and everything else (including `object`) becomes `STRING`.

## Identifier Validation

Dataset, table, and project IDs are validated before we call the API:

- Dataset/table IDs must start with a letter or underscore and contain only letters, numbers, and underscores.
- Project IDs must start with a letter and can include numbers, hyphens, underscores, and apostrophes.
- Empty identifiers—or anything longer than 1024 characters—raise `ValueError` immediately.

This keeps typos from being sent to BigQuery and makes failures easier to debug locally.

## Access to Underlying Client

The `_gcp` attribute provides access to the underlying Google Cloud client for advanced operations:

```python
# Access underlying client
gcp_client = client._gcp

# Use any official BigQuery client method
job = gcp_client.extract_table(...)
```

## Write Dispositions

- `WRITE_TRUNCATE`: Overwrite (default for `table.write()`)
- `WRITE_APPEND`: Append (default for `client.load_data()`)
- `WRITE_EMPTY`: Only write if empty (fails otherwise)

## File Formats

Auto-detected from extension:
- CSV: `.csv`
- JSON: `.json`, `.jsonl`, `.ndjson`
- Parquet: `.parquet`
- Avro: `.avro`
- ORC: `.orc`

## Examples

### Complete Workflow

```python
from gcpeasy import bq
import pandas as pd

# Initialize
client = bq.init()

# Create dataset
dataset = client.dataset("analytics")
dataset.create(location="EU", exists_ok=True)

# Create table with data
df = pd.DataFrame({
    "user_id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [95.5, 87.3, 92.1]
})

table = dataset.table("users")
table.write(df)

# Query the data
results = client("""
    SELECT name, score
    FROM `analytics.users`
    WHERE score > 90
    ORDER BY score DESC
""")

print(results)

# Update table
table.update(
    description="User scores",
    labels={"team": "data"}
)

# Export to GCS
table.to_gcs("gs://my-bucket/users/*.csv")
```

### Loading from Multiple Sources

```python
# From DataFrame
client.load_data(df, "dataset.table")

# From local CSV
client.load_data("users.csv", "dataset.table")

# From GCS
table.write("gs://bucket/data.parquet")

# From multiple GCS files with wildcard
table.write("gs://bucket/data/*.csv")
```

### Method Chaining

```python
# Create and populate in one go
client.dataset("new_dataset") \
    .create(location="US", exists_ok=True) \
    .table("new_table") \
    .create(schema={"id": "INT64", "name": "STRING"})
```
