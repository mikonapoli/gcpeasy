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

### Writing Data

```python
# Write DataFrame
import pandas as pd
df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
table.write(df)

# Write from local file
table.write("data.csv")
table.write("data.parquet")

# Write from GCS
table.write("gs://my-bucket/data.csv")

# Write with explicit schema
schema = {"name": "STRING", "age": "INT64"}
table.write(df, schema=schema)

# Append instead of truncate
table.write(df, write_disposition="WRITE_APPEND")

# Create empty table with schema
table.write(None, schema={"name": "STRING", "age": "INT64"})
```

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

## Advanced Operations

### Stream Inserts

```python
# Insert rows for real-time ingestion
rows = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
]
errors = table.insert(rows)
if errors:
    print(f"Errors: {errors}")
```

### Export to GCS

```python
# Export to CSV
table.to_gcs("gs://my-bucket/exports/*.csv")

# Export to Parquet
table.to_gcs(
    "gs://my-bucket/exports/*.parquet",
    export_format="PARQUET"
)

# Wait for completion
job = table.to_gcs("gs://my-bucket/data.csv")
job.result()
```

### Copy Tables

```python
# Copy table
source = client.dataset("dataset1").table("table1")
dest = client.dataset("dataset2").table("table2")

job = source.copy(dest)
job.result()  # Wait for completion

# Append instead of truncate
source.copy(dest, write_disposition="WRITE_APPEND")
```

## Loading Data (Client-Level Convenience)

```python
# Load DataFrame
df = pd.DataFrame({"name": ["Alice"], "age": [30]})
client.load_data(df, "my_dataset.my_table")

# Load from file
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

## Working with Schemas

The library accepts schemas as simple dictionaries:

```python
schema = {
    "name": "STRING",
    "age": "INTEGER",      # Normalized to INT64
    "score": "FLOAT",      # Normalized to FLOAT64
    "active": "BOOL",      # Normalized to BOOLEAN
    "created": "TIMESTAMP"
}
```

Type aliases are automatically normalized:
- `INTEGER`, `INT`, `BIGINT` → `INT64`
- `FLOAT`, `DOUBLE` → `FLOAT64`
- `BOOL` → `BOOLEAN`
- `TEXT`, `VARCHAR` → `STRING`

## Access to Underlying Client

The `_gcp` attribute provides access to the underlying Google Cloud client for advanced operations:

```python
# Access underlying client
gcp_client = client._gcp

# Use any official BigQuery client method
job = gcp_client.extract_table(...)
```

## Write Dispositions

When writing data, control how existing data is handled:

- `WRITE_TRUNCATE`: Overwrite existing data (default for `table.write()`)
- `WRITE_APPEND`: Append to existing data (default for `client.load_data()`)
- `WRITE_EMPTY`: Only write if table is empty (fails otherwise)

## File Format Support

Supported file formats for loading:
- CSV (`.csv`)
- JSON (`.json`, `.jsonl`, `.ndjson`)
- Parquet (`.parquet`)
- Avro (`.avro`)
- ORC (`.orc`)

Format is auto-detected from file extension.

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
