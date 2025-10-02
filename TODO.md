# BigQuery Implementation TODO

## Slice 1: Basic Query Execution ✓
**Goal:** Run SQL queries and get DataFrames back
- [x] Create `gcpeasy/bq.py` module structure
- [x] Implement `init()` function returning Client
- [x] Implement `Client` class with `_gcp` attribute
- [x] Add `Client.__call__()` for query execution → DataFrame
- [x] Add `Client.query()` with basic parameterized query support
- [x] Handle query results → DataFrame conversion
- [x] Basic error handling and authentication
- [x] Type hints and docstrings
- [x] Basic tests

## Slice 2: Dataset Discovery & Navigation ✓
**Goal:** List and access datasets and tables
- [x] Implement `Dataset` class with `id` property
- [x] Add `Client.datasets()` to list all datasets
- [x] Add `Client.dataset(name)` to get Dataset object
- [x] Add `Dataset.exists()` method
- [x] Add `Dataset.tables()` to list tables
- [x] Add `Client.tables(dataset_id)` convenience method
- [x] Type hints and docstrings
- [x] Tests for listing/navigation

## Slice 3: Table Reading ✓
**Goal:** Read table data into DataFrames
- [x] Implement `Table` class with `id` property (fully qualified)
- [x] Add `Dataset.table(name)` to get Table object
- [x] Add `Table.exists()` method
- [x] Add `Table.read()` returning DataFrame
- [x] Handle pagination properly
- [x] Type hints and docstrings
- [x] Tests for table reading

## Slice 4: DataFrame → Table (Simple) ✓
**Goal:** Write DataFrames to tables
- [x] Add schema auto-detection for DataFrames
- [x] Add DataFrame → BigQuery type conversion
- [x] Implement dict → SchemaField conversion ({"name": "STRING"})
- [x] Add `Table.write(df)` with WRITE_TRUNCATE default
- [x] Add `Client.load_data(table_id, df)` convenience method
- [x] Handle write dispositions (TRUNCATE, APPEND, EMPTY)
- [x] Tests for DataFrame writing

## Slice 5: Table Creation & Management ✓
**Goal:** Create/delete tables and datasets explicitly
- [x] Add `Dataset.create()` with location, description, expiration
- [x] Add `Dataset.delete()` with delete_contents option
- [x] Add `Table.create()` with schema dict, partitioning, clustering
- [x] Add `Table.delete()` method
- [x] Handle exists_ok/not_found_ok flags
- [x] Validate table_id formats (dataset.table vs project.dataset.table)
- [x] Tests for CRUD operations

## Slice 6: File Loading ✓
**Goal:** Load data from files (CSV, JSON, Parquet, etc.)
- [x] Add file format auto-detection
- [x] Add schema auto-detection for files
- [x] Extend `Client.load_data()` to support file paths
- [x] Extend `Table.write()` to support file paths
- [x] Support CSV, JSON, Parquet, Avro, ORC formats
- [x] Handle schema=None case for table creation
- [x] Tests for file loading

## Slice 7: Metadata & Inspection ✓
**Goal:** Inspect and update table/dataset metadata
- [x] Add `Dataset.get_metadata()` method
- [x] Add `Table.get_metadata()` method
- [x] Add `Table.get_schema()` method
- [x] Add `Dataset.update()` for description, labels, expiration
- [x] Add `Table.update()` for schema, description, labels
- [x] Tests for metadata operations

## Slice 8: Advanced Operations ✓
**Goal:** Streaming inserts, exports, and table copying
- [x] Add `Table.insert()` for streaming inserts
- [x] Add `Table.to_gcs()` export with format options
- [x] Add `Table.copy()` for table copying
- [x] Extend `Table.write()` to support GCS URIs
- [x] Tests for advanced operations
