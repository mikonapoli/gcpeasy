# BigQuery Implementation TODO

## Slice 1: Basic Query Execution ✓
**Goal:** Run SQL queries and get DataFrames back
- [x] Create `geasyp/bq.py` module structure
- [x] Implement `init()` function returning Client
- [x] Implement `Client` class with `_gcp` attribute
- [x] Add `Client.__call__()` for query execution → DataFrame
- [x] Add `Client.query()` with basic parameterized query support
- [x] Handle query results → DataFrame conversion
- [x] Basic error handling and authentication
- [x] Type hints and docstrings
- [x] Basic tests

## Slice 2: Dataset Discovery & Navigation
**Goal:** List and access datasets and tables
- [ ] Implement `Dataset` class with `id` property
- [ ] Add `Client.datasets()` to list all datasets
- [ ] Add `Client.dataset(name)` to get Dataset object
- [ ] Add `Dataset.exists()` method
- [ ] Add `Dataset.tables()` to list tables
- [ ] Add `Client.tables(dataset_id)` convenience method
- [ ] Type hints and docstrings
- [ ] Tests for listing/navigation

## Slice 3: Table Reading
**Goal:** Read table data into DataFrames
- [ ] Implement `Table` class with `id` property (fully qualified)
- [ ] Add `Dataset.table(name)` to get Table object
- [ ] Add `Table.exists()` method
- [ ] Add `Table.read()` returning DataFrame
- [ ] Handle pagination properly
- [ ] Type hints and docstrings
- [ ] Tests for table reading

## Slice 4: DataFrame → Table (Simple)
**Goal:** Write DataFrames to tables
- [ ] Add schema auto-detection for DataFrames
- [ ] Add DataFrame → BigQuery type conversion
- [ ] Implement dict → SchemaField conversion ({"name": "STRING"})
- [ ] Add `Table.write(df)` with WRITE_TRUNCATE default
- [ ] Add `Client.load_data(table_id, df)` convenience method
- [ ] Handle write dispositions (TRUNCATE, APPEND, EMPTY)
- [ ] Tests for DataFrame writing

## Slice 5: Table Creation & Management
**Goal:** Create/delete tables and datasets explicitly
- [ ] Add `Dataset.create()` with location, description, expiration
- [ ] Add `Dataset.delete()` with delete_contents option
- [ ] Add `Table.create()` with schema dict, partitioning, clustering
- [ ] Add `Table.delete()` method
- [ ] Handle exists_ok/not_found_ok flags
- [ ] Validate table_id formats (dataset.table vs project.dataset.table)
- [ ] Tests for CRUD operations

## Slice 6: File Loading
**Goal:** Load data from files (CSV, JSON, Parquet, etc.)
- [ ] Add file format auto-detection
- [ ] Add schema auto-detection for files
- [ ] Extend `Client.load_data()` to support file paths
- [ ] Extend `Table.write()` to support file paths
- [ ] Support CSV, JSON, Parquet, Avro, ORC formats
- [ ] Handle schema=None case for table creation
- [ ] Tests for file loading

## Slice 7: Metadata & Inspection
**Goal:** Inspect and update table/dataset metadata
- [ ] Add `Dataset.get_metadata()` method
- [ ] Add `Table.get_metadata()` method
- [ ] Add `Table.get_schema()` method
- [ ] Add `Dataset.update()` for description, labels, expiration
- [ ] Add `Table.update()` for schema, description, labels
- [ ] Tests for metadata operations

## Slice 8: Advanced Operations
**Goal:** Streaming inserts, exports, and table copying
- [ ] Add `Table.insert()` for streaming inserts
- [ ] Add `Table.to_gcs()` export with format options
- [ ] Add `Table.copy()` for table copying
- [ ] Extend `Table.write()` to support GCS URIs
- [ ] Tests for advanced operations
