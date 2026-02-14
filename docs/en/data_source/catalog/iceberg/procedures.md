---
displayed_sidebar: docs
keywords: ['iceberg', 'procedures', 'fast forward', 'cherry pick', 'expire snapshots', 'rewrite data files', 'add files', 'register table', 'rollback to snapshot', 'remove orphan files']
---

# Iceberg Procedures

StarRocks Iceberg Catalog supports a variety of procedures for managing Iceberg tables, including snapshot management, branch management, data maintenance, metadata management, and table management.

You must have the appropriate privileges to execute procedures. For more information about privileges, see [Privileges](../../../administration/user_privs/authorization/privilege_item.md).

## Snapshot management

### Rollback to snapshot

Rolls back the table to a specific snapshot. This operation sets the table's current snapshot to the specified snapshot ID.

#### `rollback_to_snapshot` Syntax

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE rollback_to_snapshot(<snapshot_id>)
```

#### Parameters

`snapshot_id`: ID of the snapshot to which you want to roll back the table.

#### Example

Roll back the table to snapshot with ID 98765:

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE rollback_to_snapshot(98765);
```

### Cherry pick a snapshot

Cherry picks a specific snapshot and applies it to the current state of the table. This operation creates a new snapshot based on an existing snapshot, while the original snapshot remains unchanged.

#### `cherrypick_snapshot` Syntax

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

#### Parameters

`snapshot_id`: ID of the snapshot which you want to cherry pick.

#### Example

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

## Branch management

### Fast forward a branch to another

Fast-forwards one branch to another branch's latest snapshot. This operation updates the source branch's snapshot to match the target branch's snapshot.

#### `fast_forward` Syntax

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

#### Parameters

- `from_branch`: The branch you want to fast forward. Wrap the branch name in quotes.
- `to_branch`: The branch to which you want to fast forward the `from_branch`. Wrap the branch name in quotes.

#### Example

Fast forward the `main` branch to the branch `test-branch`:

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

## Data maintenance

### Rewrite data files

Rewrites data files to optimize file layout. This procedure merges small files to improve query performance and reduce metadata overhead.

#### `rewrite_data_files` Syntax

```SQL
ALTER TABLE [catalog.][database.]table_name 
EXECUTE rewrite_data_files
("key"=value [,"key"=value, ...]) 
[WHERE <predicate>]
```

#### Parameters

##### `rewrite_data_files` properties

`"key"=value` pairs that declare the manual compaction behaviors. Note that you need to wrap the key in double quotes.

###### `min_file_size_bytes`

- Description: The upper limit of a small data file. Data files whose size is less than this value will be merged during the compaction.
- Unit: Byte
- Type: Int
- Default: 268,435,456 (256 MB)

###### `batch_size`

- Description: The maximum size of data that can be processed in each batch.
- Unit: Byte
- Type: Int
- Default: 10,737,418,240 (10 GB)

###### `rewrite_all`

- Description: Whether to rewrite all data files during the compaction, ignoring the parameters that filter data files with specific requirements.
- Unit: -
- Type: Boolean
- Default: false

###### `batch_parallelism`

- Description: The number of parallel batches to process during the compaction.
- Unit: -
- Type: Int
- Default: 1

##### `WHERE` clause

- Description: The filter predicate used to specify the partition(s) to be involved in the compaction.

#### Example

The following example performs manual Compaction on specific partitions in the Iceberg table `t1`. The partitions are represented by the clause `WHERE part_col = 'p1'`. In these partitions, data files that are smaller than 134,217,728 bytes (128 MB) will be merged during the Compaction.

```SQL
ALTER TABLE t1 EXECUTE rewrite_data_files("min_file_size_bytes"= 134217728) WHERE part_col = 'p1';
```

## Metadata management

### Expire snapshots

Expires snapshots older than a specific timestamp. This operation deletes the data files of the expired snapshots, helping to manage storage usage.

#### `expire_snapshots` Syntax

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshots(
    [ [older_than =] '<datetime>' ] [,  [retain_last =] <int> ]
)
```

#### Parameters

##### `older_than`

- Description: The timestamp before which snapshots will be removed. If not specified, files older than 5 days (from the current time) will be removed by default. Format: 'YYYY-MM-DD HH:MM:SS'.
- Type: DATETIME
- Required: No

##### `retain_last`

- Description: The maximum number of most recent snapshots to retain. The less recent snapshots will be removed when this threshold is reached. If not specified, only one snapshot will be retained by default.
- Type: Integer
- Required: No

#### Example

Expire snapshots before '2023-12-17 00:14:38' and retain two snapshots:

```SQL
-- With the parameter key specified:
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots(older_than = '2023-12-17 00:14:38', retain_last = 2);

-- With the parameter key unspecified:
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots('2023-12-17 00:14:38', 2);
```

### Remove orphan files

Removes orphan files from the table that are not referenced by any valid snapshot and are older than a specified timestamp. This operation helps clean up unused files and reclaim storage space.

#### `remove_orphan_files` Syntax

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE remove_orphan_files(
    [ [older_than =] '<datetime>' ] [,  [location =] '<string>' ]
)
```

#### Parameters

##### `older_than`

- Description: The timestamp before which orphan files will be removed. If not specified, files older than 7 days (from the current time) will be removed by default. Format: 'YYYY-MM-DD HH:MM:SS'.
- Type: DATETIME
- Required: No

##### `location`

- Description: The directory from which you want to remove orphan files. It must be a sub-directory of the table location. If not specified, the table location will be used by default.
- Type: STRING
- Required: No

#### Example

Remove orphan files older than '2024-01-01 00:00:00' from the sub-directory `sub_dir` of the table location:

```SQL
-- With the parameter key specified:
ALTER TABLE iceberg.sales.order
EXECUTE remove_orphan_files(older_than = '2024-01-01 00:00:00', location = 's3://iceberg-bucket/iceberg_db/iceberg_table/sub_dir');

-- With the parameter key unspecified:
ALTER TABLE iceberg.sales.order
EXECUTE remove_orphan_files('2024-01-01 00:00:00', 's3://bucket-test/iceberg_db/iceberg_table/sub_dir');
```

## Table management

### Add files

Adds data files to an Iceberg table from either a source table or a specific location. This procedure supports Parquet and ORC file formats.

#### `add_files` Syntax

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE add_files(
    [source_table = '<source_table>' | location = '<location>', file_format = '<format>']
    [, recursive = <boolean>]
)
```

#### Parameters

Either `source_table` or `location` must be provided, but not both.

##### `source_table`

- Description: The source table from which to add files. Format: 'catalog.database.table'.
- Type: String
- Required: No

##### `location`

- Description: The directory path or file path from which to add files.
- Type: String
- Required: No

##### `file_format`

- Description: The format of the data files. Supported values: 'parquet', 'orc'.
- Type: String
- Required: No (required when using `location`)

##### `recursive`

- Description: Whether to recursively scan subdirectories when adding files from a location.
- Type: Boolean
- Default: true
- Required: No

#### Example

Add files from a source table:

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(source_table = 'hive_catalog.sales.source_order');
```

Add files from a specific location with Parquet format:

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(location = 's3://bucket/data/order/', file_format = 'parquet', recursive = true);
```

Add files from a single file:

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(location = 's3://bucket/data/order/data.parquet', file_format = 'parquet');
```

### Register table

Registers an Iceberg table using a metadata file. This procedure allows you to add an existing Iceberg table to the catalog without migrating data.

#### `register_table` Syntax

```SQL
CALL [catalog.]system.register_table(
    database_name = '<database_name>',
    table_name = '<table_name>',
    metadata_file = '<metadata_file_path>'
)
```

#### Parameters

##### `database_name`

- Description: The name of the database in which to register the table.
- Type: String
- Required: Yes

##### `table_name`

- Description: The name of the table to register.
- Type: String
- Required: Yes

##### `metadata_file`

- Description: The path to the Iceberg table metadata file (e.g., metadata.json).
- Type: String
- Required: Yes

#### Example

Register a table using a metadata file:

```SQL
CALL iceberg_catalog.system.register_table(
    database_name = 'sales',
    table_name = 'order',
    metadata_file = 's3://bucket/metadata/sales/order/metadata/00001-xxxxx-xxxxx-xxxxx.metadata.json'
);
```

Or use the current catalog:

```SQL
CALL system.register_table(
    database_name = 'sales',
    table_name = 'order',
    metadata_file = 's3://bucket/metadata/sales/order/metadata/00001-xxxxx-xxxxx-xxxxx.metadata.json'
);
```
