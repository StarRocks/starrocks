---
displayed_sidebar: docs
---

# Iceberg Metadata Tables

This topic describes how to inspect the metadata information of Iceberg tables in StarRocks.

## Overview

From V3.4.1 onwards, StarRocks supports Iceberg metadata tables. These metadata tables contain a variety of information about Iceberg tables, such as table change history, snapshots, and manifests. You can query each metadata table by appending the metadata table name to the original table name.

Currently, StarRocks supports the following Iceberg metadata tables:

| Metadata table         | Description                                                  |
| :--------------------- | :----------------------------------------------------------- |
| `history`              | Shows a log of metadata changes made to the table.           |
| `metadata_log_entries` | Shows the metadata log entries for the table.                |
| `snapshots`            | Shows details about the table snapshots.                     |
| `manifests`            | Shows an overview of the manifests associated with the snapshots in the table’s log. |
| `partitions`           | Shows details about the partitions in the table.             |
| `files`                | Shows details about the data files and delete files in the current snapshot of the table. |
| `refs`                 | Shows details about the Iceberg references, including branches and tags. |

## `history` table

Usage:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$history;
```

Output:

| Field               | Description                                                  |
| :------------------ | :----------------------------------------------------------- |
| made_current_at     | The time when the snapshot became the current snapshot.      |
| snapshot_id         | The ID of the snapshot.                                      |
| parent_id           | The ID of the parent snapshot.                               |
| is_current_ancestor | Whether this snapshot is an ancestor of the current snapshot. |

## `metadata_log_entries` table

Usage:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$metadata_log_entries;
```

Output:

| Field                  | Description                                                  |
| :--------------------- | :----------------------------------------------------------- |
| timestamp              | The time when the metadata was recorded.                     |
| file                   | The location of the metadata file.                           |
| latest_snapshot_id     | The ID of the latest snapshot when the metadata was updated. |
| latest_schema_id       | The ID of the latest schema when the metadata was updated.   |
| latest_sequence_number | The data sequence number of the metadata file.               |

## `snapshots` table

Usage:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$snapshots;
```

Output:

| Field         | Description                                                  |
| :------------ | :----------------------------------------------------------- |
| committed_at  | The time when the snapshot was committed.                    |
| snapshot_id   | The ID for the snapshot.                                     |
| parent_id     | The ID for the parent snapshot.                              |
| operation     | The type of operation performed on the Iceberg table. Valid values:<ul><li>`append`: New data is appended.</li><li>`replace`: Files are removed and replaced without changing the data in the table.</li><li>`overwrite`: Old data is overwritten by new data.</li><li>`delete`: Data is deleted from the table.</li></ul> |
| manifest_list | The list of Avro manifest files that contain detailed information about snapshot changes. |
| summary       | A summary of the changes made from the previous snapshot to the current snapshot. |

## `manifests` table

Usage:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$manifests;
```

Output:

| Field                     | Description                                                  |
| :------------------------ | :----------------------------------------------------------- |
| path                      | The location of the manifest file.                           |
| length                    | The length of the manifest file.                             |
| partition_spec_id         | The ID for the partition specification that is used to write the manifest file. |
| added_snapshot_id         | The ID of the snapshot during which this manifest entry has been added. |
| added_data_files_count    | The number of data files with status `ADDED` in the manifest file. |
| added_rows_count          | The total number of rows in all data files with status `ADDED` in the manifest file. |
| existing_data_files_count | The number of data files with status `EXISTING` in the manifest file. |
| existing_rows_count       | The total number of rows in all data files with status `EXISTING` in the manifest file. |
| deleted_data_files_count  | The number of data files with status `DELETED` in the manifest file. |
| deleted_rows_count        | The total number of rows in all data files with status `DELETED` in the manifest file. |
| partition_summaries       | Partition range metadata.                                    |

## `partitions` table

Usage:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$partitions;
```

Output:

| Field                         | Description                                                  |
| :---------------------------- | :----------------------------------------------------------- |
| partition_value               | The mapping of the partition column names to the partition column values. |
| spec_id                       | The partition Spec ID of files.                              |
| record_count                  | The number of records in the partition.                      |
| file_count                    | The number of files mapped in the partition.                 |
| total_data_file_size_in_bytes | The size of all the data files in the partition.             |
| position_delete_record_count  | The total row count of Position Delete files in the partition. |
| position_delete_file_count    | The number of Position Delete files in the partition.        |
| equality_delete_record_count  | The total row count of Equality Delete files in the partition. |
| equality_delete_file_count    | The number of Position Equality files in the partition.      |
| last_updated_at               | The time when the partition was updated most recently.       |

## `files` table

Usage:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$files;
```

Output:

| Field              | Description                                                  |
| :----------------- | :----------------------------------------------------------- |
| content            | The type of content stored in the file. Valid values: `DATA(0)`, `POSITION_DELETES(1)`, and `EQUALITY_DELETES(2)`. |
| file_path          | The location of the data file.                               |
| file_format        | The format of the data file.                                 |
| spec_id            | The Spec ID that is used to track the file containing a row. |
| record_count       | The number of entries contained in the data file.            |
| file_size_in_bytes | The size of the data file.                                   |
| column_sizes       | The mapping between the Iceberg column ID and its corresponding size in the file. |
| value_counts       | The mapping between the Iceberg column ID and its corresponding count of entries in the file. |
| null_value_counts  | The mapping between the Iceberg column ID and its corresponding count of `NULL` values in the file. |
| nan_value_counts   | The mapping between the Iceberg column ID and its corresponding count of non- numerical values in the file. |
| lower_bounds       | The mapping between the Iceberg column ID and its corresponding lower bound in the file. |
| upper_bounds       | The mapping between the Iceberg column ID and its corresponding upper bound in the file. |
| split_offsets      | The list of recommended split locations.                     |
| sort_id            | The ID representing sort order for this file.                |
| equality_ids       | The set of field IDs used for equality comparison in equality delete files. |
| key_metadata       | The metadata about the encryption key that is used to encrypt this file, if applicable. |

## `refs` table

Usage:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$refs;
```

Output:

| Field                   | Description                                                  |
| :---------------------- | :----------------------------------------------------------- |
| name                    | The name of the reference.                                   |
| type                    | The type of the reference. Valid values: `BRANCH` or `TAG`.  |
| snapshot_id             | The snapshot ID of the reference.                            |
| max_reference_age_in_ms | The maximum age of the reference before it could be expired. |
| min_snapshots_to_keep   | For branch only, the minimum number of snapshots to keep in a branch. |
| max_snapshot_age_in_ms  | For branch only, the max snapshot age allowed in a branch. Older snapshots in the branch will be expired. |
