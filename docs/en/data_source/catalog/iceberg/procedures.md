---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg', 'procedures', 'fast forward', 'cherry pick', 'expire snapshots', 'rewrite data files']
---

# Iceberg Procedures

This document describes stored procedures available for Iceberg catalogs in StarRocks, including operations for branch management, snapshot manipulation, and data optimization.

You must have the appropriate privileges to execute procedures. For more information about privileges, see [Privileges](../../../administration/user_privs/authorization/privilege_item.md).

---

## Fast forward a branch to another

Fast-forwards one branch to another branch's latest snapshot. This operation updates the source branch's snapshot to match the target branch's snapshot.

**`fast_forward` Syntax**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

**Parameters**

- `from_branch`: The branch you want to fast forward. Wrap the branch name in quotes.
- `to_branch`: The branch to which you want to fast forwards the `from_branch`. Wrap the branch name in quotes.

**Example**

Fast forward the `main` branch to the branch `test-branch`.

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

---

## Cherry pick a snapshot 

Cherry picks a specific snapshot and applies it to the current state of the table. This operation creates a new snapshot based on an existing snapshot, while the original snapshot remains unchanged.

**`cherrypick_snapshot` Syntax**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

**Parameter**

`snapshot_id`: ID of the snapshot which you want to cherry pick.

**Example**

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

---

## Expire snapshots

Expires snapshots older than a specific timestamp. This operation deletes the data files of the expired snapshots, helping to manage storage usage.

**`expire_snapshots` Syntax**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshots('<datetime>')
```

**Parameter**

- `datetime`: The timestamp before which snapshots will be expired. Format: 'YYYY-MM-DD HH:MM:SS'.

**Example**

Expire snapshots before '2023-12-17 00:14:38':

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots('2023-12-17 00:14:38')
```

---

## rewrite_data_files

Rewrites data files to optimize file layout. This procedure merges small files to improve query performance and reduce metadata overhead.

**Syntax**

```SQL
ALTER TABLE [catalog.][database.]table_name 
EXECUTE rewrite_data_files
("key"=value [,"key"=value, ...]) 
[WHERE <predicate>]
```

**Parameters**

##### `rewrite_data_files` properties

`"key"=value` pairs that declare the manual compaction behaviors. Note that you need to wrap the key in double quotes.

###### `min_file_size_bytes`

- Description: The upper limit of a small data file. Data files whose size is less this value will be merged during the compaction.
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

##### `WHERE` clause

- Description: The filter predicate used to specify the partition(s) to be involved in the compaction.

**Example**

The following example performs manual compaction on specific partitions in the Iceberg table `t1`. The partitions are represented by the clause `WHERE part_col = 'p1'`. In these partitions, data files that are smaller than 134,217,728 bytes (128 MB) will be merged during the compaction.

```SQL
ALTER TABLE t1 EXECUTE rewrite_data_files("min_file_size_bytes"= 134217728) WHERE part_col = 'p1';
```

---
