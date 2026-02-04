---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg', 'procedures', 'fast forward', 'cherry pick', 'expire snapshots', 'rewrite data files', 'add files', 'register table', 'rollback to snapshot', 'remove orphan files']
---

# Iceberg Procedures

本文档介绍 StarRocks 中 Iceberg catalog 可用的存储过程，包括快照管理、分支管理、数据维护、元数据管理和表管理操作。

您必须具有适当的权限才能执行存储过程。有关权限的更多信息，请参阅[权限](../../../administration/user_privs/authorization/privilege_item.md)。

## 快照管理

### Rollback to snapshot

将表回滚到指定的快照。此操作将表的当前快照设置为指定的快照 ID。

**`rollback_to_snapshot` 语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE rollback_to_snapshot(<snapshot_id>)
```

**参数**

`snapshot_id`: 要将表回滚到的快照 ID。

**示例**

将表回滚到 ID 为 98765 的快照：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE rollback_to_snapshot(98765);
```

### Cherry pick a snapshot

选择特定的快照并将其应用到表的当前状态。此操作将基于现有快照创建一个新快照，原始快照不受影响。

**`cherrypick_snapshot` 语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

**参数**

`snapshot_id`: 要选择的快照的 ID。

**示例**

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

---

## 分支管理

### Fast forward a branch to another

将一个分支快进合并到另一个分支的最新快照。此操作将源分支的快照更新为目标分支的快照。

**`fast_forward` 语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

**参数**

- `from_branch`: 要快进的分支名称。将分支名称用引号括起来。
- `to_branch`: 要将 `from_branch` 快进到的目标分支。将分支名称用引号括起来。

**示例**

将 `main` 分支快进到 `test-branch` 分支：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

---

## 数据维护

### Rewrite data files

重写数据文件以优化文件布局。此过程合并小文件以提高查询性能并减少元数据开销。

**`rewrite_data_files` 语法**

```SQL
ALTER TABLE [catalog.][database.]table_name 
EXECUTE rewrite_data_files
("key"=value [, "key"=value, ...]) 
[WHERE <predicate>]
```

**参数**

##### `rewrite_data_files` 属性

声明手动压缩行为的 `"key"=value` 键值对。请注意，需要用双引号包裹其中的键。

###### `min_file_size_bytes`

- 描述：小文件的上限大小。小于此值的数据文件将在压缩期间被合并。
- 单位：字节
- 类型：Int
- 默认值：268,435,456 (256 MB)

###### `batch_size`

- 描述：每个批次可以处理的最大数据大小。
- 单位：字节
- 类型：Int
- 默认值：10,737,418,240 (10 GB)

###### `rewrite_all`

- 描述：是否在压缩期间重写所有数据文件，忽略过滤特定要求数据文件的参数。
- 单位：-
- 类型：Boolean
- 默认值：false

###### `batch_parallelism`

- 描述：压缩期间要处理的并行批次数。
- 单位：-
- 类型：Int
- 默认值：1

##### `WHERE` 子句

- 描述：用于指定参与压缩的分区的过滤器谓词。

**示例**

以下示例对 Iceberg 表 `t1` 中的特定分区执行手动压缩。分区由子句 `WHERE part_col = 'p1'` 表示。在这些分区中，小于 134,217,728 字节（128 MB）的数据文件将在压缩期间被合并。

```SQL
ALTER TABLE t1 EXECUTE rewrite_data_files("min_file_size_bytes"= 134217728) WHERE part_col = 'p1';
```

---

## 元数据管理

### Expire snapshots

过期特定时间点之前的快照。此操作将删除过期快照的数据文件，有助于管理存储使用。

**`expire_snapshots` 语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshots('<datetime>')
```

**参数**

`datetime`: 要过期快照的时间戳。格式：'YYYY-MM-DD HH:MM:SS'。

**示例**

过期 '2023-12-17 00:14:38' 之前的快照：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots('2023-12-17 00:14:38')
```

### Remove orphan files

从表中删除未被任何有效快照引用且早于指定时间戳的孤立文件。此操作有助于清理未使用的文件并回收存储空间。

**`remove_orphan_files` 语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE remove_orphan_files([older_than = '<datetime>'])
```

**参数**

`older_than`（可选）：要删除孤立文件的时间戳。如果未指定，默认将删除 7 天前的文件。格式：'YYYY-MM-DD HH:MM:SS'。

**示例**

删除早于 '2024-01-01 00:00:00' 的孤立文件：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE remove_orphan_files(older_than = '2024-01-01 00:00:00');
```

使用默认保留期（7 天）删除孤立文件：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE remove_orphan_files();
```

---

## 表管理

### Add files

从源表或特定位置向 Iceberg 表添加数据文件。此过程支持 Parquet 和 ORC 文件格式。

**`add_files` 语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE add_files(
    [source_table = '<source_table>' | location = '<location>', file_format = '<format>']
    [, recursive = <boolean>]
)
```

**参数**

必须提供 `source_table` 或 `location` 中的一个，但不能同时提供两者。

##### `source_table`（可选）

- 描述：要从中添加文件的源表。格式：'catalog.database.table'。
- 类型：String

##### `location`（可选）

- 描述：要从中添加文件的目录路径或文件路径。
- 类型：String

##### `file_format`（使用 `location` 时必需）

- 描述：数据文件的格式。支持的值：'parquet'、'orc'。
- 类型：String

##### `recursive`（可选）

- 描述：从位置添加文件时是否递归扫描子目录。
- 类型：Boolean
- 默认值：true

**示例**

从源表添加文件：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(source_table = 'hive_catalog.sales.source_order');
```

从特定位置添加 Parquet 格式的文件：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(location = 's3://bucket/data/order/', file_format = 'parquet', recursive = true);
```

从单个文件添加：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(location = 's3://bucket/data/order/data.parquet', file_format = 'parquet');
```

### Register table

使用元数据文件注册 Iceberg 表。此过程允许您将现有的 Iceberg 表添加到目录中，而无需迁移数据。

**`register_table` 语法**

```SQL
CALL [catalog.]system.register_table(
    database_name = '<database_name>',
    table_name = '<table_name>',
    metadata_file = '<metadata_file_path>'
)
```

**参数**

##### `database_name`

- 描述：要在其中注册表的数据库的名称。
- 类型：String
- 必需：是

##### `table_name`

- 描述：要注册的表的名称。
- 类型：String
- 必需：是

##### `metadata_file`

- 描述：Iceberg 表元数据文件的路径（例如 metadata.json）。
- 类型：String
- 必需：是

**示例**

使用元数据文件注册表：

```SQL
CALL iceberg_catalog.system.register_table(
    database_name = 'sales',
    table_name = 'order',
    metadata_file = 's3://bucket/metadata/sales/order/metadata/00001-xxxxx-xxxxx-xxxxx.metadata.json'
);
```

或使用当前 catalog：

```SQL
CALL system.register_table(
    database_name = 'sales',
    table_name = 'order',
    metadata_file = 's3://bucket/metadata/sales/order/metadata/00001-xxxxx-xxxxx-xxxxx.metadata.json'
);
```
