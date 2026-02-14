---
displayed_sidebar: docs
keywords: ['iceberg', 'procedures', 'fast forward', 'cherry pick', 'expire snapshots', 'rewrite data files', 'add files', 'register table', 'rollback to snapshot', 'remove orphan files']
---

# Iceberg Procedures

StarRocks Iceberg Catalog 支持多种用于管理 Iceberg 表的存储过程，包括快照管理、分支管理、数据维护、元数据管理和表管理。

您必须具有相应的权限才能执行存储过程。有关权限的更多信息，请参见 [Privileges](../../../administration/user_privs/authorization/privilege_item.md) 。

## 快照管理

### 回滚到快照

将表回滚到特定快照。此操作将表的当前快照设置为指定的快照 ID。

#### `rollback_to_snapshot` 语法

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE rollback_to_snapshot(<snapshot_id>)
```

#### Parameters

`snapshot_id`: 您想要回滚到的快照 ID。

#### 示例

将表回滚到 ID 为 98765 的快照：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE rollback_to_snapshot(98765);
```

### 选择快照

选择特定的快照并将其应用到表的当前状态。此操作基于现有快照创建一个新快照，而原始快照保持不变。

#### `cherrypick_snapshot` 语法

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

#### Parameters

`snapshot_id`: 您要进行 cherry pick 的快照的 ID。

#### 示例

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

## 分支管理

### 将一个分支快速前移到另一个分支

将一个分支快速前移到另一个分支的最新快照。此操作会将源分支的快照更新为与目标分支的快照相匹配。

#### `fast_forward` 语法

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

#### 参数

- `from_branch`: 您想要快进的分支。用引号将分支名称引起来。
- `to_branch`: 您想要将 `from_branch` 快进到的分支。用引号将分支名称引起来。

#### 示例

将 `main` 分支快进到 `test-branch` 分支：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

## 数据维护

### 重写数据文件

重写数据文件以优化文件布局。此过程合并小文件，以提高查询性能并减少元数据开销。

#### `rewrite_data_files` 语法

```SQL
ALTER TABLE [catalog.][database.]table_name 
EXECUTE rewrite_data_files
("key"=value [,"key"=value, ...]) 
[WHERE <predicate>]
```

#### Parameters

##### `rewrite_data_files` 属性

`"key"=value` 键值对，用于声明手动 Compaction 行为。请注意，您需要将 key 包含在双引号中。

###### `min_file_size_bytes`

- 说明：小数据文件的上限。Compaction 期间，将合并大小小于此值的数据文件。
- 单位：Byte
- 类型：Int
- 默认值：268,435,456 (256 MB)

###### `batch_size`

- 说明：每个批次中可以处理的最大数据大小。
- 单位：Byte
- 类型：Int
- 默认值：10,737,418,240 (10 GB)

###### `rewrite_all`

- 说明：是否在 Compaction 期间重写所有数据文件，忽略使用特定要求筛选数据文件的参数。
- 单位：-
- 类型：Boolean
- 默认值：false

###### `batch_parallelism`

- 说明：Compaction 期间要处理的并行批次数。
- 单位：-
- 类型：Int
- 默认值：1

##### `WHERE` clause

- Description: 用于指定参与 Compaction 的分区所使用的过滤条件。

#### 示例

以下示例对 Iceberg 表 `t1` 中的特定分区执行手动 Compaction。这些分区由子句 `WHERE part_col = 'p1'` 表示。在这些分区中，小于 134,217,728 字节（128 MB）的数据文件将在 Compaction 期间合并。

```SQL
ALTER TABLE t1 EXECUTE rewrite_data_files("min_file_size_bytes"= 134217728) WHERE part_col = 'p1';
```

## 元数据管理

### 过期快照

过期早于特定时间戳的快照。此操作会删除过期快照的数据文件，从而帮助管理存储使用情况。

#### `expire_snapshots` 语法

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshots(
    [ [older_than =] '<datetime>' ] [,  [retain_last =] <int> ]
)
```

#### Parameters

##### `older_than`

- 描述：快照将被删除的时间戳。如果未指定，则默认删除早于 5 天（从当前时间开始计算）的文件。格式：'YYYY-MM-DD HH:MM:SS'。
- 类型：DATETIME
- 是否必须：否

##### `retain_last`

- 描述：要保留的最新快照的最大数量。达到此阈值时，将删除较早的快照。如果未指定，则默认仅保留一个快照。
- 类型：整数
- 必填：否

#### 示例

删除 '2023-12-17 00:14:38' 之前的快照，并保留两个快照：

```SQL
-- With the parameter key specified:
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots(older_than = '2023-12-17 00:14:38', retain_last = 2);

-- With the parameter key unspecified:
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots('2023-12-17 00:14:38', 2);
```

### 删除孤立文件

从表中删除未被任何有效快照引用且早于指定时间戳的孤立文件。此操作有助于清理未使用的文件并回收存储空间。

#### `remove_orphan_files` 语法

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE remove_orphan_files(
    [ [older_than =] '<datetime>' ] [,  [location =] '<string>' ]
)
```

#### Parameters

##### `older_than`

- 说明：删除孤立文件的时间戳。如果未指定，则默认删除早于 7 天（从当前时间开始计算）的文件。格式：'YYYY-MM-DD HH:MM:SS'。
- 类型：DATETIME
- 是否必须：否

##### `location`

- 描述：您要从中删除孤立文件的目录。它必须是表位置的子目录。如果未指定，则默认使用表位置。
- 类型：STRING
- 是否必须：否

#### 示例

从表位置的子目录 `sub_dir` 中删除早于 '2024-01-01 00:00:00' 的孤立文件：

```SQL
-- With the parameter key specified:
ALTER TABLE iceberg.sales.order
EXECUTE remove_orphan_files(older_than = '2024-01-01 00:00:00', location = 's3://iceberg-bucket/iceberg_db/iceberg_table/sub_dir');

-- With the parameter key unspecified:
ALTER TABLE iceberg.sales.order
EXECUTE remove_orphan_files('2024-01-01 00:00:00', 's3://bucket-test/iceberg_db/iceberg_table/sub_dir');
```

## 表管理

### 添加文件

从源表或特定位置向 Iceberg 表添加数据文件。此过程支持 Parquet 和 ORC 文件格式。

#### `add_files` 语法

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE add_files(
    [source_table = '<source_table>' | location = '<location>', file_format = '<format>']
    [, recursive = <boolean>]
)
```

#### 参数

必须提供 `source_table` 或 `location`，但不能同时提供两者。

##### `source_table`

- 描述：从中添加文件的源表。格式：'catalog.database.table'。
- 类型：String
- 是否必须：否

##### `location`

- 描述：从中添加文件的目录路径或文件路径。
- 类型：String
- 是否必需：否

##### `file_format`

- 描述：数据文件的格式。支持 'parquet'、'orc' 格式。
- 类型：String
- 必填：否（当使用 `location` 时，该参数为必填。）

##### `recursive`

- 描述：从某个位置添加文件时，是否递归扫描子目录。
- 类型：Boolean
- 默认值：true
- 是否必须：否

#### 示例

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

从单个文件添加文件：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(location = 's3://bucket/data/order/data.parquet', file_format = 'parquet');
```

### 注册表

使用元数据文件注册 Iceberg 表。此过程允许您将现有的 Iceberg 表添加到 catalog 中，而无需迁移数据。

#### `register_table` 语法

```SQL
CALL [catalog.]system.register_table(
    database_name = '<database_name>',
    table_name = '<table_name>',
    metadata_file = '<metadata_file_path>'
)
```

#### Parameters

##### `database_name`

- 描述：要在其中注册表的数据库的名称。
- 类型：String
- 必需：是

##### `table_name`

- 描述：要注册的表的名称。
- 类型：String
- 必填：是

##### `metadata_file`

- 描述：Iceberg 表元数据文件的路径（例如，metadata.json）。
- 类型：String
- 必需：是

#### 示例

使用元数据文件注册表：

```SQL
CALL iceberg_catalog.system.register_table(
    database_name = 'sales',
    table_name = 'order',
    metadata_file = 's3://bucket/metadata/sales/order/metadata/00001-xxxxx-xxxxx-xxxxx.metadata.json'
);
```

或者使用当前的 catalog：

```SQL
CALL system.register_table(
    database_name = 'sales',
    table_name = 'order',
    metadata_file = 's3://bucket/metadata/sales/order/metadata/00001-xxxxx-xxxxx-xxxxx.metadata.json'
);
```
