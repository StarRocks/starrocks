---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg', 'procedures', 'fast forward', 'cherry pick', 'expire snapshots', 'rewrite data files']
---

# Iceberg Procedures

本文档介绍 StarRocks 中 Iceberg catalog 可用的存储过程，包括分支管理、快照操作和数据优化操作。

您必须具有适当的权限才能执行存储过程。有关权限的更多信息，请参阅[权限](../../../administration/user_privs/authorization/privilege_item.md)。

---

## Fast forward a branch to another

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

## Cherry pick a snapshot

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

## Expire snapshots

过期特定时间点之前的快照。此操作将删除过期快照的数据文件，有助于管理存储使用。

**`expire_snapshots` 语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshots('<datetime>')
```

**参数**

- `datetime`: 要过期快照的时间戳。格式：'YYYY-MM-DD HH:MM:SS'。

**示例**

过期 '2023-12-17 00:14:38' 之前的快照：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots('2023-12-17 00:14:38')
```

---

## rewrite_data_files

重写数据文件以优化文件布局。此过程合并小文件以提高查询性能并减少元数据开销。

**语法**

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

##### `WHERE` 子句

- 描述：用于指定参与压缩的分区的过滤器谓词。

**示例**

以下示例对 Iceberg 表 `t1` 中的特定分区执行手动压缩。分区由子句 `WHERE part_col = 'p1'` 表示。在这些分区中，小于 134,217,728 字节（128 MB）的数据文件将在压缩期间被合并。

```SQL
ALTER TABLE t1 EXECUTE rewrite_data_files("min_file_size_bytes"= 134217728) WHERE part_col = 'p1';
```

---
