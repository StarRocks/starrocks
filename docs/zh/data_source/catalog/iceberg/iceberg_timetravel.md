---
displayed_sidebar: docs
---

import Beta from '../../../_assets/commonMarkdown/_beta.mdx'

# 使用 Iceberg Catalog 的时间旅行

<Beta />

本文介绍了 StarRocks 针对 Iceberg catalogs 的时间旅行功能。此功能从 v3.4.0 开始支持。

## 概述

每个 Iceberg 表都会维护一个元数据快照日志，记录其所做的更改。数据库可以通过访问这些历史快照对 Iceberg 表执行时间旅行查询。Iceberg 支持分支和标记快照，以实现复杂的快照生命周期管理，允许每个分支或标记根据自定义保留策略维护其自身的生命周期。有关 Iceberg 分支和标记功能的更多信息，请参见[官方文档](https://iceberg.apache.org/docs/latest/branching/)。

通过集成 Iceberg 的快照分支和标记功能，StarRocks 支持在 Iceberg catalogs 中创建和管理分支和标记，并对表进行时间旅行查询。

## 管理分支、标记和快照

### 创建分支

**语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
CREATE [OR REPLACE] BRANCH [IF NOT EXISTS] <branch_name>
[AS OF VERSION <snapshot_id>]
[RETAIN <int> { DAYS | HOURS | MINUTES }]
[WITH SNAPSHOT RETENTION 
    { minSnapshotsToKeep | maxSnapshotAge | minSnapshotsToKeep maxSnapshotAge }]

minSnapshotsToKeep ::= <int> SNAPSHOTS

maxSnapshotAge ::= <int> { DAYS | HOURS | MINUTES }
```

**参数**

- `branch_name`: 要创建的分支名称。
- `AS OF VERSION`: 用于创建分支的快照（版本）ID。
- `RETAIN`: 分支的保留时间。格式：`<int> <unit>`。支持的单位：`DAYS`，`HOURS` 和 `MINUTES`。例如：`7 DAYS`，`12 HOURS` 或 `30 MINUTES`。
- `WITH SNAPSHOT RETENTION`: 要保留的最小快照数量和/或快照的最大保留时间。

**示例**

基于表 `iceberg.sales.order` 的版本（快照 ID）`12345` 创建一个分支 `test-branch`，保留该分支 `7` 天，并在该分支上至少保留 `2` 个快照。

```SQL
ALTER TABLE iceberg.sales.order CREATE BRANCH `test-branch` 
AS OF VERSION 12345
RETAIN 7 DAYS
WITH SNAPSHOT RETENTION 2 SNAPSHOTS;
```

基于表 `iceberg.sales.order` 的版本（快照 ID）`12345` 创建一个分支 `test-branch2`，保留该分支 `7` 天，该分支上的快照至多保留 `2` 天。

```SQL
ALTER TABLE iceberg.sales.order CREATE BRANCH `test-branch2` 
AS OF VERSION 12345
RETAIN 7 DAYS
WITH SNAPSHOT RETENTION 2 DAYS;
```

基于表 `iceberg.sales.order` 的版本（快照 ID）`12345` 创建一个分支 `test-branch3`，保留该分支 `7` 天，并在该分支上至少保留 `2` 个快照，每个快照至多保留 `2` 天。

```SQL
ALTER TABLE iceberg.sales.order CREATE BRANCH `test-branch3` 
AS OF VERSION 12345
RETAIN 7 DAYS
WITH SNAPSHOT RETENTION 2 SNAPSHOTS 2 DAYS;
```

### 将数据导入到表的特定分支

**语法**

```SQL
INSERT INTO [catalog.][database.]table_name
[FOR] VERSION AS OF <branch_name>
<query_statement>
```

**参数**

- `branch_name`: 要导入数据的表分支名称。
- `query_statement`: 查询语句，其结果将被导入到目标表中。可以是 StarRocks 支持的任何 SQL 语句。

**示例**

将查询结果导入到表 `iceberg.sales.order` 的分支 `test-branch` 中。

```SQL
INSERT INTO iceberg.sales.order
FOR VERSION AS OF `test-branch`
SELECT c1, k1 FROM tbl;
```

### 创建标记

**语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
CREATE [OR REPLACE] TAG [IF NOT EXISTS] <tag_name>
[AS OF VERSION <snapshot_id>]
[RETAIN <int> { DAYS | HOURS | MINUTES }]
```

**参数**

- `tag_name`: 要创建的标记名称。
- `AS OF VERSION`: 用于创建标记的快照（版本）ID。
- `RETAIN`: 标记的保留时间。格式：`<int> <unit>`。支持的单位：`DAYS`，`HOURS` 和 `MINUTES`。例如：`7 DAYS`，`12 HOURS` 或 `30 MINUTES`。

**示例**

基于表 `iceberg.sales.order` 的版本（快照 ID）`12345` 创建一个标记 `test-tag`，并保留该标记 `7` 天。

```SQL
ALTER TABLE iceberg.sales.order CREATE TAG `test-tag` 
AS OF VERSION 12345
RETAIN 7 DAYS;
```

### 快进一个分支到另一个分支

**语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

**参数**

- `from_branch`: 要快进的分支。用引号括住分支名称。
- `to_branch`: 要快进到的分支。用引号括住分支名称。

**示例**

将 `main` 分支快进到分支 `test-branch`。

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

### 拣选一个快照

您可以拣选一个特定的快照并将其应用到表的当前状态。此操作将基于现有快照创建一个新快照，原始快照不会受到影响。

**语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

**参数**

`snapshot_id`: 您要拣选的快照 ID。

**示例**

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

### 过期快照

您可以使快照在特定时间点之前过期。此操作将删除过期快照的数据文件。

**语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshot('<datetime>')
```

**示例**

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshot('2023-12-17 00:14:38')
```

### 删除分支或标记

**语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
DROP { BRANCH <branch_name> | TAG <tag_name> }
```

**示例**

```SQL
ALTER TABLE iceberg.sales.order
DROP BRANCH `test-branch`;

ALTER TABLE iceberg.sales.order
DROP TAG `test-tag`;
```

## 使用时间旅行查询

### 时间旅行到特定分支或标记

**语法**

```SQL
[FOR] VERSION AS OF '<branch_or_tag>'
```

**参数**

`tag_or_branch`: 您想要时间旅行到的分支或标记的名称。如果指定了分支名称，查询将时间旅行到该分支的头快照。如果指定了标记名称，查询将时间旅行到标记引用的快照。

**示例**

```SQL
-- 时间旅行到分支的头快照。
SELECT * FROM iceberg.sales.order VERSION AS OF 'test-branch';
-- 时间旅行到标记引用的快照。
SELECT * FROM iceberg.sales.order VERSION AS OF 'test-tag';
```

### 时间旅行到特定快照

**语法**

```SQL
[FOR] VERSION AS OF '<snapshot_id>'
```

**参数**

`snapshot_id`: 您想要时间旅行到的快照 ID。

**示例**

```SQL
SELECT * FROM iceberg.sales.order VERSION AS OF 12345;
```

### 时间旅行到特定日期时间或日期

**语法**

```SQL
[FOR] TIMESTAMP AS OF { '<datetime>' | '<date>' | date_and_time_function }
```

**参数**

`date_and_time_function`: StarRocks 支持的任何[日期和时间函数](../../../sql-reference/sql-functions/date-time-functions/now.md)。

**示例**

```SQL
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF '1986-10-26 01:21:00';
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF '1986-10-26';
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF now();
```