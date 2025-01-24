---
displayed_sidebar: docs
---

# Time Travel with Iceberg Catalog

本文介绍了 StarRocks 针对 Iceberg Catalog 支持的 Time Travel 功能。该功能从 v3.4.0 开始支持。

## 概述

每个 Iceberg 表都维护一个元数据快照日志，用于记录表的变更历史。数据库可以通过访问 Iceberg 表的历史快照时间进行 Time Travel 查询。同时，Iceberg 提供了快照分支（Branching）和标记（Tagging）功能，用于复杂的快照生命周期管理。每个分支或标签可以基于自定义的保留策略独立管理其生命周期。有关 Iceberg 快照分支和标记功能的更多信息，请参阅 [Iceberg 官方文档](https://iceberg.apache.org/docs/latest/branching/)。

通过集成 Iceberg 的快照分支和标记功能，StarRocks 支持在 Iceberg Catalog 中创建和管理分支与标签，并对表执行 Time Travel 查询。

## 管理分支、标签和快照

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

- `branch_name`：要创建的分支名称。
- `AS OF VERSION`：用于创建分支的快照（版本）ID。
- `RETAIN`：分支保留时间，格式为 `<int> <unit>`。支持的单位有 `DAYS`、`HOURS` 和 `MINUTES`。示例：`7 DAYS`、`12 HOURS` 或 `30 MINUTES`。
- `WITH SNAPSHOT RETENTION`：快照保留策略，包括最少保留快照数量和/或快照的最长保留时间。

**示例**

基于表 `iceberg.sales.order` 的快照（ID 为 `12345`）创建名为 `test-branch` 的分支，分支保留时间为 `7` 天，并至少保留 `2` 个快照。

```SQL
ALTER TABLE iceberg.sales.order CREATE BRANCH `test-branch` 
AS OF VERSION 12345
RETAIN 7 DAYS
WITH SNAPSHOT RETENTION 2 SNAPSHOTS;
```

### 向特定分支导入数据

**语法**

```SQL
INSERT INTO [catalog.][database.]table_name
[FOR] VERSION AS OF <branch_name>
<query_statement>
```

**参数**

- `branch_name`：要导入数据的分支名称。
- `query_statement`：查询语句，查询的结果会导入至目标表中。查询语句支持任意 StarRocks 支持的 SQL 查询语法。

**示例**

将查询结果导入到表 `iceberg.sales.order` 的 `test-branch` 分支中。

```SQL
INSERT INTO iceberg.sales.order
FOR VERSION AS OF `test-branch`
SELECT c1, k1 FROM tbl;
```

### 创建标签

**语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
CREATE [OR REPLACE] BRANCH [IF NOT EXISTS] <tag_name>
[AS OF VERSION <snapshot_id>]
[RETAIN <int> { DAYS | HOURS | MINUTES }]
```

**参数**

- `tag_name`：要创建的标签名称。
- `AS OF VERSION`：用于创建标签的快照（版本）ID。
- `RETAIN`：标签保留时间，格式为 `<int> <unit>`。支持的单位有 `DAYS`、`HOURS` 和 `MINUTES`。示例：`7 DAYS`、`12 HOURS` 或 `30 MINUTES`。

**示例**

基于表 `iceberg.sales.order` 的快照（ID 为 `12345`）创建名为 `test-tag` 的标签，标签保留时间为 `7` 天。

```SQL
ALTER TABLE iceberg.sales.order CREATE TAG `test-tag` 
AS OF VERSION 12345
RETAIN 7 DAYS;
```

### 快进分支至其他分支

**语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

**参数**

- `from_branch`：需要快进的分支名称，用引号包裹。
- `to_branch`：目标分支名称，用引号包裹。

**示例**

将 `main` 分支快进到 `test-branch` 分支。

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

### Cherry Pick 快照

您可以 Cherry Pick 一个特定的快照，并将其应用到表的当前状态。此操作将在现有快照的基础上创建一个新快照，而原始快照不会受到影响。

**语法**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

**参数**

`snapshot_id`：要 Cherry Pick 的快照 ID。

**示例**

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

### 删除过期快照

您可以将特定时间点之前的快照删除。此操作将删除过期快照的数据文件。

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

### 删除分支或标签

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

## 使用 Time Travel 查询

### Time Travel 至特定分支或标签

**语法**

```SQL
[FOR] VERSION AS OF '<branch_or_tag>'
```

**参数**

`branch_or_tag`：要 Time Travel 到的分支或标签名称。如果指定分支名称，则查询将 Time Travel 到分支的最新快照；如果指定标签名称，则查询将 Time Travel 到标签所引用的快照。

**示例**

```SQL
-- Time Travel to the head snapshot of a branch.
SELECT * FROM iceberg.sales.order VERSION AS OF 'test-branch';
-- Time Travel to the snapshot that the tag referenced.
SELECT * FROM iceberg.sales.order VERSION AS OF 'test-tag';
```

### Time Travel 至特定快照

**语法**

```SQL
[FOR] VERSION AS OF <snapshot_id>
```

**参数**

`snapshot_id`：要 Time Travel 到的快照 ID。

**示例**

```SQL
SELECT * FROM iceberg.sales.order VERSION AS OF 12345;
```

### Time Travel 至特定时间点

**语法**

```SQL
[FOR] TIMESTAMP AS OF { '<datetime>' | '<date>' | date_and_time_function }
```

**参数**

`date_and_time_function`：StarRocks 支持的任何[日期时间函数](../../../sql-reference/sql-functions/date-time-functions/now.md)。

**示例**

```SQL
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF '1986-10-26 01:21:00';
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF '1986-10-26';
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF now();
```
