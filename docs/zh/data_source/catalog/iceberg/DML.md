---
displayed_sidebar: docs
description: "StarRocks Iceberg catalog 支持 DML 操作，包括向 Iceberg 表插入数据。"
toc_max_heading_level: 5
keywords: ['iceberg', 'dml', 'insert', 'sink data', 'overwrite']
---

# Iceberg DML 操作

本文档介绍 StarRocks 中 Iceberg catalog 的数据操作语言（DML）操作，包括向 Iceberg 表中插入数据。

您必须具有适当的权限才能执行 DML 操作。有关权限的更多信息，请参阅[权限](../../../administration/user_privs/authorization/privilege_item.md)。

---

## INSERT

向 Iceberg 表中插入数据。此功能从 v3.1 开始支持。

与 StarRocks 的内部表类似，如果您在 Iceberg 表上具有 [INSERT](../../../administration/user_privs/authorization/privilege_item.md#table) 权限，您可以使用 [INSERT](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) 语句将 StarRocks 表的数据下沉到该 Iceberg 表（目前仅支持 Parquet 格式的 Iceberg 表）。

### 语法

```SQL
INSERT {INTO | OVERWRITE} <table_name>
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }

-- 如果要将数据下沉到指定的分区，请使用以下语法：
INSERT {INTO | OVERWRITE} <table_name>
PARTITION (par_col1=<value> [, par_col2=<value>...])
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

:::note

分区列不允许 `NULL` 值。因此，您必须确保没有空值被加载到 Iceberg 表的分区列中。

:::

### 参数

#### INTO

将 StarRocks 表的数据追加到 Iceberg 表中。

#### OVERWRITE

用 StarRocks 表的数据覆盖 Iceberg 表的现有数据。

#### column_name

要加载数据的目标列的名称。您可以指定一个或多个列。如果指定多个列，请用逗号（`,`）分隔。您只能指定 Iceberg 表中实际存在的列，并且您指定的目标列必须包括 Iceberg 表的分区列。无论目标列名称是什么，您指定的目标列按顺序与 StarRocks 表的列一一对应。如果未指定目标列，数据将加载到 Iceberg 表的所有列中。如果 StarRocks 表的非分区列无法映射到 Iceberg 表的任何列，StarRocks 会将默认值 `NULL` 写入 Iceberg 表列。如果 INSERT 语句包含的查询语句返回的列类型与目标列的数据类型不匹配，StarRocks 会对不匹配的列进行隐式转换。如果转换失败，将返回语法解析错误。

#### expression

为目标列分配值的表达式。

#### DEFAULT

为目标列分配默认值。

#### query

其结果将加载到 Iceberg 表中的查询语句。它可以是 StarRocks 支持的任何 SQL 语句。

#### PARTITION

要加载数据的分区。您必须在此属性中指定 Iceberg 表的所有分区列。您在此属性中指定的分区列可以与您在表创建语句中定义的分区列顺序不同。如果指定此属性，则不能指定 `column_name` 属性。

### 示例

1. 向 `partition_tbl_1` 表中插入三行数据：

   ```SQL
   INSERT INTO partition_tbl_1
   VALUES
       ("buy", 1, "2023-09-01"),
       ("sell", 2, "2023-09-02"),
       ("buy", 3, "2023-09-03");
   ```

2. 将包含简单计算的 SELECT 查询结果插入到 `partition_tbl_1` 表中：

   ```SQL
   INSERT INTO partition_tbl_1 (id, action, dt) SELECT 1+1, 'buy', '2023-09-03';
   ```

3. 从 `partition_tbl_1` 表中读取数据的 SELECT 查询结果插入到同一表中：

   ```SQL
   INSERT INTO partition_tbl_1 SELECT 'buy', 1, date_add(dt, INTERVAL 2 DAY)
   FROM partition_tbl_1
   WHERE id=1;
   ```

4. 将 SELECT 查询结果插入到 `partition_tbl_2` 表中满足 `dt='2023-09-01'` 和 `id=1` 两个条件的分区：

   ```SQL
   INSERT INTO partition_tbl_2 SELECT 'order', 1, '2023-09-01';
   ```

   或

   ```SQL
   INSERT INTO partition_tbl_2 partition(dt='2023-09-01',id=1) SELECT 'order';
   ```

5. 用 `close` 覆盖 `partition_tbl_1` 表中满足 `dt='2023-09-01'` 和 `id=1` 两个条件的分区中的所有 `action` 列值：

   ```SQL
   INSERT OVERWRITE partition_tbl_1 SELECT 'close', 1, '2023-09-01';
   ```

   或

   ```SQL
   INSERT OVERWRITE partition_tbl_1 partition(dt='2023-09-01',id=1) SELECT 'close';
   ```

## DELETE

您可以使用 DELETE 语句根据指定条件从 Iceberg 表中删除数据。此功能在 StarRocks v4.1 及更高版本中受支持。

### 语法

```SQL
DELETE FROM <table_name> WHERE <condition>
```

### 参数

- `table_name`: 要删除数据的 Iceberg 表名称。您可以使用：
  - 完全限定名称：`catalog_name.database_name.table_name`
  - 数据库限定名称（设置 catalog 后）：`database_name.table_name`
  - 仅表名（设置 catalog 和数据库后）：`table_name`

- `condition`: 用于标识要删除哪些行的条件。可以包括：
  - 比较运算符：`=`、`!=`、`>`、`<`、`>=`、`<=`、`<>`
  - 逻辑运算符：`AND`、`OR`、`NOT`
  - `IN` 和 `NOT IN` 子句
  - `BETWEEN` 和 `LIKE` 运算符
  - `IS NULL` 和 `IS NOT NULL`
  - 带 `IN` 或 `EXISTS` 的子查询

### 示例

#### Basic DELETE 操作

使用简单条件删除行：

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE id = 3;
```

#### 带 IN 和 NOT IN 的 DELETE

使用 IN 子句删除多行：

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE id IN (18, 20, 22);
DELETE FROM iceberg_catalog.db.table1 WHERE id NOT IN (100, 101, 102);
```

#### 带逻辑运算符的 DELETE

组合多个条件：

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE age > 30 AND salary < 70000;
DELETE FROM iceberg_catalog.db.table1 WHERE status = 'inactive' OR last_login < '2023-01-01';
```

#### 带模式匹配的 DELETE

使用 LIKE 进行基于模式的删除：

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE name LIKE 'A%';
DELETE FROM iceberg_catalog.db.table1 WHERE email LIKE '%@example.com';
```

#### 带范围条件的 DELETE

使用 BETWEEN 进行基于范围的删除：

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE age BETWEEN 30 AND 40;
DELETE FROM iceberg_catalog.db.table1 WHERE created_date BETWEEN '2023-01-01' AND '2023-12-31';
```

#### 带 NULL 检查的 DELETE

删除包含或不包含 NULL 值的行：

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE name IS NULL;
DELETE FROM iceberg_catalog.db.table1 WHERE email IS NULL AND phone IS NULL;
DELETE FROM iceberg_catalog.db.table1 WHERE age IS NOT NULL;
```

#### 带子查询的 DELETE

使用子查询确定要删除的行：

```SQL
-- 带 IN 子查询的 DELETE
DELETE FROM iceberg_catalog.db.table1 WHERE id IN (SELECT id FROM temp_table WHERE expired = true);

-- 带 EXISTS 子查询的 DELETE
DELETE FROM iceberg_catalog.db.table1 t1 WHERE EXISTS (SELECT user_id FROM inactive_users t2 WHERE t2.user_id = t1.user_id);
```

## UPDATE

您可以使用 UPDATE 语句根据指定条件更新 Iceberg 表中的数据。此功能从 v4.2 起支持。

UPDATE 通过 Iceberg V2 的 **Merge-On-Read** 模型实现：每次 UPDATE 会在同一个 Iceberg 快照中原子提交位置删除文件（position delete file，用于标记被更新的旧行）和新数据文件（包含更新后的新行）。读者始终只能看到 UPDATE 前或 UPDATE 后的完整状态，不会出现中间态，结果表对 Spark 等其它 Iceberg 引擎完全兼容。

### 语法

```SQL
UPDATE <table_name>
SET <column_name> = <expression> [, <column_name> = <expression> ...]
WHERE <condition>
```

### 参数

- `table_name`：要更新的 Iceberg 表名称。您可以使用：
  - 完全限定名称：`catalog_name.database_name.table_name`
  - 数据库限定名称（设置 catalog 后）：`database_name.table_name`
  - 仅表名（设置 catalog 和数据库后）：`table_name`

- `column_name = expression`：要更新的列以及新值。表达式可以引用同一行的其它列，以及系统支持的标量函数。

- `condition`：用于标识需要更新的行的谓词。支持的运算符与 `DELETE` 一致（比较运算符、逻辑运算符、`IN` / `NOT IN`、`BETWEEN`、`LIKE`、`IS NULL` / `IS NOT NULL`，以及 `IN` / `EXISTS` 子查询）。

### 使用说明

- 仅支持 **format-version 2** 的 Iceberg 表。对 V1 和 V3 表执行 UPDATE 会在分析阶段被拒绝。
- 必须指定 `WHERE` 子句，以防误更新全表。
- 不支持更新分区列。 如需修改分区归属，请使用 `INSERT OVERWRITE`。
- 不允许在 `SET` 中赋值给隐藏的元数据列 `_file` 和 `_pos`。
- 对 Iceberg 表的 UPDATE 不支持 `WITH`（CTE）子句和 `FROM` 子句。
- 不支持 `DEFAULT` 值，因为 Iceberg V2 没有列默认值语义（initial-default / write-default 是 V3 特性）。
- 仅支持 Parquet 格式的 Iceberg 表，与已有的 Iceberg sink 一致。
- 并发 UPDATE 使用 **可串行化（serializable）隔离**：提交时 StarRocks 会基于读取快照重新校验数据文件，若与并发写入冲突，UPDATE 会直接失败，而不是默默覆盖。

### 示例

#### 基本 UPDATE 操作

使用字面量更新单个列：

```SQL
UPDATE iceberg_catalog.db.table1 SET status = 'inactive' WHERE id = 3;
```

#### 同时更新多个列

```SQL
UPDATE iceberg_catalog.db.table1
SET status = 'archived', archived_at = '2026-05-21'
WHERE last_login < '2024-01-01';
```

#### 使用表达式更新

新值可以基于当前行的其它列计算得到：

```SQL
UPDATE iceberg_catalog.db.table1
SET salary = salary * 1.05
WHERE department = 'engineering';
```

#### 带 IN 和逻辑运算符的 UPDATE

```SQL
UPDATE iceberg_catalog.db.table1
SET status = 'flagged'
WHERE id IN (18, 20, 22);

UPDATE iceberg_catalog.db.table1
SET status = 'inactive'
WHERE age > 60 OR last_login IS NULL;
```

#### WHERE 子句中带子查询的 UPDATE

```SQL
UPDATE iceberg_catalog.db.orders
SET state = 'cancelled'
WHERE customer_id IN (SELECT id FROM inactive_customers);
```

#### 将列设置为 NULL

```SQL
UPDATE iceberg_catalog.db.table1
SET email = NULL
WHERE email_verified = false;
```

### 监控指标

针对 Iceberg 表的每条 UPDATE 语句都会更新以下 FE 端指标。它们与现有的 `iceberg_write_*`、`iceberg_delete_*` 共用 `iceberg_*` 命名空间，可通过标准 FE 指标接口拉取。完整说明请参考 [监控指标](../../../administration/management/monitoring/metric_details/i-p.md)。

| 指标 | 单位 | 标签 | 描述 |
| --- | --- | --- | --- |
| `iceberg_update_total` | 计数 | `status`（`success`、`failed`），`reason`（`none`、`timeout`、`oom`、`access_denied`、`unknown`） | 针对 Iceberg 表的 UPDATE 任务总数，每个任务结束后加 1。 |
| `iceberg_update_duration_ms_total` | 毫秒 | — | Iceberg UPDATE 任务的累计执行时间。 |
| `iceberg_update_rows` | 行 | — | Iceberg UPDATE 影响的总行数（每行只计一次，与生成的文件数无关）。 |
| `iceberg_update_bytes` | 字节 | `file_type`（`data`、`position_delete`） | Iceberg UPDATE 写入的总字节数，按新数据文件和位置删除文件分别统计。 |
| `iceberg_update_files` | 计数 | `file_type`（`data`、`position_delete`） | Iceberg UPDATE 写入的文件总数，按新数据文件和位置删除文件分别统计。 |

## MERGE INTO

您可以使用 MERGE INTO 语句，根据源关系（source relation）中的每一行是否与目标表匹配，在一条原子语句中对 Iceberg 表进行有条件的更新、删除和插入。此功能从 v4.2 起支持。

MERGE INTO 复用了与 UPDATE 相同的 Iceberg V2 **Merge-On-Read** 提交路径：被更新或删除的匹配行生成位置删除文件（position delete file），被更新的行和新插入的行生成新数据文件，所有文件在同一个 Iceberg 快照中一起提交。读者始终只能看到 MERGE 前或 MERGE 后的完整状态，不会出现中间态，结果表对 Spark 等其它 Iceberg 引擎完全兼容。

### 语法

```SQL
MERGE INTO <target_table> [ [AS] <target_alias> ]
USING <source_relation> [ [AS] <source_alias> ]
ON <merge_condition>
[ WHEN MATCHED [ AND <condition> ] THEN { UPDATE SET <column_name> = <expression> [, ...] | DELETE } ]
[ ... ]
[ WHEN NOT MATCHED [ AND <condition> ] THEN { INSERT (<column_name> [, ...]) VALUES (<expression> [, ...]) | INSERT * } ]
[ ... ]
```

### 参数

- `target_table`：要修改的 Iceberg 表。您可以使用：
  - 完全限定名称：`catalog_name.database_name.table_name`
  - 数据库限定名称（设置 catalog 后）：`database_name.table_name`
  - 仅表名（设置 catalog 和数据库后）：`table_name`

- `source_relation`：驱动 merge 的数据源。可以是表、视图或带括号的子查询。当 `ON` 条件或 `WHEN` 子句需要引用其列时，请为其指定别名。

- `merge_condition`：`ON` 谓词，用于判断目标行与源行是否匹配。通常按键列关联目标表和源。

- `WHEN MATCHED [AND <condition>] THEN ...`：作用于匹配到源行的目标行。动作为 `UPDATE SET`（改写所列的列）或 `DELETE`（删除该匹配行）。可以编写多个 `WHEN MATCHED` 子句，每个可带一个额外的 `AND <condition>`；对某一行而言，第一个条件成立的子句生效。

- `WHEN NOT MATCHED [AND <condition>] THEN ...`：作用于未匹配到任何目标行的源行。动作为插入一行新数据，可通过显式的列与值列表（`INSERT (...) VALUES (...)`），或通过 `INSERT *`（按同名列将源的每一列映射到目标列）。

### 使用说明

- 仅支持 **format-version 2** 的 Iceberg 表。对 V1、V3 表或非 Iceberg 表（例如原生 OLAP 表）执行 MERGE INTO 会在分析阶段被拒绝。
- 至少需要一个 `WHEN` 子句。
- 每个目标行**最多只能被一个源行匹配**。若某个目标行匹配了多个源行，语句会在运行时失败，而不是执行有歧义的变更。必要时请提前对源进行去重或聚合。
- 在 `WHEN MATCHED ... THEN UPDATE` 子句中不能更新分区列。
- 隐藏的元数据列 `_file` 和 `_pos` 不能在 `UPDATE SET` 中赋值，也不能作为 `INSERT` 的目标列。
- 不支持 `DEFAULT` 值，因为 Iceberg V2 没有列默认值语义。
- 在 `INSERT (...) VALUES (...)` 中，值的数量必须与所列列的数量一致，且同一列不能重复出现。
- 无条件的 `WHEN MATCHED` 子句必须是所有 `WHEN MATCHED` 子句中的最后一个；无条件的 `WHEN NOT MATCHED` 子句必须是所有 `WHEN NOT MATCHED` 子句中的最后一个。
- `INSERT *` 要求源能被一个名字限定引用：可以是显式别名，或裸表（此时使用表名）。当源是子查询时，必须显式指定别名。
- 仅支持 Parquet 格式的 Iceberg 表，与已有的 Iceberg sink 一致。

### 示例

以下示例针对 Iceberg 表 `iceberg_catalog.db.t_merge`，其包含 `id`、`name`、`age`、`salary` 列。

#### Upsert（更新匹配行，插入新行）

最常见的 MERGE 模式：更新已存在的行，插入不存在的行：

```SQL
MERGE INTO iceberg_catalog.db.t_merge AS t
USING source_updates AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age, salary = s.salary
WHEN NOT MATCHED THEN INSERT (id, name, age, salary) VALUES (s.id, s.name, s.age, s.salary);
```

#### 仅更新匹配行

```SQL
MERGE INTO iceberg_catalog.db.t_merge AS t
USING (SELECT 3 AS id, 'UPDATED' AS name, 75000 AS salary) AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET name = s.name, salary = s.salary;
```

#### 删除匹配行

```SQL
MERGE INTO iceberg_catalog.db.t_merge AS t
USING (SELECT 2 AS id) AS s
ON t.id = s.id
WHEN MATCHED THEN DELETE;
```

#### 插入未匹配的行

```SQL
MERGE INTO iceberg_catalog.db.t_merge AS t
USING (SELECT 10 AS id, 'Frank' AS name, 40 AS age, 80000 AS salary) AS s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (id, name, age, salary) VALUES (s.id, s.name, s.age, s.salary);
```

#### 条件子句

在 `WHEN` 子句上添加 `AND <condition>`，即可为每一行选择动作。一个常见场景是应用变更流（CDC）：源表 `t_changes` 带有一个 `op` 列，标记每行是更新、删除还是插入，一条 MERGE 语句即可分派这三种动作：

```SQL
MERGE INTO iceberg_catalog.db.t_merge AS t
USING iceberg_catalog.db.t_changes AS s
ON t.id = s.id
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED AND s.op = 'UPDATE' THEN UPDATE SET name = s.name, age = s.age, salary = s.salary
WHEN NOT MATCHED AND s.op <> 'DELETE' THEN INSERT (id, name, age, salary) VALUES (s.id, s.name, s.age, s.salary);
```

#### INSERT *

当源暴露出与目标同名的列时，`INSERT *` 会插入所有目标列而无需一一列出。源必须能被一个名字限定引用；若源是子查询，则必须带别名：

```SQL
MERGE INTO iceberg_catalog.db.t_merge AS t
USING source_new_rows AS s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT *;
```

### 监控指标

对 Iceberg 表实际写入的 MERGE INTO 语句会在提交后更新以下 FE 端指标。不产生任何文件的 no-op MERGE（例如空源，或所有匹配/未匹配行都被过滤掉、没有任何动作生效）会被跳过、不提交，也不会增加这些计数。它们与现有的 `iceberg_write_*`、`iceberg_delete_*`、`iceberg_update_*` 共用 `iceberg_*` 命名空间，可通过标准 FE 指标接口拉取。完整说明请参考 [监控指标](../../../administration/management/monitoring/metric_details/i-p.md)。

| 指标 | 单位 | 标签 | 描述 |
| --- | --- | --- | --- |
| `iceberg_merge_total` | 计数 | `status`（`success`、`failed`），`reason`（`none`、`timeout`、`oom`、`access_denied`、`unknown`） | 针对 Iceberg 表的 MERGE INTO 任务总数，每个任务结束后加 1。 |
| `iceberg_merge_duration_ms_total` | 毫秒 | — | Iceberg MERGE INTO 任务的累计执行时间。 |
| `iceberg_merge_rows` | 行 | `file_type`（`data`、`position_delete`） | Iceberg MERGE INTO 处理的总行数，按文件类型拆分。`position_delete` 统计被 UPDATE 或 DELETE 命中的目标行（写为位置删除）；`data` 统计写入的数据行（更新的行加上插入的行）。 |
| `iceberg_merge_bytes` | 字节 | `file_type`（`data`、`position_delete`） | Iceberg MERGE INTO 写入的总字节数，按新数据文件和位置删除文件分别统计。 |
| `iceberg_merge_files` | 计数 | `file_type`（`data`、`position_delete`） | Iceberg MERGE INTO 写入的文件总数，按新数据文件和位置删除文件分别统计。 |

## TRUNCATE

您可以使用 TRUNCATE TABLE 语句快速删除 Iceberg 表中的所有数据。

### 语法

```SQL
TRUNCATE TABLE <table_name>
```

### 参数

- `table_name`: 要清空数据的 Iceberg 表名称。您可以使用：
  - 完全限定名称：`catalog_name.database_name.table_name`
  - 数据库限定名称（设置 catalog 后）：`database_name.table_name`
  - 仅表名（设置 catalog 和数据库后）：`table_name`

### 示例

#### 示例 1：使用完全限定名称清空表

```SQL
TRUNCATE TABLE iceberg_catalog.my_db.my_table;
```

#### 示例 2：设置 catalog 后清空表

```SQL
SET CATALOG iceberg_catalog;
TRUNCATE TABLE my_db.my_table;
```

#### 示例 3：设置 catalog 和数据库后清空表

```SQL
SET CATALOG iceberg_catalog;
USE my_db;
TRUNCATE TABLE my_table;
```

---
