---
displayed_sidebar: docs
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

您可以使用 UPDATE 语句根据指定条件更新 Iceberg 表中的数据。此功能从 StarRocks v4.2 起支持。

StarRocks 通过 Iceberg V2 的 **Merge-On-Read** 模型实现 UPDATE：每次 UPDATE 会在同一个 Iceberg 快照中原子提交位置删除文件（position delete file，用于标记被更新的旧行）和新数据文件（包含更新后的新行）。读者始终只能看到 UPDATE 前或 UPDATE 后的完整状态，不会出现中间态，结果表对 Spark 等其它 Iceberg 引擎完全兼容。

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

- `column_name = expression`：要更新的列以及新值。表达式可以引用同一行的其它列，以及 StarRocks 支持的标量函数。

- `condition`：用于标识需要更新的行的谓词。支持的运算符与 `DELETE` 一致（比较运算符、逻辑运算符、`IN` / `NOT IN`、`BETWEEN`、`LIKE`、`IS NULL` / `IS NOT NULL`，以及 `IN` / `EXISTS` 子查询）。

### 使用说明

- 仅支持 **format-version 2** 的 Iceberg 表。对 V1 表执行 UPDATE 会在分析阶段被拒绝。
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

针对 Iceberg 表的每条 UPDATE 语句都会更新以下 FE 端指标。它们与现有的 `iceberg_write_*`、`iceberg_delete_*` 共用 `iceberg_*` 命名空间，可通过标准 FE 指标接口拉取。完整说明请参考[Metrics](../../../administration/management/monitoring/metric_details/i-p.md)。

| 指标 | 单位 | 标签 | 描述 |
| --- | --- | --- | --- |
| `iceberg_update_total` | 计数 | `status`（`success`、`failed`），`reason`（`none`、`timeout`、`oom`、`access_denied`、`unknown`） | 针对 Iceberg 表的 UPDATE 任务总数，每个任务结束后加 1。 |
| `iceberg_update_duration_ms_total` | 毫秒 | — | Iceberg UPDATE 任务的累计执行时间。 |
| `iceberg_update_rows` | 行 | — | Iceberg UPDATE 影响的总行数（每行只计一次，与生成的文件数无关）。 |
| `iceberg_update_bytes` | 字节 | `file_type`（`data`、`position_delete`） | Iceberg UPDATE 写入的总字节数，按新数据文件和位置删除文件分别统计。 |
| `iceberg_update_files` | 计数 | `file_type`（`data`、`position_delete`） | Iceberg UPDATE 写入的文件总数，按新数据文件和位置删除文件分别统计。 |

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
