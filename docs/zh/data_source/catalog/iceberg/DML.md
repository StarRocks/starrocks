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
