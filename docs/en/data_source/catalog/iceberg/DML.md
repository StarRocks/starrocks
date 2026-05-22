---
displayed_sidebar: docs
keywords: ['iceberg', 'dml', 'insert', 'sink data', 'overwrite']
---

# Iceberg DML operations

StarRocks Iceberg Catalog supports a variety of Data Manipulation Language (DML) operations, including inserting data into Iceberg tables.

You must have the appropriate privileges to perform DML operations. For more information about privileges, see [Privileges](../../../administration/user_privs/authorization/privilege_item.md).

## INSERT

Inserts data into an Iceberg table. This feature is supported from v3.1 onwards.

Similar to loading data into StarRocks native tables, if you have the [INSERT privilege](../../../administration/user_privs/authorization/privilege_item.md#table) on an Iceberg table, you can use the [INSERT](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) statement to sink the data to the Iceberg table. Currently, only Parquet-formatted Iceberg tables are supported.

### Syntax

```SQL
INSERT {INTO | OVERWRITE} <table_name>
[ (column_name [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }

-- If you want to sink data to specified partitions, use the following syntax:
INSERT {INTO | OVERWRITE} <table_name>
PARTITION (par_col1=<value> [, par_col2=<value>...])
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

:::note

`NULL` values are not allowed in partition columns. Therefore, you must make sure that no empty values are loaded into the partition columns of the Iceberg table.

:::

### Parameters

#### INTO

Appends the data to the Iceberg table.

#### OVERWRITE

Overwrites the existing data of the Iceberg table.

#### column_name

The name of the destination column to which you want to load data. You can specify one or more columns. Multiple columns are separated with commas (`,`).
- You can only specify columns that actually exist in the Iceberg table.
- The destination columns must include the partition columns of the Iceberg table.
- The destination columns are mapped one on one in sequence to the columns in the SELECT statement (source columns), regardless of what the destination column names are.
- If no destination columns are specified, the data is loaded into all columns of the Iceberg table.
- If a non-partition source column cannot be mapped to any destination column, StarRocks writes the default value `NULL` to the destination column.
- If the data types of the source and destination columns mismatch, StarRocks performs an implicit conversion on the mismatched columns. If the conversion fails, a syntax parsing error will be returned.

:::note
You cannot specify the `column_name` property if you have specified the PARTITION clause.
:::

#### expression

Expression that assigns values to the destination column.

#### DEFAULT

Assigns a default value to the destination column.

#### query

Query statement whose result will be loaded into the Iceberg table. It can be any SQL statement supported by StarRocks.

#### PARTITION

The partitions into which you want to load data. You must specify all partition columns of the Iceberg table in this property. The partition columns that you specify in this property can be in a different sequence than the partition columns that you have defined in the table creation statement.

:::note
You cannot specify the `column_name` property if you have specified the PARTITION clause.
:::

### Examples

1. Insert three data rows into the `partition_tbl_1` table:

   ```SQL
   INSERT INTO partition_tbl_1
   VALUES
       ("buy", 1, "2023-09-01"),
       ("sell", 2, "2023-09-02"),
       ("buy", 3, "2023-09-03");
   ```

2. Insert the result of a SELECT query, which contains simple computations, into the `partition_tbl_1` table:

   ```SQL
   INSERT INTO partition_tbl_1 (id, action, dt) SELECT 1+1, 'buy', '2023-09-03';
   ```

3. Insert the result of a SELECT query, which reads data from the `partition_tbl_1` table, into the same table:

   ```SQL
   INSERT INTO partition_tbl_1 SELECT 'buy', 1, date_add(dt, INTERVAL 2 DAY)
   FROM partition_tbl_1
   WHERE id=1;
   ```

4. Insert the result of a SELECT query into the partitions that meet two conditions, `dt='2023-09-01'` and `id=1`, of the `partition_tbl_2` table:

   ```SQL
   INSERT INTO partition_tbl_2 SELECT 'order', 1, '2023-09-01';
   ```

   Or

   ```SQL
   INSERT INTO partition_tbl_2 partition(dt='2023-09-01',id=1) SELECT 'order';
   ```

5. Overwrite all `action` column values in the partitions that meet two conditions, `dt='2023-09-01'` and `id=1`, of the `partition_tbl_1` table with `close`:

   ```SQL
   INSERT OVERWRITE partition_tbl_1 SELECT 'close', 1, '2023-09-01';
   ```

   Or

   ```SQL
   INSERT OVERWRITE partition_tbl_1 partition(dt='2023-09-01',id=1) SELECT 'close';
   ```

## DELETE

You can use the DELETE statement to delete data from Iceberg tables based on specified conditions. This feature is supported from v4.1 and later.

### Syntax

```SQL
DELETE FROM <table_name> WHERE <condition>
```

### Parameters

- `table_name`: The name of the Iceberg table you want to delete data from. You can use:
  - Fully qualified name: `catalog_name.database_name.table_name`
  - Database-qualified name (after setting catalog): `database_name.table_name`
  - Table name only (after setting catalog and database): `table_name`

- `condition`: The condition to identify which rows to delete. It can include:
  - Comparison operators: `=`, `!=`, `>`, `<`, `>=`, `<=`, `<>`
  - Logical operators: `AND`, `OR`, `NOT`
  - `IN` and `NOT IN` clauses
  - `BETWEEN` and `LIKE` operators
  - `IS NULL` and `IS NOT NULL`
  - Sub-queries with `IN` or `EXISTS`

### Examples

#### Basic DELETE operations

Delete rows matching a simple condition:

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE id = 3;
```

#### DELETE with IN and NOT IN

Delete multiple rows using IN clause:

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE id IN (18, 20, 22);
DELETE FROM iceberg_catalog.db.table1 WHERE id NOT IN (100, 101, 102);
```

#### DELETE with logical operators

Combine multiple conditions:

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE age > 30 AND salary < 70000;
DELETE FROM iceberg_catalog.db.table1 WHERE status = 'inactive' OR last_login < '2023-01-01';
```

#### DELETE with pattern matching

Use LIKE for pattern-based deletion:

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE name LIKE 'A%';
DELETE FROM iceberg_catalog.db.table1 WHERE email LIKE '%@example.com';
```

#### DELETE with range conditions

Use BETWEEN for range-based deletion:

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE age BETWEEN 30 AND 40;
DELETE FROM iceberg_catalog.db.table1 WHERE created_date BETWEEN '2023-01-01' AND '2023-12-31';
```

#### DELETE with NULL checks

Delete rows with or without NULL values:

```SQL
DELETE FROM iceberg_catalog.db.table1 WHERE name IS NULL;
DELETE FROM iceberg_catalog.db.table1 WHERE email IS NULL AND phone IS NULL;
DELETE FROM iceberg_catalog.db.table1 WHERE age IS NOT NULL;
```

#### DELETE with sub-queries

Use sub-queries to identify rows to delete:

```SQL
-- DELETE with IN sub-query
DELETE FROM iceberg_catalog.db.table1 WHERE id IN (SELECT id FROM temp_table WHERE expired = true);

-- DELETE with EXISTS sub-query
DELETE FROM iceberg_catalog.db.table1 t1 WHERE EXISTS (SELECT user_id FROM inactive_users t2 WHERE t2.user_id = t1.user_id);
```

## UPDATE

You can use the UPDATE statement to modify rows in an Iceberg table based on specified conditions. This feature is supported from v4.2 and later.

StarRocks implements UPDATE using the Iceberg V2 **Merge-On-Read** model: each UPDATE atomically commits both a position-delete file (marking the old rows) and a new data file (containing the updated rows) in a single Iceberg snapshot. Readers always observe either the pre-UPDATE or post-UPDATE state, never an intermediate one, and the resulting table remains interoperable with Spark and other Iceberg-aware engines.

### Syntax

```SQL
UPDATE <table_name>
SET <column_name> = <expression> [, <column_name> = <expression> ...]
WHERE <condition>
```

### Parameters

- `table_name`: The name of the Iceberg table you want to update. You can use:
  - Fully qualified name: `catalog_name.database_name.table_name`
  - Database-qualified name (after setting catalog): `database_name.table_name`
  - Table name only (after setting catalog and database): `table_name`

- `column_name = expression`: The target column and the new value. The expression may reference other columns in the same row and any scalar function supported by StarRocks.

- `condition`: The predicate that identifies which rows to update. The supported operators match those of `DELETE` (comparison, logical, `IN`/`NOT IN`, `BETWEEN`, `LIKE`, `IS NULL`/`IS NOT NULL`, and `IN`/`EXISTS` sub-queries).

### Usage notes

- Only Iceberg tables with **format version 2** are supported. UPDATE on V1 tables is rejected at analyze time.
- A `WHERE` clause is **required** to prevent accidental full-table updates.
- Partition columns cannot be updated. Use `INSERT OVERWRITE` if you need to rewrite partition assignments.
- The hidden metadata columns `_file` and `_pos` cannot be assigned in `SET`.
- `WITH` (CTE) clauses and `FROM` clauses are not allowed in UPDATE on Iceberg tables.
- `DEFAULT` values are not supported, because Iceberg V2 has no column-default semantics (initial-default / write-default are V3 features).
- Only Parquet-formatted Iceberg tables are supported, matching the existing Iceberg sink.
- Concurrent UPDATEs use **serializable isolation**: at commit time StarRocks re-checks the data files against the read snapshot, and a conflicting concurrent write causes the UPDATE to fail rather than silently overwriting.

### Examples

#### Basic UPDATE

Update a single column with a literal value:

```SQL
UPDATE iceberg_catalog.db.table1 SET status = 'inactive' WHERE id = 3;
```

#### UPDATE multiple columns

Update several columns in one statement:

```SQL
UPDATE iceberg_catalog.db.table1
SET status = 'archived', archived_at = '2026-05-21'
WHERE last_login < '2024-01-01';
```

#### UPDATE with an expression

The new value can be computed from the row's existing columns:

```SQL
UPDATE iceberg_catalog.db.table1
SET salary = salary * 1.05
WHERE department = 'engineering';
```

#### UPDATE with IN and logical operators

```SQL
UPDATE iceberg_catalog.db.table1
SET status = 'flagged'
WHERE id IN (18, 20, 22);

UPDATE iceberg_catalog.db.table1
SET status = 'inactive'
WHERE age > 60 OR last_login IS NULL;
```

#### UPDATE with a sub-query in the WHERE clause

```SQL
UPDATE iceberg_catalog.db.orders
SET state = 'cancelled'
WHERE customer_id IN (SELECT id FROM inactive_customers);
```

#### UPDATE setting NULL

```SQL
UPDATE iceberg_catalog.db.table1
SET email = NULL
WHERE email_verified = false;
```

### Monitoring

Each UPDATE statement against an Iceberg table emits the following FE-side metrics. They share the `iceberg_*` namespace with the existing `iceberg_write_*` and `iceberg_delete_*` metrics, and can be scraped via the standard FE metrics endpoint. See [Metrics](../../../administration/management/monitoring/metric_details/i-p.md) for full per-metric documentation.

| Metric | Unit | Labels | Description |
| --- | --- | --- | --- |
| `iceberg_update_total` | Count | `status` (`success`, `failed`), `reason` (`none`, `timeout`, `oom`, `access_denied`, `unknown`) | Total number of Iceberg UPDATE tasks, incremented by 1 after each task ends. |
| `iceberg_update_duration_ms_total` | Millisecond | — | Total execution time of Iceberg UPDATE tasks. |
| `iceberg_update_rows` | Rows | — | Total number of rows affected by Iceberg UPDATE tasks (counted once per row, not per file). |
| `iceberg_update_bytes` | Bytes | `file_type` (`data`, `position_delete`) | Total bytes written by Iceberg UPDATE, split between new data files and position-delete files. |
| `iceberg_update_files` | Count | `file_type` (`data`, `position_delete`) | Total number of files written by Iceberg UPDATE, split between new data files and position-delete files. |

## TRUNCATE

You can use the TRUNCATE TABLE statement to quickly delete all data from Iceberg tables.

### Syntax

```SQL
TRUNCATE TABLE <table_name>
```

### Parameters

- `table_name`: The name of the Iceberg table that you want to truncate data from. You can use:
  - Fully qualified name: `catalog_name.database_name.table_name`
  - Database-qualified name (after setting catalog): `database_name.table_name`
  - Table name only (after setting catalog and database): `table_name`

### Examples

#### Example 1: Truncate a table using fully qualified name

```SQL
TRUNCATE TABLE iceberg_catalog.my_db.my_table;
```

#### Example 2: Truncate a table after setting catalog

```SQL
SET CATALOG iceberg_catalog;
TRUNCATE TABLE my_db.my_table;
```

#### Example 3: Truncate a table after setting catalog and database

```SQL
SET CATALOG iceberg_catalog;
USE my_db;
TRUNCATE TABLE my_table;
```
