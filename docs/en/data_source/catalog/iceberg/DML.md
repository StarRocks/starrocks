---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg', 'dml', 'insert', 'sink data', 'overwrite']
---

# Iceberg DML operations

This document describes Data Manipulation Language (DML) operations for Iceberg catalogs in StarRocks, including inserting data into Iceberg tables.

You must have the appropriate privileges to perform DML operations. For more information about privileges, see [Privileges](../../../administration/user_privs/authorization/privilege_item.md).

---

## INSERT

Inserts data into an Iceberg table. This feature is supported from v3.1 onwards.

Similar to the internal tables of StarRocks, if you have the [INSERT](../../../administration/user_privs/authorization/privilege_item.md#table) privilege on an Iceberg table, you can use the [INSERT](../../../sql-reference/sql-statements/loading_unloading/INSERT.md) statement to sink the data of a StarRocks table to that Iceberg table (currently only Parquet-formatted Iceberg tables are supported).

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

Partition columns do not allow `NULL` values. Therefore, you must make sure that no empty values are loaded into the partition columns of the Iceberg table.

:::

### Parameters

#### INTO

To append the data of the StarRocks table to the Iceberg table.

#### OVERWRITE

To overwrite the existing data of the Iceberg table with the data of the StarRocks table.

#### column_name

The name of the destination column to which you want to load data. You can specify one or more columns. If you specify multiple columns, separate them with commas (`,`). You can only specify columns that actually exist in the Iceberg table, and the destination columns that you specify must include the partition columns of the Iceberg table. The destination columns you specify are mapped one on one in sequence to the columns of the StarRocks table, regardless of what the destination column names are. If no destination columns are specified, the data is loaded into all columns of the Iceberg table. If a non-partition column of the StarRocks table cannot be mapped to any column of the Iceberg table, StarRocks writes the default value `NULL` to the Iceberg table column. If the INSERT statement contains a query statement whose returned column types differ from the data types of the destination columns, StarRocks performs an implicit conversion on the mismatched columns. If the conversion fails, a syntax parsing error will be returned.

#### expression

Expression that assigns values to the destination column.

#### DEFAULT

Assigns a default value to the destination column.

#### query

Query statement whose result will be loaded into the Iceberg table. It can be any SQL statement supported by StarRocks.

#### PARTITION

The partitions into which you want to load data. You must specify all partition columns of the Iceberg table in this property. The partition columns that you specify in this property can be in a different sequence than the partition columns that you have defined in the table creation statement. If you specify this property, you cannot specify the `column_name` property.

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

---
