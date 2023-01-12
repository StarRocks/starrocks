# SHOW CREATE TABLE

Returns the CREATE TABLE statement that was used to create a given table. The SHOW CREATE TABLE statement requires you to have the `SELECT_PRIV` permission on the table. Note that the CREATE TABLE statements of tables managed by an [external catalog](../../../data_source/Manage_data.md) cannot be displayed.

## Syntax

```SQL
SHOW CREATE TABLE [db_name.]table_name
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       | No           | The database name. If this parameter is not specified, the CREATE TABLE statement of a given table in your current database is returned by default. |
| table_name    | Yes          | The table name.                                              |

## Output

```Plain
+-----------+----------------+
| Table     | Create Table   |                                               
+-----------+----------------+
```

The following table describes the parameters returned by this statement.

| **Parameter** | **Description**                          |
| ------------- | ---------------------------------------- |
| Table         | The table name.                          |
| Create Table  | The CREATE TABLE statement of the table. |

## Examples

Create a table named `example_table`.

```SQL
CREATE TABLE example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1) BUCKETS 10;
```

Display the CREATE TABLE statement of `example_table`.

```Plain
SHOW CREATE TABLE example_db.example_table;

+---------------+--------------------------------------------------------+
| Table         | Create Table                                           |
+---------------+--------------------------------------------------------+
| example_table | CREATE TABLE `example_table` (
  `k1` tinyint(4) NULL COMMENT "",
  `k2` decimal64(10, 2) NULL DEFAULT "10.5" COMMENT "",
  `v1` char(10) REPLACE NULL COMMENT "",
  `v2` int(11) SUM NULL COMMENT ""
) ENGINE=OLAP 
AGGREGATE KEY(`k1`, `k2`)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(`k1`) BUCKETS 10 
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false"
); |
+---------------+----------------------------------------------------------+
```
