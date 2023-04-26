# SHOW CREATE TABLE

Returns the CREATE TABLE statement that was used to create a given table. In versions earlier than v3.0, the SHOW CREATE TABLE statement requires you to have the `SELECT_PRIV` privilege on the table. Since v3.0, the SHOW CREATE TABLE statement requires you to have the `SELECT` privilege on the table. Note that from v3.0 onwards you can use the SHOW CREATE TABLE statement to view the CREATE TABLE statements of the tables that are managed by an external catalog and are stored in Apache Hive™, Apache Iceberg, Apache Hudi, or Delta Lake.

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
