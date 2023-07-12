# SHOW CREATE TABLE

Returns the CREATE TABLE statement that was used to create a given table.

> **NOTE**
>
> The SHOW CREATE TABLE statement requires you to have the `SELECT_PRIV` privilege on the table.

Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [Determine the number of buckets](../../../table_design/Data_distribution.md#determine-the-number-of-buckets).

- If you specified the number of buckets when creating a table, the output of SHOW CREATE TABLE will display the number of buckets.
- If you did not specify the number of buckets when creating a table, the output of SHOW CREATE TABLE will not display the number of buckets. You can run [SHOW PARTITIONS](SHOW%20PARTITIONS.md) to view the number of buckets for each partition.

In versions earlier than v2.5.7, you are required to set the number of buckets when creating a table. Therefore, SHOW CREATE TABLE displays the number of buckets by default.

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

### Bucket number is not specified

Create a table named `example_table` with no bucket number specified in DISTRIBUTED BY.

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

Run SHOW CREATE TABLE to display the CREATE TABLE statement of `example_table`. No bucket number is displayed in DISTRIBUTED BY. Note that if you did not specify PROPERTIES when you create the table, the default properties are displayed in the output of SHOW CREATE TABLE.

```Plain
SHOW CREATE TABLE example_table\G
*************************** 1. row ***************************
       Table: example_table
Create Table: CREATE TABLE `example_table` (
  `k1` tinyint(4) NULL COMMENT "",
  `k2` decimal64(10, 2) NULL DEFAULT "10.5" COMMENT "",
  `v1` char(10) REPLACE NULL COMMENT "",
  `v2` int(11) SUM NULL COMMENT ""
) ENGINE=OLAP 
AGGREGATE KEY(`k1`, `k2`)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(`k1`)
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```

### Bucket number is specified

Create a table named `example_table1` with bucket number set to 10 in DISTRIBUTED BY.

```SQL
CREATE TABLE example_table1
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

Run SHOW CREATE TABLE to display the CREATE TABLE statement of `example_table`. The bucket number (`BUCKETS 10`) is displayed in DISTRIBUTED BY. Note that if you did not specify PROPERTIES when you create the table, the default properties are displayed in the output of SHOW CREATE TABLE.

```plain
SHOW CREATE TABLE example_table1\G
*************************** 1. row ***************************
       Table: example_table
Create Table: CREATE TABLE `example_table` (
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
"enable_persistent_index" = "false",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```

## References

- [CREATE TABLE](../data-definition/CREATE%20TABLE.md)
- [SHOW TABLES](../data-manipulation/SHOW%20TABLES.md)
- [ALTER TABLE](ALTER%20TABLE.md)
- [DROP TABLE](DROP%20TABLE.md)
