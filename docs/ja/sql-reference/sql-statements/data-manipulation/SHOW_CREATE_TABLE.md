---
displayed_sidebar: docs
---

# SHOW CREATE TABLE

指定されたテーブルを作成するために使用された CREATE TABLE ステートメントを返します。

> **NOTE**
>
> SHOW CREATE TABLE ステートメントを使用するには、テーブルに対する `SELECT_PRIV` 権限が必要です。

v2.5.7以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できます。手動でバケット数を設定する必要はありません。詳細については、[Determine the number of buckets](../../../table_design/Data_distribution.md#determine-the-number-of-buckets) を参照してください。

- テーブル作成時にバケット数を指定した場合、SHOW CREATE TABLE の出力にはバケット数が表示されます。
- テーブル作成時にバケット数を指定しなかった場合、SHOW CREATE TABLE の出力にはバケット数が表示されません。[SHOW PARTITIONS](SHOW_PARTITIONS.md) を実行して、各パーティションのバケット数を確認できます。

v2.5.7より前のバージョンでは、テーブル作成時にバケット数を設定する必要があります。そのため、SHOW CREATE TABLE はデフォルトでバケット数を表示します。

## Syntax

```SQL
SHOW CREATE TABLE [db_name.]table_name
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| db_name       | No           | データベース名。このパラメータを指定しない場合、現在のデータベース内の指定されたテーブルの CREATE TABLE ステートメントがデフォルトで返されます。 |
| table_name    | Yes          | テーブル名。                                                 |

## Output

```Plain
+-----------+----------------+
| Table     | Create Table   |                                               
+-----------+----------------+
```

このステートメントによって返されるパラメータを以下に示します。

| **Parameter** | **Description**                          |
| ------------- | ---------------------------------------- |
| Table         | テーブル名。                             |
| Create Table  | テーブルの CREATE TABLE ステートメント。 |

## Examples

### バケット数が指定されていない場合

`example_table` という名前のテーブルを、DISTRIBUTED BY でバケット数を指定せずに作成します。

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

SHOW CREATE TABLE を実行して `example_table` の CREATE TABLE ステートメントを表示します。DISTRIBUTED BY にはバケット数が表示されません。テーブル作成時に PROPERTIES を指定しなかった場合、SHOW CREATE TABLE の出力にはデフォルトのプロパティが表示されます。

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

### バケット数が指定されている場合

`example_table1` という名前のテーブルを、DISTRIBUTED BY でバケット数を10に設定して作成します。

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

SHOW CREATE TABLE を実行して `example_table` の CREATE TABLE ステートメントを表示します。DISTRIBUTED BY にはバケット数 (`BUCKETS 10`) が表示されます。テーブル作成時に PROPERTIES を指定しなかった場合、SHOW CREATE TABLE の出力にはデフォルトのプロパティが表示されます。

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

- [CREATE TABLE](../data-definition/CREATE_TABLE.md)
- [SHOW TABLES](../data-manipulation/SHOW_TABLES.md)
- [ALTER TABLE](../data-definition/ALTER_TABLE.md)
- [DROP TABLE](../data-definition/DROP_TABLE.md)