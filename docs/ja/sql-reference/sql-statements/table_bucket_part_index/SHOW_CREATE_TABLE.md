---
displayed_sidebar: docs
---

# SHOW CREATE TABLE

指定されたテーブルを作成するために使用された CREATE TABLE ステートメントを返します。

> **注意**
>
> v3.0 より前のバージョンでは、SHOW CREATE TABLE ステートメントを実行するには、そのテーブルに対する `SELECT_PRIV` 権限が必要です。v3.0 以降では、SHOW CREATE TABLE ステートメントを実行するには、そのテーブルに対する `SELECT` 権限が必要です。

v3.0 以降、SHOW CREATE TABLE ステートメントを使用して、external catalog によって管理され、Apache Hive™、Apache Iceberg、Apache Hudi、または Delta Lake に保存されているテーブルの CREATE TABLE ステートメントを表示できます。

v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細については、[バケット数の設定](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)を参照してください。

- テーブル作成時にバケット数を指定した場合、SHOW CREATE TABLE の出力にはバケット数が表示されます。
- テーブル作成時にバケット数を指定しなかった場合、SHOW CREATE TABLE の出力にはバケット数が表示されません。[SHOW PARTITIONS](SHOW_PARTITIONS.md) を実行して、各パーティションのバケット数を確認できます。

v2.5.7 より前のバージョンでは、テーブル作成時にバケット数を設定する必要がありました。そのため、SHOW CREATE TABLE はデフォルトでバケット数を表示します。

## 構文

```SQL
SHOW CREATE TABLE [db_name.]table_name
```

## パラメータ

| **パラメータ** | **必須** | **説明**                                              |
| ------------- | -------- | ----------------------------------------------------- |
| db_name       | いいえ   | データベース名。このパラメータを指定しない場合、現在のデータベース内の指定されたテーブルの CREATE TABLE ステートメントがデフォルトで返されます。 |
| table_name    | はい     | テーブル名。                                           |

## 出力

```Plain
+-----------+----------------+
| Table     | Create Table   |                                               
+-----------+----------------+
```

このステートメントによって返されるパラメータを以下に示します。

| **パラメータ** | **説明**                              |
| ------------- | ------------------------------------- |
| Table         | テーブル名。                           |
| Create Table  | テーブルの CREATE TABLE ステートメント。 |

## 例

### バケット数が指定されていない場合

DISTRIBUTED BY でバケット数を指定せずに `example_table` という名前のテーブルを作成します。

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
DISTRIBUTED BY HASH(k1);
```

SHOW CREATE TABLE を実行して、`example_table` の CREATE TABLE ステートメントを表示します。DISTRIBUTED BY にはバケット数が表示されません。テーブル作成時に PROPERTIES を指定しなかった場合、SHOW CREATE TABLE の出力にはデフォルトのプロパティが表示されます。

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
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```

### バケット数が指定されている場合

DISTRIBUTED BY でバケット数を 10 に設定して `example_table1` という名前のテーブルを作成します。

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

SHOW CREATE TABLE を実行して、`example_table1` の CREATE TABLE ステートメントを表示します。DISTRIBUTED BY にはバケット数 (`BUCKETS 10`) が表示されます。テーブル作成時に PROPERTIES を指定しなかった場合、SHOW CREATE TABLE の出力にはデフォルトのプロパティが表示されます。

```plain
SHOW CREATE TABLE example_table1\G
*************************** 1. row ***************************
       Table: example_table1
Create Table: CREATE TABLE `example_table1` (
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
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```

## 参考

- [CREATE TABLE](CREATE_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [DROP TABLE](DROP_TABLE.md)