---
displayed_sidebar: docs
---

# CREATE TABLE LIKE

## 説明

他のテーブルの定義に基づいて、同一の空のテーブルを作成します。定義には、カラム定義、パーティション、およびテーブルプロパティが含まれます。MySQLのような外部テーブルをコピーすることができます。

v3.2では、新しいテーブルに対して、元のテーブルとは異なるパーティション化の手法、バケッティングの手法、およびプロパティを指定することができます。

:::tip
この操作を行うには、テーブルを作成したいデータベースに対する CREATE TABLE 権限と、テーブルを作成する元となるテーブルに対する `SELECT` 権限が必要です。
:::

## 構文

- v3.2より前のバージョンでサポートされている構文。

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]<table_name>
LIKE [database.]<source_table_name>
```

- v3.2では、新しいテーブルに対してプロパティを指定することができます。

```sql
CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [database.]<table_name>
[partition_desc]
[distribution_desc]
[PROPERTIES ("key" = "value",...)]
LIKE [database.]<source_table_name>
```

## パラメータ

- `TEMPORARY`: 一時テーブルを作成します。v3.3.1から、StarRocksは Default Catalog での一時テーブルの作成をサポートしています。詳細は [Temporary Table](../../../table_design/StarRocks_table_design.md#temporary-table) を参照してください。
- `database`: データベース。
- `table_name`: 作成したいテーブルの名前。命名規則については、[System limits](../../System_limit.md) を参照してください。
- `source_table_name`: コピーしたい元のテーブルの名前。
- `partition_desc`: パーティション化の手法。詳細は [CREATE TABLE](./CREATE_TABLE.md#partition_desc) を参照してください。
- `distribution_desc`: バケッティングの手法。詳細は [CREATE TABLE](./CREATE_TABLE.md#distribution_desc) を参照してください。
- `PROPERTIES`: テーブルのプロパティ。すべてのテーブルプロパティがサポートされています。詳細は [ALTER TABLE](ALTER_TABLE.md#modify-table-properties) を参照してください。

## 例

データベース `test1` にテーブル `orders` があるとします。

```sql
create table orders (
    dt date NOT NULL,
    order_id bigint NOT NULL,
    user_id int NOT NULL,
    merchant_id int NOT NULL,
    good_id int NOT NULL,
    good_name string NOT NULL,
    price int NOT NULL,
    cnt int NOT NULL,
    revenue int NOT NULL,
    state tinyint NOT NULL
) PRIMARY KEY (dt, order_id)
PARTITION BY RANGE(`dt`) (
    PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')),
    PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')),
    PARTITION p20210929 VALUES [('2021-09-29'), ('2021-09-30')),
    PARTITION p20210930 VALUES [('2021-09-30'), ('2021-10-01'))
) DISTRIBUTED BY HASH(order_id)
PROPERTIES (
    "replication_num" = "3",
    "enable_persistent_index" = "true"
);
```

例1: データベース `test1` において、`orders` と同じテーブル構造を持つ空のテーブル `order_1` を作成します。

```sql
CREATE TABLE test1.order_1 LIKE test1.orders;
```

```plaintext
show create table order_1\G
*************************** 1. row ***************************
       Table: order_1
Create Table: CREATE TABLE `order_1` (
  `dt` date NOT NULL COMMENT "",
  `order_id` bigint(20) NOT NULL COMMENT "",
  `user_id` int(11) NOT NULL COMMENT "",
  `merchant_id` int(11) NOT NULL COMMENT "",
  `good_id` int(11) NOT NULL COMMENT "",
  `good_name` varchar(65533) NOT NULL COMMENT "",
  `price` int(11) NOT NULL COMMENT "",
  `cnt` int(11) NOT NULL COMMENT "",
  `revenue` int(11) NOT NULL COMMENT "",
  `state` tinyint(4) NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`dt`, `order_id`)
PARTITION BY RANGE(`dt`)
(PARTITION p20210820 VALUES [("2021-08-20"), ("2021-08-21")),
PARTITION p20210821 VALUES [("2021-08-21"), ("2021-08-22")),
PARTITION p20210929 VALUES [("2021-09-29"), ("2021-09-30")),
PARTITION p20210930 VALUES [("2021-09-30"), ("2021-10-01")))
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```

例2: `orders` に基づいて空のテーブル `order_2` を作成し、`order_2` にプロパティを指定します。

```sql
CREATE TABLE order_2
PARTITION BY date_trunc('day',dt)
DISTRIBUTED BY hash(dt)
PROPERTIES ("replication_num" = "1")
LIKE orders;
```

```plaintext
show create table order_2\G
*************************** 1. row ***************************
       Table: order_2
Create Table: CREATE TABLE `order_2` (
  `dt` date NOT NULL COMMENT "",
  `order_id` bigint(20) NOT NULL COMMENT "",
  `user_id` int(11) NOT NULL COMMENT "",
  `merchant_id` int(11) NOT NULL COMMENT "",
  `good_id` int(11) NOT NULL COMMENT "",
  `good_name` varchar(65533) NOT NULL COMMENT "",
  `price` int(11) NOT NULL COMMENT "",
  `cnt` int(11) NOT NULL COMMENT "",
  `revenue` int(11) NOT NULL COMMENT "",
  `state` tinyint(4) NOT NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`dt`, `order_id`)
PARTITION BY RANGE(date_trunc('day', dt))
()
DISTRIBUTED BY HASH(`dt`)
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);
```

例3: MySQL 外部テーブル `table1` と同じテーブル構造を持つ空のテーブル `table2` を作成します。

```sql
CREATE TABLE test1.table2 LIKE test1.table1
```