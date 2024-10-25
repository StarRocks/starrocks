---
displayed_sidebar: docs
---

# DESC

## 功能

您可以使用该语句进行如下操作：

- 查看 StarRocks 表结构、[排序键](../../../table_design/indexes/Prefix_index_sort_key.md) (Sort Key) 类型和[物化视图](../../../using_starrocks/async_mv/Materialized_view.md)。
- 查看外部数据源（如 Apache Hive™）中的表结构。仅 StarRocks 2.4 及以上版本支持该操作。

## 语法

```SQL
DESC[RIBE] { [[<catalog_name>.]<db_name>.]<table_name> [ALL] | FILES(files_loading_properties) }
```

## 参数说明

| **参数**     | **必选** | **说明**                                                     |
| ------------ | -------- | ------------------------------------------------------------ |
| catalog_name | 否       | Internal catalog 或 external catalog 的名称。<ul><li>如指定 internal catalog 名称，即 `default_catalog`，则查看当前 StarRocks 集群的指定表结构。</li><li>如指定 external catalog 名称，则查看外部数据源的指定表结构。</li></ul> |
| db_name      | 否       | 数据库名称。                                                 |
| table_name   | 是       | 表名称。                                                     |
| ALL          | 否       | <ul><li>如要查看 StarRocks 表的排序键类型和物化视图，则指定该关键字；如只查看 StarRocks 表结构，则可以不指定。</li><li>如查看外部数据源表结构，不能指定该关键词。</li></ul> |
| FILES        | 否           | FILES() 表函数。自 v3.3.4 起，您可以使用 DESC 和 FILES() 查看远端存储中文件的 Schema 信息。详细信息，参考 [Function reference - FILES()](../../sql-functions/table-functions/files.md)。 |

## 返回信息说明

```Plain
+-----------+---------------+-------+------+------+-----+---------+-------+
| IndexName | IndexKeysType | Field | Type | Null | Key | Default | Extra |
+-----------+---------------+-------+------+------+-----+---------+-------+
```

返回信息中的参数说明：

| **参数**      | **说明**                                                     |
| ------------- | ------------------------------------------------------------ |
| IndexName     | 表名。如查看外部数据源表结构，则不返回该参数。               |
| IndexKeysType | 表的排序键类型。如查看外部数据源表结构，则不返回该参数。     |
| Field         | 列名。                                                       |
| Type          | 列的数据型。                                               |
| Null          | 是否允许为 NULL。<ul><li>`yes`: 表示允许为 NULL。</li><li>`no`：表示不允许为 NULL。</li></ul> |
| Key           | 是否为排序键。<ul><li>`true`: 表示为排序键。</li><li>`false`：表示不为排序键。</li></ul> |
| Default       | 数据类型的默认值。如该数据类型没有默认值，则返回 NULL。      |
| Extra         | <ul><li>如果是查看 StarRocks 表结构，该参数会根据情况返回以下信息：<ul><li>该列使用了哪种聚合函数，如 SUM 和 MIN。</li><li>该列是否创建了 bloom filter 索引。如创建，则追加显示 `BLOOM_FILTER`。</li></ul></li><li>如果是查看外部数据源表结构，该参数会显示该列是否为分区键 (partition column)。如是，则显示 `partition key`。</li></ul> |

> 说明：有关物化视图的展示，请参见示例二。

## 示例

示例一：查看 StarRocks 的 `example_table` 表结构信息。

```SQL
DESC example_table;
```

或

```SQL
DESC default_catalog.example_db.example_table;
```

返回信息如下：

```Plain
+-------+---------------+------+-------+---------+-------+
| Field | Type          | Null | Key   | Default | Extra |
+-------+---------------+------+-------+---------+-------+
| k1    | TINYINT       | Yes  | true  | NULL    |       |
| k2    | DECIMAL(10,2) | Yes  | true  | 10.5    |       |
| k3    | CHAR(10)      | Yes  | false | NULL    |       |
| v1    | INT           | Yes  | false | NULL    |       |
+-------+---------------+------+-------+---------+-------+
```

示例二：查看 StarRocks 的 `sales_records` 表结构、排序键类型和物化视图。如下所示， `sales_records` 表只有一张物化视图 `store_amt`。

```Plain
DESC db1.sales_records ALL;

+---------------+---------------+-----------+--------+------+-------+---------+-------+
| IndexName     | IndexKeysType | Field     | Type   | Null | Key   | Default | Extra |
+---------------+---------------+-----------+--------+------+-------+---------+-------+
| sales_records | DUP_KEYS      | record_id | INT    | Yes  | true  | NULL    |       |
|               |               | seller_id | INT    | Yes  | true  | NULL    |       |
|               |               | store_id  | INT    | Yes  | true  | NULL    |       |
|               |               | sale_date | DATE   | Yes  | false | NULL    | NONE  |
|               |               | sale_amt  | BIGINT | Yes  | false | NULL    | NONE  |
|               |               |           |        |      |       |         |       |
| store_amt     | AGG_KEYS      | store_id  | INT    | Yes  | true  | NULL    |       |
|               |               | sale_amt  | BIGINT | Yes  | false | NULL    | SUM   |
+---------------+---------------+-----------+--------+------+-------+---------+-------+
```

示例三：查看 Hive 中 `hive_table` 表结构。

```SQL
DESC hive_catalog.hive_db.hive_table;

+-------+----------------+------+-------+---------+---------------+ 
| Field | Type           | Null | Key   | Default | Extra         | 
+-------+----------------+------+-------+---------+---------------+ 
| id    | INT            | Yes  | false | NULL    |               | 
| name  | VARCHAR(65533) | Yes  | false | NULL    |               | 
| date  | DATE           | Yes  | false | NULL    | partition key | 
+-------+----------------+------+-------+---------+---------------+
```

示例四：使用 DESC 查看 AWS S3 中 Parquet 文件 `lineorder` 的 Schema 信息。

> **说明**
>
> 对于远端存储中的文件，DESC 仅返回以下三列：`Field`、`Type` 以及 `Null`。

```Plain
DESC FILES(
    "path" = "s3://inserttest/lineorder.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    "aws.s3.region" = "us-west-2"
);

+------------------+------------------+------+
| Field            | Type             | Null |
+------------------+------------------+------+
| lo_orderkey      | int              | YES  |
| lo_linenumber    | int              | YES  |
| lo_custkey       | int              | YES  |
| lo_partkey       | int              | YES  |
| lo_suppkey       | int              | YES  |
| lo_orderdate     | int              | YES  |
| lo_orderpriority | varchar(1048576) | YES  |
| lo_shippriority  | int              | YES  |
| lo_quantity      | int              | YES  |
| lo_extendedprice | int              | YES  |
| lo_ordtotalprice | int              | YES  |
| lo_discount      | int              | YES  |
| lo_revenue       | int              | YES  |
| lo_supplycost    | int              | YES  |
| lo_tax           | int              | YES  |
| lo_commitdate    | int              | YES  |
| lo_shipmode      | varchar(1048576) | YES  |
+------------------+------------------+------+
17 rows in set (0.05 sec)
```

## 相关文档

- [CREATE DATABASE](../Database/CREATE_DATABASE.md)
- [SHOW CREATE DATABASE](../Database/SHOW_CREATE_DATABASE.md)
- [USE](../Database/USE.md)
- [SHOW DATABASES](../Database/SHOW_DATABASES.md)
- [DROP DATABASE](../Database/DROP_DATABASE.md)
