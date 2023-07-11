# DESC

## 功能

您可以使用该语句进行如下操作：

- 查看 StarRocks 表结构、[排序键](/table_design/Sort_key.md) (Sort Key) 类型和[物化视图](/using_starrocks/Materialized_view.md)。
- 查看外部数据源 Apache Hive™、 Apache Iceberg 和 Apache Hudi 表结构。仅 StarRocks 2.4 及以上版本支持该操作。

## 语法

```SQL
DESC[RIBE] [catalog_name.][db_name.]table_name [ALL];
```

## 参数说明

| **参数**     | **必选** | **说明**                                                     |
| ------------ | -------- | ------------------------------------------------------------ |
| catalog_name | 否       | Internal catalog 或 external catalog 的名称。<ul><li>如指定 internal catalog 名称，即 `default_catalog`，则查看当前 StarRocks 集群的指定表结构。</li><li>如指定 external catalog 名称，则查看外部数据源的指定表结构。</li></ul> |
| db_name      | 否       | 数据库名称。                                                 |
| table_name   | 是       | 表名称。                                                     |
| ALL          | 否       | <ul><li>如要查看 StarRocks 表的排序键类型和物化视图，则指定该关键字；如只查看 StarRocks 表结构，则可以不指定。</li><li>如查看外部数据源表结构，不能指定该关键词。</li></ul> |

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

## 相关文档

- [CREATE DATABASE](../data-definition/CREATE%20DATABASE.md)
- [SHOW CREATE DATABASE](SHOW%20CREATE%20DATABASE.md)
- [USE](../data-definition/USE.md)
- [SHOW DATABASES](../data-manipulation/SHOW%20DATABASES.md)
- [DROP DATABASE](../data-definition/DROP%20DATABASE.md)
