---
displayed_sidebar: docs
---

# percentile_union

## 功能

用于对分组结果进行聚合。

## 语法

```sql
percentile_union(expr);
```

## 参数说明

`expr`: 支持的数据类型为 PERCENTILE。

## 返回值说明

返回值的数据类型为 PERCENTILE。

## 示例

示例一：创建 PERCENTILE 类型物化视图。

创建一张基表。

```sql
CREATE TABLE sales_records(
    record_id int, 
    seller_id int, 
    store_id int, 
    sale_date date, 
    sale_amt bigint
)
DISTRIBUTED BY hash(record_id) 
PROPERTIES ("replication_num" = "3");
```

对 `sale_amt` 列创建 PERCENTILE 类型物化视图。

```sql
CREATE MATERIALIZED VIEW mv AS
SELECT store_id, percentile_union(percentile_hash(sale_amt)) FROM sales_records GROUP BY store_id;
```

示例二：导入 PERCENTILE 类型数据。

创建包含 PERCENTILE 类型的聚合表。

```sql
CREATE TABLE sales_records(
    record_id int, 
    seller_id int, 
    store_id int, 
    sale_amt_per percentile percentile_union
) ENGINE=OLAP
AGGREGATE KEY(`record_id`, `seller_id`, `store_id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`record_id`)
PROPERTIES (
"replication_num" = "3",
"storage_format" = "DEFAULT"
);
```

查询 PERCENTILE 类型列。

```sql
select percentile_approx_raw(percentile_union(sale_amt_per), 0.99) from sales_records;
```

导入 PERCENTILE 数据。

```sql
curl --location-trusted -u root -H "columns: record_id, seller_id, store_id,tmp, sale_amt_per =percentile_hash(tmp)" -H "column_separator:," -T a http://ip:port/api/test/sales_records/_stream_load
```
