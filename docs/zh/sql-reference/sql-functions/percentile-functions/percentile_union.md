---
displayed_sidebar: "Chinese"
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

percentile物化视图使用。

```sql
CREATE TABLE sales_records(
    record_id int, 
    seller_id int, 
    store_id int, 
    sale_date date, 
    sale_amt bigint
) distributed BY hash(record_id) 
properties("replication_num" = "1");
```

对`sale_amt`建立 PERCENTILE 类型物化视图表。

```sql
create materialized view mv as
select store_id, percentile_union(percentile_hash(sale_amt)) from sales_records group by store_id;
```

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
DISTRIBUTED BY HASH(`record_id`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"storage_format" = "DEFAULT"
);
```

查询 PERCENTILE 类型列。

```sql
select percentile_approx_raw(percentile_union(sale_amt_per), 0.99) from sales_records;
```

导入包含PERCENTILE的聚合表。

```sql
curl --location-trusted -u root -H "columns: record_id, seller_id, store_id,tmp, sale_amt_per =percentile_hash(tmp)" -H "column_separator:," -T a http://ip:port/api/test/sales_records/_stream_load
```
