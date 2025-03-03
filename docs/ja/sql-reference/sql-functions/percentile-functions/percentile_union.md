---
displayed_sidebar: docs
---

# percentile_union

PERCENTILE データを集計します。

## 構文

```sql
percentile_union(expr);
```

## パラメータ

`expr`: サポートされているデータ型は PERCENTILE です。

## 戻り値

PERCENTILE 値を返します。

## 例

例 1: マテリアライズドビューで PERCENTILE データを使用する。

テーブルを作成します。

```sql
CREATE TABLE sales_records(
    record_id int, 
    seller_id int, 
    store_id int, 
    sale_date date, 
    sale_amt bigint
) distributed BY hash(record_id) 
PROPERTIES ("replication_num" = "3");
```

テーブルの `sale_amt` 列に基づいてマテリアライズドビューを作成します。

```sql
create materialized view mv as
select store_id, percentile_union(percentile_hash(sale_amt))
from sales_records
group by store_id;
```

例 2: PERCENTILE データをロードする。

PERCENTILE 列 `sale_amt_per` を含む集計テーブルを作成します。

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

`sale_amt_per` からデータをクエリします。

```sql
select percentile_approx_raw(percentile_union(sale_amt_per), 0.99) from sales_records;
```

PERCENTILE データを含むデータを `sales_records` テーブルにロードします。

```sql
curl --location-trusted -u root
    -H "columns: record_id, seller_id, store_id,tmp, sale_amt_per =percentile_hash(tmp)"
    -H "column_separator:,"
    -T a http://<ip:port>/api/test/sales_records/_stream_load
```