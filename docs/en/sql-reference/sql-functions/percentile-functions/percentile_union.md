---
displayed_sidebar: "English"
---

# percentile_union

## Description

Aggregates PERCENTILE data.

## Syntax

```sql
percentile_union(expr);
```

## Parameters

`expr`: The supported data type is PERCENTILE.

## Return value

Returns a PERCENTILE value.

## Examples

Example 1: Use percentile data in materialized views.

Create a table.

```sql
CREATE TABLE sales_records(
    record_id int, 
    seller_id int, 
    store_id int, 
    sale_date date, 
    sale_amt bigint
) distributed BY hash(record_id) 
properties("replication_num" = "3");
```

Create a materialized view based on the `sale_amt` column of the table.

```sql
create materialized view mv as
select store_id, percentile_union(percentile_hash(sale_amt))
from sales_records
group by store_id;
```

Example 2: Load PERCENTILE data.

Create an aggregate table that contains a PERCENTILE column `sale_amt_per`.

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

Query data from `sale_amt_per`.

```sql
select percentile_approx_raw(percentile_union(sale_amt_per), 0.99) from sales_records;
```

Load data that contains PERCENTILE data into the `sales_records` table.

```sql
curl --location-trusted -u root
    -H "columns: record_id, seller_id, store_id,tmp, sale_amt_per =percentile_hash(tmp)"
    -H "column_separator:,"
    -T a http://<ip:port>/api/test/sales_records/_stream_load
```
