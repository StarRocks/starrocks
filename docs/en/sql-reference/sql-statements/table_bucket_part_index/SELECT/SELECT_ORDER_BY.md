---
displayed_sidebar: docs
sidebar_label: "ORDER BY"
---

# ORDER BY

The ORDER BY clause of a SELECT statement sorts the result set by comparing the values from one or more columns.

ORDER BY is a time- and resource-consuming operation because all the results must be sent to one node for merging before the results can be sorted. Sorting consumes more memory resources than a query without ORDER BY.

Therefore, if you only need the first `N` results from the sorted result set, you can use the LIMIT clause, which reduces memory usage and network overhead. If the LIMIT clause is not specified, the first 65535 results are returned by default.

## Syntax

```sql
ORDER BY <column_name> 
    [ASC | DESC]
    [NULLS FIRST | NULLS LAST]
```

## Parameters

- `ASC` specifies that the results should be returned in ascending order.
- `DESC` specifies that the results should be returned in descending order. If the order is not specified, ASC (ascending) is the default.
- `NULLS FIRST` indicates that NULL values should be returned before non-NULL values.
- `NULLS LAST` indicates that NULL values should be returned after non-NULL values.

## Examples

```sql
select * from big_table order by tiny_column, short_column desc;
select  *  from  sales_record  order by  employee_id  nulls first;
```
