---
displayed_sidebar: docs
sidebar_label: "EXCEPT/MINUS"
---

# EXCEPT/MINUS

Returns distinct results of the left-hand query that do not exist in the right-hand query. EXCEPT is equivalent to MINUS.

## Syntax

```SQL
query_1 {EXCEPT | MINUS} [DISTINCT] query_2
```

> **NOTE**
>
> - EXCEPT is equivalent to EXCEPT DISTINCT. The ALL keyword is not supported.
> - Each query statement must return the same number of columns and the columns must have compatible data types.

## Examples

The two tables in UNION  are used.

Return distinct `(id, price)` combinations in `select1` that cannot be found in `select2`.

```Plaintext
mysql> (select id, price from select1) except (select id, price from select2)
order by id;
+------+-------+
| id   | price |
+------+-------+
|    1 |     2 |
+------+-------+

mysql> (select id, price from select1) minus (select id, price from select2)
order by id;
+------+-------+
| id   | price |
+------+-------+
|    1 |     2 |
+------+-------+
```
