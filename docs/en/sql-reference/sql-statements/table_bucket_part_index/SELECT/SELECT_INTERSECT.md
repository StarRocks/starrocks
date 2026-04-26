---
displayed_sidebar: docs
sidebar_label: "INTERSECT"
---

# INTERSECT

Calculates the intersection of the results of multiple queries, that is, the results that appear in all the result sets. This clause returns only unique rows among the result sets. The ALL keyword is not supported.

## Syntax

```SQL
query_1 INTERSECT [DISTINCT] query_2
```

> **NOTE**
>
> - INTERSECT is equivalent to INTERSECT DISTINCT.
> - Each query statement must return the same number of columns and the columns must have compatible data types.

## Examples

The two tables in UNION  are used.

Return distinct `(id, price)` combinations that are common to both tables. The following two statements are equivalent.

```Plaintext
mysql> (select id, price from select1) intersect (select id, price from select2)
order by id;

+------+-------+
| id   | price |
+------+-------+
|    2 |     3 |
|    5 |     6 |
+------+-------+

mysql> (select id, price from select1) intersect distinct (select id, price from select2)
order by id;

+------+-------+
| id   | price |
+------+-------+
|    2 |     3 |
|    5 |     6 |
+------+-------+
```
