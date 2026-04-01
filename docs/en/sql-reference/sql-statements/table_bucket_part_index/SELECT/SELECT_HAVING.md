---
displayed_sidebar: docs
sidebar_label: "HAVING"
---

# HAVING

The HAVING clause does not filter row data in a table, but filters the results of aggregate functions.

Generally speaking, HAVING is used with aggregate functions (such as COUNT(), SUM(), AVG(), MIN(), MAX()) and GROUP BY clauses.

## Examples

```sql
select tiny_column, sum(short_column) 
from small_table 
group by tiny_column 
having sum(short_column) = 1;
```

```plain text
+-------------+---------------------+
|tiny_column  | sum('short_column') |
+-------------+---------------------+
|         2   |        1            |
+-------------+---------------------+

1 row in set (0.07 sec)
```

```sql
select tiny_column, sum(short_column) 
from small_table 
group by tiny_column 
having tiny_column > 1;
```

```plain text
+-------------+---------------------+
|tiny_column  | sum('short_column') |
+-------------+---------------------+
|      2      |          1          |
+-------------+---------------------+

1 row in set (0.07 sec)
```
