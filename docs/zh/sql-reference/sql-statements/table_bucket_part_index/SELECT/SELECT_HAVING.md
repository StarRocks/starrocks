---
displayed_sidebar: docs
sidebar_label: "HAVING"
---

# HAVING

HAVING 子句不用于过滤表中的行数据，而是用于过滤聚合函数的结果。

通常来说，HAVING 与聚合函数（例如 COUNT()、SUM()、AVG()、MIN()、MAX()）和 GROUP BY 子句一起使用。

## 示例

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
