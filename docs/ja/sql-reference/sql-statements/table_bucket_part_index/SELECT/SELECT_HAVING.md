---
displayed_sidebar: docs
sidebar_label: "HAVING"
---

# HAVING

HAVING 句は、テーブル内の行データをフィルタリングするのではなく、集計関数の結果をフィルタリングします。

一般的に、HAVING は集計関数 (COUNT()、SUM()、AVG()、MIN()、MAX() など) および GROUP BY 句とともに使用されます。

## 例

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
