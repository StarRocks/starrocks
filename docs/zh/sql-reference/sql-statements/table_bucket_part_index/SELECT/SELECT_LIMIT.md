---
displayed_sidebar: docs
sidebar_label: "LIMIT"
---

# LIMIT

LIMIT 语句用于限制返回的最大行数。设置返回的最大行数可以帮助 StarRocks 优化内存使用。

此语句主要用于以下场景：

返回 top-N 查询的结果。

考虑一下下表包含的内容。

由于表中的数据量很大，或者因为 WHERE 语句没有过滤掉太多的数据，所以需要限制查询结果集的大小。

使用说明：LIMIT 语句的值必须是数字字面常量。

## 示例

```plain text
mysql> select tiny_column from small_table limit 1;

+-------------+
|tiny_column  |
+-------------+
|     1       |
+-------------+

1 row in set (0.02 sec)
```

```plain text
mysql> select tiny_column from small_table limit 10000;

+-------------+
|tiny_column  |
+-------------+
|      1      |
|      2      |
+-------------+

2 rows in set (0.01 sec)
```
