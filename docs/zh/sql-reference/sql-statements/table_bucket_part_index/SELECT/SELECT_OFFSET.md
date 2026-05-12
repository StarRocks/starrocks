---
displayed_sidebar: docs
sidebar_label: "OFFSET"
---

# OFFSET

`OFFSET` 子句使结果集跳过前几行，然后直接返回后面的结果。

结果集默认从第 0 行开始，因此 `OFFSET 0` 和没有 `OFFSET` 返回相同的结果。

一般来说，`OFFSET` 子句需要与 `ORDER BY` 和 `LIMIT` 子句一起使用才有效。

## 示例

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 3;

+----------------+
| varchar_column | 
+----------------+
|    beijing     | 
|    chongqing   | 
|    tianjin     | 
+----------------+

3 rows in set (0.02 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 0;

+----------------+
|varchar_column  |
+----------------+
|     beijing    |
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 1;

+----------------+
|varchar_column  |
+----------------+
|    chongqing   | 
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 2;

+----------------+
|varchar_column  |
+----------------+
|     tianjin    |     
+----------------+

1 row in set (0.02 sec)
```

## 使用说明

允许在没有 order by 的情况下使用 offset 语法，但此时 offset 没有意义。

在这种情况下，仅采用 limit 值，而忽略 offset 值。因此，没有 order by。

Offset 超过结果集中的最大行数，但仍然会返回结果。建议用户将 offset 与 order by 一起使用。
