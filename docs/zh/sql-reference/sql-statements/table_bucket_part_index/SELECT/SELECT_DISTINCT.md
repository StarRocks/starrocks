---
displayed_sidebar: docs
sidebar_label: "DISTINCT"
---

# DISTINCT

DISTINCT 关键字可以对结果集进行去重。例如：

```SQL
-- Returns the unique values from one column.
select distinct tiny_column from big_table limit 2;

-- Returns the unique combinations of values from multiple columns.
select distinct tiny_column, int_column from big_table limit 2;
```

`DISTINCT` 可以与聚合函数（通常是计数函数）一起使用，`count (distinct)` 用于计算一列或多列中包含多少不同的组合。

```SQL
-- Counts the unique values from one column.
select count(distinct tiny_column) from small_table;
```

```plain text
+-------------------------------+
| count(DISTINCT 'tiny_column') |
+-------------------------------+
|             2                 |
+-------------------------------+
1 row in set (0.06 sec)
```

```SQL
-- Counts the unique combinations of values from multiple columns.
select count(distinct tiny_column, int_column) from big_table limit 2;
```

StarRocks 支持同时使用多个 `distinct` 聚合函数。

```SQL
-- Count the unique value from multiple aggregation function separately.
select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;
```
