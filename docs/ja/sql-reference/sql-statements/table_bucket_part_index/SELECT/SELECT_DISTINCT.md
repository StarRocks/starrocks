---
displayed_sidebar: docs
sidebar_label: "DISTINCT"
---

# DISTINCT

DISTINCT キーワードは、結果セットから重複する行を削除します。 例：

```SQL
-- Returns the unique values from one column.
select distinct tiny_column from big_table limit 2;

-- Returns the unique combinations of values from multiple columns.
select distinct tiny_column, int_column from big_table limit 2;
```

DISTINCT は、集計関数（通常はカウント関数）とともに使用でき、count (distinct) は、1つ以上のカラムに含まれる異なる組み合わせの数を計算するために使用されます。

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

StarRocks は、distinct を使用した複数の集計関数を同時にサポートしています。

```SQL
-- Count the unique value from multiple aggregation function separately.
select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;
```
