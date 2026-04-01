---
displayed_sidebar: docs
sidebar_label: "DISTINCT"
---

# DISTINCT

The DISTINCT keyword deduplicates the result set.

Example:

```SQL
-- Returns the unique values from one column.
select distinct tiny_column from big_table limit 2;

-- Returns the unique combinations of values from multiple columns.
select distinct tiny_column, int_column from big_table limit 2;
```

DISTINCT can be used with aggregate functions (usually count functions), and count (distinct) is used to calculate how many different combinations are contained on one or more columns.

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

StarRocks supports multiple aggregate functions using distinct at the same time.

```SQL
-- Count the unique value from multiple aggregation function separately.
select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;
```
