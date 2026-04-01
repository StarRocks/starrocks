---
displayed_sidebar: docs
sidebar_label: "LIMIT"
---

# LIMIT

LIMIT clauses are used to limit the maximum number of rows returned. Setting the maximum number of rows returned can help StarRocks optimize memory usage.

This clause is mainly used in the following scenarios:

Returns the result of the top-N query.

Think about what's included in the table below.

The size of the query result set needs to be limited because of the large amount of data in the table or because the where clause does not filter too much data.

Instructions for use: The value of the LIMIT clause must be a numeric literal constant.

## Examples

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
