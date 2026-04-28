---
displayed_sidebar: docs
sidebar_label: "OFFSET"
---

# OFFSET

The OFFSET clause causes the result set to skip the first few rows and return the following results directly.

The result set defaults to start at line 0, so offset 0 and no offset return the same results.

Generally speaking, OFFSET clauses need to be used with ORDER BY and LIMIT clauses to be valid.

## Examples

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

## Usage notes

It is allowed to use offset syntax without order by, but offset does not make sense at this time.

In this case, only the limit value is taken, and the offset value is ignored. So without order by.

Offset exceeds the maximum number of rows in the result set and is still a result. It is recommended that users use offset with order by.
