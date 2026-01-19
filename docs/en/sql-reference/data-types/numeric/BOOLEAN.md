---
displayed_sidebar: docs
---

# BOOLEAN

BOOL, BOOLEAN

Like TINYINT, 0 stands for false and 1 for true.

## Examples

Create a table with a `BOOLEAN` column. Note that StarRocks converts `TRUE` to `1` and `FALSE` to `0`.

```sql
CREATE TABLE booleanDemo (
    pk INT COMMENT "Primary Key",
    ispass BOOLEAN COMMENT "true/false"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO booleanDemo VALUES (1, true), (2, false), (3, 1), (4, 0);
```

```Plaintext
MySQL > SELECT * FROM booleanDemo;
+------+--------+
| pk   | ispass |
+------+--------+
|    1 |      1 |
|    2 |      0 |
|    3 |      1 |
|    4 |      0 |
+------+--------+
```
