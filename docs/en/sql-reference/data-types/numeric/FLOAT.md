---
displayed_sidebar: docs
---

# FLOAT

FLOAT

4-byte floating point number

## Examples

Create a table with a `FLOAT` column (4 bytes). Note that `FLOAT` has lower precision than `DOUBLE` and may round off values.

```sql
CREATE TABLE floatDemo (
    pk BIGINT(20) NOT NULL,
    channel FLOAT COMMENT "4 bytes, single precision"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO floatDemo VALUES (1, 12345.67890123456789);
```

```Plaintext
MySQL > SELECT * FROM floatDemo;
+------+-----------+
| pk   | channel   |
+------+-----------+
|    1 | 12345.679 |
+------+-----------+
```
