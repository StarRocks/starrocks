---
displayed_sidebar: docs
---

# FLOAT

FLOAT

4-byte floating point number

## Examples

Create a table with a `FLOAT` column (4 bytes). `FLOAT` has a precision of approximately 7 significant decimal digits.

```sql
CREATE TABLE floatDemo (
    pk BIGINT(20) NOT NULL,
    channel FLOAT COMMENT "4 bytes"
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
