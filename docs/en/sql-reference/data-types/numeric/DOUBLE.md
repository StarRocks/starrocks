---
displayed_sidebar: docs
---

# DOUBLE

DOUBLE is an 8-byte floating point number

## Examples

Create a table with a `DOUBLE` column (8 bytes). `DOUBLE` has a precision of approximately 15 significant decimal digits.

```sql
CREATE TABLE doubleDemo (
    pk BIGINT(20) NOT NULL,
    income DOUBLE COMMENT "8 bytes"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO doubleDemo VALUES (1, 12345.67890123456789);
```

```Plaintext
MySQL > SELECT * FROM doubleDemo;
+------+--------------------+
| pk   | income             |
+------+--------------------+
|    1 | 12345.678901234567 |
+------+--------------------+
```
