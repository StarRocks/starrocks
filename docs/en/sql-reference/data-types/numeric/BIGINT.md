---
displayed_sidebar: docs
---

# BIGINT

BIGINT is an 8-byte signed integer. The value range is [-9223372036854775808, 9223372036854775807].

## Examples

Create a table with a `BIGINT` column and insert a value near the maximum limit.

```sql
CREATE TABLE bigIntDemo (
    pk BIGINT(20) NOT NULL COMMENT "Range: -2^63 to 2^63-1"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO bigIntDemo VALUES (123456789012345), (9223372036854775807);
```

```Plaintext
MySQL > SELECT * FROM bigIntDemo;
+---------------------+
| pk                  |
+---------------------+
|     123456789012345 |
| 9223372036854775807 |
+---------------------+
```
