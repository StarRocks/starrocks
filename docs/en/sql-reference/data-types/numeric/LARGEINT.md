---
displayed_sidebar: docs
---

# LARGEINT

LARGEINT is a 16-byte signed integer. The value range is [-2^127 + 1, 2^127 - 1].

## Examples

Create a table with a `LARGEINT` column (128-bit) to store numbers that exceed the range of `BIGINT`.

```sql
CREATE TABLE largeIntDemo (
    pk LARGEINT COMMENT "range [-2^127 + 1 ~ 2^127 - 1]"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

-- Inserting a value larger than the maximum BIGINT (approx 9 quintillion)
INSERT INTO largeIntDemo VALUES (10000000000000000000000000);
```

```Plaintext
MySQL > SELECT * FROM largeIntDemo;
+----------------------------+
| pk                         |
+----------------------------+
| 10000000000000000000000000 |
+----------------------------+
```
