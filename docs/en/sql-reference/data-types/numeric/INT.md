---
displayed_sidebar: docs
---

# INT

INT is a 4-byte signed integer. The value range is [-2147483648, 2147483647].

## Examples

Create a table with an `INT` column and insert the minimum and maximum allowed values.

```sql
CREATE TABLE intDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO intDemo VALUES (2147483647), (-2147483648);
```

```Plaintext
MySQL > SELECT * FROM intDemo;
+-------------+
| pk          |
+-------------+
| -2147483648 |
|  2147483647 |
+-------------+
```
