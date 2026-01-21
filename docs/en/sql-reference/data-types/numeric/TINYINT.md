---
displayed_sidebar: docs
---

# TINYINT

TINYINT is a 1-byte signed integer. The value range is [-128, 127].

## Examples

Create a table with a `TINYINT` column.

```sql
CREATE TABLE tinyIntDemo (
    pk TINYINT COMMENT "range [-128, 127]",
    pd_type VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO tinyIntDemo VALUES (127, 'Max Value');
```

```Plaintext
MySQL > SELECT * FROM tinyIntDemo;
+------+-----------+
| pk   | pd_type   |
+------+-----------+
|  127 | Max Value |
+------+-----------+
```
