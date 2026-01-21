---
displayed_sidebar: docs
---

# SMALLINT

SMALLINT is a 2-byte signed integer. The value range is [-32768, 32767].

## Examples

Create a table with a `SMALLINT` column.

```sql
CREATE TABLE smallintDemo (
    pk SMALLINT COMMENT "range [-32768, 32767]"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO smallintDemo VALUES (32767);
```

```Plaintext
MySQL > SELECT * FROM smallintDemo;
+-------+
| pk    |
+-------+
| 32767 |
+-------+
```
