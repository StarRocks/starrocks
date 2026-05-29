---
displayed_sidebar: docs
---

# FLOAT

## 描述

4 字节浮点数。

## 示例

创建表时指定字段类型为 FLOAT。

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
