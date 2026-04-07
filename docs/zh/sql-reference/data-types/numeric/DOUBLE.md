---
displayed_sidebar: docs
---

# DOUBLE

## 描述

8 字节浮点数。

## 示例

创建表时指定字段类型为 DOUBLE。

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
