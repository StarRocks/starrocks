---
displayed_sidebar: docs
---

# SMALLINT

## 描述

2 字节有符号整数，范围 [-32768, 32767]。

## 示例

创建表时指定字段类型为 SMALLINT。

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