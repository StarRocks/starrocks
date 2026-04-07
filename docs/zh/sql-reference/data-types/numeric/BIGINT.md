---
displayed_sidebar: docs
---

# BIGINT

## 描述

8 字节有符号整数，范围 [-9223372036854775808, 9223372036854775807]。

## 示例

创建表时指定字段类型为 BIGINT。

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
