---
displayed_sidebar: docs
---

# INT

## 描述

4 字节有符号整数，范围 [-2147483648, 2147483647]。

## 示例

创建表时指定字段类型为 INT。

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