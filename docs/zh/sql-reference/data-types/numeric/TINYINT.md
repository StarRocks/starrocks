---
displayed_sidebar: docs
---

# TINYINT

## 描述

1 字节有符号整数，范围 [-128, 127]。

## 示例

创建表时指定字段类型为 TINYINT。

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