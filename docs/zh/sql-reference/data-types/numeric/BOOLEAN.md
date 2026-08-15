---
displayed_sidebar: docs
description: "BOOLEAN 类型用 0 表示 false，1 表示 true，与 TINYINT 相同。"
---

# BOOLEAN

## 描述

BOOL, BOOLEAN

与 TINYINT 一样，0 代表 false，1 代表 true。

## 示例

创建表时指定字段类型为 BOOLEAN。

```sql
CREATE TABLE booleanDemo (
    pk INT COMMENT "Sort Key",
    ispass BOOLEAN COMMENT "true/false"
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 1;

INSERT INTO booleanDemo VALUES (1, true), (2, false), (3, 1), (4, 0);
```

```Plaintext
MySQL > SELECT * FROM booleanDemo;
+------+--------+
| pk   | ispass |
+------+--------+
|    1 |      1 |
|    2 |      0 |
|    3 |      1 |
|    4 |      0 |
+------+--------+
```