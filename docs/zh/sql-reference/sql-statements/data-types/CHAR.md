---
displayed_sidebar: "Chinese"
---

# CHAR

## 描述

CHAR(M)

定长字符串，M 代表的是定长字符串的长度。M 的范围是 1~255。

## 示例

创建表时指定字段类型为 CHAR。

```sql
CREATE TABLE charDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    pd_type CHAR(20) NOT NULL COMMENT "range char(m),m in (1-255) "
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
