---
displayed_sidebar: "Chinese"
---

# FLOAT

## 描述

4 字节浮点数。

## 示例

创建表时指定字段类型为 FLOAT。

```sql
CREATE TABLE floatDemo (
    pk BIGINT(20) NOT NULL COMMENT "",
    channel FLOAT COMMENT "4 bytes"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk) BUCKETS 4;
```
