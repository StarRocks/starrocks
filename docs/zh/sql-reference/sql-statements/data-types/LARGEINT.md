---
displayed_sidebar: "Chinese"
---

# LARGEINT

## 描述

16 字节有符号整数，范围 [-2^127 + 1 ~ 2^127 - 1]。

## 示例

创建表时指定字段类型为 LARGEINT。

```sql
CREATE TABLE largeIntDemo (
    pk LARGEINT COMMENT "range [-2^127 + 1 ~ 2^127 - 1]"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk) BUCKETS 4;
```
