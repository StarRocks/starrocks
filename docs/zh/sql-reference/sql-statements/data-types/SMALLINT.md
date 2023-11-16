---
displayed_sidebar: "Chinese"
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
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
