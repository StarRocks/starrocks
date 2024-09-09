---
displayed_sidebar: docs
---

# DATETIME

## 描述

日期时间类型，取值范围是 ['0000-01-01 00:00:00', '9999-12-31 23:59:59']。

打印的形式是 `YYYY-MM-DD HH: MM: SS`。

## 示例

创建表时指定字段类型为 DATETIME。

```sql
CREATE TABLE dateTimeDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    relTime DATETIME COMMENT "YYYY-MM-DD HH:MM:SS"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
