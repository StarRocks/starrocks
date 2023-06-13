# TINYINT

## 描述

1 字节有符号整数，范围 [-128, 127]。

## 示例

创建表时指定字段类型为 TINYINT。

```sql
CREATE TABLE tinyIntDemo (
    pk TINYINT COMMENT "range [-128, 127]",
    pd_type VARCHAR(20) COMMENT "range char(m),m in (1-255) "
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
