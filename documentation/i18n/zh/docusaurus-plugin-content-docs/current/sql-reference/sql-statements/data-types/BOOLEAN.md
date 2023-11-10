# BOOLEAN

## 描述

BOOL, BOOLEAN

与 TINYINT 一样，0 代表 false，1 代表 true。

## 示例

创建表时指定字段类型为 BOOLEAN。

```sql
CREATE TABLE booleanDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    ispass BOOLEAN COMMENT "true/false"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
