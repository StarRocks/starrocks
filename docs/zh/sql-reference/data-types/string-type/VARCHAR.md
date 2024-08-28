---
displayed_sidebar: docs
---

# VARCHAR

## 描述

VARCHAR(M)

变长字符串。`M` 代表变长字符串长度，单位：字节，默认取值为 `1`。

- StarRocks 2.1 之前的版本，`M` 的取值范围为 [1, 65533]。
- 【公测中】自 StarRocks 2.1 版本开始，`M` 的取值范围为 [1, 1048576]。

## 示例

创建表时指定字段类型为 VARCHAR。

```sql
CREATE TABLE varcharDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    pd_type VARCHAR(20) COMMENT "range char(m),m in (1-65533) "
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
