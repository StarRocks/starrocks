---
displayed_sidebar: docs
description: "VARCHAR(M) 是变长字符串类型。自 StarRocks 4.2 起，M 的取值范围为 [1, 2147482624] 字节。"
---

# VARCHAR

## 描述

VARCHAR(M)

变长字符串。`M` 代表变长字符串长度，单位：字节，默认取值为 `1`。

- StarRocks 2.1 之前的版本，`M` 的取值范围为 [1, 65533]。
- StarRocks 2.1 至 4.1 版本，`M` 的取值范围为 [1, 1048576]。
- 自 StarRocks 4.2 起，`M` 的取值范围为 [1, 2147482624]（2 GiB 减 1 KiB）。

## 示例

创建表时指定字段类型为 VARCHAR。

```sql
CREATE TABLE varcharDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]",
    pd_type VARCHAR(20) COMMENT "variable-length string"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
