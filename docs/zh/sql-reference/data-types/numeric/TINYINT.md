---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "TINYINT 是 1 字节有符号整数，取值范围为 [-128, 127]。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

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
