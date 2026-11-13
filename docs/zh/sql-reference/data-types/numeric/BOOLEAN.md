---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "BOOLEAN 类型用 0 表示 false，1 表示 true，与 TINYINT 相同。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

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
