---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "LARGEINT 是 16 字节有符号整数，取值范围为 [-2^127 + 1, 2^127 - 1]。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# LARGEINT

## 描述

16 字节有符号整数，范围 [-2^127 + 1, 2^127 - 1]。

## 示例

创建表时指定字段类型为 LARGEINT。

```sql
CREATE TABLE largeIntDemo (
    pk LARGEINT COMMENT "range [-2^127 + 1 ~ 2^127 - 1]"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
