---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "INT 是 4 字节有符号整数，取值范围为 [-2147483648, 2147483647]。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# INT

## 描述

4 字节有符号整数，范围 [-2147483648, 2147483647]。

## 示例

创建表时指定字段类型为 INT。

```sql
CREATE TABLE intDemo (
    pk INT COMMENT "range [-2147483648, 2147483647]"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
