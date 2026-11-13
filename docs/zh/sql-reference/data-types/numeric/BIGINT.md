---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "BIGINT 是 8 字节有符号整数，取值范围为 [-9223372036854775808, 9223372036854775807]。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# BIGINT

## 描述

8 字节有符号整数，范围 [-9223372036854775808, 9223372036854775807]。

## 示例

创建表时指定字段类型为 BIGINT。

```sql
CREATE TABLE bigIntDemo (
    pk BIGINT(20) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
