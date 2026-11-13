---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "SMALLINT 是 2 字节有符号整数，取值范围为 [-32768, 32767]。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SMALLINT

## 描述

2 字节有符号整数，范围 [-32768, 32767]。

## 示例

创建表时指定字段类型为 SMALLINT。

```sql
CREATE TABLE smallintDemo (
    pk SMALLINT COMMENT "range [-32768, 32767]"
) ENGINE=OLAP 
DUPLICATE KEY(pk)
COMMENT "OLAP"
DISTRIBUTED BY HASH(pk);
```
