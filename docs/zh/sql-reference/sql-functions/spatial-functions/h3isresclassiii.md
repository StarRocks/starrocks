---
displayed_sidebar: docs
description: "如果 H3 单元的精度级别为 Class III（奇数精度 1,3,5,7,9,11,13,15），则返回 1。"
---

# h3IsResClassIII

如果给定 H3 单元的精度级别为 Class III，则返回 1。H3 精度交替使用 Class II（偶数：0,2,4,6,8,10,12,14）和 Class III（奇数：1,3,5,7,9,11,13,15）。Class III 网格相对于 Class II 网格旋转了 19.1°。

## 语法

```Haskell
BOOLEAN h3IsResClassIII(BIGINT h3index)
```

## 参数说明

- `h3index`：H3 单元索引。支持的数据类型为 BIGINT。

## 返回值说明

如果该单元处于 Class III 精度级别，则返回 1（true）；如果处于 Class II 精度级别，则返回 0（false）。如果参数为 NULL 或不是有效的 H3 单元索引，则返回 NULL。

## 示例

```sql
SELECT h3IsResClassIII(617420388352917503);
+--------------------------------------+
| h3IsResClassIII(617420388352917503)  |
+--------------------------------------+
|                                    1 |
+--------------------------------------+
```

## 关键词

H3ISRESCLASSIII,H3,SPATIAL
