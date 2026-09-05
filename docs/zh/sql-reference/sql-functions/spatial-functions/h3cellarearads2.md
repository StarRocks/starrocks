---
displayed_sidebar: docs
description: "返回指定 H3 单元的精确面积（以球面度为单位）。"
---

# h3CellAreaRads2

返回指定 H3 单元的精确面积（以球面度为单位）。这是该单元投影到单位球面上的测地面积。

## 语法

```Haskell
DOUBLE h3CellAreaRads2(BIGINT h3index)
```

## 参数说明

- `h3index`：H3 单元索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 DOUBLE 类型，表示该单元的精确面积（球面度）。如果参数为 NULL 或不是有效的 H3 单元索引，则返回 NULL。

## 示例

```sql
SELECT h3CellAreaRads2(579205133326352383);
+--------------------------------------+
| h3CellAreaRads2(579205133326352383)  |
+--------------------------------------+
| 0.10116268528089567                  |
+--------------------------------------+
```

## 关键词

H3CELLAREARADS2,H3,SPATIAL
