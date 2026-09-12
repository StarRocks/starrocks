---
displayed_sidebar: docs
description: "如果 H3 单元索引表示五边形单元，则返回 1。"
---

# h3IsPentagon

如果给定 H3 单元索引表示五边形，则返回 1。H3 网格在每个精度级别包含 12 个五边形（每个二十面体顶点各一个）；其余所有单元均为六边形。

## 语法

```Haskell
BOOLEAN h3IsPentagon(BIGINT h3index)
```

## 参数说明

- `h3index`：H3 单元索引。支持的数据类型为 BIGINT。

## 返回值说明

如果该单元是五边形，则返回 1（true）；如果是六边形，则返回 0（false）。如果参数为 NULL 或不是有效的 H3 单元索引，则返回 NULL。

## 示例

```sql
SELECT h3IsPentagon(644721767722457330);
+-----------------------------------+
| h3IsPentagon(644721767722457330)  |
+-----------------------------------+
|                                 0 |
+-----------------------------------+
```

## 关键词

H3ISPENTAGON,H3,SPATIAL
