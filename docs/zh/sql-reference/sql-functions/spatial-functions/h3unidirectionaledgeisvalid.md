---
displayed_sidebar: docs
description: "如果给定值是有效的 H3 有向（单向）边索引，则返回 1。"
---

# h3UnidirectionalEdgeIsValid

如果给定值是有效的 H3 有向（单向）边索引，则返回 1。有向边编码了两个相邻 H3 单元之间的共享边界，具有特定的起始和目标单元。

## 语法

```Haskell
BOOLEAN h3UnidirectionalEdgeIsValid(BIGINT edge)
```

## 参数说明

- `edge`：待验证的 H3 有向边索引。支持的数据类型为 BIGINT。

## 返回值说明

如果该值是有效的 H3 有向边索引，则返回 1（true）；否则返回 0（false）。如果参数为 NULL，则返回 NULL。

## 示例

```sql
SELECT h3UnidirectionalEdgeIsValid(1248204388774707199);
+--------------------------------------------------+
| h3UnidirectionalEdgeIsValid(1248204388774707199) |
+--------------------------------------------------+
|                                                1 |
+--------------------------------------------------+
```

## 关键词

H3UNIDIRECTIONALEDGEISVALID,H3,SPATIAL
