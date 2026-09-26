---
displayed_sidebar: docs
description: "返回有向（单向）边的目标 H3 单元索引。"
---

# h3GetDestinationIndexFromUnidirectionalEdge

返回有向（单向）边的目标 H3 单元索引。有向边连接两个相邻单元，目标单元是该边指向的单元。

## 语法

```Haskell
BIGINT h3GetDestinationIndexFromUnidirectionalEdge(BIGINT edge)
```

## 参数说明

- `edge`：有效的 H3 有向边索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 BIGINT 类型，表示有向边的目标单元索引。如果参数为 NULL 或不是有效的 H3 有向边索引，则返回 NULL。

## 示例

```sql
SELECT h3GetDestinationIndexFromUnidirectionalEdge(1248204388774707197);
+------------------------------------------------------------------+
| h3GetDestinationIndexFromUnidirectionalEdge(1248204388774707197) |
+------------------------------------------------------------------+
| 599686043507097597                                               |
+------------------------------------------------------------------+
```

## 关键词

H3GETDESTINATIONINDEXFROMUNIDIRECTIONALEDGE,H3,SPATIAL
