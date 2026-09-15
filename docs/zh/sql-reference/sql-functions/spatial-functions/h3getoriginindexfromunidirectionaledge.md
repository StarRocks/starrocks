---
displayed_sidebar: docs
description: "返回有向（单向）边的起始 H3 单元索引。"
---

# h3GetOriginIndexFromUnidirectionalEdge

返回有向（单向）边的起始 H3 单元索引。有向边连接两个相邻单元，起始单元是该边的出发点。

## 语法

```Haskell
BIGINT h3GetOriginIndexFromUnidirectionalEdge(BIGINT edge)
```

## 参数说明

- `edge`：有效的 H3 有向边索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 BIGINT 类型，表示有向边的起始单元索引。如果参数为 NULL 或不是有效的 H3 有向边索引，则返回 NULL。

## 示例

```sql
SELECT h3GetOriginIndexFromUnidirectionalEdge(1248204388774707197);
+-------------------------------------------------------------+
| h3GetOriginIndexFromUnidirectionalEdge(1248204388774707197) |
+-------------------------------------------------------------+
| 599686042433355773                                          |
+-------------------------------------------------------------+
```

## 关键词

H3GETORIGININDEXFROMUNIDIRECTIONALEDGE,H3,SPATIAL
