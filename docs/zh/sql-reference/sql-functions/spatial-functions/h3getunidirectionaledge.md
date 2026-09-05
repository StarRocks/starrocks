---
displayed_sidebar: docs
description: "返回两个相邻单元之间的有向 H3 边索引。"
---

# h3GetUnidirectionalEdge

返回从起始单元到目标单元边界的有向（单向）H3 边索引。两个单元必须在相同精度级别且直接相邻。

## 语法

```Haskell
BIGINT h3GetUnidirectionalEdge(BIGINT origin, BIGINT destination)
```

## 参数说明

- `origin`：起始 H3 单元索引。支持的数据类型为 BIGINT。
- `destination`：目标 H3 单元索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 BIGINT 类型，表示从起始单元到目标单元的有向边索引。如果任意参数为 NULL、不是有效的 H3 单元索引，或两个单元不相邻，则返回 NULL。

## 示例

```sql
SELECT h3GetUnidirectionalEdge(599686042433355775, 599686043507097599);
+-----------------------------------------------------------------+
| h3GetUnidirectionalEdge(599686042433355775, 599686043507097599) |
+-----------------------------------------------------------------+
| 1248204388774707199                                             |
+-----------------------------------------------------------------+
```

## 关键词

H3GETUNIDIRECTIONALEDGE,H3,SPATIAL
