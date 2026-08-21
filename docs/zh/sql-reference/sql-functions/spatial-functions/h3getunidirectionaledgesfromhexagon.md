---
displayed_sidebar: docs
description: "返回从给定单元出发的所有有向 H3 边索引。"
---

# h3GetUnidirectionalEdgesFromHexagon

返回从给定单元出发的所有有向（单向）H3 边索引。六边形有 6 条边，五边形有 5 条边。

## 语法

```Haskell
ARRAY<BIGINT> h3GetUnidirectionalEdgesFromHexagon(BIGINT h3index)
```

## 参数说明

- `h3index`：H3 单元索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 `ARRAY<BIGINT>` 类型，包含从该单元出发的有向边索引。六边形返回 6 个元素，五边形返回 5 个元素。如果参数为 NULL 或不是有效的 H3 单元索引，则返回 NULL。

## 示例

```sql
SELECT h3GetUnidirectionalEdgesFromHexagon(599686042433355775);
+-------------------------------------------------------------------------------------------+
| h3GetUnidirectionalEdgesFromHexagon(599686042433355775)                                   |
+-------------------------------------------------------------------------------------------+
| [1248204388774707199,1320261982812635135,1392319576850563071,1464377170888491007,1536434764926418943,1608492358964346879] |
+-------------------------------------------------------------------------------------------+
```

## 关键词

H3GETUNIDIRECTIONALEDGESFROMHEXAGON,H3,SPATIAL
