---
displayed_sidebar: docs
description: "返回给定精度级别下所有 12 个五边形 H3 单元索引。"
---

# h3GetPentagonIndexes

返回给定精度级别下所有 12 个五边形 H3 单元索引。每个 H3 精度级别恰好有 12 个五边形，分别位于二十面体的 12 个顶点处。

## 语法

```Haskell
ARRAY<BIGINT> h3GetPentagonIndexes(INT resolution)
```

## 参数说明

- `resolution`：H3 精度级别，取值范围为 0 到 15（含）。支持的数据类型为 INT。

## 返回值说明

返回 `ARRAY<BIGINT>` 类型，包含指定精度级别下的 12 个五边形单元索引。如果参数为 NULL 或超出有效范围，则返回 NULL。

## 示例

```sql
SELECT array_length(h3GetPentagonIndexes(3));
+---------------------------------------+
| array_length(h3GetPentagonIndexes(3)) |
+---------------------------------------+
| 12                                    |
+---------------------------------------+
```

## 关键词

H3GETPENTAGONINDEXES,H3,SPATIAL
