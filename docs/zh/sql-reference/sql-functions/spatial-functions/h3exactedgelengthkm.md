---
displayed_sidebar: docs
description: "返回有向 H3 边的精确长度（以千米为单位）。"
---

# h3ExactEdgeLengthKm

返回有向 H3 边的精确长度（以千米为单位）。与返回某精度级别平均边长的 `h3EdgeLengthKm` 不同，此函数计算该单条边的精确测地长度。

## 语法

```Haskell
DOUBLE h3ExactEdgeLengthKm(BIGINT h3edge)
```

## 参数说明

- `h3edge`：有向 H3 边索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 DOUBLE 类型，表示该边的精确长度（千米）。如果参数为 NULL 或不是有效的 H3 有向边索引，则返回 NULL。

## 示例

```sql
SELECT h3ExactEdgeLengthKm(1310277011704381439);
+------------------------------------------+
| h3ExactEdgeLengthKm(1310277011704381439) |
+------------------------------------------+
| 195.44963163407317                       |
+------------------------------------------+
```

## 关键词

H3EXACTEDGELENGTHKM,H3,SPATIAL
