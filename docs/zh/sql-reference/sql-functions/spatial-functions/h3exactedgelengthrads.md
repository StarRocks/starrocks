---
displayed_sidebar: docs
description: "返回有向 H3 边的精确长度（以弧度为单位）。"
---

# h3ExactEdgeLengthRads

返回有向 H3 边的精确长度（以弧度为单位），即在单位球面上的大圆弧长度。

## 语法

```Haskell
DOUBLE h3ExactEdgeLengthRads(BIGINT h3edge)
```

## 参数说明

- `h3edge`：有向 H3 边索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 DOUBLE 类型，表示该边的精确长度（弧度）。如果参数为 NULL 或不是有效的 H3 有向边索引，则返回 NULL。

## 示例

```sql
SELECT h3ExactEdgeLengthRads(1310277011704381439);
+--------------------------------------------+
| h3ExactEdgeLengthRads(1310277011704381439) |
+--------------------------------------------+
| 0.030677980118976447                       |
+--------------------------------------------+
```

## 关键词

H3EXACTEDGELENGTHRADS,H3,SPATIAL
