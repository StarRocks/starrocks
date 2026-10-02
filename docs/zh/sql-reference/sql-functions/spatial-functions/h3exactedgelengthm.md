---
displayed_sidebar: docs
description: "返回有向 H3 边的精确长度（以米为单位）。"
---

# h3ExactEdgeLengthM

返回有向 H3 边的精确长度（以米为单位）。与返回某精度级别平均边长的 `h3EdgeLengthM` 不同，此函数计算该单条边的精确测地长度。

## 语法

```Haskell
DOUBLE h3ExactEdgeLengthM(BIGINT h3edge)
```

## 参数说明

- `h3edge`：有向 H3 边索引。支持的数据类型为 BIGINT。

## 返回值说明

返回 DOUBLE 类型，表示该边的精确长度（米）。如果参数为 NULL 或不是有效的 H3 有向边索引，则返回 NULL。

## 示例

```sql
SELECT h3ExactEdgeLengthM(1310277011704381439);
+-----------------------------------------+
| h3ExactEdgeLengthM(1310277011704381439) |
+-----------------------------------------+
| 195449.63163407316                      |
+-----------------------------------------+
```

## 关键词

H3EXACTEDGELENGTHM,H3,SPATIAL
