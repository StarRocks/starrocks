---
displayed_sidebar: docs
description: "返回指定精度级别下唯一 H3 单元的总数。"
---

# h3NumHexagons

返回指定精度级别下唯一 H3 单元的总数。H3 采用层级网格，精度 0 下有 122 个基础单元，每提升一级精度，单元数量约增加 7 倍。

## 语法

```Haskell
BIGINT h3NumHexagons(INT resolution)
```

## 参数说明

- `resolution`：H3 精度级别，取值范围为 0 到 15（含）。支持的数据类型为 INT。

## 返回值说明

返回 BIGINT 类型，表示指定精度级别下唯一 H3 单元的总数。如果参数为 NULL 或超出有效范围，则返回 NULL。

## 示例

```sql
SELECT h3NumHexagons(3);
+--------------------+
| h3NumHexagons(3)   |
+--------------------+
| 41162              |
+--------------------+
```

## 关键词

H3NUMHEXAGONS,H3,SPATIAL
