---
displayed_sidebar: docs
description: "返回指定精度级别下 H3 单元的平均面积（以平方千米为单位）。"
---

# h3HexAreaKm2

返回指定精度级别下 H3 单元的平均面积，以平方千米为单位。

## 语法

```Haskell
DOUBLE h3HexAreaKm2(INT resolution)
```

## 参数说明

- `resolution`：H3 精度级别，取值范围为 0 到 15（含）。支持的数据类型为 INT。

## 返回值说明

返回 DOUBLE 类型，表示指定精度级别下的平均单元面积（平方千米）。如果参数为 NULL 或超出有效范围，则返回 NULL。

## 示例

```sql
SELECT h3HexAreaKm2(13);
+-------------------+
| h3HexAreaKm2(13)  |
+-------------------+
| 0.0000439         |
+-------------------+
```

## 关键词

H3HEXAREAKM2,H3,SPATIAL
