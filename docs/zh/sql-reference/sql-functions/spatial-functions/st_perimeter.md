---
displayed_sidebar: docs
description: "返回原生 GEO 值中面分量的边界长度。"
---

# ST_Perimeter

返回原生 GEO 值中二维分量的总边界长度，包括外环和内环。`GEOGRAPHY` 沿大圆边计算，单位为米；`GEOMETRY` 使用输入 CRS 的坐标单位。

## 语法

```SQL
ST_Perimeter(GEOGRAPHY value)
ST_Perimeter(GEOMETRY value)
```

`POLYGON`、`MULTIPOLYGON` 以及 `GEOMETRYCOLLECTION` 中的面分量会递归求和。点和线贡献 0。

`NULL` 返回 `NULL`，`EMPTY` 返回 `0`。WKB 格式错误、不支持的维度或 descriptor，以及拓扑无效输入会返回受控错误。球面环使用与 `ST_Area` 相同的较小区域、与环方向无关的模型。

## 示例

```SQL
SELECT ST_Perimeter(ST_GeomFromText(
    'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 'EPSG:3857'));
-- 16
```

## 关键字

ST_PERIMETER,ST,PERIMETER,GEOGRAPHY,GEOMETRY
