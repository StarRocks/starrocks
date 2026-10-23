---
displayed_sidebar: docs
description: "返回原生 GEOGRAPHY 或 GEOMETRY 值的二维面积。"
---

# ST_Area

返回原生 GEO 值中二维分量的面积。`GEOGRAPHY` 基于 StarRocks 球面地球模型，单位为平方米；`GEOMETRY` 的单位为输入 CRS 坐标单位的平方。该函数不执行坐标转换。

## 语法

```SQL
ST_Area(GEOGRAPHY value)
ST_Area(GEOMETRY value)
```

`POLYGON` 和 `MULTIPOLYGON` 贡献填充面积，并扣除洞的面积。`GEOMETRYCOLLECTION` 中的面分量会递归求和；点和线贡献 0。重叠的面分量会使输入在拓扑上无效，并返回错误，而不会重复计数。

对于 `GEOGRAPHY`，环的方向不决定内部区域。每个环都会归一化为球面上较小的区域，因此单个环不能表示大于半球的区域。洞和 MultiPolygon 分量遵循同一规则。

`NULL` 返回 `NULL`，`EMPTY` 返回 `0`。WKB 格式错误、不支持的维度或 descriptor，以及拓扑无效的输入会返回受控错误。

## 示例

```SQL
SELECT ST_Area(ST_GeomFromText(
    'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))',
    'EPSG:3857'));
-- 15
```

## 关键字

ST_AREA,ST,AREA,GEOGRAPHY,GEOMETRY
