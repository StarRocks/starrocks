---
displayed_sidebar: docs
description: "计算两个原生 GEOMETRY 面的平面差集。"
---

# ST_Difference

计算两个原生 `GEOMETRY` 值的平面差集。两个输入必须是拓扑有效的 XY `POLYGON` 或 `MULTIPOLYGON`，且 CRS 描述符兼容。结果保留输入 CRS，不进行重投影。

## 语法

```SQL
ST_Difference(GEOMETRY lhs, GEOMETRY rhs)
```

根据边界关系，结果可能是点、线、面、多重几何、几何集合或空几何。`NULL` 返回 `NULL`。拓扑无效、维度或输入类型不受支持、CRS 不匹配以及 `GEOGRAPHY` 输入会返回错误。

运算以输入双精度坐标使用 GEOS 默认浮点精度。StarRocks 不请求吸附网格，也不修复无效输入；结果边界可能受浮点舍入影响。

## 示例

```SQL
SELECT ST_AsText(ST_Difference(
    ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))', 'EPSG:3857')));
```

对于示例输入，结果的 `ST_Area` 为 `3` 平方 CRS 单位。

## 关键字

ST_DIFFERENCE,ST,GEOMETRY,POLYGON
