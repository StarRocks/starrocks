---
displayed_sidebar: docs
description: "返回原生 GEO 值按维度加权的质心。"
---

# ST_Centroid

返回与输入具有相同原生逻辑类型和 descriptor 的 `POINT` 质心。质心是加权几何中心，不保证位于面内部。

## 语法

```SQL
ST_Centroid(GEOGRAPHY value)
ST_Centroid(GEOMETRY value)
```

面按面积加权，线按长度加权，点采用相等权重。只有输入中最高的非空维度参与计算：面优先于线，线优先于点。该规则递归应用于 `MULTI*` 和 `GEOMETRYCOLLECTION`；面的洞会扣除其面积和矩。

`GEOGRAPHY` 在单位球面上计算，并按 OGC:CRS84 的经度/纬度顺序返回。`GEOMETRY` 使用平面坐标。结果保留输入的 CRS 和逻辑类型，不执行坐标转换。

`NULL` 返回 `NULL`，`EMPTY` 返回 `POINT EMPTY`。无法定义的零球面向量或没有非空分量的输入返回 `POINT EMPTY`。WKB 格式错误、不支持的维度或 descriptor，以及拓扑无效输入会返回受控错误。

## 示例

```SQL
SELECT ST_AsText(ST_Centroid(ST_GeomFromText(
    'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 'EPSG:3857')));
-- POINT (2 2)
```

## 关键字

ST_CENTROID,ST,CENTROID,GEOGRAPHY,GEOMETRY
