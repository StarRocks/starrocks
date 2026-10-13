---
displayed_sidebar: docs
description: "检查原生 GEOGRAPHY 或 GEOMETRY 值的拓扑有效性。"
---

# ST_IsValid

检查结构上可读取的原生 GEO 值是否在拓扑上有效。该函数不会修复或改写输入。

## 语法

```SQL
ST_IsValid(GEOGRAPHY value)
ST_IsValid(GEOMETRY value)
```

支持全部七种 XY OGC family，包括 `MULTI*` 和 `GEOMETRYCOLLECTION`。`GEOMETRY` 使用稳健的平面拓扑检查。`GEOGRAPHY` 检查经纬度、大圆线、球面面环和洞，并拒绝内部重叠的 MultiPolygon 分量。同一组坐标的球面结果可能与平面结果不同。

拓扑无效但可读取的值返回 `false`。`EMPTY` 返回 `true`，SQL `NULL` 返回 `NULL`。WKB 格式错误、不支持的维度，以及无效或不支持的 descriptor 会返回受控错误，而不是 `false`。

## 示例

```SQL
SELECT ST_IsValid(ST_GeomFromText(
    'POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))', 'EPSG:3857'));
-- false：环发生自相交
```

## 关键字

ST_ISVALID,ST,ISVALID,VALID,GEOGRAPHY,GEOMETRY
