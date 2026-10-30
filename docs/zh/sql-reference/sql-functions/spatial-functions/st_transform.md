---
displayed_sidebar: docs
sidebar_position: 52
description: "在 EPSG:4326 与 EPSG:3857 之间重投影原生 GEOMETRY 坐标。"
---

# ST_Transform

在 EPSG:4326（经纬度）和 EPSG:3857（Web Mercator）之间重投影原生 `GEOMETRY`。结果描述符记录目标 CRS；与 [ST_SetSRID](st_setsrid.md) 不同，此函数会修改坐标。转换使用 Web Mercator 公式，无需外部 CRS 库。

## 语法

```SQL
GEOMETRY ST_Transform(GEOMETRY value, INT target_srid)
```

`target_srid` 必须是可由 FE 折叠为常量的 4326 或 3857。输入 CRS 必须是 EPSG:4326、EPSG:3857 或 OGC:CRS84（此函数将其视为 EPSG:4326 的别名）。EPSG:4326 的 SQL X/Y 表示经度/纬度。源与目标 SRID 相同时，坐标不变。

正向转换接受 -180 至 180 度的经度和约 -85.05112878 至 85.05112878 度的纬度。反向转换接受约 ±20037508.3427893 米范围内的 Web Mercator X/Y。超出范围的坐标、不支持的 CRS、错误的 WKB 以及不一致的描述符都会报错。

函数会转换七种 XY OGC 几何类型及嵌套集合中的所有坐标；`EMPTY` 保持为空，`NULL` 返回 `NULL`。不支持 Z/M 坐标和 `GEOGRAPHY`。

## 示例

```SQL
SELECT ST_X(ST_Transform(
    ST_GeomFromText('POINT (10 20)', 'EPSG:4326'), 3857));
-- 约 1113194.90793274 米
```

## 关键词

ST_TRANSFORM, GEOMETRY, CRS, EPSG, Web Mercator
