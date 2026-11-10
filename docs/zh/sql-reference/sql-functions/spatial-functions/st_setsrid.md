---
displayed_sidebar: docs
sidebar_position: 51
description: "仅修改原生 GEOMETRY 的 CRS 元数据，不转换坐标。"
---

# ST_SetSRID

为原生 `GEOMETRY` 指定 EPSG:4326 或 EPSG:3857 的 CRS 元数据。WKB 字节和所有坐标均保持不变。需要重投影时请使用 [ST_Transform](st_transform.md)。

## 语法

```SQL
GEOMETRY ST_SetSRID(GEOMETRY value, INT target_srid)
```

`target_srid` 必须是可由 FE 折叠为常量的 4326 或 3857。源 CRS 必须是 EPSG:4326、EPSG:3857 或 OGC:CRS84。限制 CRS 范围可以在不依赖外部 CRS 数据库的情况下保证二维元数据。SQL 坐标使用 X/Y 顺序；EPSG:4326 使用经度/纬度顺序。仅支持 XY WKB，包括七种 OGC 几何类型、集合和 `EMPTY`。`NULL` 输入返回 `NULL`；错误的 WKB 或冲突的 CRS 元数据会报错。不接受 `GEOGRAPHY`。

## 示例

```SQL
SELECT ST_SRID(ST_SetSRID(
    ST_GeomFromText('POINT (10 20)', 'EPSG:4326'), 3857));
-- 3857；点的坐标仍为 (10, 20)
```

## 关键词

ST_SETSRID, GEOMETRY, CRS, EPSG
