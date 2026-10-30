---
displayed_sidebar: docs
sidebar_position: 50
description: "返回原生 GEOMETRY 描述符中的数字 EPSG SRID。"
---

# ST_SRID

返回原生 `GEOMETRY` 的 CRS 对应的数字 SRID。`OGC:CRS84` 对应 4326。如果 CRS 没有明确的数字 EPSG 映射，则返回 `NULL`，不会使用 EPSG:0 代替。

## 语法

```SQL
INT ST_SRID(GEOMETRY value)
```

输入为 `NULL` 时返回 `NULL`。此函数仅读取元数据，不转换坐标。不接受 `GEOGRAPHY`。

## 示例

```SQL
SELECT ST_SRID(ST_GeomFromText('POINT (10 20)', 'EPSG:4326'));
-- 4326
```

## 关键词

ST_SRID, GEOMETRY, CRS, EPSG
