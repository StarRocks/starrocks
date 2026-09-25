---
displayed_sidebar: docs
description: "返回 GEOGRAPHY 值的 OGC 几何类型。"
---

# ST_GEOMETRYTYPE

返回 `GEOGRAPHY` 值的开放地理空间信息联盟 (OGC) 几何类型。

## 语法

```SQL
VARCHAR ST_GEOMETRYTYPE(GEOGRAPHY geography)
```

## 返回值说明

返回 `ST_Point`、`ST_LineString`、`ST_Polygon`、`ST_MultiPoint`、`ST_MultiLineString`、`ST_MultiPolygon` 或 `ST_GeometryCollection`。带类型的 EMPTY 值保留其几何类型名称。输入为 `NULL` 时返回 `NULL`。

不支持的维度或描述符会返回错误。不支持 `GEOMETRY` 重载。

## 示例

```SQL
SELECT ST_GEOMETRYTYPE(ST_GeogFromText('LINESTRING (0 0, 1 1)'));
-- ST_LineString

SELECT ST_GEOMETRYTYPE(ST_GeogFromText('POINT EMPTY'));
-- ST_Point
```

## 关键字

ST_GEOMETRYTYPE,ST,GEOMETRYTYPE
