---
displayed_sidebar: docs
description: "GEOGRAPHY または GEOMETRY 値の OGC ジオメトリファミリーを返します。"
---

# ST_GEOMETRYTYPE

`GEOGRAPHY` または `GEOMETRY` 値の Open Geospatial Consortium (OGC) ジオメトリファミリーを返します。

## 構文

```SQL
VARCHAR ST_GEOMETRYTYPE(GEOGRAPHY geography)
VARCHAR ST_GEOMETRYTYPE(GEOMETRY geometry)
```

## 戻り値

`ST_Point`、`ST_LineString`、`ST_Polygon`、`ST_MultiPoint`、`ST_MultiLineString`、`ST_MultiPolygon`、または `ST_GeometryCollection` を返します。型付き EMPTY 値はそのファミリー名を保持します。入力が `NULL` の場合は `NULL` を返します。

ネイティブ計算では、有効なディスクリプターを持つ XY 値をサポートします。サポートされていない次元またはディスクリプターはエラーになります。

## 例

```SQL
SELECT ST_GEOMETRYTYPE(ST_GeogFromText('LINESTRING (0 0, 1 1)'));
-- ST_LineString

SELECT ST_GEOMETRYTYPE(ST_GeogFromText('POINT EMPTY'));
-- ST_Point

SELECT ST_GEOMETRYTYPE(
    ST_GeomFromText('GEOMETRYCOLLECTION (POINT (0 0))', 'EPSG:3857'));
-- ST_GeometryCollection
```

## キーワード

ST_GEOMETRYTYPE,ST,GEOMETRYTYPE
