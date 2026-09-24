---
displayed_sidebar: docs
description: "GEOGRAPHY 値の OGC ジオメトリファミリーを返します。"
---

# ST_GEOMETRYTYPE

`GEOGRAPHY` 値の Open Geospatial Consortium (OGC) ジオメトリファミリーを返します。

## 構文

```SQL
VARCHAR ST_GEOMETRYTYPE(GEOGRAPHY geography)
```

## 戻り値

`ST_Point`、`ST_LineString`、`ST_Polygon`、`ST_MultiPoint`、`ST_MultiLineString`、`ST_MultiPolygon`、または `ST_GeometryCollection` を返します。型付き EMPTY 値はそのファミリー名を保持します。入力が `NULL` の場合は `NULL` を返します。

サポートされていない次元またはディスクリプターはエラーになります。`GEOMETRY` オーバーロードはサポートされません。

## 例

```SQL
SELECT ST_GEOMETRYTYPE(ST_GeogFromText('LINESTRING (0 0, 1 1)'));
-- ST_LineString

SELECT ST_GEOMETRYTYPE(ST_GeogFromText('POINT EMPTY'));
-- ST_Point
```

## キーワード

ST_GEOMETRYTYPE,ST,GEOMETRYTYPE
