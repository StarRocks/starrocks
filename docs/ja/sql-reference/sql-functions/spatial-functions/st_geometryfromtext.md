---
displayed_sidebar: docs
description: "2 次元 OGC WKT をネイティブ GEOMETRY 値に変換します。"
---

# ST_GeomFromText

2 次元 OGC Well-Known Text (WKT) をネイティブ `GEOMETRY` 値に変換します。

## 構文

```Haskell
GEOMETRY ST_GeomFromText(VARCHAR wkt)
```

互換性維持のため、`ST_GeometryFromText` は引き続き従来のインメモリ geography 表現を返します。

この関数は `POINT`、`LINESTRING`、`POLYGON`、`MULTIPOINT`、`MULTILINESTRING`、`MULTIPOLYGON`、`GEOMETRYCOLLECTION` と、それぞれの `EMPTY` 形式をサポートします。EWKT、SRID、Z/M 座標はサポートされません。

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))'));
+-----------------------------------------------------------------------------------------------+
| st_astext(st_geomfromtext('GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))'))          |
+-----------------------------------------------------------------------------------------------+
| GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))                                       |
+-----------------------------------------------------------------------------------------------+
```

## キーワード

ST_GEOMFROMTEXT,ST,GEOMFROMTEXT
