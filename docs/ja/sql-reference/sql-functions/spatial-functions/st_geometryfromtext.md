---
displayed_sidebar: docs
description: "明示的な CRS を指定して WKT からネイティブ GEOMETRY を構築するか、従来の 1 引数形式を使用します。"
---

# ST_GeomFromText, ST_GeometryFromText

`ST_GeomFromText(wkt, crs)` は、2 次元 OGC Well-Known Text（WKT）からネイティブな平面 `GEOMETRY` 値を構築します。CRS は必須で、ネイティブ型記述子の一部になります。

既存の 1 引数 `ST_GeomFromText` と `ST_GeometryFromText` は変更されず、従来の `VARCHAR` 表現を返します。

## 構文

```Haskell
GEOMETRY ST_GeomFromText(VARCHAR wkt, VARCHAR crs)
VARCHAR ST_GeomFromText(VARCHAR wkt)
VARCHAR ST_GeometryFromText(VARCHAR wkt)
```

ネイティブ形式では、`crs` は空でない文字列リテラルである必要があります。デフォルト CRS はなく、暗黙的な座標変換や再投影も行いません。7 種類すべての 2D OGC ジオメトリ、`EMPTY`、および空の子要素をサポートします。無効な WKT または `NULL` の WKT は `NULL` を返します。

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('LINESTRING (1 1, 2 2)', 'EPSG:3857'));
+----------------------------------------------------------------------------+
| st_astext(st_geomfromtext('LINESTRING (1 1, 2 2)', 'EPSG:3857'))           |
+----------------------------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                                      |
+----------------------------------------------------------------------------+
```

## キーワード

ST_GEOMETRYFROMTEXT, ST_GEOMFROMTEXT, GEOMETRY, WKT, CRS
