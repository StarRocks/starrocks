---
displayed_sidebar: docs
---

# ST_GeometryFromText,ST_GeomFromText

WKT (Well Known Text) を対応するメモリジオメトリに変換します。

## 構文

```Haskell
GEOMETRY ST_GeometryFromText(VARCHAR wkt)
```

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeometryFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                   |
+---------------------------------------------------------+
```

## キーワード

ST_GEOMETRYFROMTEXT,ST_GEOMFROMTEXT,ST,GEOMETRYFROMTEXT,GEOMFROMTEXT