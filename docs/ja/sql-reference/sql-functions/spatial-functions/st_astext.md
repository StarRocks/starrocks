---
displayed_sidebar: docs
description: "GEOMETRY または GEOGRAPHY 値を WKT（Well-Known Text）形式に変換します。"
---

# ST_AsText, ST_AsWKT

`GEOMETRY` または `GEOGRAPHY` 値を WKT（Well-Known Text）形式に変換します。`ST_AsWKT` は `ST_AsText` のエイリアスです。

## 構文

```Haskell
VARCHAR ST_AsText(GEOMETRY geo)
VARCHAR ST_AsText(GEOGRAPHY geography)
VARCHAR ST_AsWKT(GEOMETRY geo)
VARCHAR ST_AsWKT(GEOGRAPHY geography)
```

ネイティブ `GEOMETRY` と `GEOGRAPHY` では、7 種類すべての OGC ジオメトリ、`EMPTY`、および空の子要素が保持されます。入力が `NULL` の場合は `NULL` を返します。シリアライズによって入力の CRS や座標は変更されません。

```SQL
SELECT ST_AsText(ST_GeogFromText('GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (1 2, 3 4))'));
```

```text
GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (1 2, 3 4))
```

```SQL
SELECT ST_AsText(ST_GeomFromText('POINT (1000000 2000000)', 'EPSG:3857'));
```

```text
POINT (1000000 2000000)
```

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```

## キーワード

ST_ASTEXT, ST_ASWKT, ST, ASTEXT, ASWKT
