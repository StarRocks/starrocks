---
displayed_sidebar: docs
description: "明示的な CRS を指定して WKB からネイティブ GEOMETRY 値を構築します。"
---

# ST_GeomFromWKB

2 次元 OGC Well-Known Binary（WKB）からネイティブな平面 `GEOMETRY` 値を構築します。

## 構文

```Haskell
GEOMETRY ST_GeomFromWKB(VARBINARY wkb, VARCHAR crs)
```

`crs` は空でない文字列リテラルである必要があります。デフォルト CRS はなく、暗黙的な座標変換や再投影も行いません。両方の WKB バイト順、7 種類すべての 2D OGC ジオメトリ、`EMPTY`、および空の子要素をサポートします。EWKB と Z/M 座標はサポートしません。無効な WKB または `NULL` の WKB は `NULL` を返します。

## 例

```SQL
SELECT ST_AsText(ST_GeomFromWKB(
    ST_AsBinary(ST_GeomFromText('POINT (1 2)', 'EPSG:3857')),
    'EPSG:3857'));
```

```text
POINT (1 2)
```

## キーワード

ST_GEOMFROMWKB, GEOMETRY, WKB, CRS
