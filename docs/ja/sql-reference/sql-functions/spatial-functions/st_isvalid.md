---
displayed_sidebar: docs
description: "ネイティブ GEOGRAPHY または GEOMETRY 値のトポロジ妥当性を検査します。"
---

# ST_IsValid

構造的に読み取れるネイティブ GEO 値がトポロジ的に妥当かを返します。入力の修復や書き換えは行いません。

## 構文

```SQL
ST_IsValid(GEOGRAPHY value)
ST_IsValid(GEOMETRY value)
```

`MULTI*` と `GEOMETRYCOLLECTION` を含む 7 種類すべての XY OGC family をサポートします。`GEOMETRY` は堅牢な平面トポロジ検査を使用します。`GEOGRAPHY` は経度/緯度、大円ライン、球面ポリゴンのリングと穴を検査し、内部が重なる MultiPolygon 要素を拒否します。同じ座標でも球面と平面の結果は異なる場合があります。

読み取り可能でもトポロジ的に無効な値は `false` を返します。`EMPTY` は `true`、SQL `NULL` は `NULL` です。不正な WKB、未対応の次元、無効または未対応の descriptor は `false` ではなく制御されたエラーになります。

## 例

```SQL
SELECT ST_IsValid(ST_GeomFromText(
    'POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))', 'EPSG:3857'));
-- false: リングが自己交差します
```

## キーワード

ST_ISVALID,ST,ISVALID,VALID,GEOGRAPHY,GEOMETRY
