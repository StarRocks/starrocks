---
displayed_sidebar: docs
description: "ポリゴンが点を内部に含むかどうかを判定します。"
---

# ST_Contains

点がポリゴン内部にあるかを返します。外周または穴の境界上にある点は含まれません。

## 構文

```SQL
ST_Contains(GEOGRAPHY polygon, GEOGRAPHY point)
ST_Contains(GEOMETRY polygon, GEOMETRY point)
ST_Contains(VARCHAR shape1, VARCHAR shape2)
```

ネイティブオーバーロードでは、第 2 引数に XY `POINT`、第 1 引数に XY `POLYGON` または `MULTIPOLYGON` を指定します。2 つの `GEOMETRY` 引数は同じディスクリプタを持つ必要があります。ネイティブ `GEOGRAPHY`/`GEOMETRY` の混在呼び出しは拒否されます。`NULL` は伝播し、`EMPTY` 入力は `false` を返します。

`VARCHAR` シグネチャは既存のレガシーオーバーロードで、動作は変更されません。

## 例

```SQL
SELECT ST_Contains(
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POINT (5 5)', 'EPSG:3857'));
-- true

SELECT ST_Contains(
    ST_GeogFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),
    ST_GeogFromText('POINT (0 5)'));
-- false: 点は境界上
```

## キーワード

ST_CONTAINS,ST,CONTAINS
