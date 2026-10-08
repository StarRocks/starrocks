---
displayed_sidebar: docs
description: "ネイティブ GEO 値の 1 次元要素の長さを返します。"
---

# ST_Length

ネイティブ GEO 値に含まれる 1 次元要素の合計長を返します。`GEOGRAPHY` は大円エッジに沿って計算し、単位はメートルです。`GEOMETRY` は入力 CRS の座標単位を使用します。

## 構文

```SQL
ST_Length(GEOGRAPHY value)
ST_Length(GEOMETRY value)
```

`LINESTRING`、`MULTILINESTRING`、および `GEOMETRYCOLLECTION` 内の線要素を再帰的に合計します。点とポリゴン境界は 0 です。ポリゴン境界には `ST_Perimeter` を使用します。

`NULL` は `NULL`、`EMPTY` は `0` を返します。不正な WKB、未対応の次元または descriptor、球面上の曖昧な対蹠線分、トポロジ的に無効な入力は制御されたエラーになります。

## 例

```SQL
SELECT ST_Length(ST_GeomFromText('LINESTRING (0 0, 3 4)', 'EPSG:3857'));
-- 5

SELECT ST_Length(ST_GeogFromText('LINESTRING (0 0, 1 0)'));
-- 約 111195.101177484 メートル
```

## キーワード

ST_LENGTH,ST,LENGTH,GEOGRAPHY,GEOMETRY
