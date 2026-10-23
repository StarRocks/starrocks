---
displayed_sidebar: docs
description: "GEOGRAPHY または GEOMETRY の点の X 座標を返します。"
---

# ST_X

点の X 座標を返します。

`GEOGRAPHY` 値の場合、OGC:CRS84 の球面セマンティクスにおいて X は度単位の経度です。`GEOMETRY` 値の場合、X は宣言された CRS の単位で返され、必ずしも経度ではありません。値は空でない `POINT` である必要があります。

## 構文

```SQL
DOUBLE ST_X(VARCHAR point)
DOUBLE ST_X(GEOGRAPHY point)
DOUBLE ST_X(GEOMETRY point)
```

## 戻り値

入力が `NULL` の場合は `NULL` を返します。ネイティブ `GEOGRAPHY` または `GEOMETRY` が空、あるいは `POINT` 以外の場合はエラーになります。ネイティブ計算では、有効なディスクリプターを持つ XY 値をサポートします。

## 例

```SQL
SELECT ST_X(ST_Point(24.7, 56.7));
-- 24.7

SELECT ST_X(ST_GeogFromText('POINT (24.7 56.7)'));
-- 24.7

SELECT ST_X(ST_GeomFromText('POINT (1000000.5 2000000)', 'EPSG:3857'));
-- 1000000.5
```

## キーワード

ST_X,ST,X
