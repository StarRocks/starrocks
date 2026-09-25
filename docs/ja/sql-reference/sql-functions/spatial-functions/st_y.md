---
displayed_sidebar: docs
description: "GEOGRAPHY または GEOMETRY の点の Y 座標を返します。"
---

# ST_Y

点の Y 座標を返します。

`GEOGRAPHY` 値の場合、OGC:CRS84 の球面セマンティクスにおいて Y は度単位の緯度です。`GEOMETRY` 値の場合、Y は宣言された CRS の単位で返され、必ずしも緯度ではありません。値は空でない `POINT` である必要があります。

## 構文

```SQL
DOUBLE ST_Y(VARCHAR point)
DOUBLE ST_Y(GEOGRAPHY point)
DOUBLE ST_Y(GEOMETRY point)
```

## 戻り値

入力が `NULL` の場合は `NULL` を返します。ネイティブ `GEOGRAPHY` または `GEOMETRY` が空、あるいは `POINT` 以外の場合はエラーになります。ネイティブ計算では、有効なディスクリプターを持つ XY 値をサポートします。

## 例

```SQL
SELECT ST_Y(ST_Point(24.7, 56.7));
-- 56.7

SELECT ST_Y(ST_GeogFromText('POINT (24.7 56.7)'));
-- 56.7

SELECT ST_Y(ST_GeomFromText('POINT (1000000 2000000.5)', 'EPSG:3857'));
-- 2000000.5
```

## キーワード

ST_Y,ST,Y
