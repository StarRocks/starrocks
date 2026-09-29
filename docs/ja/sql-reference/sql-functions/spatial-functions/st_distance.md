---
displayed_sidebar: docs
description: "対応する GEOGRAPHY または GEOMETRY 値間の最小距離を返します。"
---

# ST_DISTANCE

対応する 2 つの値間の最小距離を返します。点から線への距離は、線分の内部と端点を含む、すべての線分上の最近点までの距離です。

`GEOGRAPHY` は球面 OGC:CRS84 エッジで評価し、メートル単位で返します。`GEOMETRY` は平面エッジで評価し、宣言された CRS の単位で返します。`GEOMETRY` 入力には互換性のあるディスクリプタが必要です。この関数は CRS 変換を行いません。

## 構文

```SQL
DOUBLE ST_DISTANCE(GEOGRAPHY lhs, GEOGRAPHY rhs)
DOUBLE ST_DISTANCE(GEOMETRY lhs, GEOMETRY rhs)
```

## パラメータ

次のファミリの組み合わせに対応します。

- `POINT` と `POINT`
- `POINT` と `LINESTRING` または `MULTILINESTRING`（引数の順序は問いません）

すべての値は 2 次元である必要があります。`GEOGRAPHY` の線にある正確な対蹠線分、または数値的に曖昧な対蹠線分は、一意な最短球面エッジを定義しないため拒否されます。

## 戻り値

いずれかの入力が `NULL` または EMPTY の場合は `NULL` を返します。未対応のファミリまたはディスクリプタ、`GEOGRAPHY` と `GEOMETRY` の混在、互換性のない `GEOMETRY` ディスクリプタはエラーになります。

## 例

```SQL
SELECT ST_DISTANCE(
    ST_GeogFromText('POINT (0 1)'),
    ST_GeogFromText('LINESTRING (-1 0, 1 0)'));
-- 約 111195.1 メートル

SELECT ST_DISTANCE(
    ST_GeomFromText('POINT (5 3)', 'EPSG:3857'),
    ST_GeomFromText('LINESTRING (0 0, 10 0)', 'EPSG:3857'));
-- 3
```

## キーワード

ST_DISTANCE,ST,DISTANCE,GEOGRAPHY,GEOMETRY
