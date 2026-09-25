---
displayed_sidebar: docs
description: "互換性のある 2 つの GEOGRAPHY または GEOMETRY 点間の距離を返します。"
---

# ST_DISTANCE

`GEOGRAPHY` の場合、OGC:CRS84 のセマンティクスに基づき、2 点間の球面距離をメートル単位で返します。

`GEOMETRY` の場合、宣言された CRS の単位で平面ユークリッド距離を返します。両方の入力に互換性のあるディスクリプターが必要です。角度 CRS の座標も平面座標値として扱われ、このオーバーロードは球面計算や CRS 変換を行いません。

## 構文

```SQL
DOUBLE ST_DISTANCE(GEOGRAPHY lhs, GEOGRAPHY rhs)
DOUBLE ST_DISTANCE(GEOMETRY lhs, GEOMETRY rhs)
```

## 戻り値

両方の値が `POINT` である必要があります。いずれかの入力が `NULL` または EMPTY の場合は `NULL` を返します。`POINT` 以外のファミリー、サポートされていない次元またはディスクリプター、`GEOGRAPHY` と `GEOMETRY` の混在、互換性のない `GEOMETRY` ディスクリプターはエラーになります。他のジオメトリファミリーの組み合わせはサポートされません。

## 例

```SQL
SELECT ST_DISTANCE(
    ST_GeogFromText('POINT (0 0)'),
    ST_GeogFromText('POINT (1 0)'));
-- 約 111195.1 メートル

SELECT ST_DISTANCE(
    ST_GeomFromText('POINT (0 0)', 'EPSG:3857'),
    ST_GeomFromText('POINT (3 4)', 'EPSG:3857'));
-- 5
```

## キーワード

ST_DISTANCE,ST,DISTANCE,GEOGRAPHY,GEOMETRY
