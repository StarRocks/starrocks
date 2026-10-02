---
displayed_sidebar: docs
description: "ネイティブ平面ポリゴンまたはマルチポリゴンから別の領域を差し引きます。"
---

# ST_Difference

第 1 入力のうち第 2 入力の外側にある領域を返します。引数の順序で結果が変わります。穴と離れた成分を保持します。

## 構文

```SQL
ST_Difference(GEOMETRY lhs, GEOMETRY rhs)
```

## 動作と制限

引数は互換性のある CRS 記述子を持つネイティブ XY `POLYGON` または `MULTIPOLYGON` です。全 4 通りの組み合わせをサポートします。戻り値は同じ CRS と XY 座標のネイティブ `GEOMETRY` です。面成分が 1 つの場合は `POLYGON`、複数の場合は `MULTIPOLYGON`、空の場合は `POLYGON EMPTY` になります。

`NULL` 引数は `NULL` を返します。`EMPTY` は空領域であり、`NULL` とは異なります。空入力の処理より前に、NULL 以外の入力の構造とトポロジーを検証します。穴とマルチポリゴン成分間の関係も検証します。無効な入力はエラーになり、修復しません。

EPSG:4326 でも直交座標で処理します（度単位の直線の辺）。EPSG:3857 はメートル単位の投影座標を使用します。暗黙の座標変換は行いません。`GEOGRAPHY`、点、線、コレクション、Z/M、互換性のない CRS、集約や配列形式、精度グリッド引数はサポートしません。

浮動小数点演算を使用し、スナップや整数リスケーリングは行いません。WKB の出力座標は double です。リングの向き、開始点、成分の順序は変わる場合があります。丸めで出力が無効になった場合は、修復せずにエラーを返します。

各入力は 256 KiB の WKB と 5,000 座標（閉じる座標を含む）に制限されます。座標数を `n`、`m` とすると、`n*n + m*m + n*m <= 25,000,000` も必要です。出力と対称差の各中間結果は最大 5,000 座標です。中間結果の組み合わせにも同じ計算量制限が適用されます。超過時は切り詰めずにエラーを返します。入力モデルと一時割り当てにはクエリのメモリ制限が適用されます。行間および overlay 呼び出しの前後でキャンセルを確認します。単一の Boost 呼び出しにはキャンセルコールバックがありません。これらの制限は固定のピークメモリや実行時間を保証しません。

## 例

```SQL
SELECT ST_Area(ST_Difference(
    ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((2 0, 6 0, 6 4, 2 4, 2 0))', 'EPSG:3857')));
-- 8

SELECT ST_AsText(ST_Difference(
    ST_GeomFromText('POLYGON EMPTY', 'EPSG:3857'),
    ST_GeomFromText('POLYGON EMPTY', 'EPSG:3857')));
-- POLYGON EMPTY
```
