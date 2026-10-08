---
displayed_sidebar: docs
description: "2 つのネイティブ平面ポリゴンが共有する面積、辺、孤立した接触点を返します。"
---

# ST_Intersection

境界の接触を含め、2 つのポリゴンまたはマルチポリゴンが共有する集合を返します。

## 構文

```SQL
ST_Intersection(GEOMETRY lhs, GEOMETRY rhs)
```

## 動作と制限

両方の引数は、互換性のある CRS descriptor を持つネイティブ XY `POLYGON` または `MULTIPOLYGON` である必要があります。ポリゴンとマルチポリゴンの 4 通りの組み合わせをサポートします。結果は同じ CRS と XY 座標を持つネイティブ `GEOMETRY` です。

面積の部分は `POLYGON` または `MULTIPOLYGON`、共有辺は `LINESTRING` または `MULTILINESTRING`、孤立した接触点は `POINT` または `MULTIPOINT` を返します。複数の次元を含む結果は `GEOMETRYCOLLECTION` です。面積に覆われる辺と点、および返された線に覆われる点は、別の部分として重複しません。空の交差は `POLYGON EMPTY` を返します。

`NULL` は伝播します。空入力の処理前に、NULL でない引数の構造とトポロジーを検証します。無効な入力はエラーとなり、修復しません。`GEOGRAPHY`、点・線・collection の入力、Z/M 座標、互換性のない CRS descriptor、精度グリッド引数はサポートしません。

EPSG:4326 でも直交座標演算です（度単位の直線の辺）。EPSG:3857 はメートル単位です。暗黙の座標変換は行いません。入力準備と精度は [ポリゴン overlay の契約](st_union.md) に従います。整数への再スケーリングやスナップを行わず、局所座標の浮動小数点演算を使用し、WKB 座標は double です。引数の平行移動後と出力の丸め後にポリゴンのトポロジーを検証します。潰れた線分、有限でない出力、異なる孤立点が同じ WKB 点に丸められる場合はエラーです。リングの向き、開始頂点、成分の順序、線の分割は変わることがあります。

各入力は WKB 256 KiB と 5,000 座標までです。閉じる座標も含みます。座標数を `n`、`m` とすると、`n*n + m*m + n*m` は 25,000,000 以下である必要があります。すべての次元を合計した出力は 5,000 座標までです。上限を超えると、切り捨てずにエラーを返します。準備と一時割り当てにはクエリのメモリ制限が適用されます。行間、および面積・境界交差・クリッピング呼び出しの前後でキャンセルを確認します。単一の Boost 呼び出しにはキャンセルコールバックがありません。これらの上限は固定のピークメモリや実行時間を保証しません。

## 例

```SQL
SELECT ST_Area(ST_Intersection(
    ST_GeomFromText('POLYGON ((0 0,4 0,4 4,0 4,0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((2 0,6 0,6 4,2 4,2 0))', 'EPSG:3857')));
-- 8

SELECT ST_GeometryType(ST_Intersection(
    ST_GeomFromText('POLYGON ((0 0,4 0,4 4,0 4,0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((4 0,8 0,8 4,4 4,4 0))', 'EPSG:3857')));
-- ST_LineString
```
