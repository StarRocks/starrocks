---
displayed_sidebar: docs
description: "トポロジーと CRS を維持しながらネイティブ XY ジオメトリを簡略化します。"
---

# ST_SimplifyPreserveTopology

ネイティブな直交座標 XY `GEOMETRY` の頂点を減らし、有効性、次元、構成要素、リング、穴の所属、既存の接触および分離関係を維持します。安全に削除できない部分は元のまま残します。単一のジオメトリに対するスカラー演算であり、複数行にまたがるカバレッジの簡略化ではありません。

## 構文

```SQL
ST_SimplifyPreserveTopology(GEOMETRY geometry, DOUBLE tolerance)
```

許容値は定数でも行ごとに異なる値でも指定できます。入力座標の単位で有限の非負数である必要があります。第3引数はありません。

## 入力と結果

POINT、LINESTRING、POLYGON、対応する MULTI 型、および入れ子の GEOMETRYCOLLECTION をサポートします。結果は入力の型、構成要素の順序、コレクションのツリー、型付き EMPTY 子要素、CRS、および平面 XY 記述子を維持します。単一要素の MULTIPOLYGON も MULTIPOLYGON のままです。点と重複した点の要素は変更しません。線は端点を、リングは閉鎖と向きを維持します。結果には元の頂点だけを使い、各パス内の元の順序を維持します。

いずれかの引数が `NULL` の場合は `NULL` を返します。許容値がゼロの場合、構造、次元、トポロジーを検証してから元の WKB を返します。EMPTY は元の型の EMPTY として保持します。NULL でない EMPTY も有効な許容値を必要とします。負数、NaN、無限大の許容値、不正な WKB、無効なポリゴントポロジー、退化した線やリング、制限の超過はエラーになります。無効な入力の修復や、有効な結果を得るための構成要素の削除は行いません。

GEOGRAPHY、Z/M、未知または混在した次元はサポートしません。この関数のコレクション対応は他の GEO 関数の対応範囲を変更しません。集約、ウィンドウ、配列の形式はありません。

## 単位と誤差の上限

EPSG:4326 は直交座標の度を使用し、許容値 1000 は **1000 度** です。EPSG:3857 は投影されたメートルを使用し、測地線の地上距離ではありません。座標変換、ラッピング、クランプ、球面計算は行いません。

線のパスとポリゴン境界について、入力と結果の双方向 Hausdorff 距離は許容値以下です。置換する各線分を、以前に削除した頂点も含む元の部分パス全体に対して検証するため、誤差は累積しません。接触と距離の判定では、格納された有限 double 座標を正確な二進有理数として比較します。隠れた epsilon、スナップ、精度グリッドで入力を変更しません。面積の不変性、塗りつぶされたポリゴン内部の距離上限、最大限の頂点削減、PostGIS/JTS と同じ頂点は保証しません。異なる行の共有境界は制約しません。

## リソース制限

入力ごとに WKB 256 KiB、閉鎖点を含む座標位置 5,000 個、ジオメトリノードとリングの合計 1,024 個、入れ子深度 32 が上限です。準備と各評価には 2,000 万の計上作業単位という上限があります。正確な整数のサイズ、アクティブな辺、インデックス、検索結果を制限します。入力サイズの上限以下でも作業量の上限によるエラーが発生する場合があります。準備モデルと一時割り当てにはクエリのメモリ制限が適用されます。キャンセルとメモリの確認は行の間、有界な割り当て群の前、作業ループ内で行います。固定のピークメモリ量や実行時間は保証しません。

## 例

```SQL
SELECT ST_AsText(ST_SimplifyPreserveTopology(
    ST_GeomFromText('LINESTRING (0 0,1 0,2 0)', 'EPSG:3857'), 0.1));
-- LINESTRING (0 0, 2 0)

SELECT ST_AsText(ST_SimplifyPreserveTopology(
    ST_GeomFromText('MULTIPOLYGON EMPTY', 'EPSG:4326'), 1));
-- MULTIPOLYGON EMPTY

SELECT ST_SRID(ST_SimplifyPreserveTopology(
    ST_GeomFromText('POINT (1 2)', 'EPSG:4326'), 0));
-- 4326

SELECT ST_AsText(ST_SimplifyPreserveTopology(
    ST_GeomFromText('POINT (1 2)', 'EPSG:3857'), NULL));
-- NULL
```
