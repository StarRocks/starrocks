---
displayed_sidebar: docs
description: "ネイティブ XY ジオメトリの周囲に丸い直交座標バッファを生成します。"
---

# ST_Buffer

符号付き距離でネイティブ `GEOMETRY` の平面バッファを生成します。

## 構文

```SQL
ST_Buffer(GEOMETRY geometry, DOUBLE distance)
```

距離は定数または行ごとに変わる値を指定できます。オプションや分割数の引数はありません。

## 入力と結果

| XY 入力型 | 正の距離 | ゼロまたは負の距離 |
| --- | --- | --- |
| POINT、MULTIPOINT | 丸いバッファ | POLYGON EMPTY |
| LINESTRING、MULTILINESTRING | 丸い接合部と端部 | POLYGON EMPTY |
| POLYGON、MULTIPOLYGON | 拡張 | ゼロは有効な領域を保持、負は縮小 |

結果は検証済みのネイティブ XY `GEOMETRY` で、入力 CRS と平面の辺の意味を保持します。領域成分が 1 つなら `POLYGON`、複数なら `MULTIPOLYGON`、空なら `POLYGON EMPTY` です。成分の結合、穴の消失、縮小による分割が起こり得ます。成分が 1 つの MULTIPOLYGON 結果は POLYGON にまとめられます。リングの向き、開始頂点、成分順序は変わる場合があります。

いずれかの引数が `NULL` の場合は `NULL` を返します。型付き EMPTY（空の子成分を含む）は有限の距離で POLYGON EMPTY を返します。ゼロや負の距離でも、非 NULL 入力行の構造とトポロジーは有効でなければなりません。ゼロは無効な多角形を修復しません。非有限の距離、不正な WKB、無効なトポロジー、リソース上限超過は、修復や切り捨てをせずエラーを返します。

`GEOGRAPHY`、GEOMETRYCOLLECTION（EMPTY を含む）、Z/M、未知または混合次元は未対応です。スカラー関数であり、集約、ウィンドウ、配列関数ではありません。

## 単位と近似

座標は入力 CRS の直交 X/Y として扱います。EPSG:4326 の単位は度で、距離 1000 は **1000 度**です。結果が経緯度の範囲を超えることもあります。暗黙の変換、経緯度の丸めや折り返し、球面戦略への切り替えは行いません。EPSG:3857 は投影メートルであり、同じ数値の地表測地半径は保証しません。

円形戦略は全円あたり 32 点、直線の側面、対称な符号付き距離を使用します。円弧と弦の偏差は `abs(distance) * (1 - cos(pi / 32))`、半径の約 0.004815 倍です。Boost.Geometry 1.80 は非ゼロ距離の入力を `abs(distance) / 1000` の閾値で事前簡略化します。丸い接合部で分割が追加される場合があり、頂点数や WKB の順序は互換性を保証しません。これらの設定は任意の複雑な入力に対する普遍的なトポロジーや Hausdorff 誤差の保証ではありません。計算は浮動小数点数を使用します。局所座標への移動で入力精度が失われる場合、または WKB の倍精度座標への丸めで結果のトポロジーが失われる場合はエラーになります。多角形のゼロ距離はバッファ処理と事前簡略化を迂回します。

## リソース制限

入力は最大 256 KiB の WKB と 5,000 座標位置（リングの閉鎖位置を含む）、出力は最大 10,000 位置です。準備したモデルと一時割り当てにはクエリのメモリ上限が適用されます。行間とライブラリ呼び出しの前後でキャンセルとメモリエラーを確認しますが、単一の Boost 呼び出し内部は中断できません。固定のピークメモリ量や実行時間は保証しません。

## 例

```SQL
SELECT ROUND(ST_Area(ST_Buffer(
    ST_GeomFromText('POINT (0 0)', 'EPSG:3857'), 1.0)), 6);
-- 3.121445

SELECT ST_Area(ST_Buffer(
    ST_GeomFromText('POLYGON ((0 0,10 0,10 10,0 10,0 0))', 'EPSG:3857'), -1.0));
-- 64

SELECT ST_AsText(ST_Buffer(
    ST_GeomFromText('POLYGON ((0 0,10 0,10 10,0 10,0 0))', 'EPSG:3857'), -5.0));
-- POLYGON EMPTY

SELECT ST_AsText(ST_Buffer(ST_GeomFromText('POINT (0 0)', 'EPSG:4326'), 0));
-- POLYGON EMPTY

SELECT ST_AsText(ST_Buffer(ST_GeomFromText('POINT (0 0)', 'EPSG:3857'), NULL));
-- NULL
```
