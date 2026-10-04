---
displayed_sidebar: docs
description: "ネイティブ平面ポリゴンのウィンドウパーティションを共同で簡略化し、coverage のトポロジーを維持します。"
---

# ST_CoverageSimplify

辺が一致するネイティブ Cartesian XY ポリゴンを、ウィンドウパーティション全体で共同簡略化します。共有辺を同時に処理するため、隣接する行の境界が一致します。結果は元の行と CRS を維持します。各ジオメトリを独立に処理するスカラー [ST_SimplifyPreserveTopology](st_simplifypreservetopology.md) とは異なります。

## 構文

```SQL
ST_CoverageSimplify(GEOMETRY geom, DOUBLE tolerance)
    OVER (PARTITION BY coverage_id)
ST_CoverageSimplify(GEOMETRY geom, DOUBLE tolerance, BOOLEAN simplify_boundary)
    OVER (PARTITION BY coverage_id)
```

`OVER ()` はすべての入力行を一つのパーティションとします。chunk 境界を越えてパーティション全体を処理します。OVER 内の ORDER BY、明示的な window frame、DISTINCT、IGNORE NULLS、RESPECT NULLS、分析実行 hint は未サポートです。外側の ORDER BY は使用できます。スカラー、配列、通常の集約 overload はありません。spill は未サポートなので `SET enable_spill = false` を設定してください。

## パラメータと入力

両パラメータは折り畳み可能なプラン定数である必要があります。許容値は有限かつ非負で、その二乗も有限 DOUBLE で表現可能でなければなりません。2 引数形式では `simplify_boundary = true` です。明示的な NULL にデフォルト値は適用されません。

許容値は有効三角形面積の平方根に関連する、面積ベースの Visvalingam–Whyatt パラメータです。入力の座標単位を使用し、EPSG:4326 は Cartesian 度、EPSG:3857 は投影メートルです。地表の測地距離ではありません。再投影、スナップ、座標の制限は行いません。Hausdorff/最大距離、面積不変、最大の頂点削減、PostGIS と同一の結果は保証しません。

`true` は共有辺と外側の境界を簡略化します。`false` は共有内部辺だけを簡略化し、外側の境界、隙間、未充填の穴の辺を維持します。他のメンバーに充填された穴は共有内部境界です。

同じ平面 GEOMETRY CRS descriptor を持つ POLYGON/MULTIPOLYGON のみを受け付けます。非空ポリゴンの内部は重ならず、共有辺の頂点は一致する必要があります。隙間は許可されます（`gapWidth = 0`）。辺の分割不一致、T 字接合、非空ポリゴンの重複、不正な環や穴はエラーです。修復、union、自動 noding は行いません。GEOGRAPHY、POINT、LINESTRING、GEOMETRYCOLLECTION、Z/M は未サポートです。UNKNOWN/MIXED 次元メタデータは、実際の値が XY 検証を通った場合にのみ許可されます。

## 結果とエラー

入力 CRS のネイティブ XY WKB GEOMETRY を各入力行に返します。POLYGON/MULTIPOLYGON の型、メンバー順序、穴、環の向き、接触点、接合点、共有辺を維持します。単一メンバーの MULTIPOLYGON も MULTIPOLYGON のままです。行は位置で対応付け、同じジオメトリを重複排除しません。

- NULL ジオメトリは計算から除外され、元の位置に NULL を返します。
- POLYGON/MULTIPOLYGON EMPTY は型と位置を維持します。
- 許容値または明示的な境界パラメータが NULL の場合、カーネルを実行せず全行に NULL を返します。型と定数チェックは実施します。
- 許容値ゼロでは入力と coverage を検証してから、重複閉合座標を含む元の WKB を返します。
- 空入力や全 NULL/EMPTY でもパラメータを検証します。不正な coverage/WKB、未サポートの descriptor/次元、資源制限超過はパーティション結果の公開前にエラーとなります。

## 資源制限

次の正値で変更可能な BE 設定を各パーティションの開始時に取得します。

| 設定 | デフォルト | 制限 |
| --- | ---: | --- |
| `geo_coverage_max_rows_per_partition` | 10000 | NULL/EMPTY を含む全行。 |
| `geo_coverage_max_vertices_per_partition` | 1000000 | 閉合と重複を含む元の WKB 座標位置。 |
| `geo_coverage_max_input_bytes_per_partition` | 67108864 | EMPTY を含む入力 WKB バイト数。 |
| `geo_coverage_max_working_bytes_per_partition` | 268435456 | 保持入力、モデル、索引、一時領域、行マッピング、出力容量のバイト数。 |

ウィンドウへの累積前に制限を確認します。共有入出力は保持する実際の割り当てサイズで計上するため、小さなスライスでも大きな chunk を保持できます。複数パーティションにはクエリメモリ制限も適用されます。超過時は設定名を示すエラーを返し、切り捨てや部分的なパーティション結果は返しません。複雑なトポロジーはサイズ制限以内でも 1 億の計上作業単位制限に達することがあります。累積、カーネルループ、出力、制限付きライブラリ処理の前後でキャンセルを確認します。固定のキャンセル遅延や実行時間は保証しません。

## 例

```SQL
SET enable_spill = false;
WITH districts AS (
    SELECT 1 AS district_id, 7 AS coverage_id,
           'POLYGON ((0 0,0 8,4 8,4.1 6,3.8 4,4.2 2,4 0,0 0))' AS wkt
    UNION ALL
    SELECT 2, 7, 'POLYGON ((4 0,4.2 2,3.8 4,4.1 6,4 8,8 8,8 0,4 0))'
)
SELECT district_id,
       ST_AsText(ST_CoverageSimplify(ST_GeomFromText(wkt, 'EPSG:3857'), 1, false)
                 OVER (PARTITION BY coverage_id)) AS simplified
FROM districts ORDER BY district_id;
-- 1  POLYGON ((0 0, 0 8, 4 8, 4 0, 0 0))
-- 2  POLYGON ((4 0, 4 8, 8 8, 8 0, 4 0))

SELECT ST_AsText(ST_CoverageSimplify(
    ST_GeomFromText('MULTIPOLYGON EMPTY', 'EPSG:4326'), 1) OVER ());
-- MULTIPOLYGON EMPTY

SELECT ST_AsText(ST_CoverageSimplify(
    ST_GeomFromText('POLYGON EMPTY', 'EPSG:3857'), 0, NULL) OVER ());
-- NULL
```
