---
displayed_sidebar: docs
description: "7 つの H3 セル・ポリゴン関数でネイティブ GEOGRAPHY を索引化します。"
---

# H3 関数

これらの関数は H3 4.5.0 とネイティブ XY CRS84 `GEOGRAPHY`（経度、緯度、単位は度）を使用します。セル ID の公開型は正の `BIGINT` で、2^53 を超える場合があります。クライアントではロスレスの 64 ビット整数を使用するか、10 進文字列で転送してください。JavaScript の `Number` ではすべてのセル ID を正確に表現できません。`GEOMETRY` と暗黙的な CRS 変換はサポートしません。

| 関数 | 戻り値 | 規則 |
| --- | --- | --- |
| `H3_FromGeo(geography, resolution INT)` | `BIGINT` | 空でない `POINT` を含むセル。解像度は 0–15。`POINT EMPTY` は `NULL`。 |
| `H3_GridDisk(cell BIGINT, k INT)` | `ARRAY<BIGINT>` | 元のセルを含む、距離 `k` 以内のセル。`k >= 0`、`k = 0` は `[cell]`。 |
| `H3_ToParent(cell BIGINT, resolution INT)` | `BIGINT` | 0 からセルの解像度までの親セル。 |
| `H3_ToChildren(cell BIGINT, resolution INT)` | `ARRAY<BIGINT>` | セルの解像度から 15 までの子セル。 |
| `H3_Resolution(cell BIGINT)` | `INT` | 有効なセルの解像度。 |
| `H3_ToBoundary(cell BIGINT)` | `GEOGRAPHY` | H3 が返す全頂点を含む、閉じた球面 CRS84 ポリゴン。 |
| `H3_PolygonToCells(geography, resolution INT)` | `ARRAY<BIGINT>` | 有効な `POLYGON`/`MULTIPOLYGON`（穴を含む）の H3 標準中心フィル。解像度は 0–15。空ポリゴンは `[]`。 |

いずれかの引数が `NULL` なら結果は `NULL` です。配列のセル ID は有効かつ一意ですが、順序は保証されません。表示順を固定するには `array_sort` を使用します。無効なセル（0、負数、エッジ・頂点 ID を含む）、解像度、トポロジー、CRS、次元、ジオメトリ種別はエラーです。`EMPTY` でも不正な数値引数はエラーです。

`H3_PolygonToCells` は H3 標準中心フィル（`flags = 0`）を使用し、厳密な球面 `ST_Contains`/`ST_Covers` と同等ではありません。空でないポリゴンでも `[]` になり得ます。結果は保守的なカバレッジではなく、厳密な空間述語の代わりにはなりません。H3 の経緯度フィルモデルには日付変更線、極域、大きなポリゴンで制約があります。中心が境界上にある場合は H3 4.5.0 の規則に従います。正確な判定には適切な厳密述語を併用してください。

H3 4.5.0 は単一の `polygonToCells` 呼び出し中のキャンセルに対応していません。StarRocks は呼び出しの前後と行の間でキャンセルを確認します。以下の制限は作業量を抑えますが、キャンセル遅延の上限を保証しません。

BE ビルドオプション `WITH_H3` はデフォルトで有効です。`WITH_H3=OFF` でビルドした BE はこれらの関数に対して制御された非対応エラーを返します。

## リソース制限

これらの変更可能な BE 設定は、各行の割り当てとフィルの前に検査されます。超過時はエラーとなり、結果を切り詰めません。クエリと列のメモリ制限も適用されます。

| BE 設定 | デフォルト | 単位と意味 |
| --- | ---: | --- |
| `h3_max_cells_per_row` | 100000 | 出力セル数と、重複排除前の展開スロットの合計。 |
| `h3_max_grid_disk_k` | 128 | 許容される最大グリッド距離。 |
| `h3_max_polygon_vertices` | 10000 | 閉鎖位置を含む全リング・コンポーネントの座標位置数。 |
| `h3_max_polygon_components` | 256 | 空要素を含む行ごとのポリゴン要素数。 |
| `h3_max_working_bytes` | 67108864 | worker の準備・一時バッファのバイト数（64 MiB）。 |
| `h3_max_estimated_work_per_row` | 10000000 | 各要素の `U × (V + 1)` の合計。`U` は H3 の推定スロット、`V` は座標位置数。これは受け入れのヒューリスティックであり CPU 時間の上限ではありません。 |

## 例

```sql
SELECT H3_FromGeo(ST_GeogFromText('POINT (0 0)'), 3);
-- 592035265791393791
SELECT H3_Resolution(592035265791393791);
-- 3
SELECT H3_ToParent(592035265791393791, 2);
SELECT array_sort(H3_GridDisk(592035265791393791, 1));
SELECT array_sort(H3_ToChildren(592035265791393791, 4));
SELECT ST_AsText(H3_ToBoundary(592035265791393791));
SELECT array_sort(H3_PolygonToCells(
  ST_GeogFromText('POLYGON ((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 3));
```
