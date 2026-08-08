# ds_theta_combine

集計関数。行をまたいでシリアライズされた Apache DataSketches Theta スケッチ（`VARBINARY`）を和集合でマージし、単一のシリアライズされたコンパクトスケッチを返します。

入力は任意のソースのコンパクト theta スケッチ列を受け付けます: [`ds_theta_accumulate`](./ds_theta_accumulate.md) で構築、ペアごとのスカラー集合演算 [`ds_theta_union`](./ds_theta_union.md) / [`ds_theta_intersect`](./ds_theta_intersect.md) / [`ds_theta_a_not_b`](./ds_theta_a_not_b.md) で生成、または Parquet/Iceberg から生の `VARBINARY` として読み込まれた外部スケッチ。

ワイヤフォーマットは標準的な Apache DataSketches C++ コンパクト theta シリアライゼーションのため、結果スケッチはデフォルトハッシュシードを使う任意の Apache DataSketches 実装と相互運用可能です。

## 構文

```Haskell
VARBINARY ds_theta_combine(sketch)
```

- `sketch`: コンパクト theta スケッチの `VARBINARY` 列。

## 例

```SQL
-- 日次スケッチを年次にロールアップ
SELECT year(day), ds_theta_estimate(ds_theta_combine(daily_sk))
FROM daily_sketches
GROUP BY year(day);
```

## キーワード

DS_THETA_COMBINE, DS_THETA_ACCUMULATE, DS_THETA_ESTIMATE, DS_THETA_COUNT_DISTINCT
