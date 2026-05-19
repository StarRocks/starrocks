# ds_theta_estimate

スカラー関数。シリアライズされた Apache DataSketches Theta スケッチ（`VARBINARY`、コンパクト形式）が要約する近似的な distinct count を返します。入力 1 行につき 1 行を返します。

デフォルトのハッシュシードを用いた標準的な Apache DataSketches C++ コンパクト theta フォーマットで書き込まれた任意のスケッチを受け付けます。以下を含みます:

- [`ds_theta_accumulate`](./ds_theta_accumulate.md)（生の値から構築）
- [`ds_theta_combine`](./ds_theta_combine.md)（行をまたいで合併）
- [`ds_theta_union`](./ds_theta_union.md)、[`ds_theta_intersect`](./ds_theta_intersect.md)、[`ds_theta_a_not_b`](./ds_theta_a_not_b.md)（ペアごとの集合演算）
- Parquet、Iceberg または他の Apache DataSketches コンシューマから生の `VARBINARY` として読み込まれた外部スケッチ

入力が `NULL` の場合は `NULL` を返します。空スケッチは 0 を返します。

## 構文

```Haskell
BIGINT ds_theta_estimate(sketch)
```

- `sketch`: `VARBINARY` コンパクト theta スケッチ。

## 例

```SQL
-- スケッチ列の行ごとの推定
SELECT ds_theta_estimate(sk) FROM sketches;

-- 集合演算との合成
SELECT ds_theta_estimate(ds_theta_union(a.sk, b.sk)) AS u,
       ds_theta_estimate(ds_theta_intersect(a.sk, b.sk)) AS i
FROM cohort_a a JOIN cohort_b b USING (day);

-- 行をまたぐ合併後の推定
SELECT ds_theta_estimate(ds_theta_combine(daily_sk))
FROM daily_sketches;
```

## キーワード

DS_THETA_ESTIMATE, DS_THETA_ACCUMULATE, DS_THETA_COMBINE, DS_THETA_COUNT_DISTINCT
