# ds_theta_union

2 つのシリアライズされた Apache DataSketches Theta スケッチに対するスカラーのペアごと和集合演算です。distinct count が `|A ∪ B|` に推定される単一のシリアライズされたコンパクトスケッチを返します。いずれかの入力が `NULL` の場合は `NULL` を返します。

多数のスケッチに対する集計形式の和集合演算には [ds_theta_combine](./ds_theta_combine.md) を使用してください。

## 構文

```Haskell
VARBINARY ds_theta_union(sketch_a, sketch_b)
```

- `sketch_a`、`sketch_b`: `VARBINARY` コンパクト theta スケッチ。

## 例

```SQL
SELECT ds_theta_estimate(ds_theta_union(a.sk, b.sk))
FROM cohort_a a JOIN cohort_b b USING (day);
```

## キーワード

DS_THETA_UNION, DS_THETA_INTERSECT, DS_THETA_A_NOT_B, DS_THETA_COMBINE
