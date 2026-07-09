# ds_theta_a_not_b

2 つのシリアライズされた Apache DataSketches Theta スケッチに対するスカラーの差集合演算です。distinct count が `|A \ B|`（A に含まれるが B に含まれない要素）に推定されるシリアライズされたコンパクトスケッチを返します。いずれかの入力が `NULL` の場合は `NULL` を返します。

## 構文

```Haskell
VARBINARY ds_theta_a_not_b(sketch_a, sketch_b)
```

- `sketch_a`、`sketch_b`: `VARBINARY` コンパクト theta スケッチ。

## 例

```SQL
-- コホート A に現れたがコホート B には現れなかった distinct なユーザー
SELECT ds_theta_estimate(ds_theta_a_not_b(a.sk, b.sk))
FROM cohort_a a JOIN cohort_b b USING (day);
```

## キーワード

DS_THETA_A_NOT_B, DS_THETA_UNION, DS_THETA_INTERSECT
