# ds_theta_intersect

2 つのシリアライズされた Apache DataSketches Theta スケッチに対するスカラーのペアごと積集合演算です。distinct count が `|A ∩ B|` に推定される単一のシリアライズされたコンパクトスケッチを返します。いずれかの入力が `NULL` の場合は `NULL` を返します。

HyperLogLog スケッチとは異なり、theta スケッチは積集合演算をサポートするのに十分な状態を保持しているため、元の値を保存することなくこの操作を実行できます。

## 構文

```Haskell
VARBINARY ds_theta_intersect(sketch_a, sketch_b)
```

- `sketch_a`、`sketch_b`: `VARBINARY` コンパクト theta スケッチ。

## 例

```SQL
-- コホート A とコホート B の両方に現れた distinct なユーザー
SELECT ds_theta_estimate(ds_theta_intersect(a.sk, b.sk))
FROM cohort_a a JOIN cohort_b b USING (day);
```

## キーワード

DS_THETA_INTERSECT, DS_THETA_UNION, DS_THETA_A_NOT_B
