---
displayed_sidebar: docs
description: "列の値から Apache DataSketches Theta スケッチを構築し、シリアライズされたコンパクトスケッチを VARBINARY として返します。"
---

# ds_theta_accumulate

`expr` に対して Apache DataSketches Theta スケッチを構築し、シリアライズされたスケッチを `VARBINARY`（コンパクト形式）として返します。[ds_theta_combine](./ds_theta_combine.md) および [ds_theta_estimate](../scalar-functions/ds_theta_estimate.md) と組み合わせてスケッチを永続化・再利用できます。

出力は標準的な Apache DataSketches C++ コンパクトシリアライゼーション形式を使用しており、任意の Apache DataSketches コンシューマと互換性があります。

:::note
`ds_theta_accumulate` は入力値を DataSketches に渡す前にプレハッシュ処理を行います。StarRocks で累積されたスケッチと、同じ生の値から外部で構築されたスケッチに対して集合演算を行うと、このハッシュの違いにより正しい結果が得られません。`ds_theta_combine`、`ds_theta_intersect`、`ds_theta_a_not_b` は同一の累積パスで生成されたスケッチに対してのみ使用してください。
:::

## 構文

```Haskell
VARBINARY ds_theta_accumulate(expr)
```

- `expr`: 重複を除いた値を集計する対象の列。

## 例

```SQL
CREATE TABLE sketches AS
SELECT grp, ds_theta_accumulate(id) AS sk FROM t GROUP BY grp;

SELECT grp, ds_theta_estimate(sk) FROM sketches;
```

## キーワード

DS_THETA_ACCUMULATE, DS_THETA_COMBINE, DS_THETA_ESTIMATE, DS_THETA_COUNT_DISTINCT
