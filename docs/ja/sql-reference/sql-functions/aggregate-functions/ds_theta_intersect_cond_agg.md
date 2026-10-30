---
displayed_sidebar: docs
description: "anchor フラグで行を 2 グループに振り分け、単一スキャンで 2 グループの Theta スケッチの積集合基数を計算する集計関数。"
---

# ds_theta_intersect_cond_agg

集計関数。**anchor** グループ（`is_anchor = 1`）と **window** グループ（`is_anchor = 0`）の 2 つの独立した theta-union スケッチを維持し、最終化時に 2 つを交差させて、積集合の基数推定値を `DOUBLE` として返します。

以下の単一スキャン相当の処理と等価です：

```SQL
ds_theta_estimate(
    ds_theta_intersect(
        ds_theta_combine(sketch) FILTER (WHERE is_anchor = 1),
        ds_theta_combine(sketch) FILTER (WHERE is_anchor = 0)
    )
)
```

## 構文

```Haskell
DOUBLE ds_theta_intersect_cond_agg(sketch, is_anchor)
```

- `sketch`: `VARBINARY` コンパクト theta スケッチ。通常 [`ds_theta_accumulate`](./ds_theta_accumulate.md) または [`ds_theta_combine`](./ds_theta_combine.md) で生成されます。
- `is_anchor`: `INT` フラグ。0 以外の値はスケッチを anchor グループに、`0` は window グループにルーティングします。

## 戻り値

`DOUBLE` を返します。いずれかのグループにスケッチが存在しない場合は `0` を返します。

## 例

```SQL
-- anchor コホートと window コホートの両方に現れた distinct なユーザー数。
SELECT
    ds_theta_intersect_cond_agg(sketch, is_anchor) AS overlap_estimate
FROM (
    SELECT ds_theta_accumulate(user_id) AS sketch, 1 AS is_anchor
    FROM events WHERE cohort = 'anchor'
    UNION ALL
    SELECT ds_theta_accumulate(user_id) AS sketch, 0 AS is_anchor
    FROM events WHERE cohort = 'window'
) t;
```

## キーワード

DS_THETA_INTERSECT_COND_AGG, DS_THETA_COMBINE, DS_THETA_INTERSECT, DS_THETA_ESTIMATE
