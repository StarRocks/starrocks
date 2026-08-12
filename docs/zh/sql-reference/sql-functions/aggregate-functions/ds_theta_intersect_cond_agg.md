---
displayed_sidebar: docs
description: "聚合函数。通过 anchor 标志将行路由到两组 Theta 草图，单次扫描即可计算两组草图的交集基数。"
---

# ds_theta_intersect_cond_agg

聚合函数。维护两个独立的 theta-union 草图——**anchor** 组（`is_anchor = 1`）和 **window** 组（`is_anchor = 0`）——在最终化时对两组求交集，并以 `DOUBLE` 类型返回交集的基数估计值。

等价于以下单次扫描写法：

```SQL
ds_theta_estimate(
    ds_theta_intersect(
        ds_theta_combine(sketch) FILTER (WHERE is_anchor = 1),
        ds_theta_combine(sketch) FILTER (WHERE is_anchor = 0)
    )
)
```

## 语法

```Haskell
DOUBLE ds_theta_intersect_cond_agg(sketch, is_anchor)
```

- `sketch`：`VARBINARY` 紧凑 theta 草图，通常由 [`ds_theta_accumulate`](./ds_theta_accumulate.md) 或 [`ds_theta_combine`](./ds_theta_combine.md) 生成。
- `is_anchor`：`INT` 标志。`1` 表示将草图路由到 anchor 组，其他值路由到 window 组。

## 返回值

返回 `DOUBLE`。若任一组未收到草图，则返回 `0`。

## 示例

```SQL
-- 同时出现在 anchor 群组和 window 群组中的不同用户数。
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

## 关键词

DS_THETA_INTERSECT_COND_AGG, DS_THETA_COMBINE, DS_THETA_INTERSECT, DS_THETA_ESTIMATE
