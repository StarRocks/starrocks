# ds_theta_union

对两个序列化的 Apache DataSketches Theta 草图执行标量两两并集。返回一个序列化的紧凑草图，其不同值估计为 `|A ∪ B|`。如果任一输入为 `NULL`，则返回 `NULL`。

如需对多个草图一次性进行聚合式并集，请使用 [ds_theta_combine](./ds_theta_combine.md)。

## 语法

```Haskell
VARBINARY ds_theta_union(sketch_a, sketch_b)
```

- `sketch_a`、`sketch_b`: `VARBINARY` 紧凑 theta 草图。

## 示例

```SQL
SELECT ds_theta_estimate(ds_theta_union(a.sk, b.sk))
FROM cohort_a a JOIN cohort_b b USING (day);
```

## 关键词

DS_THETA_UNION, DS_THETA_INTERSECT, DS_THETA_A_NOT_B, DS_THETA_COMBINE
