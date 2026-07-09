# ds_theta_a_not_b

对两个序列化的 Apache DataSketches Theta 草图执行标量集合差运算。返回一个序列化的紧凑草图，其不同值估计为 `|A \ B|`（在 A 中但不在 B 中的元素）。如果任一输入为 `NULL`，则返回 `NULL`。

## 语法

```Haskell
VARBINARY ds_theta_a_not_b(sketch_a, sketch_b)
```

- `sketch_a`、`sketch_b`: `VARBINARY` 紧凑 theta 草图。

## 示例

```SQL
-- 在群组 A 中但未出现在群组 B 中的不同用户数
SELECT ds_theta_estimate(ds_theta_a_not_b(a.sk, b.sk))
FROM cohort_a a JOIN cohort_b b USING (day);
```

## 关键词

DS_THETA_A_NOT_B, DS_THETA_UNION, DS_THETA_INTERSECT
