# ds_theta_intersect

对两个序列化的 Apache DataSketches Theta 草图执行标量两两交集。返回一个序列化的紧凑草图，其不同值估计为 `|A ∩ B|`。如果任一输入为 `NULL`，则返回 `NULL`。

与 HyperLogLog 草图不同，theta 草图保留了足够的状态来支持交集运算，这使得本操作可以在不存储原始值的情况下完成。

## 语法

```Haskell
VARBINARY ds_theta_intersect(sketch_a, sketch_b)
```

- `sketch_a`、`sketch_b`: `VARBINARY` 紧凑 theta 草图。

## 示例

```SQL
-- 同时出现在群组 A 和群组 B 中的不同用户数
SELECT ds_theta_estimate(ds_theta_intersect(a.sk, b.sk))
FROM cohort_a a JOIN cohort_b b USING (day);
```

## 关键词

DS_THETA_INTERSECT, DS_THETA_UNION, DS_THETA_A_NOT_B
