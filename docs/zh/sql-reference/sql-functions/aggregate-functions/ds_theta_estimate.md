# ds_theta_estimate

标量函数。返回由序列化 Apache DataSketches Theta 草图（`VARBINARY`，紧凑格式）汇总的近似不同值计数。逐行输入、逐行输出。

接受任何使用默认哈希种子、以标准 Apache DataSketches C++ 紧凑 theta 格式写入的草图，包括：

- [`ds_theta_accumulate`](./ds_theta_accumulate.md)（从原始值构建）
- [`ds_theta_combine`](./ds_theta_combine.md)（跨行合并）
- [`ds_theta_union`](./ds_theta_union.md)、[`ds_theta_intersect`](./ds_theta_intersect.md)、[`ds_theta_a_not_b`](./ds_theta_a_not_b.md)（成对集合运算）
- 从 Parquet、Iceberg 或其他 Apache DataSketches 消费者作为原始 `VARBINARY` 加载的外部草图

输入为 `NULL` 时返回 `NULL`。空草图返回 0。

## 语法

```Haskell
BIGINT ds_theta_estimate(sketch)
```

- `sketch`: `VARBINARY` 紧凑 theta 草图。

## 示例

```SQL
-- 按行估计草图列
SELECT ds_theta_estimate(sk) FROM sketches;

-- 与集合运算组合
SELECT ds_theta_estimate(ds_theta_union(a.sk, b.sk)) AS u,
       ds_theta_estimate(ds_theta_intersect(a.sk, b.sk)) AS i
FROM cohort_a a JOIN cohort_b b USING (day);

-- 跨行合并后估计
SELECT ds_theta_estimate(ds_theta_combine(daily_sk))
FROM daily_sketches;
```

## 关键词

DS_THETA_ESTIMATE, DS_THETA_ACCUMULATE, DS_THETA_COMBINE, DS_THETA_COUNT_DISTINCT
