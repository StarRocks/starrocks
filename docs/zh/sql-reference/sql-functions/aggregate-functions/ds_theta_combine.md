# ds_theta_combine

聚合函数。跨行合并序列化的 Apache DataSketches Theta 草图（`VARBINARY`），返回单个序列化的紧凑草图。

输入可以是来自任何源的紧凑 theta 草图列：通过 [`ds_theta_accumulate`](./ds_theta_accumulate.md) 构建、由成对标量集合运算 [`ds_theta_union`](./ds_theta_union.md) / [`ds_theta_intersect`](./ds_theta_intersect.md) / [`ds_theta_a_not_b`](./ds_theta_a_not_b.md) 产生，或从 Parquet/Iceberg 作为原始 `VARBINARY` 加载的外部草图。

线上格式为标准的 Apache DataSketches C++ 紧凑 theta 序列化格式，结果草图与任何使用默认哈希种子的 Apache DataSketches 实现可互操作。

## 语法

```Haskell
VARBINARY ds_theta_combine(sketch)
```

- `sketch`: 紧凑 theta 草图的 `VARBINARY` 列。

## 示例

```SQL
-- 将日级草图卷起到年度
SELECT year(day), ds_theta_estimate(ds_theta_combine(daily_sk))
FROM daily_sketches
GROUP BY year(day);
```

## 关键词

DS_THETA_COMBINE, DS_THETA_ACCUMULATE, DS_THETA_ESTIMATE, DS_THETA_COUNT_DISTINCT
