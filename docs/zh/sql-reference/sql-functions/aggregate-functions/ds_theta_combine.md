# ds_theta_combine

聚合序列化的 Apache DataSketches Theta 草图（`VARBINARY`），通过并集合并它们，返回单个序列化的紧凑草图。是聚合层面 [ds_theta_accumulate](./ds_theta_accumulate.md) 的逆操作。

线上格式为标准的 Apache DataSketches C++ 紧凑 theta 序列化格式，因此由任何符合规范的 Apache DataSketches 实现生成的草图都可以直接合并，无需转换。

## 语法

```Haskell
VARBINARY ds_theta_combine(sketch)
```

- `sketch`: 紧凑 theta 草图的 `VARBINARY` 列。

## 示例

```SQL
SELECT year(day), ds_theta_estimate(ds_theta_combine(daily_sk))
FROM daily_sketches
GROUP BY year(day);
```

## 关键词

DS_THETA_COMBINE, DS_THETA_ACCUMULATE, DS_THETA_ESTIMATE, DS_THETA_COUNT_DISTINCT
