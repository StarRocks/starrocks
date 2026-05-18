# ds_theta_estimate

返回由序列化 Apache DataSketches Theta 草图（`VARBINARY`，紧凑格式）汇总的近似不同值计数。是 [ds_theta_accumulate](./ds_theta_accumulate.md) 的逆操作。

接受任何使用默认哈希种子、以标准 Apache DataSketches C++ 紧凑 theta 格式写入的草图。

## 语法

```Haskell
BIGINT ds_theta_estimate(sketch)
```

- `sketch`: `VARBINARY` 紧凑 theta 草图。

## 示例

```SQL
SELECT ds_theta_estimate(sk) FROM sketches;
```

## 关键词

DS_THETA_ESTIMATE, DS_THETA_ACCUMULATE, DS_THETA_COMBINE, DS_THETA_COUNT_DISTINCT
