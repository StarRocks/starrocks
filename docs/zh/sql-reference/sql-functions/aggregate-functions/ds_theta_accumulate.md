# ds_theta_accumulate

将值累积到 Apache DataSketches Theta 草图中，并返回序列化的草图作为 `VARBINARY`（紧凑格式）。与 [ds_theta_combine](./ds_theta_combine.md) 和 [ds_theta_estimate](./ds_theta_estimate.md) 配合使用，可持久化并复用草图。

输出使用标准的 Apache DataSketches C++ 紧凑序列化格式，因此 StarRocks 写入的草图可被任何使用默认哈希种子的 Apache DataSketches 实现读取，反之亦然。

## 语法

```Haskell
VARBINARY ds_theta_accumulate(expr)
```

- `expr`: 用于汇总不同值的列。

## 示例

```SQL
CREATE TABLE sketches AS
SELECT grp, ds_theta_accumulate(id) AS sk FROM t GROUP BY grp;

SELECT grp, ds_theta_estimate(sk) FROM sketches;
```

## 关键词

DS_THETA_ACCUMULATE, DS_THETA_COMBINE, DS_THETA_ESTIMATE, DS_THETA_COUNT_DISTINCT
