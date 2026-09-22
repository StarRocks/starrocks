---
displayed_sidebar: docs
description: "将列值累积到 Apache DataSketches Theta 草图中，以 VARBINARY 格式返回序列化的紧凑草图。"
---

# ds_theta_accumulate

将值累积到 Apache DataSketches Theta 草图中，并返回序列化的草图作为 `VARBINARY`（紧凑格式）。与 [ds_theta_combine](./ds_theta_combine.md) 和 [ds_theta_estimate](../scalar-functions/ds_theta_estimate.md) 配合使用，可持久化并复用草图。

输出使用标准的 Apache DataSketches C++ 紧凑序列化格式，可被任何 Apache DataSketches 消费者读取。

:::note
`ds_theta_accumulate` 在将输入值传递给 DataSketches 之前会先进行预哈希处理。对由 StarRocks 累积的草图与外部从相同原始值构建的草图执行集合运算，结果将不正确。请使用 `ds_theta_combine`、`ds_theta_intersect` 和 `ds_theta_a_not_b` 对同一累积路径产生的草图进行集合运算。
:::

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
