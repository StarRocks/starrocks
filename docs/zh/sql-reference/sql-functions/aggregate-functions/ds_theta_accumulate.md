# ds_theta_accumulate

将值累积到 Theta 草图中，并返回序列化的草图作为 VARBINARY。此函数是 DataSketches Theta 近似去重计数函数族的一部分。

`ds_theta_accumulate` 创建一个序列化的 Theta 草图，可以使用 `ds_theta_combine` 与其他草图合并，并使用 `ds_theta_estimate` 进行估算。

它基于 Apache DataSketches。更多信息，请参见 [Theta Sketches](https://datasketches.apache.org/docs/Theta/InverseEstimate.html)。

## 语法

```Haskell
sketch ds_theta_accumulate(expr)
sketch ds_theta_accumulate(expr, log_k)
```

### 参数

- `expr`: 要累积到草图中的表达式。可以是任何数据类型。
- `log_k`: 整数。范围 [5, 26]。默认值：12。控制草图的精度和内存使用量。

## 返回类型

返回包含序列化 Theta 草图的 VARBINARY。序列化结果中也保存了 `log_k`，因此跨节点 shuffle、agg-state 物化以及下游的 `ds_theta_combine` / `ds_theta_estimate` 都能保留它。

## 示例

```sql
-- 创建测试表
CREATE TABLE t1 (
  id BIGINT,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10)
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- 插入测试数据
INSERT INTO t1 SELECT generate_series, generate_series, generate_series % 100, "2024-07-24"
FROM table(generate_series(1, 1000));

-- 单参数基本用法（默认 log_k = 12）
SELECT ds_theta_accumulate(id) FROM t1;

-- 使用自定义 log_k 提高精度
SELECT ds_theta_accumulate(province, 14) FROM t1;

-- 分组用法
SELECT dt,
       ds_theta_accumulate(id),
       ds_theta_accumulate(province, 14),
       ds_theta_accumulate(age),
       ds_theta_accumulate(dt)
FROM t1
GROUP BY dt
ORDER BY 1
LIMIT 3;
```

## 相关函数

- `ds_theta_combine`: 将多个序列化草图合并为单个草图
- `ds_theta_estimate`: 从序列化草图估算去重计数
- `ds_theta_count_distinct`: 直接近似去重计数函数

## 关键词

DS_THETA_ACCUMULATE, THETA, APPROXIMATE, DISTINCT, COUNT
