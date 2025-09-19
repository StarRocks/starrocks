# ds_hll_accumulate

将值累积到 HyperLogLog 草图中，并返回序列化的草图作为 VARBINARY。此函数是 DataSketches HLL 近似去重计数函数族的一部分。

`ds_hll_accumulate` 创建一个序列化的 HyperLogLog 草图，可以使用 `ds_hll_combine` 与其他草图合并，并使用 `ds_hll_estimate` 进行估算。

它基于 Apache DataSketches，为近似去重计数提供高精度。更多信息，请参见 [HyperLogLog 草图](https://datasketches.apache.org/docs/HLL/HllSketches.html)。

## 语法

```Haskell
sketch ds_hll_accumulate(expr)
sketch ds_hll_accumulate(expr, log_k)
sketch ds_hll_accumulate(expr, log_k, tgt_type)
```

### 参数

- `expr`: 要累积到草图中的表达式。可以是任何数据类型。
- `log_k`: 整数。范围 [4, 21]。默认值：17。控制草图的精度和内存使用量。
- `tgt_type`: 有效值为 `HLL_4`、`HLL_6`（默认）和 `HLL_8`。控制 HyperLogLog 草图的目标类型。

## 返回类型

返回包含序列化 HyperLogLog 草图的 VARBINARY。

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

-- 单参数基本用法
SELECT ds_hll_accumulate(id) FROM t1;

-- 使用自定义 log_k 参数
SELECT ds_hll_accumulate(province, 20) FROM t1;

-- 使用 log_k 和 tgt_type 两个参数
SELECT ds_hll_accumulate(age, 12, "HLL_6") FROM t1;

-- 分组用法
SELECT dt, 
       ds_hll_accumulate(id), 
       ds_hll_accumulate(province, 20),  
       ds_hll_accumulate(age, 12, "HLL_6"), 
       ds_hll_accumulate(dt) 
FROM t1 
GROUP BY dt 
ORDER BY 1 
LIMIT 3;
```

## 相关函数

- `ds_hll_combine`: 将多个序列化草图合并为单个草图
- `ds_hll_estimate`: 从序列化草图估算去重计数
- `ds_hll_count_distinct`: 直接近似去重计数函数

## 关键词

ds_HLL_ACCUMULATE, HLL, HYPERLOGLOG, APPROXIMATE, DISTINCT, COUNT 