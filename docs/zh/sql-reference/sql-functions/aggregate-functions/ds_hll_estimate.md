# ds_hll_estimate

从序列化的 HyperLogLog 草图估算近似去重计数。此函数是 DataSketches HLL 近似去重计数函数族的一部分。

`ds_hll_estimate` 接受由 `ds_hll_accumulate` 或 `ds_hll_combine` 创建的 VARBINARY 序列化草图，并返回估算的不同值数量。

它基于 Apache DataSketches，为近似去重计数提供高精度。更多信息，请参见 [HyperLogLog 草图](https://datasketches.apache.org/docs/HLL/HllSketches.html)。

## 语法

```Haskell
bigint ds_hll_estimate(sketch)
```

### 参数

- `sketch`: 包含序列化 HyperLogLog 草图的 VARBINARY 列

## 返回类型

返回 BIGINT 类型的估算去重计数。

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

-- 创建包含草图的表
CREATE TABLE t2 (
  id BIGINT,
  dt VARCHAR(10),
  ds_id VARBINARY,
  ds_province VARBINARY,
  ds_age VARBINARY,
  ds_dt VARBINARY
)
DUPLICATE KEY(id, dt)
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- 插入草图数据
INSERT INTO t2 
SELECT id, dt,
       ds_hll_accumulate(id),
       ds_hll_accumulate(province, 20),
       ds_hll_accumulate(age, 12, "HLL_6"),
       ds_hll_accumulate(dt, 10, "HLL_8") 
FROM t1;

-- 按日期分组估算去重计数
SELECT dt, 
       ds_hll_estimate(ds_id), 
       ds_hll_estimate(ds_province), 
       ds_hll_estimate(ds_age), 
       ds_hll_estimate(ds_dt) 
FROM t2 
GROUP BY dt 
ORDER BY 1 
LIMIT 3;
```

## 相关函数

- `ds_hll_accumulate`: 将值累积到序列化草图中
- `ds_hll_combine`: 将多个序列化草图合并为单个草图
- `ds_hll_count_distinct`: 直接近似去重计数函数

## 关键词

ds_HLL_ESTIMATE, HLL, HYPERLOGLOG, APPROXIMATE, DISTINCT, COUNT 