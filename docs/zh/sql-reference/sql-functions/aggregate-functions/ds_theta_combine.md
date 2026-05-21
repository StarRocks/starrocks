# ds_theta_combine

将多个序列化的 Theta 草图合并为单个序列化草图。此函数是 DataSketches Theta 近似去重计数函数族的一部分。

`ds_theta_combine` 接受由 `ds_theta_accumulate` 创建的 VARBINARY 列，并将其各行合并为表示所有去重值并集的单个草图。

它基于 Apache DataSketches。更多信息，请参见 [Theta Sketches](https://datasketches.apache.org/docs/Theta/InverseEstimate.html)。

## 语法

```Haskell
sketch ds_theta_combine(sketch)
```

### 参数

- `sketch`: 包含由 `ds_theta_accumulate` 创建的序列化 Theta 草图的 VARBINARY 列。

## 返回类型

返回包含合并后序列化 Theta 草图的 VARBINARY。`ds_theta_accumulate` 时选定的 `log_k` 会通过合并过程保留下来。

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

-- 创建存储草图的表
CREATE TABLE t2 (
  `id` bigint,
  `dt` varchar(10),
  `ds_id` binary,
  `ds_province` binary,
  `ds_age` binary,
  `ds_dt` binary
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- 存储由 ds_theta_accumulate 创建的草图
INSERT INTO t2 SELECT id, dt,
  ds_theta_accumulate(id),
  ds_theta_accumulate(province, 14),
  ds_theta_accumulate(age),
  ds_theta_accumulate(dt)
FROM t1;

-- 按日期分组合并草图
SELECT dt,
       ds_theta_combine(ds_id),
       ds_theta_combine(ds_province),
       ds_theta_combine(ds_age),
       ds_theta_combine(ds_dt)
FROM t2
GROUP BY dt
ORDER BY 1
LIMIT 3;
```

## 相关函数

- `ds_theta_accumulate`: 从数据创建序列化 Theta 草图
- `ds_theta_estimate`: 从序列化草图估算去重计数
- `ds_theta_count_distinct`: 直接近似去重计数函数

## 关键词

DS_THETA_COMBINE, THETA, APPROXIMATE, DISTINCT, COUNT, MERGE, UNION
