# percentile_approx_weight

返回带权重的 p 分位数的近似值。`percentile_approx_weight` 是 `PERCENTILE_APPROX` 的加权版本，允许用户为每个输入值指定一个权重（一个常量值或数值列）。

此函数使用固定大小的内存，因此对于高基数的列可以使用更少的内存。它也可以用于计算诸如 tp99 之类的统计数据。

## 语法

```Haskell
DOUBLE PERCENTILE_APPROX_WEIGHT(expr, BIGINT weight, DOUBLE p[, DOUBLE compression])
```

- `expr`: 要计算分位数的列。
- `pth` : 分位数值。范围：[0, 1]。例如，`0.99` 表示第 99 个百分位数。
- `weight` : 权重列。必须是一个正的常量数或列。
- `compression` : （可选）压缩比。范围：[2048, 10000]。值越大，精度越高，内存消耗越大，计算时间越长。如果未指定此参数或值超出 [2048, 10000] 的范围，则使用默认值 `10000`。

## 示例

```plain text
CREATE TABLE t1 (
    c1 int,
    c2 double,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint,
    c7 string,
    c8 double,
    c9 date,
    c10 datetime,
    c11 array<int>,
    c12 map<double, double>,
    c13 struct<a bigint, b double>
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 
    select generate_series, generate_series,  11, 111, 1111, 11111, "111111", 1.1, "2024-09-01", "2024-09-01 18:00:00", [1, 2, 3], map(1, 5.5), row(100, 100)
    from table(generate_series(1, 50000, 3));
-- 使用常量值作为权重
mysql> select percentile_approx_weighted(c1, 1, 0.9) from t1;
+----------------------------------------+
| percentile_approx_weighted(c1, 1, 0.9) |
+----------------------------------------+
|                         45000.39453125 |
+----------------------------------------+
1 row in set (0.07 sec)
-- 使用数值列作为权重
mysql> select percentile_approx_weighted(c2, c1, 0.5) from t1;
+-----------------------------------------+
| percentile_approx_weighted(c2, c1, 0.5) |
+-----------------------------------------+
|                              35355.9375 |
+-----------------------------------------+
1 row in set (0.07 sec)
-- 使用权重和压缩比计算分位数
mysql> select percentile_approx_weighted(c2, c1, 0.5, 10000) from t1;
+------------------------------------------------+
| percentile_approx_weighted(c2, c1, 0.5, 10000) |
+------------------------------------------------+
|                                     35355.9375 |
+------------------------------------------------+
1 row in set (0.09 sec)
```

## 关键词

PERCENTILE_APPROX_WEIGHT,PERCENTILE_APPROX,PERCENTILE,APPROX
