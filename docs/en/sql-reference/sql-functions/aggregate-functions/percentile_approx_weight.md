# percentile_approx_weight

Returns the approximation of the pth percentile with weight. `percentile_approx_weight` is a weighted version of `PERCENTILE_APPROX`, and allows users to specify a weight (a constant value or numeric column) for each input value.

This function uses fixed-size memory, so less memory can be used for columns with high cardinality. It also can be used to calculate statistics such as tp99.

## Syntax

```Haskell
DOUBLE PERCENTILE_APPROX_WEIGHT(expr, BIGINT weight, DOUBLE p[, DOUBLE compression])
```

- `expr`: The column for which to calculate the percentile.
- `pth` : Percentile value. Range: [0, 1]. For example, `0.99` indicates the 99th percentile.
- `weight` : Weight column. It must be a positive constant number or column.
- `compression` : (Optional) Compression ratio. Range: [2048, 10000]. The larger the value, the higher the precision, the larger the memory consumption, and the longer the calculation time. If this parameter is not specified or the value is beyond the range of [2048, 10000], the default value `10000` is used.

## Examples

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
-- use constant value as weight
mysql> select percentile_approx_weighted(c1, 1, 0.9) from t1;
+----------------------------------------+
| percentile_approx_weighted(c1, 1, 0.9) |
+----------------------------------------+
|                         45000.39453125 |
+----------------------------------------+
1 row in set (0.07 sec)
-- use a numeric column as weight
mysql> select percentile_approx_weighted(c2, c1, 0.5) from t1;
+-----------------------------------------+
| percentile_approx_weighted(c2, c1, 0.5) |
+-----------------------------------------+
|                              35355.9375 |
+-----------------------------------------+
1 row in set (0.07 sec)
-- use weight and compression to compute percentile
mysql> select percentile_approx_weighted(c2, c1, 0.5, 10000) from t1;
+------------------------------------------------+
| percentile_approx_weighted(c2, c1, 0.5, 10000) |
+------------------------------------------------+
|                                     35355.9375 |
+------------------------------------------------+
1 row in set (0.09 sec)
```

## Keywords

PERCENTILE_APPROX_WEIGHT,PERCENTILE_APPROX,PERCENTILE,APPROX
