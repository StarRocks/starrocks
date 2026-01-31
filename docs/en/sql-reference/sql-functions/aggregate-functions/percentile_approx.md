---
displayed_sidebar: docs
---

# percentile_approx



Returns the approximate value for a given percentile p, or an array of values for corresponding percentiles if p is an array. All percentile values must be in the range [0,1].

Compression parameter is optional and has a setting range of [2048, 10000]. The larger the value, the higher the accuracy, the larger the memory consumption, and the longer the calculation time. If it is not specified or not beyond the range of [2048, 10000], the function runs with a default compression parameter of 10000.

This function uses fixed size memory, so less memory can be used for columns with high cardinality, and can be used to calculate statistics such as tp99.

## Syntax

```Haskell
DOUBLE PERCENTILE_APPROX(expr, DOUBLE|ARRAY<DOUBLE> p[, DOUBLE compression])
```

## Examples

```plain text
MySQL > select `table`, percentile_approx(cost_time,0.99)
from log_statis
group by `table`;
+----------+--------------------------------------+
| table    | percentile_approx(`cost_time`, 0.99) |
+----------+--------------------------------------+
| test     |                                54.22 |
+----------+--------------------------------------+

MySQL > select `table`, percentile_approx(cost_time,0.99, 4096)
from log_statis
group by `table`;
+----------+----------------------------------------------+
| table    | percentile_approx(`cost_time`, 0.99, 4096.0) |
+----------+----------------------------------------------+
| test     |                                        54.21 |
+----------+----------------------------------------------+

MySQL > select percentile_approx(c2, [0.1, 0.5, 0.9], 10000) from t1;
+-----------------------------------------------+
| percentile_approx(c2, [0.1, 0.5, 0.9], 10000) |
+-----------------------------------------------+
| [4999.6005859375,25000,45000.3984375]         |
+-----------------------------------------------+
```

## keyword

PERCENTILE_APPROX,PERCENTILE,APPROX
