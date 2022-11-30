# PERCENTILE_APPROX

## Description

Returns the approximation of the pth percentile, where the value of p is between 0 and 1.

Compression parameter is optional and has a setting range of [2048, 10000]. The larger the value, the higher the accuracy, the larger the memory consumption, and the longer the calculation time. If it is not specified or not beyond the range of [2048, 10000], the function runs with a default compression parameter of 10000.

This function uses fixed size memory, so less memory can be used for columns with high cardinality, and can be used to calculate statistics such as tp99.

## Syntax

```Haskell
PERCENTILE_APPROX(expr, DOUBLE p[, DOUBLE compression])
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
```

## keyword

PERCENTILE_APPROX,PERCENTILE,APPROX
