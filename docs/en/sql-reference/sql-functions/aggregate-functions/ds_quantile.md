# DS_QUANTILE



Returns the approximation of the pth percentile, where the value of p is between 0 and 1.

## Syntax

```Haskell
DS_QUANTILE(expr, DOUBLE p[, DOUBLE compression])
```

## Examples

```plain text
MySQL > CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
MySQL > insert into t1 select generate_series, generate_series, generate_series % 10, "2024-07-24" from table(generate_series(1, 100));
MySQL > select ds_quantile(id), ds_quantile(age) from t2 order by 1, 2;
+-------------------------------------------------------------+
| ds_quantile(id)            ds_quantile(age)           | 
+------------------------------------------------------------ +
| [50]                              [4]                                     |
+------------------------------------------------------------+
MySQL > select ds_quantile(id, [0.1, 0.5, 0.9]), ds_quantile(age, [0.1, 0.5, 0.9]) from t2 order by 1, 2;
+-------------------------------------------------------------+
| ds_quantile(id)            ds_quantile(age)           | 
+------------------------------------------------------------ +
| [10,50,90]                             [0,4,8]                   |
+------------------------------------------------------------+
MySQL > select ds_quantile(id, [0.1, 0.5, 0.9], 10000), ds_quantile(age, [0.1, 0.5, 0.9], 10000) from t2 order by 1, 2;
+-------------------------------------------------------------+
| ds_quantile(id)            ds_quantile(age)           | 
+------------------------------------------------------------ +
| [10,50,90]                             [0,4,8]                   |
+------------------------------------------------------------+
```
