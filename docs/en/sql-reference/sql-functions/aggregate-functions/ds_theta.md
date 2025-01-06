# DS_THETA



Returns the approximate value of aggregate function similar to the result of COUNT(DISTINCT col). Like APPROX_COUNT_DISTINCT(expr).

It is faster than the COUNT and DISTINCT combination and uses a fixed-size memory, so less memory is used for columns of high cardinality.

It is slower than APPROX_COUNT_DISTINCT(expr) but with higher precision. Which takes advantages of Apache Datasketches.

## Syntax

```Haskell
DS_THETA(expr)
```

## Examples

```plain text
MySQL >CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4;
MySQL > insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 100000));
MySQL > select DS_HLL(id)  from t1 order by 1, 2;
+-----------------------------------+
| DS_THETA(`query_id`) |
+-----------------------------------+
| 100215                             |
+-----------------------------------+
```
