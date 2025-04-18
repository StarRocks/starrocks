# ds_theta_count_distinct

Returns the approximate value of aggregate function similar to the result of `COUNT(DISTINCT col)`, but `ds_theta_count_distinct` is faster than the `COUNT(DISTINCT col)` and uses less memory for columns of high cardinality.

It is similar with `APPROX_COUNT_DISTINCT(expr)`/`DS_HLL_COUNT_DISTINCT(expr)` but with different precisions because it adopts of Apache Datasketches. 

The relative error is 3.125% (95% confidence), see the relative error table for more information(https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html).

For more information, see [Theta Sketches](https://datasketches.apache.org/docs/Theta/InverseEstimate.html).

## Syntax

```Haskell
BIGINT ds_theta_count_distinct(expr)
```

- `expr`: input column to calculate the approximate count distinct value

## Examples

```plain text
mysql> CREATE TABLE t1 (
    ->   id BIGINT NOT NULL,
    ->   province VARCHAR(64),
    ->   age SMALLINT,
    ->   dt VARCHAR(10) NOT NULL
    -> )
    -> DUPLICATE KEY(id)
    -> DISTRIBUTED BY HASH(id) BUCKETS 4;
Query OK, 0 rows affected (0.02 sec)

mysql> insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 100000));

Query OK, 100000 rows affected (0.29 sec)

mysql> select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 order by 1, 2;
+-----------------------------+-----------------------------------+------------------------------+-----------------------------+
| ds_theta_count_distinct(id) | ds_theta_count_distinct(province) | ds_theta_count_distinct(age) | ds_theta_count_distinct(dt) |
+-----------------------------+-----------------------------------+------------------------------+-----------------------------+
|                      100215 |                            100846 |                          100 |                           1 |
+-----------------------------+-----------------------------------+------------------------------+-----------------------------+
1 row in set (0.62 sec)
```

## Keywords

DS_THETA_COUNT_DISTINCT,DS_HLL_COUNT_DISTINCT,APPROX_COUNT_DISTINCT
