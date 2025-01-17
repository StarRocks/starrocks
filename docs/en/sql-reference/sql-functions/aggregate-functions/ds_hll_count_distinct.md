# ds_hll_count_distinct

Returns the approximate value of aggregate function similar to the result of COUNT(DISTINCT col). APPROX_COUNT_DISTINCT(expr) is similar function.

ds_hll_count_distinct is faster than the COUNT and DISTINCT combination and uses a fixed-size memory, so less memory is required for columns of high cardinality.

It is slower than APPROX_COUNT_DISTINCT(expr) but with higher precision because it adopts of Apache Datasketches. For more information, see [HyperLogLog Sketches](https://datasketches.apache.org/docs/HLL/HllSketches.html).

## Syntax

```Haskell
ds_hll_count_distinct(expr, [log_k], [tgt_type])
```
- `log_k`: Integer. Range [4, 21]. Default: 17.
- `tgt_type`: Valid values are `HLL_4`, `HLL_6` (default) and `HLL_8`.

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

mysql> select ds_hll_count_distinct(id), ds_hll_count_distinct(province), ds_hll_count_distinct(age), ds_hll_count_distinct(dt) from t1 order by 1, 2;
+---------------------------+---------------------------------+----------------------------+---------------------------+
| ds_hll_count_distinct(id) | ds_hll_count_distinct(province) | ds_hll_count_distinct(age) | ds_hll_count_distinct(dt) |
+---------------------------+---------------------------------+----------------------------+---------------------------+
|                    100090 |                          100140 |                        100 |                         1 |
+---------------------------+---------------------------------+----------------------------+---------------------------+
1 row in set (0.07 sec)

mysql> select ds_hll_count_distinct(id, 21), ds_hll_count_distinct(province, 21), ds_hll_count_distinct(age, 21), ds_hll_count_distinct(dt, 21) from t1 order by 1, 2;
+-------------------------------+-------------------------------------+--------------------------------+-------------------------------+
| ds_hll_count_distinct(id, 21) | ds_hll_count_distinct(province, 21) | ds_hll_count_distinct(age, 21) | ds_hll_count_distinct(dt, 21) |
+-------------------------------+-------------------------------------+--------------------------------+-------------------------------+
|                         99995 |                              100001 |                            100 |                             1 |
+-------------------------------+-------------------------------------+--------------------------------+-------------------------------+
1 row in set (0.07 sec)


mysql> select ds_hll_count_distinct(id, 10, "HLL_8"), ds_hll_count_distinct(province, 10, "HLL_8"), ds_hll_count_distinct(age, 10, "HLL_8"), ds_hll_count_distinct(dt, 10, "HLL_8") from t1 order by 1, 2;
+----------------------------------------+----------------------------------------------+-----------------------------------------+----------------------------------------+
| ds_hll_count_distinct(id, 10, 'HLL_8') | ds_hll_count_distinct(province, 10, 'HLL_8') | ds_hll_count_distinct(age, 10, 'HLL_8') | ds_hll_count_distinct(dt, 10, 'HLL_8') |
+----------------------------------------+----------------------------------------------+-----------------------------------------+----------------------------------------+
|                                  99844 |                                       101905 |                                      96 |                                      1 |
+----------------------------------------+----------------------------------------------+-----------------------------------------+----------------------------------------+
1 row in set (0.09 sec)

```

## Keywords

DS_HLL_COUNT_DISTINCT,APPROX_COUNT_DISTINCT
