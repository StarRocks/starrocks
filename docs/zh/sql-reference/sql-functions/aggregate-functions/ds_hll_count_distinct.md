# ds_hll_count_distinct

返回聚合函数的近似值，结果类似于 COUNT(DISTINCT col)。相似函数为 APPROX_COUNT_DISTINCT(expr)。

相较于 COUNT DISTINCT 速度更快，并且使用固定大小的内存，因此基于高基数列使用时内存占用更少。

相较于 APPROX_COUNT_DISTINCT(expr) 速度更慢，但由于 Apache Datasketches 的优势，导致其精度更高。更多信息，参考 [HyperLogLog Sketches](https://datasketches.apache.org/docs/HLL/HllSketches.html)。

## 语法

```Haskell
ds_hll_count_distinct(expr, [log_k], [tgt_type])
```
- `log_k`：必须为整数。范围：[4, 21]。默认值：17。
- `tgt_type`：有效值为 `HLL_4`、`HLL_6`（默认）以及 `HLL_8`。

## 示例

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
