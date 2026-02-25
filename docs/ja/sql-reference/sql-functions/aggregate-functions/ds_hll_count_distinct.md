# ds_hll_count_distinct

集計関数の近似値を返します。これは COUNT(DISTINCT col) の結果に似ています。APPROX_COUNT_DISTINCT(expr) も同様の関数です。

ds_hll_count_distinct は COUNT と DISTINCT の組み合わせよりも高速で、固定サイズのメモリを使用するため、高いカーディナリティの列に対して必要なメモリが少なくなります。

APPROX_COUNT_DISTINCT(expr) よりも遅いですが、Apache Datasketches を採用しているため、精度が高くなっています。詳細については、[HyperLogLog Sketches](https://datasketches.apache.org/docs/HLL/HllSketches.html) を参照してください。

## 構文

```Haskell
ds_hll_count_distinct(expr, [log_k], [tgt_type])
```
- `log_k`: 整数。範囲 [4, 21]。デフォルト: 17。
- `tgt_type`: 有効な値は `HLL_4`、`HLL_6`（デフォルト）、`HLL_8` です。

## 例

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

## キーワード

DS_HLL_COUNT_DISTINCT,APPROX_COUNT_DISTINCT