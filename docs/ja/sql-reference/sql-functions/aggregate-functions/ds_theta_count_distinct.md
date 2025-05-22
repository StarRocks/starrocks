# ds_theta_count_distinct

`COUNT(DISTINCT col)` の結果に似た集計関数の近似値を返します。`ds_theta_count_distinct` は `COUNT(DISTINCT col)` よりも高速で、高いカーディナリティの列に対してメモリ使用量が少なくなります。

`ds_theta_count_distinct` は `APPROX_COUNT_DISTINCT(expr)` や `DS_HLL_COUNT_DISTINCT(expr)` に似ていますが、Apache DataSketches を採用しているため、精度が異なります。詳細は [Theta Sketches](https://datasketches.apache.org/docs/Theta/InverseEstimate.html) を参照してください。

相対誤差は 3.125%（95% の信頼度）です。詳細は [relative error table](https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html) を参照してください。

## 構文

```Haskell
BIGINT ds_theta_count_distinct(expr)
```

- `expr`: 近似的な COUNT DISTINCT 値を計算する列。

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
mysql> select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 order by 1, 2;
+-----------------------------+-----------------------------------+------------------------------+-----------------------------+
| ds_theta_count_distinct(id) | ds_theta_count_distinct(province) | ds_theta_count_distinct(age) | ds_theta_count_distinct(dt) |
+-----------------------------+-----------------------------------+------------------------------+-----------------------------+
|                      100215 |                            100846 |                          100 |                           1 |
+-----------------------------+-----------------------------------+------------------------------+-----------------------------+
1 row in set (0.62 sec)
```

## キーワード

DS_THETA_COUNT_DISTINCT,DS_HLL_COUNT_DISTINCT,APPROX_COUNT_DISTINCT
