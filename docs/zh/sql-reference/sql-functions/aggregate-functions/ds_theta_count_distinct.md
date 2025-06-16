# ds_theta_count_distinct

返回类似于 `COUNT(DISTINCT col)` 结果的聚合函数的近似值。`ds_theta_count_distinct` 比 `COUNT(DISTINCT col)` 更快，并且在处理高基数列时使用更少的内存。

`ds_theta_count_distinct` 类似于 `APPROX_COUNT_DISTINCT(expr)` 和 `DS_HLL_COUNT_DISTINCT(expr)`，但由于采用了 Apache DataSketches，因此精度不同。更多信息请参见 [Theta Sketches](https://datasketches.apache.org/docs/Theta/InverseEstimate.html)。

相对误差为 3.125%（95% 置信度）。更多信息请参见 [relative error table](https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html)。

## 语法

```Haskell
BIGINT ds_theta_count_distinct(expr)
```

- `expr`: 要计算近似 COUNT DISTINCT 值的列。

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
mysql> select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 order by 1, 2;
+-----------------------------+-----------------------------------+------------------------------+-----------------------------+
| ds_theta_count_distinct(id) | ds_theta_count_distinct(province) | ds_theta_count_distinct(age) | ds_theta_count_distinct(dt) |
+-----------------------------+-----------------------------------+------------------------------+-----------------------------+
|                      100215 |                            100846 |                          100 |                           1 |
+-----------------------------+-----------------------------------+------------------------------+-----------------------------+
1 row in set (0.62 sec)
```

## 关键词

DS_THETA_COUNT_DISTINCT,DS_HLL_COUNT_DISTINCT,APPROX_COUNT_DISTINCT
