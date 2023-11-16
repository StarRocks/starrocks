# HLL(HyperLogLog)

## 描述

<<<<<<< HEAD
HyperLogLog 类型，用于近似去重。
=======
HyperLogLog 类型，用于近似去重。详细的使用方法请参考 [使用 HyperLogLog 实现近似去重](../../../using_starrocks/Using_HLL.md)。

HLL 是基于 HyperLogLog 算法的工程实现，用于保存 HyperLogLog 计算过程的中间结果，HLL 类型的列只能作为表的 value 列类型，通过聚合来不断的减少数据量，以此来实现加快查询的目的。基于 HLL 到的是一个估算结果，误差大概在 1% 左右。

HLL 列是通过其它列或者导入数据里面的数据生成的，导入的时候通过 `HLL_HASH` 函数来指定数据中哪一列用于生成 HLL 列它常用于替代 `count distinct`，通过结合 rollup 在业务上用于快速计算 UV。
>>>>>>> b8eb50e58 ([Doc] link fixes to 2.5 (#35185))

HLL类型使用的存储空间取决于 HLL 中插入的 hash 值的去重数量。分为三种情况考虑:

- HLL 为空: 未插入任何值，HLL 的存储代价最小，占用 80 字节。
- HLL 中不重复 hash 值的个数 `<=160`
  最大代价为: 80 + 160 * 8 = 1360 字节。
- HLL 中不重复 hash 值的个数 `>160`
  存储代价固定为: 80 + 16 * 1024 = 16464 字节。

实际使用中，数据量和数据分布会影响查询的内存使用量和近似结果的准确性，因此需要考虑:

- 数据量： 因为 HLL 是统计估计算法，数据量大，误差就小；数据量较小时，误差就大。
- 数据分布：数据量很大时，group by 维度列基数很高的情况下，计算会使用更多内存， 不推荐使用HLL去重。 no-group-by 去重、group by 维度列基数很低的情况下，推荐使用 HLL 去重。
- 当用户查询粒度较粗时：最好使用聚合模型或者物化视图对数据进行预聚合对聚合列进行 rollup 降低数据规模。

详细的使用方法请参考：

<<<<<<< HEAD
- [使用 HyperLogLog 实现近似去重](../../../using_starrocks/Using_HLL.md)
- [HLL](../../sql-statements/data-definition/HLL.md)
=======
**[HLL_UNION_AGG(hll)](../../../sql-reference/sql-functions/aggregate-functions/hll_union_agg.md)**：此函数为聚合函数，用于计算满足条件的所有数据的基数估算。此函数还可用于分析函数，只支持默认窗口，不支持 window 子句。

**[HLL_RAW_AGG(hll)](../../sql-functions/aggregate-functions/hll_raw_agg.md)**：此函数为聚合函数，用于聚合 hll 类型字段，并且返回的还是 hll 类型。

**[HLL_CARDINALITY(hll)](../../sql-functions/scalar-functions/hll_cardinality.md)**：此函数用于计算单条 hll 列的基数估算。

**[HLL_HASH(column_name)](../../sql-functions/aggregate-functions/hll_hash.md)**：生成 HLL 列类型，用于 insert 或导入的时候，导入的使用见相关说明。

**[HLL_EMPTY()](../../sql-functions/aggregate-functions/hll_empty.md)**：生成空 HLL 列，用于 insert 或导入的时候补充默认值，导入的使用见相关说明。
>>>>>>> b8eb50e58 ([Doc] link fixes to 2.5 (#35185))

## 示例

创建表时指定字段类型为 HLL。

```sql
CREATE TABLE hllDemo
(
    k1 TINYINT,
    v1 HLL HLL_UNION
)
ENGINE=olap
AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 8;
```
