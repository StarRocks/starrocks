# HLL(HyperLogLog)

## 描述

HyperLogLog 类型，用于近似去重。

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

- [使用 HyperLogLog 实现近似去重](/using_starrocks/Using_HLL.md)
- [HLL](/sql-reference/sql-statements/data-definition/HLL.md)

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
