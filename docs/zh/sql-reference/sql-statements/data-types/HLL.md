---
displayed_sidebar: "Chinese"
---

# HLL (HyperLogLog)

## 描述

HyperLogLog 类型，用于近似去重。详细的使用方法请参考 [使用 HyperLogLog 实现近似去重](../../../using_starrocks/Using_HLL.md)。

HLL 是基于 HyperLogLog 算法的工程实现，用于保存 HyperLogLog 计算过程的中间结果，HLL 类型的列只能作为表的 value 列类型，通过聚合来不断的减少数据量，以此来实现加快查询的目的。基于 HLL 到的是一个估算结果，误差大概在 1% 左右。

HLL 列是通过其它列或者导入数据里面的数据生成的，导入的时候通过 `HLL_HASH` 函数来指定数据中哪一列用于生成 HLL 列它常用于替代 `count distinct`，通过结合 rollup 在业务上用于快速计算 UV。

HLL类型使用的存储空间取决于 HLL 中插入的 hash 值的去重数量。分为三种情况考虑:

- HLL 为空: 未插入任何值，HLL 的存储代价最小，占用 80 字节。
- HLL 中不重复 hash 值的个数 &le;160
  最大代价为: 80 + 160 * 8 = 1360 字节。
- HLL 中不重复 hash 值的个数 >160
  存储代价固定为: 80 + 16 * 1024 = 16464 字节。

实际使用中，数据量和数据分布会影响查询的内存使用量和近似结果的准确性，因此需要考虑:

- 数据量： 因为 HLL 是统计估计算法，数据量大，误差就小；数据量较小时，误差就大。
- 数据分布：数据量很大时，group by 维度列基数很高的情况下，计算会使用更多内存， 不推荐使用HLL去重。 no-group-by 去重、group by 维度列基数很低的情况下，推荐使用 HLL 去重。
- 当用户查询粒度较粗时：最好使用聚合模型或者物化视图对数据进行预聚合对聚合列进行 rollup 降低数据规模。

## 相关函数

**[HLL_UNION_AGG(hll)](../../sql-functions/aggregate-functions/hll_union_agg.md)**：此函数为聚合函数，用于计算满足条件的所有数据的基数估算。此函数还可用于分析函数，只支持默认窗口，不支持 window 子句。

**[HLL_RAW_AGG(hll)](../../sql-functions/aggregate-functions/hll_raw_agg.md)**：此函数为聚合函数，用于聚合 hll 类型字段，并且返回的还是 hll 类型。

**[HLL_CARDINALITY(hll)](../../sql-functions/scalar-functions/hll_cardinality.md)**：此函数用于计算单条 hll 列的基数估算。

**[HLL_HASH(column_name)](../../sql-functions/aggregate-functions/hll_hash.md)**：生成 HLL 列类型，用于 insert 或导入的时候，导入的使用见相关说明。

**[HLL_EMPTY()](../../sql-functions/aggregate-functions/hll_empty.md)**：生成空 HLL 列，用于 insert 或导入的时候补充默认值，导入的使用见相关说明。

## 示例

### 创建含有 HLL 列的表

```sql
create table test(
    dt date,
    id int,
    name char(10),
    province char(10),
    os char(1),
    set1 hll hll_union,
    set2 hll hll_union
)
distributed by hash(id);
```

### 通过 [Stream load](../data-manipulation/STREAM_LOAD.md) 导入数据

```bash
a. 使用表中的列生成 HLL 列。
curl --location-trusted -uname:password -T data -H "label:load_1" \
    -H "columns:dt, id, name, province, os, set1=hll_hash(id), set2=hll_hash(name)"
http://host/api/test_db/test/_stream_load

b. 使用数据中的某一列生成 HLL 列
curl --location-trusted -uname:password -T data -H "label:load_1" \
    -H "columns:dt, id, name, province, sex, cuid, os, set1=hll_hash(cuid), set2=hll_hash(os)"
http://host/api/test_db/test/_stream_load
```

### 通过 HLL 聚合数据

聚合数据有 3 种常用方式：

```sql
-- 基于第一个示例创建的 HLL 表创建一个 rollup，让 UV 列产生聚合。
alter table test add rollup test_rollup(dt, set1);


-- 创建另外一张专门计算 UV 的表，然后 INSERT 数据。
create table test_uv(
    dt date,
    id int,
    uv_set hll hll_union
)
distributed by hash(id);

insert into test_uv select dt, id, set1 from test;


-- 创建另外一张专门计算 UV 的表，然后 INSERT  并通过 hll_hash 根据 test 其它非HLL 列生成 HLL。
create table test_uv(
    dt date,
    id int,
    id_set hll hll_union
)
distributed by hash(id);

insert into test_uv select dt, id, hll_hash(id) from test;
```

### 查询 HLL 列

查询 HLL 列不允许直接查询它的原始值，可以通过配套的函数进行查询。

```sql
-- 求总 UV。
select HLL_UNION_AGG(uv_set) from test_uv;

-- 求每一天的 UV。
select dt, HLL_CARDINALITY(uv_set) from test_uv;

-- 求 test 表中 set1 的聚合值。
select dt, HLL_CARDINALITY(uv)from (
    select dt, HLL_RAW_AGG(set1) as uv from test group by dt) tmp;
select dt, HLL_UNION_AGG(set1) as uv from test group by dt;
```
