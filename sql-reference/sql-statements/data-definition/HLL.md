# HLL

## 功能

HLL 是基于 HyperLogLog 算法的工程实现，用于保存 HyperLogLog 计算过程的中间结果，HLL 类型的列只能作为表的 value 列类型，通过聚合来不断的减少数据量，以此来实现加快查询的目的。基于 HLL 到的是一个估算结果，误差大概在 1%左右。

HLL 列是通过其它列或者导入数据里面的数据生成的，导入的时候通过 `HLL_HASH` 函数来指定数据中哪一列用于生成 HLL 列它常用于替代 `count distinct`，通过结合 rollup 在业务上用于快速计算 uv。

相关函数:

**[HLL_UNION_AGG(hll)](/sql-reference/sql-functions/aggregate-functions/hll_union_agg.md)**：此函数为聚合函数，用于计算满足条件的所有数据的基数估算。此函数还可用于分析函数，只支持默认窗口，不支持 window 子句。

**[HLL_RAW_AGG(hll)](../../sql-functions/aggregate-functions/hll_raw_agg.md)**：此函数为聚合函数，用于聚合 hll 类型字段，并且返回的还是 hll 类型。

**[HLL_CARDINALITY(hll)](../../sql-functions/scalar-functions/hll_cardinality.md)**：此函数用于计算单条 hll 列的基数估算。

**[HLL_HASH(column_name)](../../sql-functions/aggregate-functions/hll_hash.md)**：生成 HLL 列类型，用于 insert 或导入的时候，导入的使用见相关说明。

**[HLL_EMPTY()](../../sql-functions/aggregate-functions/hll_empty.md)**：生成空 HLL 列，用于 insert 或导入的时候补充默认值，导入的使用见相关说明。

## 示例

### 创建含有 hll 列的表

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
distributed by hash(id) buckets 32;
```

### 通过 [Stream load](../data-manipulation/STREAM_LOAD.md) 导入数据

```bash
a. 使用表中的列生成hll列
curl --location-trusted -uname:password -T data -H "label:load_1" \
    -H "columns:dt, id, name, province, os, set1=hll_hash(id), set2=hll_hash(name)"
http://host/api/test_db/test/_stream_load

b. 使用数据中的某一列生成hll列
curl --location-trusted -uname:password -T data -H "label:load_1" \
    -H "columns:dt, id, name, province, sex, cuid, os, set1=hll_hash(cuid), set2=hll_hash(os)"
http://host/api/test_db/test/_stream_load
```

### 通过 HLL 聚合数据

如果不聚合直接对 base 表查询，速度可能跟直接使用 `approx_count_distinct` 速度差不多。

聚合数据有常用方式 3 种：

```sql
--基于第一个示例创建的HLL表创建一个rollup，让hll列产生聚合
alter table test add rollup test_rollup(dt, set1);


--创建另外一张专门计算uv的表，然后insert数据
create table test_uv(
    dt date,
    id int,
    uv_set hll hll_union
)
distributed by hash(id) buckets 32;

insert into test_uv select dt, id, set1 from test;


--创建另外一张专门计算uv的表，然后insert并通过hll_hash根据test其它非hll列生成hll列
create table test_uv(
    dt date,
    id int,
    id_set hll hll_union
)
distributed by hash(id) buckets 32;

insert into test_uv select dt, id, hll_hash(id) from test;
```

### 查询 HLL 列

查询 hll 列不允许直接查询它的原始值，可以通过配套的函数进行查询。

```sql
--a. 求总uv
select HLL_UNION_AGG(uv_set) from test_uv;

--b. 求每一天的uv
select dt, HLL_CARDINALITY(uv_set) from test_uv;

--c. 求test表中set1的聚合值
select dt, HLL_CARDINALITY(uv)from (
    select dt, HLL_RAW_AGG(set1) as uv from test group by dt) tmp;
select dt, HLL_UNION_AGG(set1) as uv from test group by dt;
```
