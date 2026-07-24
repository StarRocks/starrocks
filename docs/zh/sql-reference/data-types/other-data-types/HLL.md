---
displayed_sidebar: docs
description: "HLL 用于近似计数去重。"
---

# HLL (HyperLogLog)

HLL 用于[近似计数去重](../../../using_starrocks/distinct_values/Using_HLL.md)。

HLL 支持基于 HyperLogLog 算法开发程序。它用于存储 HyperLogLog 计算过程的中间结果。它只能用作表的值列类型。HLL 通过聚合减少数据量以加快查询过程。估算结果可能存在 1% 的偏差。

HLL 列根据导入的数据或其他列的数据生成。导入数据时，[hll_hash](../../sql-functions/scalar-functions/hll_hash.md) 函数指定将使用哪一列来生成 HLL 列。HLL 通常用于替代 COUNT DISTINCT，并通过 rollup 快速计算独立访客数（UV）。

HLL 使用的存储空间由哈希值中的不同值决定。存储空间因以下三种情况而有所不同：

- HLL 为空。没有值插入 HLL，存储成本最低，为 80 字节。
- HLL 中不同哈希值的数量小于或等于 160。最高存储成本为 1360 字节（80 + 160 * 8 = 1360）。
- HLL 中不同哈希值的数量大于 160。存储成本固定为 16,464 字节（80 + 16 * 1024 = 16464）。

在实际业务场景中，数据量和数据分布会影响查询的内存使用情况以及近似结果的准确性。您需要考虑以下两个因素：

- 数据量：HLL 返回近似值。数据量越大，结果越准确。数据量越小，偏差越大。
- 数据分布：在数据量大且 GROUP BY 的维度列基数高的情况下，数据计算将使用更多内存。此情况下不推荐使用 HLL。推荐在不使用 GROUP BY 的 count distinct 或对低基数维度列进行 GROUP BY 时使用。
- 查询粒度：如果您以较大的查询粒度查询数据，建议使用聚合表或物化视图对数据进行预聚合，以减少数据量。

## 相关函数

- [HLL_UNION_AGG(hll)](../../sql-functions/aggregate-functions/hll_union_agg.md)：该函数是一个聚合函数，用于估算满足条件的所有数据的基数。也可用作分析函数。仅支持默认窗口，不支持 window 子句。

- [HLL_RAW_AGG(hll)](../../sql-functions/aggregate-functions/hll_raw_agg.md)：该函数是一个聚合函数，用于聚合 hll 类型的字段，并返回 hll 类型。

- HLL_CARDINALITY(hll)：该函数用于估算单个 hll 列的基数。

- [HLL_HASH(column_name)](../../sql-functions/scalar-functions/hll_hash.md)：生成 HLL 列类型，用于插入或导入。请参阅导入使用说明。

- [HLL_EMPTY](../../sql-functions/scalar-functions/hll_empty.md)：生成空的 HLL 列，用于在插入或导入时填充默认值。请参阅导入使用说明。

## 示例

1. 创建一个包含 HLL 列 `set1` 和 `set2` 的表。

   ```sql
   create table test(
   dt date,
   id int,
   name char(10),
   province char(10),
   os char(1),
   set1 hll hll_union,
   set2 hll hll_union)
   distributed by hash(id);
   ```

2. 使用[Stream Load](../../../loading/StreamLoad.md)加载数据。

   ```plain text
   a. Use table columns to generate an HLL column.
   curl --location-trusted -uname:password -T data -H "label:load_1" \
       -H "columns:dt, id, name, province, os, set1=hll_hash(id), set2=hll_hash(name)"
   http://host/api/test_db/test/_stream_load

   b. Use data columns to generate an HLL column.
   curl --location-trusted -uname:password -T data -H "label:load_1" \
       -H "columns:dt, id, name, province, sex, cuid, os, set1=hll_hash(cuid), set2=hll_hash(os)"
   http://host/api/test_db/test/_stream_load
   ```

3. 通过以下三种方式聚合数据：（不进行聚合时，直接查询基表的速度可能与使用 approx_count_distinct 一样慢）

   ```sql
   -- a. 创建 rollup 以聚合 HLL 列。
   alter table test add rollup test_rollup(dt, set1);

   -- b. 创建另一张表来计算 UV，并向其中插入数据。

   create table test_uv(
   dt date,
   id int,
   uv_set hll hll_union)
   distributed by hash(id);

   insert into test_uv select dt, id, set1 from test;

   -- c. 创建另一张表来计算 UV。插入数据并通过 hll_hash 对其他列进行测试，从而生成 HLL 列。

   create table test_uv(
   dt date,
   id int,
   id_set hll hll_union)
   distributed by hash(id);

   insert into test_uv select dt, id, hll_hash(id) from test;
   ```

4. 查询数据。HLL 列不支持直接查询其原始值，需通过匹配函数进行查询。

   ```plain text
   a. Calculate the total UV.
   select HLL_UNION_AGG(uv_set) from test_uv;

   b. Calculate the UV for each day.
   select dt, HLL_CARDINALITY(uv_set) from test_uv;

   c. Calculate the aggregation value of set1 in the test table.
   select dt, HLL_CARDINALITY(uv) from (select dt, HLL_RAW_AGG(set1) as uv from test group by dt) tmp;
   select dt, HLL_UNION_AGG(set1) as uv from test group by dt;
   ```
