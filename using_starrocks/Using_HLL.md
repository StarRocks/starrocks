# 使用 HLL 近似去重

本小节介绍如何通过 HLL 在 StarRocks 中近似去重。

在现实场景中，随着 StarRocks 中的数据量增大，对数据进行去重分析的压力会越来越大。当数据的规模达到一定程度时，进行精确去重的成本会比较高。此时用户通常会采用近似算法来降低计算压力。HyperLogLog（简称 HLL）是一种近似去重算法，其特点是具有非常优异的空间复杂度 O(mloglogn)，时间复杂度为 O(n)，并且计算结果的误差可控制在 1%—10% 左右，误差与数据集大小以及所采用的哈希函数有关。

## 什么是 HyperLogLog

HyperLogLog 是一种近似的去重算法，能够使用极少的存储空间计算一个数据集的不重复元素的个数。**HLL类型**是基于 HyperLogLog 算法的工程实现。用于保存 HyperLogLog 计算过程的中间结果，它只能作为数据表的指标列类型。

由于HLL 算法的原理涉及到比较多的数学知识，在这里我们仅通过一个实际例子来说明。假设我们设计随机试验A：做抛币的独立重复试验，直到首次出现正面; 记录首次出现正面的抛币次数为随机变量 X，则:

* X=1，概率P(X=1)=1/2
* X=2，概率P(X=2)=1/4
* ...
* X=n，概率P(X=n)=(1/2)<sup>n</sup>

我们用试验A构造随机试验B: 做N次独立重复试验 A，产生 N 个独立同分布的随机变量 X<sub>1</sub>, X<sub>2</sub>, X<sub>3</sub>, ..., X<sub>N</sub>; 取这组随机变量的最大值为 X<sub>max</sub>。结合极大似然估算的方法，N的估算值为 2<sup>X<sub>max</sub></sup>。
<br/>

现在，我们在给定的数据集上，使用哈希函数模拟上述试验:

* 试验 A：对数据集的元素计算哈希值，记录哈希值的二进制表示形式中，从最低位算起，记录 bit 1 首次出现的位置。
* 试验 B：遍历数据集，对数据集的元素做试验 A 的处理，每次试验时，更新 bit 1 首次出现的最大位置 m；
* 估算数据集中不重复元素的个数为 2 <sup>m</sup>。

事实上，HLL 算法根据元素哈希值的低 k 位，将元素划分到 K=2<sup>k</sup> 个桶中，统计桶内元素的第 k+1 位起 bit 1 首次出现位置的最大值 m<sub>1</sub>, m<sub>2</sub>,..., m<sub>k</sub>, 估算桶内不重复元素元素的个数 2<sup>m<sub>1</sub></sup>, 2<sup>m<sub>2</sub></sup>,..., 2<sup>m<sub>k</sub></sup>, 数据集的不重复元素个数为桶的数量乘以桶内不重复元素个数的调和平均数: N = K(K/(2<sup>\-m<sub>1</sub></sup>+2<sup>\-m<sub>2</sub></sup>,..., 2<sup>\-m<sub>K</sub></sup>))。
<br/>

HLL 为了使结果更加精确，用修正因子和估算结果相乘，得出最终结果。

为了方便读者的理解，我们参考文章[https://gist.github.com/avibryant/8275649,](https://gist.github.com/avibryant/8275649) 用 StarRocks 的 SQL 语句实现 HLL 去重算法:

~~~sql
SELECT floor((0.721 * 1024 * 1024) / (sum(pow(2, m * -1)) + 1024 - count(*))) AS estimate
FROM(select(murmur_hash3_32(c2) & 1023) AS bucket,
     max((31 - CAST(log2(murmur_hash3_32(c2) & 2147483647) AS INT))) AS m
     FROM db0.table0
     GROUP BY bucket) bucket_values;
~~~

该算法对 db0.table0 的 col2 进行去重分析。

* 使用哈希函数 murmur_hash3_32 对 col2 计算 hash 值为 32 有符号整数；
* 采用 1024 个桶，此时修正因子为 0.721，取 hash 值低 10bit 为桶的下标；
* 忽略 hash 值的符号位，从次高位开始向低位查找，确定 bit 1 首次出现的位置；
* 把算出的hash值，按 bucket 分组，桶内使用 MAX 聚合求 bit 1 的首次出现的最大位置；
* 分组聚合的结果作为子查询，最后求所有桶的估算值的调和平均数，乘以桶数和修正因子。
* 注意空桶计数为 1。

上述算法在数据规模较大时，误差很低。

这就是 HLL 算法的核心思想。有兴趣的同学可以参考[HyperLogLog论文](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)。

## 使用 HLL 去重

说明：

1. 使用 HLL 去重，需要在建表语句中，将目标的指标列的类型设置为 **HLL**，聚合函数设置为 **HLL_UNION**。
2. 目前, 只有聚合表支持 HLL 类型的指标列。
3. 当在 HLL 类型列上使用 count distinct 时，StarRocks 会自动转化为 HLL_UNION_AGG 计算。

具体函数语法参见 [HLL_UNION_AGG](../sql-reference/sql-functions/aggregate-functions/hll_union_agg.md)。

### 示例

创建一张含有**HLL**列的表，其中 UV 列为聚合列，列类型为 HLL，聚合函数为 HLL_UNION。

~~~sql
CREATE TABLE test(
        dt DATE,
        id INT,
        uv HLL HLL_UNION
)
DISTRIBUTED BY HASH(ID) BUCKETS 32;
~~~

> * 注：当数据量较大时，建议为高频率的 HLL 查询建立对应的物化视图。

1. 导入数据

     * Stream Load 模式:

     ~~~bash
     curl --location-trusted -u root: -H "label:label_1600997542287" \
         -H "column_separator:," \
         -H "columns:dt,id,user_id, uv=hll_hash(user_id)" -T /root/test.csv http://starrocks_be0:8040/api/db0/test/_stream_load
     {
         "TxnId": 2504748,
         "Label": "label_1600997542287",
         "Status": "Success",
         "Message": "OK",
         "NumberTotalRows": 5,
         "NumberLoadedRows": 5,
         "NumberFilteredRows": 0,
         "NumberUnselectedRows": 0,
         "LoadBytes": 120,
         "LoadTimeMs": 46,
         "BeginTxnTimeMs": 0,
         "StreamLoadPutTimeMs": 1,
         "ReadDataTimeMs": 0,
         "WriteDataTimeMs": 29,
         "CommitAndPublishTimeMs": 14
     }
     ~~~

     * Broker Load 模式:

     ~~~sql
     LOAD LABEL test_db.label
      (
         DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
         INTO TABLE `test`
         COLUMNS TERMINATED BY ","
         (dt, id, user_id)
         SET (
           uv = HLL_HASH(user_id)
         )
      );
     ~~~

2. 查询数据

     HLL 列不允许直接查询它的原始值，可以用函数 HLL_UNION_AGG 进行查询
     * 求总 uv

     `SELECT HLL_UNION_AGG(uv) FROM test;`

     该语句等价于

     `SELECT COUNT(DISTINCT uv) FROM test;`

## 选择去重方案

如果数据集的基数在百万、千万量级，并拥有几十台机器，那么直接使用 count distinct 即可。如果基数在亿级以上，并且需要精确去重，那么只能用 Bitmap 类型；如果可以接受近似去重，那么还可以使用 HLL 类型。

Bitmap 仅支持 TINYINT，SMALLINT，INT，BIGINT（注意不支持 LARGEINT）去重。对于其他类型数据集去重，用户需要构建词典，将原类型映射到整数类型。词典构建较复杂，需要权衡数据量，更新频率，查询效率，存储等一系列问题。HLL 去重方式则无需构建词典，仅要求对应的数据类型支持哈希函数。即便在没有内部支持 HLL 的分析系统中，依然可以使用系统提供的 hash，用 SQL 实现 HLL 去重。

对于普通列，用户还可以使用 NDV 函数进行近似去重计算。NDV 函数返回值是 COUNT(DISTINCT col) 结果的近似值聚合函数，底层实现将数据存储类型转为 HyperLogLog 类型进行计算。但 NDV 函数在计算的时候比较消耗资源，不适合于并发高的场景。

如果应用场景为用户行为分析，建议使用 INTERSECT_COUNT 或者自定义 UDAF 去重。
