---
displayed_sidebar: "Chinese"
---

# 使用 HyperLogLog 实现近似去重

本文介绍如何通过 HLL（HyperLogLog）在 StarRocks 中实现近似去重。

HLL 是一种近似去重算法，在部分对去重精度要求不高的场景下，您可以选择使用 HLL 算法减轻数据去重分析的计算压力。根据数据集大小以及所采用的哈希函数的类型，HLL 算法的误差可控制在 1% 至 10% 左右。

## 创建包含 HLL 列的表

使用 HLL 去重，需要在建表语句中，将目标指标列的类型设置为 **HLL**，聚合函数设置为 **HLL_UNION**。只有聚合表支持 HLL 类型列。

> 说明
>
> 您无需向 HLL 列导入数据。HLL 列的数据将根据您指定的 `HLL_HASH` 函数基于导入的数据自动生成。导入数据时，该函数将自动根据指定的列生成 HLL 列。HLL 算法常用于替代 `count distinct`，通过结合物化视图在业务上用于快速计算 uv。

以下示例创建 `test` 表，其中包含 DATE 数据类型列 `dt`，INT 数据类型列 `id`，以及 HLL 类型列 `uv`，其使用的 `HLL_HASH` 函数为 `HLL_UNION`。

~~~sql
CREATE TABLE test(
        dt DATE,
        id INT,
        uv HLL HLL_UNION
)
DISTRIBUTED BY HASH(ID);
~~~

> 注意
>
> * 当前版本中, 仅聚合表支持 HLL 类型的指标列。
> * 当数据量较大时，建议为高频率的 HLL 查询建立对应的物化视图。

## 导入数据

创建数据文件 **test.csv** 并将其导入先前创建的表 `test`。

当前示例使用以下原始数据，其 10 行数据中有 3 行数据重复。

~~~plain text
2022-03-10,0
2022-03-11,1
2022-03-12,2
2022-03-13,3
2022-03-14,4
2022-03-15,5
2022-03-16,6
2022-03-14,4
2022-03-15,5
2022-03-16,6
~~~

您可以通过 Stream Load 或者 Broker Load 模式导入 **test.csv**。

* [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md) 模式:

~~~bash
curl --location-trusted -u <username>:<password> -H "label:987654321" -H "column_separator:," -H "columns:dt,id,uv=hll_hash(id)" -T test.csv http://fe_host:http_port/api/db_name/test/_stream_load
~~~

* [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 模式:

~~~sql
LOAD LABEL test_db.label
     (
     DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
     INTO TABLE `test`
     COLUMNS TERMINATED BY ","
     (dt, id, uv)
     SET (
          uv = HLL_HASH(id)
     )
     );
~~~

## 通过 HLL 聚合数据

您可以通过以下三种方式聚合数据加速查询。

* 基于示例表创建物化视图，使 HLL 列产生聚合。

~~~sql
ALTER TABLE test ADD ROLLUP test_rollup(dt, uv);
~~~

* 创建针对 HLL 列计算的新表，并插入原示例表中的相关数据。

~~~sql
create table test_uv1(
id int,
uv_set hll hll_union)
distributed by hash(id);

insert into test_uv1 select id, uv from test;
~~~

* 创建针对 HLL 列计算的新表，并插入通过 `HLL_HASH` 基于原示例表中相关数据生成的 HLL 列。

~~~sql
create table test_uv2(
id int,
uv_set hll hll_union)
distributed by hash(id);

insert into test_uv2 select id, hll_hash(id) from test;
~~~

## 查询数据

HLL 列不支持直接查询原始值。您可以通过函数 `HLL_UNION_AGG` 进行查询。

~~~sql
SELECT HLL_UNION_AGG(uv) FROM test;
~~~

返回如下：

~~~plain text
+---------------------+
| hll_union_agg(`uv`) |
+---------------------+
|                   7 |
+---------------------+
~~~

当在 HLL 类型列上使用 `count distinct` 时，StarRocks 会自动将其转化为 [HLL_UNION_AGG(hll)](../sql-reference/sql-functions/aggregate-functions/hll_union_agg.md) 计算。所以以上查询等价于以下查询。

~~~sql
SELECT COUNT(DISTINCT uv) FROM test;
~~~

返回如下：

~~~plain text
+----------------------+
| count(DISTINCT `uv`) |
+----------------------+
|                    7 |
+----------------------+
~~~

## 选择去重方案

如果您的数据集基数在百万、千万量级，并拥有几十台机器，那么您可以直接使用 `count distinct` 方式。如果您的数据集基数在亿级以上，并且需要精确去重，那么您需要使用 [Bitmap 去重](./Using_bitmap.md#基于-trie-树构建全局字典)。如果您选择近似去重，那么可以使用 HLL 类型去重。

Bitmap 类型仅支持 TINYINT，SMALLINT，INT，BIGINT（注意不支持 LARGEINT）去重。对于其他类型数据集去重，您需要[构建词典](./Using_bitmap.md#基于-trie-树构建全局字典)，将原类型映射到整数类型。HLL 去重方式则无需构建词典，仅要求对应的数据类型支持哈希函数。

对于普通列，您还可以使用 `NDV` 函数进行近似去重计算。`NDV` 函数返回值是 `COUNT(DISTINCT col)` 结果的近似值聚合函数，底层实现将数据存储类型转为 HLL 类型进行计算。但 `NDV` 函数在计算的时候消耗资源较大，不适合于并发高的场景。

如果您的应用场景为用户行为分析，建议使用 `INTERSECT_COUNT` 或者自定义 UDAF 去重。

## 相关函数

* **[HLL_UNION_AGG(hll)](../sql-reference/sql-functions/aggregate-functions/hll_union_agg.md)**：此函数为聚合函数，用于计算满足条件的所有数据的基数估算。此函数还可用于分析函数，只支持默认窗口，不支持窗口子句。
* **[HLL_RAW_AGG(hll)](../sql-reference/sql-functions/aggregate-functions/hll_raw_agg.md)**：此函数为聚合函数，用于聚合 HLL 类型字段，返回 HLL 类型。
* **[HLL_CARDINALITY(hll)](../sql-reference/sql-functions/scalar-functions/hll_cardinality.md)**：此函数用于估算单条 HLL 列的基数。
* **[HLL_HASH(column_name](../sql-reference/sql-functions/aggregate-functions/hll_hash.md)**：生成 HLL 列类型，用于 `insert` 或导入 HLL 类型。
* **[HLL_EMPTY()](../sql-reference/sql-functions/aggregate-functions/hll_empty.md)**：生成空 HLL 列，用于 `insert` 或导入数据时补充默认值。
