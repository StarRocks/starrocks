# 查询分析

性能优化是StarRocks集群管理员经常遇到的问题，慢查询不仅影响用户体验，而且影响整个集群的服务能力，所以定期针对慢查询进行分析并优化是一项非常重要的工作。

我们可以在fe/log/fe.audit.log中看到所有查询和慢查询信息，每个查询对应一个QueryID，我们可以在页面或者日志中查找到查询对应的QueryPlan和Profile，QueryPlan是FE通过解析SQL生成的执行计划，而Profile是BE执行后的结果，包含了每一步的耗时和数据处理量等数据，可以通过StarRocksManager的图形界面看到可视化的Profile执行树。<br />
同时，StarRocks还支持对慢查询中的SQL语句进行归类，并为各类SQL语句计算出SQL指纹（即MD5哈希值，对应字段为`Digest`）。

## Plan分析

SQL语句在StarRocks中的生命周期可以分为查询解析(Query Parsing)、规划(Query Plan)、执行(Query Execution)三个阶段。对于StarRocks而言，查询解析一般不会成为瓶颈，因为分析型需求的QPS普遍不高。

决定StarRocks中查询性能的关键就在于查询规划(Query Plan)和查询执行(Query Execution)，二者的关系可以用一句话描述：Query Plan负责组织算子(Join/Order/Aggregation)之间的关系，Query Exectuion负责执行具体算子。

Query Plan可以从宏观的角度提供给DBA一个视角，获取Query执行的相关信息。一个好的Query Plan很大程度上决定了查询的性能，所以DBA经常需要去查看Query Plan，确定Query Plan是否生成得当。本章以TPCDS的query96为例，展示如何查看StarRocks的Query Plan。

~~~ SQL
-- query96.sql
select  count(*)
from store_sales
    ,household_demographics
    ,time_dim
    , store
where ss_sold_time_sk = time_dim.t_time_sk
    and ss_hdemo_sk = household_demographics.hd_demo_sk
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 5
    and store.s_store_name = 'ese'
order by count(*) limit 100;
~~~

Query Plan可以分为逻辑执行计划(Logical Query Plan)，和物理执行计划(Physical Query Plan)，本章节所讲述的Query Plan默认指代的都是逻辑执行计划。StarRocks中运行EXPLAIN + SQL就可以得到SQL对应的Query Plan，TPCDS query96.sql对应的Query Plan展示如下。

~~~sql
+------------------------------------------------------------------------------+
| Explain String                                                               |
+------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                              |
|  OUTPUT EXPRS:<slot 11>                                                      |
|   PARTITION: UNPARTITIONED                                                   |
|   RESULT SINK                                                                |
|   12:MERGING-EXCHANGE                                                        |
|      limit: 100                                                              |
|      tuple ids: 5                                                            |
|                                                                              |
| PLAN FRAGMENT 1                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 12                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   8:TOP-N                                                                    |
|   |  order by: <slot 11> ASC                                                 |
|   |  offset: 0                                                               |
|   |  limit: 100                                                              |
|   |  tuple ids: 5                                                            |
|   |                                                                          |
|   7:AGGREGATE (update finalize)                                              |
|   |  output: count(*)                                                        |
|   |  group by:                                                               |
|   |  tuple ids: 4                                                            |
|   |                                                                          |
|   6:HASH JOIN                                                                |
|   |  join op: INNER JOIN (BROADCAST)                                         |
|   |  hash predicates:                                                        |
|   |  colocate: false, reason: left hash join node can not do colocate        |
|   |  equal join conjunct: `ss_store_sk` = `s_store_sk`                       |
|   |  tuple ids: 0 2 1 3                                                      |
|   |                                                                          |
|   |----11:EXCHANGE                                                           |
|   |       tuple ids: 3                                                       |
|   |                                                                          |
|   4:HASH JOIN                                                                |
|   |  join op: INNER JOIN (BROADCAST)                                         |
|   |  hash predicates:                                                        |
|   |  colocate: false, reason: left hash join node can not do colocate        |
|   |  equal join conjunct: `ss_hdemo_sk`=`household_demographics`.`hd_demo_sk`|
|   |  tuple ids: 0 2 1                                                        |
|   |                                                                          |
|   |----10:EXCHANGE                                                           |
|   |       tuple ids: 1                                                       |
|   |                                                                          |
|   2:HASH JOIN                                                                |
|   |  join op: INNER JOIN (BROADCAST)                                         |
|   |  hash predicates:                                                        |
|   |  colocate: false, reason: table not in same group                        |
|   |  equal join conjunct: `ss_sold_time_sk` = `time_dim`.`t_time_sk`         |
|   |  tuple ids: 0 2                                                          |
|   |                                                                          |
|   |----9:EXCHANGE                                                            |
|   |       tuple ids: 2                                                       |
|   |                                                                          |
|   0:OlapScanNode                                                             |
|      TABLE: store_sales                                                      |
|      PREAGGREGATION: OFF. Reason: `ss_sold_time_sk` is value column          |
|      partitions=1/1                                                          |
|      rollup: store_sales                                                     |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 0                                                            |
|                                                                              |
| PLAN FRAGMENT 2                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|                                                                              |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 11                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   5:OlapScanNode                                                             |
|      TABLE: store                                                            |
|      PREAGGREGATION: OFF. Reason: null                                       |
|      PREDICATES: `store`.`s_store_name` = 'ese'                              |
|      partitions=1/1                                                          |
|      rollup: store                                                           |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 3                                                            |
|                                                                              |
| PLAN FRAGMENT 3                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 10                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   3:OlapScanNode                                                             |
|      TABLE: household_demographics                                           |
|      PREAGGREGATION: OFF. Reason: null                                       |
|      PREDICATES: `household_demographics`.`hd_dep_count` = 5                 |
|      partitions=1/1                                                          |
|      rollup: household_demographics                                          |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 1                                                            |
|                                                                              |
| PLAN FRAGMENT 4                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 09                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   1:OlapScanNode                                                             |
|      TABLE: time_dim                                                         |
|      PREAGGREGATION: OFF. Reason: null                                       |
|      PREDICATES: `time_dim`.`t_hour` = 8, `time_dim`.`t_minute` >= 30        |
|      partitions=1/1                                                          |
|      rollup: time_dim                                                        |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 2                                                            |
+------------------------------------------------------------------------------+
128 rows in set (0.02 sec)
~~~

Query96展示的查询规划中，涉及多个StarRocks概念，理解此类概念对于理解查询规划至关重要，可以通过一个表格进行阐述。

|名称|解释|
|---|---|
|avgRowSize|扫描数据行的平均大小|
|cardinality|扫描表的数据总行数|
|colocate|表是否采用了Colocate形式|
|numNodes|扫描涉及的节点数|
|rollup|物化视图|
|preaggregation|预聚合|
|predicates|谓词，也就是查询过滤条件|
|partitions|分区|
|table|表|

Query96的Query Plan分为五个Plan Fragment，编号从0~4。阅读Query Plan可以采用自底向上的方式进行，逐个进行阅读。

上图中最底部的Plan Fragment为Fragment 4，它负责扫描time_dim表，并提前执行相关查询条件time_dim.t_hour = 8 and time_dim.t_minute >= 30，也就是大家所熟知的谓词下推。对于聚合表(Aggregate Key)，StarRocks会根据不同查询选择是否开启PREAGGREGATION，上图中time_dim的预聚合为关闭状态，关闭状态之下会读取time_dim的全部维度列，当表中维度列多的时候，这个可能会成为影响性能的一个关键因素。如果time_dim表有选择Range Partition进行数据划分，Query Plan中的partitions会表征查询命中几个分区，无关分区被自动过滤会有效减少扫描数据量。如果有物化视图，StarRocks会根据查询去自动选择物化视图，如果没有物化视图，那么查询自动命中base table，也就是上图中展示的rollup: time_dim。其他字段可以暂时不用关注。

当time_dim数据扫描完成之后，Fragment 4的执行过程也就随之结束，此时它将扫描得到的数据传递给其他Fragment，上图中的EXCHANGE ID : 09表征了数据传递给了标号为9的接收节点。

对于Query96的Query Plan而言，Fragment 2，3，4功能类似，只是负责扫描的表不同。具体到查询中的Order/Aggregation/Join算子，都在Fragment 1中进行，下面着重介绍Fragment 1。

Fragment 1集成了三个Join算子的执行，采用默认的BROADCAST方式进行执行，也就是小表向大表广播的方式进行，如果两个Join的表都是大表，建议采用SHUFFLE的方式进行。目前StarRocks只支持HASH JOIN，也就是采用哈希算法进行Join。图中有一个colocate字段，这个用来表述两张Join表采用同样的分区/分桶方式，如此，Join的过程可以直接在本地执行，不用进行数据的移动。Join执行完成之后，就是执行上层的Aggregation、Order by和TOP-N的算子，Query96的上述算子都比较浅显易懂。

至此，关于Query96的Query Plan的解释就告一段落，去掉具体的表达式，只保留算子的话，Query Plan可以以一个更加宏观的角度展示，就是下图。

![8-5](../assets/8-5.png)

## Profile分析

在理解了Plan的作用以后，我们来分析以下BE的执行结果Profile，通过在StarRocksManager中执行查询，点击查询历史，就可看在“执行详情”tab中看到Profile的详细文本信息，在“执行时间”tab中能看到图形化的展示，这里我们采用TPCH的Q4查询来作为例子

~~~sql
-- TPCH Q4
select  o_orderpriority,  count(*) as order_count
from  orders
where
  o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and exists (
    select  * from  lineitem
    where  l_orderkey = o_orderkey and l_commitdate < l_receiptdate
  )
group by o_orderpriority
order by o_orderpriority;
~~~

可以看到这是一个包含了相关子查询，group by，ordery by和count的查询，其中order是订单表，lineitem是货品明细表，这两张都是比较大的事实表，这个查询的含义是按照订单的优先级分组，统计每个分组的订单数量，同时有两个过滤条件：

* 订单创建时间是1993年7月 到1993年10月之间
* 这个订单对应的产品的提交日期(l_commitdate)小于收货日期（l_receiptadate）

执行Query后我们能看到 ：

![8-6](../assets/8-6.png)
  
左上角我们能看到整个查询执行了3.106s，点击每个节点可以看到每一部分的执行信息，Active表示这个节点（包含其所有子节点）的时间，整体结构上可以看到最下面的子节点是两个scan node，他们分别scan了 573万和3793万数据，然后进行了一次shuffle join，完成后输出525万条数据，然后经过两层聚合，最后通过一个Sort Node后输出结果，其中Exchange node是数据交换节点，在这个case中是进行了两次Shuffle。

一般分析Profile的核心就是找到执行时间最长的性能瓶颈所在的节点，比如我们可以从上往下依次查看，可以看到这个Hash Join Node占了主要时间：

~~~sql
HASH_JOIN_NODE (id=2):(Active: 3s85ms, % non-child: 34.59%)
- AvgInputProbeChunkSize: 3.653K (3653)
- AvgOutputChunkSize: 3.57K (3570)
- BuildBuckets: 67.108864M (67108864)
- BuildRows: 31.611425M (31611425)
- BuildTime: 3s56ms
    - 1-FetchRightTableTime: 2s14ms
    - 2-CopyRightTableChunkTime: 0ns
    - 3-BuildHashTableTime: 1s19ms
    - 4-BuildPushDownExprTime: 0ns
- PeakMemoryUsage: 376.59 MB
- ProbeRows: 478.599K (478599)
- ProbeTime: 28.820ms
    - 1-FetchLeftTableTimer: 369.315us
    - 2-SearchHashTableTimer: 19.252ms
    - 3-OutputBuildColumnTimer: 893.29us
    - 4-OutputProbeColumnTimer: 7.612ms
    - 5-OutputTupleColumnTimer: 34.593us
- PushDownExprNum: 0
- RowsReturned: 439.16K (439160)
- RowsReturnedRate: 142.323K /sec
~~~

从这里信息可以看到 hash join的执行主要分成两部分时间，也就是BuildTime和ProbeTime，BuildTime是扫描右表并构建hash表的过程，ProbeTime是获取左表并搜索hashtable进行匹配并输出的过程。可以明显的看出这个节点的时间大部分花在了BuildTime的FetchRightTableTime和BuildHashTableTime，对比刚才的数据Scan行数数据，我们意识到这个查询的左表和右表的顺序其实不理想，应该把左表设置为大表，右表Build hash table会效果更好，而且对于这两个表都是事实表，数据都比较多，我们也可以考虑采用 colocate Join来避免 最下面的数据shuffle，同时减少Join的数据量，于是我们参考[“Colocate join”](../using_starrocks/Colocation_join.md)建立了colocation关系后，并且改写SQL如下：

~~~sql
with t1 as (
    select l_orderkey from  lineitem
    where  l_commitdate < l_receiptdate
) select o_orderpriority,  count(*)as order_count from t1 right semi join orders_co  on l_orderkey = o_orderkey 
    where o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
group by o_orderpriority
order by o_orderpriority;
~~~

执行结果：

![8-7](../assets/8-7.png)
  
新的SQL执行从3.106s降低到了1.042s，可以明显看到两张大表没有了Exchange节点，直接通过Colocated Join进行，而且左右表顺序调换了以后整体性能有了大幅提升，新的Join Node信息如下：

~~~sql
HASH_JOIN_NODE (id=2):(Active: 996.337ms, % non-child: 52.05%)
- AvgInputProbeChunkSize: 2.588K (2588)
- AvgOutputChunkSize: 35
- BuildBuckets: 1.048576M (1048576)
- BuildRows: 478.171K (478171)
- BuildTime: 187.794ms
    - 1-FetchRightTableTime: 175.810ms
    - 2-CopyRightTableChunkTime: 5.942ms
    - 3-BuildHashTableTime: 5.811ms
    - 4-BuildPushDownExprTime: 0ns
- PeakMemoryUsage: 22.38 MB
- ProbeRows: 31.609786M (31609786)
- ProbeTime: 807.406ms
    - 1-FetchLeftTableTimer: 282.257ms
    - 2-SearchHashTableTimer: 457.235ms
    - 3-OutputBuildColumnTimer: 26.135ms
    - 4-OutputProbeColumnTimer: 16.138ms
    - 5-OutputTupleColumnTimer: 1.127ms
- PushDownExprNum: 0
- RowsReturned: 438.502K (438502)
- RowsReturnedRate: 440.114K /sec
~~~

## SQL指纹

StarRocks支持规范化慢查询（ 路径`fe.audit.log.slow_query` ）中 SQL 语句，并进行归类。然后针对各个类型的SQL语句，计算出其的MD5 哈希值，对应字段为 `Digest`。

~~~test
2021-12-27 15:13:39,108 [slow_query] |Client=172.26.xx.xxx:54956|User=root|Db=default_cluster:test|State=EOF|Time=2469|ScanBytes=0|ScanRows=0|ReturnRows=6|StmtId=3|QueryId=824d8dc0-66e4-11ec-9fdc-00163e04d4c2|IsQuery=true|feIp=172.26.92.195|Stmt=select count(*) from test_basic group by id_bigint|Digest=51390da6b57461f571f0712d527320f4
~~~

SQL语句规范化仅保留重要的语法结构：

* 保留对象标识符，如数据库和表的名称。
* 转换常量为?。
* 删除注释并调整空格。

如下两个SQL语句，规范化后属于同一类SQL。

~~~sql
SELECT * FROM orders WHERE customer_id=10 AND quantity>20

SELECT * FROM orders WHERE customer_id = 20 AND quantity > 100
~~~

规范化后如下类型的SQL。

~~~sql
SELECT * FROM orders WHERE customer_id=? AND quantity>?
~~~
