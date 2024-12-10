# Query Feedback
本文介绍 StarRocks Query Feedback的基本概念和使用场景，以及如何使用Query Feedback来优化查询计划。
StarRocks 3.4 版本开始引入Query Feedback.

## 什么是 Query Feedback

Query Feedback是一个框架，属于CBO 优化器的重要组成部分。它允许记录查询执行过程中的统计信息，以便在类似计划的未来查询中重用这些统计信息。其主要目的是协助CBO优化器产生最佳的查询计划。当前CBO 优化器主要是使用估算的统计信息对Plan进行优化，由于统计信息更新不及时或者统计信息估算错误，CBO 优化器选出的Plan可能会非常差，比如会把一张大表进行broadcast或者左右表顺序混乱，这些bad plan导致执行时间超时，占用大量系统资源。如果优化器不能根据执行详情的反馈来调整执行计划，大量Bad Plan的执行可能会导致系统崩溃。

## Query Feedback简要流程

针对执行过程中的慢SQL启动反馈调优，主要分为三个阶段
- 观测阶段: BE或CN记录每个执行计划PlanNode的input/output rows等指标。
  
- 分析阶段: 在任务结束后，返回结果前，只针对慢SQL和手动设置了analyze标记的SQL进行分析。FE将执行计划和实际执行详情进行对比分析，确认是否是Plan异常导致的执行慢。当前大部分问题是统计信息估算不准确带来的，在分析统计信息不准问题的同时会为每条SQL生成对应的SQL Tuning Guides用于提示优化器进行动态调优以及告诉用户推荐执行的调优操作。
  
- 动态调整阶段: 在CBO 优化器生成最佳的物理计划后，可以根据当前执行计划来寻找其是否存在已经生成的Tuning Guides. 如果存在，则根据Tuning Guides和相关策略对计划进行局部调优。

## Query Feedback 使用方式
上述Query Feedback的观测和动态调整阶段对用户无感知，用户可以控制的是分析阶段。当前提供手动分析和自动分析两种模式。
两种模式生效的前提是Session Variable `enable_plan_advisor` 为true. 

### 手动 Plan Analyze
用户可以通过执行SQL来对某个Query进行强制分析。
```
ALTER PLAN ADVISOR ADD <query_statement>
```
如果Query是一个明显的Bad Case，命中了调优策略同时生成了Tuning Guide. 通过Command
```
SHOW PLAN ADVISOR
```
可以查看当前FE对所有SQL的调优详情。
也可以再次执行`EXPLAIN query`来观察相同的Query是否已经被优化。

```

mysql> explain  select count(*) from (select * from c1_skew_left_over t1 join (select * from c1_skew_left_over where c1 = 'c') t2 on t1.c2 = t2.c2 where t1.c1 > 'c' ) t;
+-----------------------------------------------------------------------------------------------+
| Explain String                                                                                |
+-----------------------------------------------------------------------------------------------+
| Plan had been tuned by Plan Advisor.                                                          |
| Original query id:8e010cf4-b178-11ef-8aa4-8a5075cec65e                                        |
| Original time cost: 148 ms                                                                    |
| 1: LeftChildEstimationErrorTuningGuide                                                        |
| Reason: left child statistics of JoinNode 5 had been overestimated.                           |
| Advice: Adjust the distribution join execution type and join plan to improve the performance. |
|                                                                                               |
| PLAN FRAGMENT 0                                                                               |
|  OUTPUT EXPRS:9: count                                                                        |
|   PARTITION: UNPARTITIONED                                           
```
上述为调优后的Query执行`EXPLAIN`的部分信息。

### 自动 Plan Analyze
如果开启session variable `enable_plan_advisor` (默认值为FALSE), 则会对每个Query进行Plan Analyze.
或者Query超过了FE Config `slow_query_analyze_threshold`(默认值为5秒), 也会对该Query进行Plan Analyze.
自动 Plan Analyze也可通过上述观测方式来查看是否某个Query已经被调优。

## Query Feedback 应用场景
当前Query Feedback主要可以优化以下三种场景:

- 优化局部Join节点的左右顺序
- 优化局部Join节点的执行方式 (eg: broadcast -> shuffle)
- 对于聚合效果较好的Case，强制一阶段pre_aggregation. 尽可能在一阶段多聚合数据

当前Query Feedback的调优策略主要依据指标是runtime exec node input/output rows和FE statistics estimated rows.
当前的调优策略阈值相对保守，如果用户从Explain或者Query Profile中发现以上三种明显错误，可以尝试Query Feedback来观察是否可以获得正向优化效果。

以下列举三种用户常见Case.

### Case 1: 错误的Join顺序
Original Bad Plan:
`small left table inner join large table(broadcast)`

Rewrite to:
`large left table inner join small right table(broadcast)`

造成Bad Plan的原因可能是统计信息未搜集或者更新不及时，CBO 优化器根据Statistics可能生成了一个这样错误的Plan.
通过执行期间搜集的Left和Right Child input/output和statistics estimated rows对比，会对其生成Tuning Guides. 再次执行该SQL，会对其校正Join Order.


### Case 2: 错误的Join执行方式
Original Bad Plan:
`large left table1 inner join large right table2(broadcast)`

Rewrite to:
`large left table1(shuffle) inner join large right table2(shuffle)`

造成Bad Plan的原因可能是数据存在倾斜，表中存在很多分区，但是某一天的分区数据特别多，造成应用predicate后右表行数估算错误。
通过执行期间搜集的Left和Right Child input/output和statistics estimated rows对比，会对其生成Tuning Guides. 再次执行该SQL，会对其校正Join执行方式，由Broadcast Join改为Shuffle Join.

### Case 3: 一阶段聚合效果差
如果用户数据的聚合效果比较好，但是对于SQL, 一阶段agg auto模式下,只聚合了少量本地数据。
通过执行期间搜集的local/global agg的input/output, 根据调优策略，优化后经过历史信息判断聚合列具有很好的聚合效果, 强制local agg使用pre_aggregation模式, 尽可能在一阶段多聚合数据。

## Query Feedback管理方式

### 强制分析Query
用户可以通过强制分析某个Query然后通过观测方法判断Query是否可以被执行反馈进行调优。
```SQL
ALTER PLAN ADVISOR ADD <query_statement>
```

**示例**

```SQL
ALTER PLAN ADVISOR ADD SELECT COUNT(*) from (SELECT * FROM TEST_TABLE t1 join (select * from TEST_TABLE where c1 = 'c') t2 ON t1.c2 = t2.c2 WHERE t1.c1 > 'c' ) t
```

### 清除某个Query的tuning guide
用户可以通过query id来清理FE中存储对于该Query的调优信息

```SQL
ALTER PLAN ADVISOR DROP <query_id>
```

**示例**

```SQL
ALTER PLAN ADVISOR DROP "8e010cf4-b178-11ef-8aa4-8a5075cec65e"

```

### 清理已经生成的所有tuning guide

```SQL
TRUNCATE PLAN ADVISOR
```

**示例**

```SQL
TRUNCATE PLAN ADVISOR
```

### 展示当前FE节点保存的Plan Advisor

```SQL
SHOW PLAN ADVISOR
```

**示例**

```SQL
SHOW PLAN ADVISOR
```

## 限制
1. 针对Query的调优信息是存储在FE内存的无状态信息。FE各自管理自己Plan调优信息, 没有实现多机的同步功能。如果存在多个FE, 一条SQL发送到不同FE可能会有不同的tuning结果。
2. 当前只能对重复执行的查询、同样的plan pattern应用之前生成的调优信息。不能够将tuning guide泛化应用到对同样pattern, 不同参数的SQL上。
3. Query Feedback调优信息使用了一个内存中的cache结构, 当cache key超过配置数量时, 会自动淘汰过期的cache. 默认cache key的数量是固定的300个。没有对历史的调优信息进行持久化。