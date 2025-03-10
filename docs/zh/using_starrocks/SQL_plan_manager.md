---
displayed_sidebar: docs
sidebar_position: 11
---

# [Preview] SQL Plan Manager

本文介绍 SQL Plan Manager 功能的基本概念和使用场景，以及如何利用 SQL Plan Manager 固定查询计划（Query Plan）。

StarRocks 自 v3.5.0 起支持 SQL Plan Manager 功能。

## 概述

SQL Plan Manager 允许用户对查询绑定查询计划，从而避免查询计划随着系统状态变化（主要是数据更新，统计信息更新）而变化，从而稳定查询性能。

## 工作流程

SQL Plan Manager 需要预先指定绑定的查询SQL以及使用的查询计划（Baseline），其中查询是指用户实际执行的查询SQL，查询计划是通过手动优化/添加hints方式的查询SQL。

SQL Plan Manager 的工作流程包括添加Baseline，以及使用：

1. **创建Baseline**：通过命令`CREATE BASELINE`为指定的查询SQL绑定查询计划。
2. **查询改写**：对发送到 StarRocks 的查询，会自动和 SQL Plan Manager 中存储的 Baseline 进行匹配，如果匹配成功，则使用 Baseline 的查询计划。

在创建Baseline时，需要注意：
* Baseline中的绑定SQL和执行计划SQL需要保证逻辑一致，StarRocks 会做简单的表/参数检查，但是无法保证能检查出两者逻辑是否一致，所以需要使用者自行保证执行计划的逻辑正确性。
* Baseline中的绑定SQL，默认存储绑定SQL的SQL指纹，默认会将SQL中的常量值替换为可变参数（例如 `t1.v1 > 1000` 变为 `t1.v1 > ?`），以提高SQL的匹配度。
* Baseline中绑定的执行计划中，除了可以自行修改SQL逻辑，也可以通过添加Hints（Join Hints/Set Var）的方式，来保证生成要求的执行计划。
* Baseline中的绑定SQL和执行计划，可能由于SQL复杂度较高，StarRocks无法自动完成SQL和执行计划绑定，可以通过手动的方式完成绑定，具体可以参考[进阶使用]部分
 
查询改写时，需要注意：
* SQL Plan Manager 主要依赖SQL指纹的方式进行匹配，会检查查询的SQL指纹是否有匹配的Baseline，如果匹配成功，会自动将查询中的参数中的参数代入Baseline的执行计划中。
* SQL Plan Manager 在匹配过程中，如果发现查询能够匹配多个Baseline，则会使优化器评估最优的Baseline。
* SQL Plan Manager 在匹配过程中，会校验baseline和查询是否匹配，如果匹配失败，则不会使用Baseline的查询计划。
* 通过 SQL Plan Manager 查询改写的执行计划，在 `explain` 语句中，会看到 `Using baseline plan[id]`。

所以这里分成两部分进行介绍。

## 创建 Baseline
### 语法

#### 创建 Baseline
```antlrv4
CREATE (GLOBAL)? BASELINE (ON querySQL)? USING planSQL (properties(....))?
```

样例：
```antlrv4
-- 创建Session级别的BASELINE，其直接绑定SQL，并使用当前其对应的查询计划SQL
CREATE BASELINE USING select t1.v2, t2.v3 from t1 join t2 on t1.v2 = t2.v2 where t1.v2 > 100;

-- 创建Global级别的BASELINE，其直接绑定SQL，并使用当前其对应的查询计划SQL，其中指定了 Join 的Hints
CREATE GLOBAL BASELINE USING select t1.v2, t2.v3 from t1 join[broadcast] t2 on t1.v2 = t2.v2 where t1.v2 > 100;

-- 创建Session级别的BASELINE，并指定了需要绑定SQL，以及对应的查询计划SQL
CREATE BASELINE ON select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v2 > 100 USING select t1.v2, t2.v3 from t1 join[broadcast] t2 on t1.v2 = t2.v2 where t1.v2 > 100;
```

#### 删除 Baseline
```antlrv4
DROP BASELINE id;
```

样例：
```antlrv4
-- 140035 为 Baseline 的 id
DROP BASELINE 140035；
```

#### 展示 Baseline
```antlrv4
SHOW BASELINE;
```

## 查询改写

通过`set enable_sql_plan_manager_rewrite = true;` 打开 SQL Plan Manager 查询改写功能。在绑定执行计划后，StarRocks 会自动将对应的查询改写成对应的查询计划。

样例：

原始SQL以及Plan：
```antlrv4
MySQL td> explain select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 20
+-----------------------------------------+
| Explain String                          |
+-----------------------------------------+
| PLAN FRAGMENT 0                         |
|  OUTPUT EXPRS:2: v2 | 5: v3             |
|   PARTITION: UNPARTITIONED              |
|                                         |
|   RESULT SINK                           |
|                                         |
|   7:EXCHANGE                            |
|                                         |
| PLAN FRAGMENT 1                         |
|  OUTPUT EXPRS:                          |
|   PARTITION: HASH_PARTITIONED: 4: v2    |
|                                         |
|   STREAM DATA SINK                      |
|     EXCHANGE ID: 07                     |
|     UNPARTITIONED                       |
|                                         |
|   6:Project                             |
|   |  <slot 2> : 2: v2                   |
|   |  <slot 5> : 5: v3                   |
|   |                                     |
|   5:HASH JOIN                           |
|   |  join op: INNER JOIN (PARTITIONED)  |
|   |  colocate: false, reason:           |
|   |  equal join conjunct: 4: v2 = 2: v2 |
|   |                                     |
|   |----4:EXCHANGE                       |
|   |                                     |
|   1:EXCHANGE                            |
......
```


创建 Baseline：
```antlrv4
MySQL td> show baseline\G;
0 rows in set
Time: 0.010s
MySQL td> create global baseline on select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 1000 using select t1.v2, 
t2.v3 from t1 join[broadcast] t2 on t1.v2 = t2.v2 where t1.v1 > 1000;
Query OK, 0 rows affected
Time: 0.062s
MySQL td> show baseline\G;
***************************[ 1. row ]***************************
Id            | 435269
global        | Y
bindSQLDigest | SELECT `td`.`t1`.`v2`, `td`.`t2`.`v3`
FROM `td`.`t1` , `td`.`t2` 
WHERE (`td`.`t1`.`v2` = `td`.`t2`.`v2`) AND (`td`.`t1`.`v1` > ?)
bindSQLHash   | 1085294
bindSQL       | SELECT `td`.`t1`.`v2`, `td`.`t2`.`v3`
FROM `td`.`t1` , `td`.`t2` 
WHERE (`td`.`t1`.`v2` = `td`.`t2`.`v2`) AND (`td`.`t1`.`v1` > _spm_const_var(1))
planSQL       | SELECT c_2, c_5 FROM (SELECT t_0.v2 AS c_2, t2.v3 AS c_5 FROM (SELECT v2 FROM t1 WHERE v1 > _spm_const_var(1)) t_0 INNER JOIN[BROADCAST] t2 ON t_0.v2 = t2.v2) t2
costs         | 263.0
updateTime    | 2025-03-10 16:01:50
1 row in set
Time: 0.013s
```


查询改写：
```antlrv4
MySQL td> show session variables like "%enable_sql_plan_manager_rewrite%";
+---------------------------------+-------+
| Variable_name                   | Value |
+---------------------------------+-------+
| enable_sql_plan_manager_rewrite | false |
+---------------------------------+-------+
1 row in set
Time: 0.006s
MySQL td> set enable_sql_plan_manager_rewrite = true;
Query OK, 0 rows affected
Time: 0.002s
MySQL td> explain select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 20
+-----------------------------------------+
| Explain String                          |
+-----------------------------------------+
| Using baseline plan[435269]             |
|                                         |
| PLAN FRAGMENT 0                         |
|  OUTPUT EXPRS:2: v2 | 5: v3             |
|   PARTITION: UNPARTITIONED              |
|                                         |
|   RESULT SINK                           |
|                                         |
|   6:EXCHANGE                            |
|                                         |
| PLAN FRAGMENT 1                         |
|  OUTPUT EXPRS:                          |
|   PARTITION: RANDOM                     |
|                                         |
|   STREAM DATA SINK                      |
|     EXCHANGE ID: 06                     |
|     UNPARTITIONED                       |
|                                         |
|   5:Project                             |
|   |  <slot 2> : 2: v2                   |
|   |  <slot 5> : 5: v3                   |
|   |                                     |
|   4:HASH JOIN                           |
|   |  join op: INNER JOIN (BROADCAST)    |
|   |  colocate: false, reason:           |
|   |  equal join conjunct: 2: v2 = 4: v2 |
|   |                                     |
|   |----3:EXCHANGE                       |
|   |                                     |
|   1:Project                             |
|   |  <slot 2> : 2: v2                   |
|   |                                     |
|   0:OlapScanNode                        |
|      TABLE: t1                          |
.......
```

## 高级功能
对于以下场景，可以尝试使用手动绑定查询计划的方式：
* 对于比较复杂的SQL，SQL Plan Manager无法自动绑定上SQL和查询计划
* 对于一些特殊场景（固定参数/有条件参数），自动绑定无法满足需求

相比自动绑定，手动绑定的灵活性更强，但需要了解一部分SQL Plan Manager的执行机制。

### 执行逻辑
#### Baseline 创建流程
1. 执行`CREATE BASELINE`，获得需要绑定的BindSQL和查询计划PlanSQL
2. 参数化BindSQL：将字面值/表达式替换为SPM函数，例如 `id > 200` 替换为 `id > _spm_const_var(0)`，其中参数`0`是占位符ID，用于确认表达式的在BindSQL和PlanSQL中的位置。
3. 在PlanSQL中绑定占位符：找到占位符应在PlanSQL中的位置，并替换原始表达式。
4. 使用优化器优化PlanSQL，并获取查询计划。
5. 将查询计划序列化为携带Hints的SQL。 
6. 保存baseline（BindSQL的SQL指纹，优化后的执行计划SQL）。

#### 查询改写流程
查询改写的逻辑类似于PrepareStatement。
1. 执行查询。
2. 将查询规范化为SQL指纹。
3. 通过SQL指纹查找Baseline（与Baseline的BindSQL匹配）。 
4. 绑定查询与Baseline，检查查询是否与Baseline的BindSQL相同，并且通过BindSQL中的SPM函数提取查询的对应的参数值。例如：查询中`id > 1000`绑定到BindSQL中的`id > _spm_const_var(0)`，提取得到`_spm_const_var(0) = 1000`。
5. 在Baseline的PlanSQL中替换SPM参数
6. 返回PlanSQL以替换原始查询

### SPMFunction
SPMFunction 是SQL Plan Manager中的占位符函数，其作用有两个：
* 用于标记参数化SQL中的表达式，后续流程中的做参数提取&替换。
* 用于检查参数条件，通过参数条件将不同参数的SQL映射到不同的查询计划上。

当前 StarRocks 内部支持的 SPMFunction 有：
- `_spm_const_var`：用于标记单个常量值
- `_spm_const_list`：用于标记多个常量值，通常用于标记 IN 条件中的多个常量值

未来会继续添加新的 SPMFunction，提供例如 `_spm_const_range`、`_spm_const_enum` 等带条件参数的占位符函数。

### 手动绑定
我们可以通过直接使用SPMFunction，完成更复杂的SQL绑定。

例如，我们绑定的SQL如下：
```antlrv4
with ss as (
    select i_item_id, sum(ss_ext_sales_price) total_sales
    from store_sales, item
    where i_color in ('slate', 'blanched', 'burnished') and ss_item_sk = i_item_sk
    group by i_item_id
),
cs as (
    select i_item_id, sum(cs_ext_sales_price) total_sales
    from catalog_sales, item
    where i_color in ('slate', 'blanched', 'burnished') and cs_item_sk = i_item_sk
    group by i_item_id
)
select i_item_id, sum(total_sales) total_sales
from (  select * from ss
        union all
        select * from cs) tmp1
group by i_item_id;
```

其中由于`i_color in ('slate', 'blanched', 'burnished')`中常量值都是相同的，所以SQL会被识别为：
```antlrv4
with ss as (
    select i_item_id, sum(ss_ext_sales_price) total_sales
    from store_sales, item
    where i_color IN (_spm_const_list(1)) and ss_item_sk = i_item_sk
    group by i_item_id
),
cs as (
    select i_item_id, sum(cs_ext_sales_price) total_sales
    from catalog_sales, item
    where i_color IN (_spm_const_list(1)) and cs_item_sk = i_item_sk
    group by i_item_id
)
select i_item_id, sum(total_sales) total_sales
from (  select * from ss
        union all
        select * from cs) tmp1
group by i_item_id;
```

即将两个`i_color in ('xxx', 'xxx')`都识别为同一个参数，这样当分别使用不同参数的SQL时，SQL Plan Manager就无法识别，所以这时候我们可以手动指定BindSQL和PlanSQL中的参数：
```antlrv4
with ss as (
    select i_item_id, sum(ss_ext_sales_price) total_sales
    from store_sales, item
    where i_color IN (_spm_const_list(1)) and ss_item_sk = i_item_sk
    group by i_item_id
),
cs as (
    select i_item_id, sum(cs_ext_sales_price) total_sales
    from catalog_sales, item
    where i_color IN (_spm_const_list(2)) and cs_item_sk = i_item_sk
    group by i_item_id
)
select i_item_id, sum(total_sales) total_sales
from (  select * from ss
        union all
        select * from cs) tmp1
group by i_item_id;
```

 查询改写时：
```antlrv4
MySQL tpcds> show baseline\G;
***************************[ 1. row ]***************************
Id            | 436115
global        | N
bindSQL       | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales`
FROM `tpcds`.`store_sales` , `tpcds`.`item` 
WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`)
GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales`
FROM `tpcds`.`catalog_sales` , `tpcds`.`item` 
WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(2))) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`)
GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales`
FROM (SELECT *
FROM `ss` UNION ALL SELECT *
FROM `cs`) `tmp1`
GROUP BY `tmp1`.`i_item_id`
.......
***************************[ 2. row ]***************************
Id            | 436119
global        | N
bindSQL       | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales`
FROM `tpcds`.`store_sales` , `tpcds`.`item` 
WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`)
GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales`
FROM `tpcds`.`catalog_sales` , `tpcds`.`item` 
WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`)
GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales`
FROM (SELECT *
FROM `ss` UNION ALL SELECT *
FROM `cs`) `tmp1`
GROUP BY `tmp1`.`i_item_id`
.......
2 rows in set
Time: 0.011s
MySQL tpcds>
MySQL tpcds> explain with ss as (
    select i_item_id, sum(ss_ext_sales_price) total_sales
    from store_sales, item
    where i_color IN ("a", "b", "c") and ss_item_sk = i_item_sk
    group by i_item_id
),
cs as (
    select i_item_id, sum(cs_ext_sales_price) total_sales
    from catalog_sales, item
    where i_color IN ("A", "B", "D") and cs_item_sk = i_item_sk
    group by i_item_id
)
select i_item_id, sum(total_sales) total_sales
from (  select * from ss
        union all
        select * from cs) tmp1
group by i_item_id;
+-------------------------------------------------------------------------------------------+
| Explain String                                                                            |
+-------------------------------------------------------------------------------------------+
| Using baseline plan[436115]                                                               |
|                                                                                           |
| PLAN FRAGMENT 0                                                                           |
|  OUTPUT EXPRS:104: i_item_id | 106: sum                                                   |
|   PARTITION: UNPARTITIONED                                                                |
|                                                                                           |
|   RESULT SINK                                                                             |
|                                                                                           |
|   24:EXCHANGE                                                                             |
|                                                                                           |
| PLAN FRAGMENT 1                                                                           |
......
```

## 未来计划
未来 StarRocks 会基于 SQL Plan Manager 的功能，提供更多高级功能，例如：
* 升级检查SQL Plan的稳定性
* 自动优化固化的查询计划
* 提供更多条件参数的绑定方式