---
displayed_sidebar: docs
sidebar_position: 11
---

# [Preview] SQL Plan Manager

本主题介绍 SQL Plan Manager 功能的基本概念和使用场景，以及如何使用 SQL Plan Manager 规范化查询计划。

从 v3.5.0 开始，StarRocks 支持 SQL Plan Manager 功能。

## 概述

SQL Plan Manager 允许用户将查询计划绑定到查询上，从而防止查询计划因系统状态变化（主要是数据更新和统计信息更新）而改变，从而稳定查询性能。

### 工作流程

SQL Plan Manager 需要预先指定要绑定的查询 SQL 和要使用的查询计划（Baseline）。这里，查询指的是用户实际执行的查询 SQL，而查询计划是手动优化或添加提示的查询 SQL。

SQL Plan Manager 的工作流程如下：

1. **创建 Baseline**：使用 `CREATE BASELINE` 命令将查询计划绑定到指定的查询 SQL。
2. **查询改写**：提交到 StarRocks 的查询会自动与 SQL Plan Manager 中存储的 Baseline 进行匹配。如果匹配成功，则使用 Baseline 的查询计划执行查询。

关于 Baseline 创建的注意事项：

- 必须确保 Baseline 中绑定的 SQL 与执行计划 SQL 之间的逻辑一致性。StarRocks 会进行基本的表和参数检查，但无法保证完全的逻辑一致性检查。确保执行计划的逻辑正确性是用户自己的责任。
- 默认情况下，Baseline 中绑定的 SQL 存储其自身的 SQL 指纹。默认情况下，SQL 中的常量值将被替换为变量参数（例如，将 `t1.v1 > 1000` 改为 `t1.v1 > ?`）以提高 SQL 匹配度。
- Baseline 中绑定的执行计划可以通过修改 SQL 逻辑或添加提示（Join Hints 或 `Set_Var`）进行定制，以确保生成所需的执行计划。
- 对于复杂的 SQL，StarRocks 可能无法自动将 SQL 和执行计划绑定到 Baseline。在这种情况下，可以使用手动绑定，详细信息请参见[高级用法](#高级用法)部分。

关于查询改写的注意事项：

- SQL Plan Manager 主要依赖于 SQL 指纹匹配。它检查查询的 SQL 指纹是否与某个 Baseline 匹配。如果查询匹配某个 Baseline，则查询中的参数会自动替换到 Baseline 的执行计划中。
- 在匹配过程中，如果查询匹配多个 Baseline，优化器会评估并选择最佳 Baseline。
- 在匹配过程中，SQL Plan Manager 会验证 Baseline 和查询是否匹配。如果匹配失败，则不会使用 Baseline 的查询计划。
- 对于 SQL Plan Manager 改写的执行计划，`EXPLAIN` 语句将返回 `Using baseline plan[id]`。

## 管理 Baseline

### 创建 Baseline

**语法**：

```SQL
CREATE [GLOBAL] BASELINE [ON <BindSQL>] USING <PlanSQL> 
[PROPERTIES ("key" = "value"[, ...])]
```

**参数**：

- `GLOBAL`：（可选）创建全局级别的 Baseline。
- `BindSQL`：（可选）要绑定到 Baseline（执行计划）查询的具体查询。如果未指定此参数，Baseline 查询将绑定到自身并使用其自己的查询计划。
- `PlanSQL`：用于定义执行的查询。

**示例**：

```SQL
-- 创建会话级别的 BASELINE，直接将 Baseline SQL 绑定到自身并使用其自己的查询计划。
CREATE BASELINE
USING SELECT t1.v2, t2.v3 FROM t1 JOIN t2 ON t1.v2 = t2.v2 WHERE t1.v2 > 100;

-- 创建全局级别的 BASELINE，直接将 Baseline SQL 绑定到自身并使用其自己的查询计划，指定 Join Hints。
CREATE GLOBAL BASELINE
USING SELECT t1.v2, t2.v3 FROM t1 JOIN[BROADCAST] t2 ON t1.v2 = t2.v2 WHERE t1.v2 > 100;

-- 创建会话级别的 BASELINE，将查询绑定到 Baseline SQL，并使用指定 Join Hints 的 Baseline SQL 查询计划。
CREATE BASELINE ON SELECT t1.v2, t2.v3 FROM t1, t2 WHERE t1.v2 = t2.v2 AND t1.v2 > 100
USING SELECT t1.v2, t2.v3 FROM t1 JOIN[BROADCAST] t2 on t1.v2 = t2.v2 where t1.v2 > 100;
```

### 查看 Baseline

**语法**：

```SQL
SHOW BASELINE
```

**示例**：

```Plain
mysql> SHOW BASELINE\G;
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

### 删除 Baseline

**语法**：

```SQL
DROP BASELINE <id>
```

**参数**：

`id`：Baseline 的 ID。您可以通过执行 `SHOW BASELINE` 获取 Baseline ID。

**示例**：

```SQL
-- 删除 ID 为 140035 的 Baseline。
DROP BASELINE 140035;
```

## 查询改写

您可以通过将变量 `enable_sql_plan_manager_rewrite` 设置为 `true` 来启用 SQL Plan Manager 查询改写功能。

```SQL
SET enable_sql_plan_manager_rewrite = true;
```

在绑定执行计划后，StarRocks 将自动将相应的查询改写为相应的查询计划。

**示例**：

检查原始 SQL 和执行计划：

```Plain
mysql> EXPLAIN SELECT t1.v2, t2.v3 FROM t1, t2 WHERE t1.v2 = t2.v2 AND t1.v1 > 20;
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

创建一个 Baseline，将原始 SQL 绑定到具有 Join Hints 的 SQL 执行计划：

```SQL
mysql> create global baseline on select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 1000
using select t1.v2, t2.v3 from t1 join[broadcast] t2 on t1.v2 = t2.v2 where t1.v1 > 1000;
Query OK, 0 rows affected
Time: 0.062s
mysql> show baseline\G;
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

启用 SQL Plan Manager 查询改写并检查原始查询是否被 Baseline 改写：

```Plain
mysql> show session variables like "%enable_sql_plan_manager_rewrite%";
+---------------------------------+-------+
| Variable_name                   | Value |
+---------------------------------+-------+
| enable_sql_plan_manager_rewrite | false |
+---------------------------------+-------+
1 row in set
Time: 0.006s
mysql> set enable_sql_plan_manager_rewrite = true;
Query OK, 0 rows affected
Time: 0.002s
mysql> explain select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 20;
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

## 高级用法

对于以下场景，您可以尝试使用手动查询计划绑定：
- 对于复杂的 SQL，SQL Plan Manager 无法自动将 SQL 和查询计划绑定。
- 对于特定场景（例如，固定参数或条件参数），自动绑定无法满足要求。

与自动绑定相比，手动绑定提供了更大的灵活性，但需要了解 SQL Plan Manager 的一些执行机制。

### 执行逻辑

#### Baseline 创建过程

1. 执行 `CREATE BASELINE` 以获取要绑定的 `BindSQL` 和执行计划查询 `PlanSQL`。
2. 参数化 `BindSQL`：用 SQL Plan Manager 函数替换字面量值或表达式。例如，将 `id > 200` 替换为 `id > _spm_const_var(0)`，其中参数 `0` 是用于确认 `BindSQL` 和 `PlanSQL` 中表达式位置的占位符 ID。有关更多 SQL Plan Manager 函数，请参见 [SQL Plan Manager 函数](#sql-plan-manager-函数)。
3. 在 `PlanSQL` 中绑定占位符：定位 `PlanSQL` 中占位符的位置，并用原始表达式替换它们。
4. 使用优化器优化 `PlanSQL` 并获得查询计划。
5. 将查询计划序列化为带有 Hint 的 SQL。
6. 保存 Baseline（BindSQL 的 SQL 指纹，优化后的执行计划 SQL）。

#### 查询改写过程

查询改写逻辑类似于 `PREPARE` 语句。
1. 执行查询。
2. 将查询标准化为 SQL 指纹。
3. 使用 SQL 指纹查找 Baseline（与 Baseline 的 `BindSQL` 匹配）。
4. 将查询绑定到 Baseline，检查查询是否与 Baseline 的 `BindSQL` 匹配，并使用 `BindSQL` 中的 SQL Plan Manager 函数从查询中提取相应的参数值。例如，查询中的 `id > 1000` 绑定到 BindSQL 中的 `id > _spm_const_var(0)`，提取 `_spm_const_var(0) = 1000`。
5. 替换 Baseline 的 `PlanSQL` 中的 SQL Plan Manager 参数。
6. 返回 `PlanSQL` 以替换原始查询。

### SQL Plan Manager 函数

SQL Plan Manager 函数是 SQL Plan Manager 中的占位符函数，具有两个主要目的：
- 标记参数化 SQL 中的表达式，以便在过程中进行后续的参数提取和替换。
- 检查参数条件，通过参数条件将具有不同参数的 SQL 映射到不同的查询计划。

目前，StarRocks 支持以下 SQL Plan Manager 函数：
- `_spm_const_var`：用于标记单个常量值。
- `_spm_const_list`：用于标记多个常量值，通常用于标记 IN 条件中的多个常量值。

未来将支持新的 SQL Plan Manager 函数，如 `_spm_const_range` 和 `_spm_const_enum`，以提供具有条件参数的占位符函数。

### 手动绑定查询

您可以使用 SQL Plan Manager 函数将更复杂的 SQL 绑定到 Baseline。

例如，要绑定的 SQL 如下：

```SQL
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

由于 `i_color in ('slate', 'blanched', 'burnished')` 中的常量值相同，该 SQL 将基于 SQL Plan Manager 函数被识别为：

```SQL
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

这意味着两个 `i_color in ('xxx', 'xxx')` 实例被识别为相同的参数，使得 SQL Plan Manager 无法在 SQL 中使用不同参数时区分它们。在这种情况下，您可以在 `BindSQL` 和 `PlanSQL` 中手动指定参数：

```SQL
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

检查查询是否被 Baseline 改写：

```Plain
mysql> show baseline\G;
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

mysql> explain with ss as (
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

从上面的输出可以看到，查询被 SQL Plan Manager 函数中使用了不同参数的 Baseline 改写。

## 未来计划

未来，StarRocks 将基于 SQL Plan Manager 提供更多高级功能，包括：
- 增强 SQL 计划的稳定性检查。
- 固定查询计划的自动优化。
- 支持更多条件参数绑定方法。
