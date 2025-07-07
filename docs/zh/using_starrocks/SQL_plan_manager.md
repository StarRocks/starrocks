---
displayed_sidebar: docs
sidebar_position: 11
---

import Beta from '../_assets/commonMarkdown/_beta.mdx'

# SQL Plan Manager

<Beta />

本文介绍 SQL Plan Manager 功能的基本概念和使用场景，以及如何使用 SQL Plan Manager 规范化查询计划。

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
- 在匹配过程中，如果查询能匹配多个状态为 `enable` 的 Baseline，优化器会评估并选择最佳 Baseline。
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
- `PlanSQL`：用于定义执行计划的查询。

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
SHOW BASELINE [WHERE <condition>]

SHOW BASELINE [ON <query>]
```

**示例**：

```Plain
MySQL > show baseline\G;
***************************[ 1. row ]***************************
Id            | 646125
global        | N
enable        | N
bindSQLDigest | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` LIMIT 2
bindSQLHash   | 1085294
bindSQL       | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` LIMIT 2
planSQL       | SELECT t2.v1 AS c_1, t2.v2 AS c_2, t2.v3 AS c_3, t1.v1 AS c_4, t1.v2 AS c_5 FROM t2 INNER JOIN[SHUFFLE] t1 ON t2.v2 = t1.v2 LIMIT 2
costs         | 582.0
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 14:50:45
***************************[ 2. row ]***************************
Id            | 636134
global        | Y
enable        | Y
bindSQLDigest | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` WHERE `td`.`t2`.`v3` = ?
bindSQLHash   | 1085294
bindSQL       | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` WHERE `td`.`t2`.`v3` = _spm_const_range(1, 10, 20)
planSQL       | SELECT t_0.v1 AS c_1, t_0.v2 AS c_2, t_0.v3 AS c_3, t1.v1 AS c_4, t1.v2 AS c_5 FROM (SELECT * FROM t2 WHERE v3 = _spm_const_range(1, 10, 20)) t_0 INNER JOIN[SHUFFLE] t1 ON t_0.v2 = t1.v2
costs         | 551.0204081632653
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-13 15:29:04
2 rows in set
Time: 0.019s

MySQL > show baseline where global = true\G;
***************************[ 1. row ]***************************
Id            | 636134
global        | Y
enable        | Y
bindSQLDigest | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` WHERE `td`.`t2`.`v3` = ?
bindSQLHash   | 1085294
bindSQL       | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` WHERE `td`.`t2`.`v3` = _spm_const_range(1, 10, 20)
planSQL       | SELECT t_0.v1 AS c_1, t_0.v2 AS c_2, t_0.v3 AS c_3, t1.v1 AS c_4, t1.v2 AS c_5 FROM (SELECT * FROM t2 WHERE v3 = _spm_const_range(1, 10, 20)) t_0 INNER JOIN[SHUFFLE] t1 ON t_0.v2 = t1.v2
costs         | 551.0204081632653
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-13 15:29:04
1 row in set
Time: 0.013s

MySQL > show baseline on SELECT count(1) AS `count(1)` FROM `old`.`t1` INNER JOIN `old`.`t2` ON `old`.`t1`.`k2` = `old`.`t2`.`k2` LIMIT 10\G;
***************************[ 1. row ]***************************
Id            | 679817
global        | Y
enable        | Y
bindSQLDigest | SELECT count(?) AS `count(1)` FROM `old`.`t1` INNER JOIN `old`.`t2` ON `old`.`t1`.`k2` = `old`.`t2`.`k2` LIMIT 10
bindSQLHash   | 1085927
bindSQL       | SELECT count(_spm_const_var(1)) AS `count(1)` FROM `old`.`t1` INNER JOIN `old`.`t2` ON `old`.`t1`.`k2` = `old`.`t2`.`k2` LIMIT 10
planSQL       | SELECT count(_spm_const_var(1)) AS c_7 FROM (SELECT 1 AS c_9 FROM t1 INNER JOIN[SHUFFLE] t2 ON t1.k2 = t2.k2) t_0 LIMIT 10
costs         | 2532.6
queryMs       | 35.0
source        | CAPTURE
updateTime    | 2025-05-27 11:17:48
1 row in set
Time: 0.026s
```

### 删除 Baseline

**语法**：

```SQL
DROP BASELINE <id>,<id>...
```

**参数**：

`id`：Baseline 的 ID。您可以通过执行 `SHOW BASELINE` 获取 Baseline ID。

**示例**：

```SQL
-- 删除 ID 为 140035 的 Baseline。
DROP BASELINE 140035;
```

### 开启或关闭 Baseline
**语法**：

```SQL
-- 启用 Baseline
ENABLE BASELINE <id>,<id>...
-- 关闭 Baseline
DISABLE BASELINE <id>,<id>...
```

**参数**：

`id`：Baseline 的 ID。您可以通过执行 `SHOW BASELINE` 获取 Baseline ID。

**示例**：

```SQL
-- 开启 ID 为 140035 的 Baseline。
ENABLE BASELINE 140035;
-- 关闭 ID 为 140035, 140037 的 Baseline。
DISABLE BASELINE 140035, 140037;
```

## 查询改写

您可以通过将变量 `enable_spm_rewrite` 设置为 `true` 来启用 SQL Plan Manager 查询改写功能。

```SQL
SET enable_spm_rewrite = true;
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
......
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
MySQL td> create global baseline on select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 1000
using select t1.v2, t2.v3 from t1 join[broadcast] t2 on t1.v2 = t2.v2 where t1.v1 > 1000;
Query OK, 0 rows affected
Time: 0.074s
MySQL td> show baseline\G;
***************************[ 1. row ]***************************
Id            | 647139
global        | Y
enable        | Y
bindSQLDigest | SELECT `td`.`t1`.`v2`, `td`.`t2`.`v3` FROM `td`.`t1` , `td`.`t2`  WHERE (`td`.`t1`.`v2` = `td`.`t2`.`v2`) AND (`td`.`t1`.`v1` > ?)
bindSQLHash   | 1085294
bindSQL       | SELECT `td`.`t1`.`v2`, `td`.`t2`.`v3` FROM `td`.`t1` , `td`.`t2`  WHERE (`td`.`t1`.`v2` = `td`.`t2`.`v2`) AND (`td`.`t1`.`v1` > _spm_const_var(1))
planSQL       | SELECT t_0.v2 AS c_2, t2.v3 AS c_5 FROM (SELECT v2 FROM t1 WHERE v1 > _spm_const_var(1)) t_0 INNER JOIN[BROADCAST] t2 ON t_0.v2 = t2.v2
costs         | 1193.0
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 15:51:36
1 rows in set
Time: 0.016s
```

启用 SQL Plan Manager 查询改写并检查原始查询是否被 Baseline 改写：

```Plain
MySQL td> show variables like '%enable_spm_re%'
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| enable_spm_rewrite | false |
+--------------------+-------+
1 row in set
Time: 0.007s
MySQL td> set enable_spm_rewrite=true
Query OK, 0 rows affected
Time: 0.001s
MySQL td> explain select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 20;
+-----------------------------------------+
| Explain String                          |
+-----------------------------------------+
| Using baseline plan[647139]             |
|                                         |
| PLAN FRAGMENT 0                         |
|  OUTPUT EXPRS:2: v2 | 5: v3             |
.............
|   4:HASH JOIN                           |
|   |  join op: INNER JOIN (BROADCAST)    |
|   |  colocate: false, reason:           |
|   |  equal join conjunct: 2: v2 = 4: v2 |
|   |                                     |
|   |----3:EXCHANGE                       |
|   |                                     |
|   1:Project                             |
|   |  <slot 2> : 2: v2                   |
.............
```

## 高级用法

对于以下场景，您可以尝试使用手动查询计划绑定：
- 对于参数复杂的 SQL，SQL Plan Manager 可能无法自动将 SQL 和查询计划绑定。
- 对于特定场景（例如，固定参数或条件参数），自动绑定无法满足要求。

与自动绑定相比，手动绑定提供了更大的灵活性，但需要了解 SQL Plan Manager 的一些执行机制。

### 执行逻辑

#### SQL Plan Manager 函数

SQL Plan Manager 函数是一组特殊的占位符函数，具有两个主要目的：
- 标记参数化 SQL 中的表达式，以便在过程中进行后续的参数提取和替换。
- 检查参数条件，通过参数条件将具有不同参数的 SQL 映射到不同的查询计划。

目前，StarRocks 支持以下 SQL Plan Manager 函数：
- `_spm_const_var(placeholdID)`：用于标记单个常量值。
- `_spm_const_list(placeholdID)`：用于标记多个常量值，通常用于标记 IN 条件中的多个常量值。
- `_spm_const_range(placeholdID, min, max)`：用于标记单个常量值，但要求常量值在指定范围`[min, max]`之内。
- `_spm_const_num(placeholdID, value...)`：用于标记单个常量值，但要求常量值为指定`value...`中的一个值。

其中 `placeholdID` 是一个整数，作为参数的唯一标识符，在绑定 Baseline 和改写 Plan 时，查找对应的参数使用。

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

### 手动绑定查询

您可以使用 SQL Plan Manager 函数将更复杂的 SQL 绑定到 Baseline。

#### 示例 1
例如，要绑定的 SQL 如下：

```SQL
create global baseline using
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

由于 `i_color in ('slate', 'blanched', 'burnished')` 中的常量值相同，该 SQL 会被 SQL Plan Manager 识别为：

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

这意味着两个 `i_color in ('xxx', 'xxx')` 实例被识别为相同的参数，使得 SQL Plan Manager 无法在 SQL 中使用不同参数时区分它们。例如以下查询：

```SQL
-- 可以匹配 baseline 
MySQL tpcds> explain with ss as ( 
    select i_item_id, sum(ss_ext_sales_price) total_sales 
    from store_sales, item 
    where i_color in ('A', 'B', 'C') and ss_item_sk = i_item_sk 
    group by i_item_id 
), 
cs as ( 
    select i_item_id, sum(cs_ext_sales_price) total_sales 
    from catalog_sales, item 
    where i_color in ('A', 'B', 'C') and cs_item_sk = i_item_sk 
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
| Using baseline plan[646215]                                                               |
|                                                                                           |
| PLAN FRAGMENT 0                                                                           |
|  OUTPUT EXPRS:104: i_item_id | 106: sum                                                   |
|   PARTITION: UNPARTITIONED                                                                |
................................                                                            |
|      avgRowSize=3.0                                                                       |
+-------------------------------------------------------------------------------------------+
184 rows in set
Time: 0.095s

-- 无法匹配 basline
MySQL tpcds> explain with ss as ( 
    select i_item_id, sum(ss_ext_sales_price) total_sales 
    from store_sales, item 
    where i_color in ('A', 'B', 'C') and ss_item_sk = i_item_sk 
    group by i_item_id 
), 
cs as ( 
    select i_item_id, sum(cs_ext_sales_price) total_sales 
    from catalog_sales, item 
    where i_color in ('E', 'F', 'G') and cs_item_sk = i_item_sk 
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
| PLAN FRAGMENT 0                                                                           |
|  OUTPUT EXPRS:104: i_item_id | 106: sum                                                   |
|   PARTITION: UNPARTITIONED                                                                |
................................                                                            |
|      avgRowSize=3.0                                                                       |
+-------------------------------------------------------------------------------------------+
182 rows in set
Time: 0.040s
```


在这种情况下，您可以在 `BindSQL` 和 `PlanSQL` 中手动指定参数：

```SQL
create global baseline using
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
MySQL td> show baseline\G;
***************************[ 1. row ]***************************
Id            | 646215
global        | Y
enable        | Y
bindSQLDigest | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (?)) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales` FROM `tpcds`.`catalog_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (?)) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales` FROM (SELECT * FROM `ss` UNION ALL SELECT * FROM `cs`) `tmp1` GROUP BY `tmp1`.`i_item_id`
bindSQLHash   | 203487418
bindSQL       | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales` FROM `tpcds`.`catalog_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales` FROM (SELECT * FROM `ss` UNION ALL SELECT * FROM `cs`) `tmp1` GROUP BY `tmp1`.`i_item_id`
planSQL       | SELECT c_104, sum(c_105) AS c_106 FROM (SELECT * FROM (SELECT i_item_id AS c_104, c_46 AS c_105 FROM (SELECT i_item_id, sum(ss_ext_sales_price) AS c_46 FROM (SELECT i_item_id, ss_ext_sales_price FROM store_sales INNER JOIN[BROADCAST] (SELECT i_item_sk, i_item_id FROM item WHERE i_color IN (_spm_const_list(1))) t_0 ON ss_item_sk = i_item_sk) t_1 GROUP BY i_item_id) t_2 UNION ALL SELECT i_item_id AS c_104, c_103 AS c_105 FROM (SELECT i_item_id, sum(cs_ext_sales_price) AS c_103 FROM (SELECT i_item_id, cs_ext_sales_price FROM catalog_sales INNER JOIN[BROADCAST] (SELECT i_item_sk, i_item_id FROM item WHERE i_color IN (_spm_const_list(1))) t_3 ON cs_item_sk = i_item_sk) t_4 GROUP BY i_item_id) t_5) t_6) t_7 GROUP BY c_104
costs         | 2.608997082E8
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 15:30:29
***************************[ 2. row ]***************************
Id            | 646237
global        | Y
enable        | Y
bindSQLDigest | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (?)) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales` FROM `tpcds`.`catalog_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (?)) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales` FROM (SELECT * FROM `ss` UNION ALL SELECT * FROM `cs`) `tmp1` GROUP BY `tmp1`.`i_item_id`
bindSQLHash   | 203487418
bindSQL       | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales` FROM `tpcds`.`catalog_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(2))) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales` FROM (SELECT * FROM `ss` UNION ALL SELECT * FROM `cs`) `tmp1` GROUP BY `tmp1`.`i_item_id`
planSQL       | SELECT c_104, sum(c_105) AS c_106 FROM (SELECT * FROM (SELECT i_item_id AS c_104, c_46 AS c_105 FROM (SELECT i_item_id, sum(ss_ext_sales_price) AS c_46 FROM (SELECT i_item_id, ss_ext_sales_price FROM store_sales INNER JOIN[BROADCAST] (SELECT i_item_sk, i_item_id FROM item WHERE i_color IN (_spm_const_list(1))) t_0 ON ss_item_sk = i_item_sk) t_1 GROUP BY i_item_id) t_2 UNION ALL SELECT i_item_id AS c_104, c_103 AS c_105 FROM (SELECT i_item_id, sum(cs_ext_sales_price) AS c_103 FROM (SELECT i_item_id, cs_ext_sales_price FROM catalog_sales INNER JOIN[BROADCAST] (SELECT i_item_sk, i_item_id FROM item WHERE i_color IN (_spm_const_list(2))) t_3 ON cs_item_sk = i_item_sk) t_4 GROUP BY i_item_id) t_5) t_6) t_7 GROUP BY c_104
costs         | 2.635637082E8
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 15:37:35
2 rows in set
Time: 0.013s
MySQL td> explain with ss as ( 
    select i_item_id, sum(ss_ext_sales_price) total_sales 
    from store_sales, item 
    where i_color in ('A', 'B', 'C') and ss_item_sk = i_item_sk 
    group by i_item_id 
), 
cs as ( 
    select i_item_id, sum(cs_ext_sales_price) total_sales 
    from catalog_sales, item 
    where i_color in ('E', 'F', 'G') and cs_item_sk = i_item_sk 
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
| Using baseline plan[646237]                                                               |
|                                                                                           |
| PLAN FRAGMENT 0                                                                           |
|  OUTPUT EXPRS:104: i_item_id | 106: sum                                                   |
|   PARTITION: UNPARTITIONED                                                                |
......
```

从上面的输出可以看到，查询被 SQL Plan Manager 函数中使用了不同参数的 Baseline 改写。

#### 示例 2
对于以下查询，想针对 `i_color` 不同参数时，分别使用不同的 baseline 改写。
```SQL
select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join item on ss_item_sk = i_item_sk
where i_color = 25 
group by i_item_id
```

可以使用 `_spm_const_range` 来实现：
```SQL
-- 10 <= i_color <= 50 时，使用 SHUFFLE JOIN
MySQL tpcds> create baseline  using select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join[SHUFFLE] item on ss_item_sk = i_item_sk
where i_color = _spm_const_range(1, 10, 50)
group by i_item_id
Query OK, 0 rows affected
Time: 0.017s
-- i_color 为 60，70，80 时，使用 BROADCAST JOIN
MySQL tpcds> create baseline  using select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join[BROADCAST] item on ss_item_sk = i_item_sk
where i_color = _spm_const_enum(1, 60, 70, 80)
group by i_item_id
Query OK, 0 rows affected
Time: 0.009s
MySQL tpcds> show baseline\G;
***************************[ 1. row ]***************************
Id            | 647167
global        | N
enable        | Y
bindSQLDigest | SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` INNER JOIN `tpcds`.`item` ON `tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk` WHERE `tpcds`.`item`.`i_color` = ? GROUP BY `tpcds`.`item`.`i_item_id`
bindSQLHash   | 68196091
bindSQL       | SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` INNER JOIN `tpcds`.`item` ON `tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk` WHERE `tpcds`.`item`.`i_color` = _spm_const_range(1, 10, 50) GROUP BY `tpcds`.`item`.`i_item_id`
planSQL       | SELECT i_item_id, sum(ss_ext_sales_price) AS c_46 FROM (SELECT i_item_id, ss_ext_sales_price FROM store_sales INNER JOIN[SHUFFLE] (SELECT i_item_sk, i_item_id FROM item WHERE i_color = _spm_const_range(1, '10', '50')) t_0 ON ss_item_sk = i_item_sk) t_1 GROUP BY i_item_id
costs         | 1.612502146E8
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 16:02:46
***************************[ 2. row ]***************************
Id            | 647171
global        | N
enable        | Y
bindSQLDigest | SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` INNER JOIN `tpcds`.`item` ON `tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk` WHERE `tpcds`.`item`.`i_color` = ? GROUP BY `tpcds`.`item`.`i_item_id`
bindSQLHash   | 68196091
bindSQL       | SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` INNER JOIN `tpcds`.`item` ON `tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk` WHERE `tpcds`.`item`.`i_color` = _spm_const_enum(1, 60, 70, 80) GROUP BY `tpcds`.`item`.`i_item_id`
planSQL       | SELECT i_item_id, sum(ss_ext_sales_price) AS c_46 FROM (SELECT i_item_id, ss_ext_sales_price FROM store_sales INNER JOIN[BROADCAST] (SELECT i_item_sk, i_item_id FROM item WHERE i_color = _spm_const_enum(1, '60', '70', '80')) t_0 ON ss_item_sk = i_item_sk) t_1 GROUP BY i_item_id
costs         | 1.457490986E8
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 16:03:23
2 rows in set
Time: 0.011s
MySQL tpcds>
MySQL tpcds> explain select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join item on ss_item_sk = i_item_sk 
where i_color = 40 -- 命中 SHUFFLE JOIN
group by i_item_id
+-------------------------------------------------------------------------------------------+
| Explain String                                                                            |
+-------------------------------------------------------------------------------------------+
| Using baseline plan[647167]                                                               |
|                                                                                           |
| PLAN FRAGMENT 0                                                                           |
.................
|   |                                                                                       |
|   5:HASH JOIN                                                                             |
|   |  join op: INNER JOIN (PARTITIONED)                                                    |
|   |  colocate: false, reason:                                                             |
|   |  equal join conjunct: 2: ss_item_sk = 24: i_item_sk                                   |
|   |                                                                                       |
|   |----4:EXCHANGE                                                                         |
|   |                                                                                       |
|   1:EXCHANGE                                                                              |
.................
MySQL tpcds> explain select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join item on ss_item_sk = i_item_sk
where i_color = 70 -- 命中 BROADCAST JOIN
group by i_item_id
+-------------------------------------------------------------------------------------------+
| Explain String                                                                            |
+-------------------------------------------------------------------------------------------+
| Using baseline plan[647171]                                                               |
|                                                                                           |
| PLAN FRAGMENT 0                                                                           |
.................
|   4:HASH JOIN                                                                             |
|   |  join op: INNER JOIN (BROADCAST)                                                      |
|   |  colocate: false, reason:                                                             |
|   |  equal join conjunct: 2: ss_item_sk = 24: i_item_sk                                   |
|   |                                                                                       |
|   |----3:EXCHANGE                                                                         |
|   |                                                                                       |
|   0:OlapScanNode                                                                          |
|      TABLE: store_sales                                                                   |
.................
MySQL tpcds> explain select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join item on ss_item_sk = i_item_sk 
where i_color = 100  -- 未命中 Baseline
group by i_item_id
+-------------------------------------------------------------------------------------------+
| Explain String                                                                            |
+-------------------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                                           |
|  OUTPUT EXPRS:25: i_item_id | 46: sum                                                     |
|   PARTITION: UNPARTITIONED                                                                |
|                                                                                           |
|   RESULT SINK                                                                             |
.................
```

### 自动捕获

自动捕获会查询过去一段时间内(默认3小时)的查询 SQL，并基于这些查询生成并保存对应的 baseline，生成 baseline 默认为 `disable` 状态，并不会立刻生效。在以下场景中：
* 升级后，执行计划变更，导致查询耗时变高
* 导入数据后，统计信息变更，导致执行计划变更，进一步影响查询耗时变高

可以通过`show baseline`查找历史生成的 baseline，并通过`enable baseline`的方式手动回滚Plan。

自动捕获功能依赖存储查询历史功能，需要设置：
```SQL
set global enable_query_history=true;
```
开启后查询历史存储在 `_statistics_.query_history` 表中。

开启自动捕获功能：
```SQL
set global enable_plan_capture=true;
```

其他配置：
```SQL
-- 存储查询历史时长，单位为秒，默认为 3 天
set global query_history_keep_seconds = 259200;
-- 捕获SQL的间隔时间，单位为秒，默认为 3 小时
set global plan_capture_interval=10800;
-- 捕获SQL的正则检查，仅捕获匹配 plan_capture_include_pattern 的表名(db.table)，默认为.*，表示所有表
set global plan_capture_include_pattern=".*";
```

:::note
1. 存储查询历史&自动捕获在一定程度上会占用存储和计算资源，请根据自身场景合理设置。
2. 绑定 Basline 后，可能会导致升级后新增的优化失效，所以自动捕获的 Baseline 默认为 `disable`
:::

#### 使用样例

在升级时使用自动捕获功能，避免升级后 plan 回退导致问题，操作流程：
1. 在升级前1~2天，打开自动捕获功能：
```SQL
set global enable_query_history=true;
set global enable_plan_capture=true;
```
2. StarRocks开始定期记录查询 plan，可以通过`show baseline`查看
3. 升级 StarRocks
4. 升级完成后，检查查询耗时，或者可以通过以下 SQL 判断是否存在 Plan 变更的 SQL ：
```SQL
WITH recent_queries AS (
    -- 以 3 天内的查询耗时作为平均耗时
    SELECT 
        dt,                     -- 查询执行的时间
        sql_digest,             -- 查询对应的SQL指纹
        `sql`,                  -- 查询SQL
        query_ms,               -- 耗时
        plan,                   -- 查询使用的Plan
        AVG(query_ms) OVER (PARTITION BY sql_digest) AS avg_ms, -- SQL指纹组内的平均耗时
        RANK() OVER (PARTITION BY sql_digest ORDER BY plan) != 1 AS is_changed -- 统计不同 Plan 格式， 作为是否变更标示
    FROM _statistics_.query_history
    WHERE dt >= NOW() - INTERVAL 3 DAY
)
-- 最近 12 小时内耗时高于平均 1.5 倍的查询
SELECT *, RANK() OVER (PARTITION BY sql_digest ORDER BY query_ms DESC) AS rnk
FROM recent_queries
WHERE query_ms > avg_ms * 1.5 and dt >= now() - INTERVAL 12 HOUR
```
5. 通过 Plan 变更信息或查询耗时信息，确认是否需要回滚 Plan
6. 查找对应sql以及时间点的baseline
```SQL
show baseline on <query>
```
8. 使用 Enable baseline 回滚
```SQL
enable baseline <id>
```
9. 打开 basline 改写开关
```SQL
set enable_spm_rewrite = true;
```

## 未来计划

未来，StarRocks 将基于 SQL Plan Manager 提供更多高级功能，包括：
- 增强 SQL 计划的稳定性检查。
- 固定查询计划的自动优化。
- 支持更多条件参数绑定方法。
