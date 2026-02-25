---
displayed_sidebar: docs
sidebar_position: 11
---

import Beta from '../_assets/commonMarkdown/_beta.mdx'

# SQL Plan Manager

<Beta />

This topic introduces the basic concepts and usage scenarios of the SQL Plan Manager feature, as well as how to use SQL Plan Manager to regularize query plans.

Starting from v3.5.0, StarRocks supports the SQL Plan Manager feature.

## Overview

SQL Plan Manager allows users to bind a query plan to a query, thereby preventing the query plan from changing due to system state changes (mainly data updates and statistics updates), thus stabilizing query performance.

### Workflow

SQL Plan Manager requires pre-specifying the bound query SQL and the query plan (Baseline) to be used. Here, the query refers to the actual query SQL executed by the user, while the query plan is the query SQL optimized manually or with hints added.

The workflow of SQL Plan Manager is as follows:

1. **Create Baseline**: Use the `CREATE BASELINE` command to bind a query plan to the specified query SQL.
2. **Query Rewrite**: Queries submitted to StarRocks are automatically matched against the Baselines stored in SQL Plan Manager. If they match successfully, the query plan of the Baseline is used on the query.

Notes on Baseline creation:

- You must ensure the logical consistency between the bound SQL and the execution plan SQL in the Baseline. StarRocks performs basic table and parameter checks but cannot guarantee full logical consistency checks. Ensuring the logical correctness of the execution plan is users' own responsibility.
- By default, the bound SQL in the Baseline stores the its own SQL fingerprint. By default, the constant values in the SQL will be replaced with variable parameters (fow example, changing `t1.v1 > 1000` to `t1.v1 > ?`) to improve SQL matching.
- The execution plan bound in the Baseline can be customized by modifying the SQL logic or by adding Hints (Join Hints or `Set_Var`) to ensure the desired execution plan is generated.
- For complex SQL, StarRocks may not automatically bind the SQL and execution plan in the Baseline. In such cases, manual binding can be used, as detailed in the [Advanced Usage](#advanced-usage) section.

Notes on query rewrite:

- SQL Plan Manager primarily relies on SQL fingerprint matching. It checks whether the SQL fingerprint of the query matches a that of a Baseline. If a query matches a Baseline, the parameters in the query are automatically substituted into the Baseline’s execution plan.
- During the matching process, if a query matches multiple Baselines with status `enable`, the optimizer evaluates and selects the optimal Baseline.
- During the matching process, SQL Plan Manager validates whether the Baseline and the query match. If the match fails, the Baseline’s query plan will not be used.
- For execution plans rewritten by SQL Plan Manager, the `EXPLAIN` statement will return `Using baseline plan[id]`.

## Manage Baseline

### Create Baseline

**Syntax**:

```SQL
CREATE [GLOBAL] BASELINE [ON <BindSQL>] USING <PlanSQL> 
[PROPERTIES ("key" = "value"[, ...])]
```

**Parameters**:

- `GLOBAL`: (Optional) Creates a global-level Baseline.
- `BindSQL`: (Optional) The specific query to be bound to the Baseline (execution plan) query. If this parameter is not specified, the Baseline query is bound to itself and use its own query plan.
- `PlanSQL`: The query used to define the execution plan.

**Examples**:

```SQL
-- Create a session-level BASELINE, directly binding the Baseline SQL to itself and using its own query plan.
CREATE BASELINE
USING SELECT t1.v2, t2.v3 FROM t1 JOIN t2 ON t1.v2 = t2.v2 WHERE t1.v2 > 100;

-- Create a global-level BASELINE, directly binding the Baseline SQL to itself and using its own query plan with specified Join Hints.
CREATE GLOBAL BASELINE
USING SELECT t1.v2, t2.v3 FROM t1 JOIN[BROADCAST] t2 ON t1.v2 = t2.v2 WHERE t1.v2 > 100;

-- Create a session-level BASELINE, binding a query to the Baseline SQL and using the Baseline SQL query plan with specified Join Hints.
CREATE BASELINE ON SELECT t1.v2, t2.v3 FROM t1, t2 WHERE t1.v2 = t2.v2 AND t1.v2 > 100
USING SELECT t1.v2, t2.v3 FROM t1 JOIN[BROADCAST] t2 on t1.v2 = t2.v2 where t1.v2 > 100;
```

### Show Baseline

**Syntax**:

```SQL
SHOW BASELINE [WHERE <condition>]

SHOW BASELINE [ON <query>]
```

**Example**:

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

### Drop Baseline

**Syntax**:

```SQL
DROP BASELINE <id>,<id>...
```

**Parameter**:

`id`: The ID of the Baseline. You can obtain the Baseline ID by executing `SHOW BASELINE`.

**Example**:

```SQL
-- Drop tge Baseline with ID being 140035.
DROP BASELINE 140035;
```

### Enable/Disable Baseline
**Syntax**:

```SQL
ENABLE BASELINE <id>,<id>...
DISABLE BASELINE <id>,<id>...
```

**Parameter**:

`id`: The ID of the Baseline. You can obtain the Baseline ID by executing `SHOW BASELINE`.

**Example**:

```SQL
-- enable the baseline with id 140035
ENABLE BASELINE 140035;
-- disable the baseline with id 140035 140037
DISABLE BASELINE 140035, 140037;
```

## Query Rewrite

You can enable the SQL Plan Manager query rewrite feature by setting the variable `enable_spm_rewrite` to `true`.

```SQL
SET enable_spm_rewrite = true;
```

After binding the execution plan, StarRocks will automatically rewrite the corresponding query into the corresponding query plan.

**Example**:

Check the original SQL and execution plan:

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

Create a Baseline to bind the original SQL to a SQL execution plan with Join Hints:

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

Enable SQL Plan Manager query rewrite and check if the original query is rewritten by the Baseline:

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

## Advanced usage

For the following scenarios, you can try using manual query plan binding:
- For complex SQL, SQL Plan Manager can't automatically bind the SQL and query plan.
- For specific scenarios (for example, fixed parameters or conditional parameters), automatic binding cannot meet the requirements.

Compared to automatic binding, manual binding offers greater flexibility but requires understanding some of the execution mechanisms of SQL Plan Manager.

### Execution Logic

#### SQL Plan Manager functions

SQL Plan Manager function are placeholder functions in SQL Plan Manager with two main purposes:
- Mark expressions in parameterized SQL for subsequent parameter extraction and replacement in the process.
- Check parameter conditions, mapping SQL with different parameters to different query plans through parameter conditions.

Currently, StarRocks supports the following SQL Plan Manager functions:
- `_spm_const_var(placeholdID)`: Used to mark a single constant value.
- `_spm_const_list(placeholdID)`: Used to mark multiple constant values, typically used to mark multiple constant values in an IN condition.
- `_spm_const_range(placeholdID, min, max)`：Used to mark a single constant value, but requires the constant value to be within the specified range '[min, max]'.
- `_spm_const_num(placeholdID, value...)`：Used to mark a single constant value, but requires the constant value to be a value in the specified enum 'value...'.

The 'placeholdID' is an integer, which is used as a unique identifier for the parameter, which is used when binding Baseline and generating the Plan.

#### Baseline Creation Process

1. Execute `CREATE BASELINE` to obtain the `BindSQL` to be bound and the execution plan query `PlanSQL`.
2. Parameterize `BindSQL`: Replace literal values or expressions with SQL Plan Manager functions. For example, replace `id > 200` with `id > _spm_const_var(0)`, where parameter `0` is a placeholder ID used to confirm the position of the expression in `BindSQL` and `PlanSQL`. For more SQL Plan Manager functions, see [SQL Plan Manager Functions](#sql-plan-manager-functions).
3. Bind placeholders in `PlanSQL`: Locate the position of placeholders in `PlanSQL` and replace them with the original expressions.
4. Use the optimizer to optimize `PlanSQL` and obtain the query plan.
5. Serialize the query plan into SQL with Hints.
6. Save the Baseline (SQL fingerprint of BindSQL, optimized execution plan SQL).

#### Query Rewrite Process

The query rewrite logic is similar to the `PREPARE` statement.
1. Execute the query.
2. Normalize the query into an SQL fingerprint.
3. Use the SQL fingerprint to find the Baseline (match against the `BindSQL` of the Baseline).
4. Bind the query to the Baseline, check if the query matches the Baseline’s `BindSQL`, and extract the corresponding parameter values from the query using the SQL Plan Manager functions in `BindSQL`. For example, `id > 1000` in the query is bound to `id > _spm_const_var(0)` in BindSQL, extracting `_spm_const_var(0) = 1000`.
5. Replace the SQL Plan Manager parameters in the Baseline’s `PlanSQL`.
6. Return `PlanSQL` to replace the original query.

### Bind query manually

You can use SQL Plan Manager functions to bind more complex SQL to Baselines.

#### Example 1
For example, the SQL to be bound is as follows:

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

Since the constant values in `i_color in ('slate', 'blanched', 'burnished')` are the same, the SQL will be recognized with SQL Plan Manager as:

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

That means both `i_color in ('xxx', 'xxx')` instances are recognized as the same parameter, making it impossible for SQL Plan Manager to distinguish them when different parameters are used in the SQL. 

```SQL
-- can be bind baseline 
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

-- can't bind basline
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

In such cases, you can manually specify the parameters in `BindSQL` and `PlanSQL`:
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

Check the query is rewritten by the Baseline:

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

From the output above, you can see the query is rewritten by the Baseline with distinguished parameters in SQL Plan Manager functions.

#### Example 2

For the following query, if you want to use a different baseline for different `i_color`
```SQL
select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join item on ss_item_sk = i_item_sk
where i_color = 25 
group by i_item_id
```

You can use '_spm_const_range' to:
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
where i_color = 40 -- hit SHUFFLE JOIN
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
where i_color = 70 -- hit BROADCAST JOIN
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
where i_color = 100  -- don't use any Baseline
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

### Auto-Capture

Auto-Capture queries the query SQL statements in the past period of time (default 3 hours), and generates and saves the baseline based on these queries, and the generated baseline is in the 'disable' state by default and doesn't take effect immediately. 
In the following scenarios:
* After the upgrade, the execution plan changes, resulting in a higher query time
* After the data changed, and the statistics are changed, resulting in a change in the execution plan

You can find the historical baseline with `show baseline` and manually roll back the plan with `enable baseline`.

The Auto-Capture feature depends on the save query history feature and requires the following settings:
```SQL
set global enable_query_history=true;
```
The query history is stored in the '_statistics_.query_history' table.

To enable automatic capture:
```SQL
set global enable_plan_capture=true;
```

Other configurations：
```SQL
-- The historical duration of the storage query history, unit: seconds, defaults to 3 days
set global query_history_keep_seconds = 259200;
-- The work interval of Auto-Capture, unit: seconds, defaults to 3 hours
set global plan_capture_interval=10800;
-- Captures regular checks for SQL tables, only captures SQL when table names(db.table) can match plan_capture_include_pattern, defaults .*, which represents all tables
set global plan_capture_include_pattern=".*";
```

:::note
1. Save query history and Auto-Capture will cost some storage and computing resources, so please set it reasonably according to your own scenarios.
2. After binding baselines, newly added optimizations after upgrades may become ineffective, so auto-captured baselines are disabled by default.
:::

#### Usage Example

Use Auto-Capture feature during upgrades to avoid plan regression issues after upgrade:

1. Enable Auto-Capture functionality 1-2 days before upgrade:
```SQL
set global enable_query_history=true;
set global enable_plan_capture=true;
```

2. StarRocks starts to periodically record query plans, which can be viewed with `show baseline`

3. Upgrade StarRocks

4. After upgrade, check query execution time, or use the following SQL to identify queries with plan changes:
```SQL
WITH recent_queries AS (
    -- Use query execution time within 3 days as average execution time
    SELECT 
        dt,                     -- Query execution time
        sql_digest,             -- SQL fingerprint of the query
        `sql`,                  -- Query SQL
        query_ms,               -- Execution time
        plan,                   -- Query plan used
        AVG(query_ms) OVER (PARTITION BY sql_digest) AS avg_ms, -- Average execution time within SQL fingerprint group
        RANK() OVER (PARTITION BY sql_digest ORDER BY plan) != 1 AS is_changed -- Count different plan formats as change indicator
    FROM _statistics_.query_history
    WHERE dt >= NOW() - INTERVAL 3 DAY
)
-- Queries with execution time higher than 1.5 times the average in the last 12 hours
SELECT *, RANK() OVER (PARTITION BY sql_digest ORDER BY query_ms DESC) AS rnk
FROM recent_queries
WHERE query_ms > avg_ms * 1.5 and dt >= now() - INTERVAL 12 HOUR
```

5. Based on plan change information or query execution time information, determine if plan rollback is needed

6. Find the corresponding baseline for the SQL and time point:
```SQL
show baseline on <query>
```

7. Use Enable baseline to roll back:
```SQL
enable baseline <id>
```

8. Turn on baseline rewrite switch:
```SQL
set enable_spm_rewrite = true;
```

## Future Plans

In the future, StarRocks will provide more advanced features based on SQL Plan Manager, including:
- Enhanced stability checks for SQL Plans.
- Automatic optimization of fixed query plans.
- Support for more conditional parameter binding methods.
