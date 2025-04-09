---
displayed_sidebar: docs
sidebar_position: 11
---

# [Preview] SQL Plan Manager

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
- During the matching process, if a query matches multiple Baselines, the optimizer evaluates and selects the optimal Baseline.
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
- `PlanSQL`: The query used to define the execution.

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
SHOW BASELINE
```

**Example**:

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

### Drop Baseline

**Syntax**:

```SQL
DROP BASELINE <id>
```

**Parameter**:

`id`: The ID of the Baseline. You can obtain the Baseline ID by executing `SHOW BASELINE`.

**Example**:

```SQL
-- Drop tge Baseline with ID being 140035.
DROP BASELINE 140035;
```

## Query Rewrite

You can enable the SQL Plan Manager query rewrite feature by setting the variable `enable_sql_plan_manager_rewrite` to `true`.

```SQL
SET enable_sql_plan_manager_rewrite = true;
```

After binding the execution plan, StarRocks will automatically rewrite the corresponding query into the corresponding query plan.

**Example**:

Check the original SQL and execution plan:

```SQL
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

Create a Baseline to bind the original SQL to a SQL execution plan with Join Hints:

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

Enable SQL Plan Manager query rewrite and check if the original query is rewritten by the Baseline:

```SQL
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

## Advanced usage

For the following scenarios, you can try using manual query plan binding:
- For complex SQL, SQL Plan Manager cannot automatically bind the SQL and query plan.
- For specific scenarios (for example, fixed parameters or conditional parameters), automatic binding cannot meet the requirements.

Compared to automatic binding, manual binding offers greater flexibility but requires understanding some of the execution mechanisms of SQL Plan Manager.

### Execution Logic

#### Baseline Creation Process

1. Execute `CREATE BASELINE` to obtain the `BindSQL` to be bound and the execution plan query `PlanSQL`.
2. Parameterize `BindSQL`: Replace literal values or expressions with SQL Plan Manager functions. For example, replace `id > 200` with `id > _spm_const_var(0)`, where parameter `0` is a placeholder ID used to confirm the position of the expression in `BindSQL` and `PlanSQL`. For more SQL Plan Manager functions, see [SQL Plan Manager Functions](sql-plan-manager-functions).
3. Bind placeholders in `PlanSQL`: Locate the position of placeholders in `PlanSQL` and replace them with the original expressions.
4. Use the optimizer to optimize `PlanSQL` and obtain the query plan.
5. Serialize the query plan into SQL with Hints.
6. Save the Baseline (SQL fingerprint of BindSQL, optimized execution plan SQL).

#### Query Rewrite Process

The query rewrite logic is similar to the `PREPRARE` statement.
1. Execute the query.
2. Normalize the query into an SQL fingerprint.
3. Use the SQL fingerprint to find the Baseline (match against the `BindSQL` of the Baseline).
4. Bind the query to the Baseline, check if the query matches the Baseline’s `BindSQL`, and extract the corresponding parameter values from the query using the SQL Plan Manager functions in `BindSQL`. For example, `id > 1000` in the query is bound to `id > _spm_const_var(0)` in BindSQL, extracting `_spm_const_var(0) = 1000`.
5. Replace the SQL Plan Manager parameters in the Baseline’s `PlanSQL`.
6. Return `PlanSQL` to replace the original query.

### SQL Plan Manager functions

SQL Plan Manager function are placeholder functions in SQL Plan Manager with two main purposes:
- Mark expressions in parameterized SQL for subsequent parameter extraction and replacement in the process.
- Check parameter conditions, mapping SQL with different parameters to different query plans through parameter conditions.

Currently, StarRocks supports the following SQL Plan Manager functions:
- `_spm_const_var`: Used to mark a single constant value.
- `_spm_const_list`: Used to mark multiple constant values, typically used to mark multiple constant values in an IN condition.

Future support will include new SQL Plan Manager functions, such as `_spm_const_range`, and `_spm_const_enum`, to provide placeholder functions with conditional parameters.

### Bind query manually

You can use SQL Plan Manager functions to bind more complex SQL to Baselines.

For example, the SQL to be bound is as follows:

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

Since the constant values in `i_color in ('slate', 'blanched', 'burnished')` are the same, the SQL will be recognized with SQL Plan Manager functions as:

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

That means both `i_color in ('xxx', 'xxx')` instances are recognized as the same parameter, making it impossible for SQL Plan Manager to distinguish them when different parameters are used in the SQL. In such cases, you can manually specify the parameters in `BindSQL` and `PlanSQL`:

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

Check if the query is rewritten by the Baseline:

```SQL
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

From the output above, you can see the query is rewritten by the Baseline with distinguished parameters in SQL Plan Manager functions.

## Future Plans

In the future, StarRocks will provide more advanced features based on SQL Plan Manager, including:
- Enhanced stability checks for SQL Plans.
- Automatic optimization of fixed query plans.
- Support for more conditional parameter binding methods.
