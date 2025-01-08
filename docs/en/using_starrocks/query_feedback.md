---
displayed_sidebar: docs
sidebar_position: 11
---

# Query Feedback

This topic introduces the Query Feedback feature, its application scenarios, and how to optimize query plans based on feedback from execution statistics using Query Plan Advisor.

StarRocks supports the Query Feedback feature from v3.4.0 onwards.

## Overview

Query Feedback is a framework and a critical component of Cost-based Optimizer (CBO). It records the execution statistics during query execution, and reuses it in subsequent queries with similar query plans to help CBO generate optimized query plans. CBO optimizes query plans based on estimated statistics, so when statistical information is outdated or inaccurate, it may select inefficient query plans (bad plans), such as broadcasting a large table or misordering the left and right tables. These bad plans can cause query execution timeouts, excessive resource consumption, or even system crashes.

## Workflow

The workflow of Query Feedback-based plan optimization consists of three stages:

1. **Observation**: BE or CN records the major metrics (including `InputRows` and `OutputRows`) of PlanNode in each query plan.
2. **Analysis**: For slow queries that exceed the configured threshold and queries that are manually marked to be analyzed, the system will analyze execution details at critical nodes to identify optimization opportunities in the current query plan after the query finishes and before the result is returned. FE compares the query plan with the execution statistics, and checks if the query is a slow query caused by an abnormal query plan. While FE analyzes the inaccurate statistics, it will generate a SQL tuning guide for each query, instruct CBO to optimize the query dynamically, and recommend strategies to improve the performance.
3. **Optimization**: After CBO generates a physical plan, it will search for any existing tuning guide that applies to the plan. If there is one, CBO will dynamically optimize the plan according to the guide and strategies, correcting problematic sections, and thereby eliminating the impact on the query performance due to the repetitive usage of bad query plans. The execution time of the optimized plan is compared to that of the original plan to evaluate the effectiveness of the tuning.

## Usage

Controlled by system variable `enable_plan_advisor` (Default: `true`), Query Plan Advisor is enabled by default for slow queries, that is, queries with execution time exceeding the threshold defined in the FE configuration item `slow_query_analyze_threshold` (Default: `5` seconds).

In addition, you can manually analyze a specific query or enable automatic analysis for all queries executed.

### Manually analyze a specific query

You can manually analyze a specific query statement even if its execution time does not exceed `slow_query_analyze_threshold`.

```SQL
ALTER PLAN ADVISOR ADD <query_statement>
```

Example:

```SQL
ALTER PLAN ADVISOR ADD SELECT COUNT(*) FROM (
    SLECT * FROM c1_skew_left_over t1 
    JOIN (SELECT * FROM c1_skew_left_over WHERE c1 = 'c') t2 
    ON t1.c2 = t2.c2 WHERE t1.c1 > 'c' ) t;
```

### Enable automatic analysis for all queries

To enable automatic analysis for all queries, you need to set the system variable `enable_plan_analyzer` (Default: `false) to `true`.

```SQL
SET enable_plan_analyzer = true;
```

### Show the tuning guides on the current FE

Each FE maintains its own record of tuning guides. You can use the following statement to view the tuning guides generated for the entitled queries on the current FE:

```SQL
SHOW PLAN ADVISOR
```

### Check if Tuning Guide takes effect

Execute [EXPLAIN](../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md) against the query statement. In the EXPLAIN string, the message `Plan had been tuned by Plan Advisor` indicates that a tuning guide has been applied to the corresponding query.

Example:

```SQL
EXPLAIN SELECT COUNT(*) FROM (
    SLECT * FROM c1_skew_left_over t1 
    JOIN (SELECT * FROM c1_skew_left_over WHERE c1 = 'c') t2 
    ON t1.c2 = t2.c2 WHERE t1.c1 > 'c' ) t;
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

### Delete the tuning guide of a specific query

You can delete the tuning guide of a specific query based on the query ID returned from `SHOW PLAN ADVISOR`.

```SQL
ALTER PLAN ADVISOR DROP <query_id>
```

Example:

```SQL
ALTER PLAN ADVISOR DROP "8e010cf4-b178-11ef-8aa4-8a5075cec65e";
```

### Clear all tuning guides on the current FE

To clear all tuning guides on the current FE, execute the following statement:

```SQL
TRUNCATE PLAN ADVISOR
```

## Use Cases

Currently, Query Feedback is primarily used to optimize the following scenarios:

- Optimizing the order of left and right sides in local Join nodes
- Optimizing the execution method of local Join nodes (e.g., switching from Broadcast to Shuffle)
- For cases with significant aggregation potential, enforcing `pre_aggregation` mode to maximize data aggregation in the first phase

The tuning guides are mainly based on metrics `Runtime Exec Node Input/Output Rows` and FE `statistics estimated rows`. Since the current tuning thresholds are relatively conservative, you are encouraged to leverage Query Feedback to check for potential performance improvements if issues are observed in the Query Profile or EXPLAIN string.

Below are three common user cases.

### Case 1: Incorrect Join Order

Original Bad Plan:

```SQL
small left table inner join large table (broadcast)
```

Optimized Plan:

```SQL
large left table inner join small right table (broadcast)
```

**Cause** The issue might be caused by outdated or missing statistics, which leads the Cost-Based Optimizer (CBO) to generate an incorrect plan based on inaccurate data.

**Solution** During query execution, the system compares the `input/output rows` and `statistics estimated rows` of Left Child and Right Child, generating tuning guides. Upon re-execution, the system automatically adjusts the Join order.

### Case 2: Incorrect Join Execution Method

Original Bad Plan:

```SQL
large left table1 inner join large right table2 (broadcast)
```

Optimized Plan:

```SQL
large left table1 (shuffle) inner join large right table2 (shuffle)
```

**Cause** The issue may result from data skew. When the right table has many partitions and one of them contains disproportionately large amounts of data, the system may incorrectly estimate the row count of right table after predicates are applied.

**Solution** During query execution, the system compares the `input/output rows` and `statistics estimated rows` of Left Child and Right Child, generating tuning guides. After optimization, the Join method is adjusted from Broadcast Join to Shuffle Join.

### Case 3: Inefficient First-Phase Pre-aggregation Mode

**Symptom** For data with good aggregation potential, the `auto` mode of First-phase aggregation may only aggregate a small amount of local data, missing the opportunity for performance gains.

**Solution** During query execution, the system collects `Input/Output Rows` for both local and global aggregations. Based on historical data, it evaluates the potential of the aggregation columns. If the potential is significant, the system enforces the use of `pre_aggregation` mode in local aggregations, maximizing data aggregation in the first phase and improving overall query performance.

## Limitations

- A tuning guide can only be used for the exact same query for which it was generated. It is not applicable to queries with the same pattern but different parameters.
- Each FE manages its Query Plan Advisor independently, and synchronization across FE nodes is not supported. If the same query is submitted to different FE nodes, the tuning results may vary.
- Query Plan Advisor uses an in-memory cache structure:
  - When the number of tuning guides exceeds the limit, expired tuning guides are automatically evicted.
  - The default limit of tuning guides is 300 and the persistence of historical tuning guides is not supported.
