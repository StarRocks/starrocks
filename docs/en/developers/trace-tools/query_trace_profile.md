---
displayed_sidebar: docs
---

# Query Trace Profile

This topic introduces how to obtain and analyze query trace profiles. A query trace profile records the debug information for a specified query statement, including time costs, variables & values, and logs. Such information is categorized into several modules, allowing you to debug and identify the performance bottlenecks from different aspects. This feature is supported from v3.2.0 onwards.

## Syntax

You can use the following syntax to obtain the trace profile of a query:

```SQL
TRACE { TIMES | VALUES | LOGS | ALL } [ <module> ] <query_statement>
```

- `TIMES`: Traces the time costs of events in each stage of the specified query.
- `VALUES`: Traces the variables and their values of the specified query.
- `LOGS`: Traces the log records of the specified query.
- `ALL`: Lists all the `TIMES`, `VALUES`, and `LOGS` information in chronological order.
- `<module>`: The module you want to trace information from. Valid values:
  - `BASE`: The base module.
  - `MV`: The materialized view module.
  - `OPTIMIZER`: The optimizer module.
  - `SCHEDULE`: The schedule module.
  - `EXTERNAL`: The external table-related module.

  If no module is specified, `BASE` is used.

- `<query_statement>`: The query statement whose query trace profile you want to obtain.

## Use cases

### Trace the time costs of a query

The following example traces the time costs of a query's optimizer module.

```Plain
MySQL > TRACE TIMES OPTIMIZER SELECT * FROM t1 JOIN t2 ON t1.v1 = t2.v1;
+---------------------------------------------------------------------+
| Explain String                                                      |
+---------------------------------------------------------------------+
|  2ms|-- Total[1] 15ms                                               |
|  2ms|    -- Analyzer[1] 1ms                                         |
|  4ms|    -- Transformer[1] 1ms                                      |
|  6ms|    -- Optimizer[1] 11ms                                       |
|  6ms|        -- preprocessMvs[1] 0                                  |
|  6ms|        -- RuleBaseOptimize[1] 3ms                             |
|  6ms|            -- RewriteTreeTask[41] 2ms                         |
|  7ms|                -- PushDownJoinOnClauseRule[1] 0               |
|  7ms|                -- PushDownPredicateProjectRule[2] 0           |
|  7ms|                -- PushDownPredicateScanRule[2] 0              |
|  8ms|                -- MergeTwoProjectRule[3] 0                    |
|  8ms|                -- PushDownJoinOnExpressionToChildProject[1] 0 |
|  8ms|                -- PruneProjectColumnsRule[6] 0                |
|  8ms|                -- PruneJoinColumnsRule[2] 0                   |
|  8ms|                -- PruneScanColumnRule[4] 0                    |
|  9ms|                -- PruneSubfieldRule[2] 0                      |
|  9ms|                -- PruneProjectRule[6] 0                       |
|  9ms|                -- PartitionPruneRule[2] 0                     |
|  9ms|                -- DistributionPruneRule[2] 0                  |
|  9ms|                -- MergeProjectWithChildRule[3] 0              |
| 10ms|        -- CostBaseOptimize[1] 6ms                             |
| 10ms|            -- OptimizeGroupTask[6] 0                          |
| 10ms|            -- OptimizeExpressionTask[9] 0                     |
| 10ms|            -- ExploreGroupTask[4] 0                           |
| 10ms|            -- DeriveStatsTask[9] 3ms                          |
| 13ms|            -- ApplyRuleTask[16] 0                             |
| 13ms|                -- OnlyScanRule[2] 0                           |
| 14ms|                -- HashJoinImplementationRule[2] 0             |
| 14ms|            -- EnforceAndCostTask[12] 1ms                      |
| 14ms|                -- OlapScanImplementationRule[2] 0             |
| 15ms|                -- OnlyJoinRule[2] 0                           |
| 15ms|                -- JoinCommutativityRule[1] 0                  |
| 16ms|        -- PhysicalRewrite[1] 0                                |
| 17ms|        -- PlanValidate[1] 0                                   |
| 17ms|            -- InputDependenciesChecker[1] 0                   |
| 17ms|            -- TypeChecker[1] 0                                |
| 17ms|            -- CTEUniqueChecker[1] 0                           |
| 17ms|    -- ExecPlanBuild[1] 0                                      |
| Tracer Cost: 273us                                                  |
+---------------------------------------------------------------------+
39 rows in set (0.029 sec)
```

In the **Explain String** returned by the TRACE TIMES statement, each row (except the last row) corresponds to an event in the specified module (stage) of the query. The last row `Tracer Cost` records the time cost of the tracing process.

Take `|  4ms|    -- Transformer[1] 1ms` as an example:

- The left column records the time point, in the lifecycle of the query, when the event was first executed.
- In the right column, following the consecutive hyphens is the name of the event, for example, `Transformer`.
- Following the event name, the number in the brackets (`[1]`) indicates the number of times the event was executed.
- The last part of this column is the overall time cost of the event, for example, `1ms`.
- The records of events are indented based on the depth of the method stack. That is to say, in this example, the first execution of `Transformer` always happens within `Total`.

### Trace the variables of a query

The following example traces the variables & values of a query's MV module.

```Plain
MySQL > TRACE VALUES MV SELECT t1.v2, sum(t1.v3) FROM t1 JOIN t0 ON t1.v1 = t0.v1 GROUP BY t1.v2;
+----------------------------+
| Explain String             |
+----------------------------+
| 32ms| mv2: Rewrite Succeed |
| Tracer Cost: 66us          |
+----------------------------+
2 rows in set (0.045 sec)
```

The structure of the **Explain String** returned by the TRACE VALUES statement is similar to that of the TRACE TIMES statement, except that the right column records the variables and settings of the event in the specified module. The above example records that the materialized view is successfully used to rewrite the query.

### Trace the logs of a query

The following example traces the logs of a query's MV module.

```Plain
MySQL > TRACE LOGS MV SELECT v2, sum(v3) FROM t1 GROUP BY v2;
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Explain String                                                                                                                                                                |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|  3ms|    [MV TRACE] [PREPARE cac571e8-47f9-11ee-abfb-2e95bcb5f199][mv2] [SYNC=false] Prepare MV mv2 success                                                                   |
|  3ms|    [MV TRACE] [PREPARE cac571e8-47f9-11ee-abfb-2e95bcb5f199][GLOBAL] [SYNC=false] RelatedMVs: [mv2], CandidateMVs: [mv2]                                                |
|  4ms|    [MV TRACE] [PREPARE cac571e8-47f9-11ee-abfb-2e95bcb5f199][GLOBAL] [SYNC=true] There are no related mvs for the query plan                                            |
| 35ms|    [MV TRACE] [REWRITE cac571e8-47f9-11ee-abfb-2e95bcb5f199 TF_MV_AGGREGATE_SCAN_RULE mv2] Rewrite ViewDelta failed: cannot compensate query by using PK/FK constraints |
| 35ms|    [MV TRACE] [REWRITE cac571e8-47f9-11ee-abfb-2e95bcb5f199 TF_MV_ONLY_SCAN_RULE mv2] MV is not applicable: mv expression is not valid                                  |
| 43ms|    Query cannot be rewritten, please check the trace logs or `set enable_mv_optimizer_trace_log=on` to find more infos.                                                 |
| Tracer Cost: 400us                                                                                                                                                            |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
7 rows in set (0.056 sec)
```

Alternatively, you can print these logs in the FE log file **fe.log** by setting the variable `trace_log_mode` as follows:

```SQL
SET trace_log_mode='file';
```

The default value of `trace_log_mode` is `command`, indicating that the logs are returned as the **Explain String** as shown above. If you set its value to `file`, the logs are printed in the FE log file **fe.log** with the class name being `FileLogTracer`.

After you set `trace_log_mode` to `file`, no logs will be returned when you execute the TRACE LOGS statement.

Example:

```Plain
MySQL > TRACE LOGS OPTIMIZER  SELECT v1 FROM t1 ;
+---------------------+
| Explain String      |
+---------------------+
| Tracer Cost: 3422us |
+---------------------+
1 row in set (0.023 sec)
```

The log will be printed in **fe.log**.

![img](../../_assets/query_trace_profile.png)

