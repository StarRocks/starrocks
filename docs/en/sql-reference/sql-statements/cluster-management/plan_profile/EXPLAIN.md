---
displayed_sidebar: docs
---

# EXPLAIN

EXPLAIN shows the logical or physical execution plans for a query statement. For instructions on how to analyze a query plan, refer to [Plan analysis](../../../../best_practices/query_tuning/query_planning.md#plan-analysis).

:::tip

This operation does not require privileges.

:::

## Syntax

```SQL
EXPLAIN [ LOGICAL | VERBOSE | COSTS ] [ MOCK ] <query>
```

:::tip

The level of execution plan detail returned: EXPLAIN LOGICAL < EXPLAIN < EXPLAIN VERBOSE < EXPLAIN COSTS.

From v3.3.5, the default detail level is changed to `COSTS`. You can configure the level of details returned by EXPLAIN using the dynamic FE parameter `query_detail_explain_level`.

In most cases, using EXPLAIN is sufficient. EXPLAIN VERBOSE and EXPLAIN COSTS will print a lot of internal information, which is mainly used for debugging the execution plan.

:::

## Parameters

| **Parameter** | **Description**                                            |
| ------------- | ---------------------------------------------------------- |
| LOGICAL       | Shows the simple logical execution plans.                  |
| VERBOSE       | Shows the detailed logical execution plans, including data types, nullable information, and optimization strategy. |
| COSTS (Default) | Shows the detailed logical execution plans, including column statistics. |
| MOCK          | Replaces every column reference in the rendered plan with a stable `mock_col_<N>` and prints a `Mocked SQL:` block that uses the same mock names. Useful for sharing a plan externally without leaking column names. Table names, literal values, and function names are not mocked; pair with `enable_desensitize_explain` to also digest literals. |
| query         | The query statement whose execution plan you want to view. |

> **NOTE**
>
> You can configure the level of details returned by EXPLAIN using the dynamic FE parameter `query_detail_explain_level`.

## Return value

Returns the execution plan of the input query statement.

| **Return**     | **Description**                                                                  |
| -------------- |----------------------------------------------------------------------------------|
| avgRowSize     | The average size of the scanned data rows.                                       |
| cardinality    | The total number of data rows in the scanned table.                              |
| colocate       | Whether the table is in colocate mode.                                           |
| numNodes       | The number of nodes to scan.                                                     |
| rollup         | The Rollup materialized view that is used.                                       |
| preaggregation | The status of preaggregation.                                                    |
| predicates     | Predicates in the query.                                                         |
| column statistics | The statictics of the column，sequence: MIN, MAX, NULL numbers，average size，and cardinality. |

## Examples

### EXPLAIN

```Plain
MySQL tpcds> explain select 
    sr_customer_sk as ctr_customer_sk, 
    sr_store_sk as ctr_store_sk, 
    sum(SR_RETURN_AMT) as ctr_total_return 
  from 
    store_returns, 
    date_dim 
  where 
    sr_returned_date_sk = d_date_sk and d_year = 2000 
  group by 
    sr_customer_sk, 
    sr_store_sk
+-----------------------------------------------------------------------------+
| Explain String                                                              |
+-----------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                             |
|  OUTPUT EXPRS:5: sr_customer_sk | 9: sr_store_sk | 49: sum                  |
|   PARTITION: UNPARTITIONED                                                  |
|                                                                             |
|   RESULT SINK                                                               |
|                                                                             |
|   9:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 1                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: HASH_PARTITIONED: 5: sr_customer_sk, 9: sr_store_sk            |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 09                                                         |
|     UNPARTITIONED                                                           |
|                                                                             |
|   8:AGGREGATE (merge finalize)                                              |
|   |  output: sum(49: sum)                                                   |
|   |  group by: 5: sr_customer_sk, 9: sr_store_sk                            |
|   |                                                                         |
|   7:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 2                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: RANDOM                                                         |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 07                                                         |
|     HASH_PARTITIONED: 5: sr_customer_sk, 9: sr_store_sk                     |
|                                                                             |
|   6:AGGREGATE (update serialize)                                            |
|   |  STREAMING                                                              |
|   |  output: sum(12: sr_return_amt)                                         |
|   |  group by: 5: sr_customer_sk, 9: sr_store_sk                            |
|   |                                                                         |
|   5:Project                                                                 |
|   |  <slot 5> : 5: sr_customer_sk                                           |
|   |  <slot 9> : 9: sr_store_sk                                              |
|   |  <slot 12> : 12: sr_return_amt                                          |
|   |                                                                         |
|   4:HASH JOIN                                                               |
|   |  join op: INNER JOIN (BROADCAST)                                        |
|   |  colocate: false, reason:                                               |
|   |  equal join conjunct: 3: sr_returned_date_sk = 21: d_date_sk            |
|   |                                                                         |
|   |----3:EXCHANGE                                                           |
|   |                                                                         |
|   0:OlapScanNode                                                            |
|      TABLE: store_returns                                                   |
|      PREAGGREGATION: ON                                                     |
|      PREDICATES: 3: sr_returned_date_sk IS NOT NULL                         |
|      partitions=1/1                                                         |
|      rollup: store_returns                                                  |
|      tabletRatio=10/10                                                      |
|      tabletList=10241,10243,10245,10247,10249,10251,10253,10255,10257,10259 |
|      cardinality=277502                                                     |
|      avgRowSize=20.0                                                        |
|                                                                             |
| PLAN FRAGMENT 3                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: RANDOM                                                         |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 03                                                         |
|     UNPARTITIONED                                                           |
|                                                                             |
|   2:Project                                                                 |
|   |  <slot 21> : 21: d_date_sk                                              |
|   |                                                                         |
|   1:OlapScanNode                                                            |
|      TABLE: date_dim                                                        |
|      PREAGGREGATION: ON                                                     |
|      PREDICATES: 27: d_year = 2000                                          |
|      partitions=1/1                                                         |
|      rollup: date_dim                                                       |
|      tabletRatio=5/5                                                        |
|      tabletList=11543,11545,11547,11549,11551                               |
|      cardinality=362                                                        |
|      avgRowSize=8.0                                                         |
+-----------------------------------------------------------------------------+
79 rows in set
```

### EXPLAIN LOGICAL

```Plain
MySQL tpcds> explain logical select 
    sr_customer_sk as ctr_customer_sk, 
    sr_store_sk as ctr_store_sk, 
    sum(SR_RETURN_AMT) as ctr_total_return 
  from 
    store_returns, 
    date_dim 
  where 
    sr_returned_date_sk = d_date_sk and d_year = 2000 
  group by 
    sr_customer_sk, 
    sr_store_sk
 
+---------------------------------------------------------------------------------------------------------------------------------+
| Explain String                                                                                                                  |
+---------------------------------------------------------------------------------------------------------------------------------+
| - Output => [5:sr_customer_sk, 9:sr_store_sk, 49:sum]                                                                           |
|     - AGGREGATE(GLOBAL) [5:sr_customer_sk, 9:sr_store_sk]                                                                       |
|             Estimates: {row: 49728, cpu: 795660.58, memory: 795660.58, network: 0.00, cost: 8405375.45}                         |
|             49:sum := sum(49:sum)                                                                                               |
|         - EXCHANGE(SHUFFLE) [5, 9]                                                                                              |
|                 Estimates: {row: 49728, cpu: 79566.06, memory: 0.00, network: 0.00, cost: 6416223.99}                           |
|             - AGGREGATE(LOCAL) [5:sr_customer_sk, 9:sr_store_sk]                                                                |
|                     Estimates: {row: 49728, cpu: 119349.09, memory: 79566.06, network: 0.00, cost: 6376440.96}                  |
|                     49:sum := sum(12:sr_return_amt)                                                                             |
|                 - HASH/INNER JOIN [3:sr_returned_date_sk = 21:d_date_sk] => [5:sr_customer_sk, 9:sr_store_sk, 12:sr_return_amt] |
|                         Estimates: {row: 49728, cpu: 6744977.39, memory: 1446.51, network: 0.00, cost: 6157634.30}              |
|                     - SCAN [store_returns] => [3:sr_returned_date_sk, 5:sr_customer_sk, 9:sr_store_sk, 12:sr_return_amt]        |
|                             Estimates: {row: 277502, cpu: 5550040.00, memory: 0.00, network: 0.00, cost: 2775020.00}            |
|                             partitionRatio: 1/1, tabletRatio: 10/10                                                             |
|                             predicate: 3:sr_returned_date_sk IS NOT NULL                                                        |
|                     - EXCHANGE(BROADCAST)                                                                                       |
|                             Estimates: {row: 361, cpu: 1446.51, memory: 1446.51, network: 1446.51, cost: 7232.57}               |
|                         - SCAN [date_dim] => [21:d_date_sk]                                                                     |
|                                 Estimates: {row: 361, cpu: 2893.03, memory: 0.00, network: 0.00, cost: 1446.51}                 |
|                                 partitionRatio: 1/1, tabletRatio: 5/5                                                           |
|                                 predicate: 27:d_year = 2000                                                                     |
+---------------------------------------------------------------------------------------------------------------------------------+
21 rows in set
```

### EXPLAIN VERBOSE

```Plain
MySQL tpcds> explain verbose select 
    sr_customer_sk as ctr_customer_sk, 
    sr_store_sk as ctr_store_sk, 
    sum(SR_RETURN_AMT) as ctr_total_return 
  from 
    store_returns, 
    date_dim 
  where 
    sr_returned_date_sk = d_date_sk and d_year = 2000 
  group by 
    sr_customer_sk, 
    sr_store_sk
 
+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| Explain String                                                                                                                                          |
+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| RESOURCE GROUP: default_wg                                                                                                                              |
|                                                                                                                                                         |
| PLAN FRAGMENT 0(F04)                                                                                                                                    |
|   Fragment Cost: 0.0                                                                                                                                    |
|   Output Exprs:5: sr_customer_sk | 9: sr_store_sk | 49: sum                                                                                             |
|   Input Partition: UNPARTITIONED                                                                                                                        |
|   RESULT SINK                                                                                                                                           |
|                                                                                                                                                         |
|   9:EXCHANGE                                                                                                                                            |
|      cardinality: 49729                                                                                                                                 |
|                                                                                                                                                         |
| PLAN FRAGMENT 1(F03)                                                                                                                                    |
|   Fragment Cost: 2028934.4876271961                                                                                                                     |
|                                                                                                                                                         |
|   Input Partition: HASH_PARTITIONED: 5: sr_customer_sk, 9: sr_store_sk                                                                                  |
|   OutPut Partition: UNPARTITIONED                                                                                                                       |
|   OutPut Exchange Id: 09                                                                                                                                |
|                                                                                                                                                         |
|   8:AGGREGATE (merge finalize)                                                                                                                          |
|   |  aggregate: sum[([49: sum, DECIMAL128(38,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]         |
|   |  group by: [5: sr_customer_sk, INT, true], [9: sr_store_sk, INT, true]                                                                              |
|   |  cardinality: 49729                                                                                                                                 |
|   |                                                                                                                                                     |
|   7:EXCHANGE                                                                                                                                            |
|      distribution type: SHUFFLE                                                                                                                         |
|      partition exprs: [5: sr_customer_sk, INT, true], [9: sr_store_sk, INT, true]                                                                       |
|      cardinality: 49729                                                                                                                                 |
|                                                                                                                                                         |
| PLAN FRAGMENT 2(F00)                                                                                                                                    |
|   Fragment Cost: 6376440.959353934                                                                                                                      |
|                                                                                                                                                         |
|   Input Partition: RANDOM                                                                                                                               |
|   OutPut Partition: HASH_PARTITIONED: 5: sr_customer_sk, 9: sr_store_sk                                                                                 |
|   OutPut Exchange Id: 07                                                                                                                                |
|                                                                                                                                                         |
|   6:AGGREGATE (update serialize)                                                                                                                        |
|   |  STREAMING                                                                                                                                          |
|   |  aggregate: sum[([12: sr_return_amt, DECIMAL64(7,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true] |
|   |  group by: [5: sr_customer_sk, INT, true], [9: sr_store_sk, INT, true]                                                                              |
|   |  cardinality: 49729                                                                                                                                 |
|   |                                                                                                                                                     |
|   5:Project                                                                                                                                             |
|   |  output columns:                                                                                                                                    |
|   |  5 <-> [5: sr_customer_sk, INT, true]                                                                                                               |
|   |  9 <-> [9: sr_store_sk, INT, true]                                                                                                                  |
|   |  12 <-> [12: sr_return_amt, DECIMAL64(7,2), true]                                                                                                   |
|   |  cardinality: 49729                                                                                                                                 |
|   |                                                                                                                                                     |
|   4:HASH JOIN                                                                                                                                           |
|   |  join op: INNER JOIN (BROADCAST)                                                                                                                    |
|   |  equal join conjunct: [3: sr_returned_date_sk, INT, true] = [21: d_date_sk, INT, false]                                                             |
|   |  build runtime filters:                                                                                                                             |
|   |  - filter_id = 0, build_expr = (21: d_date_sk), remote = false                                                                                      |
|   |  output columns: 5, 9, 12                                                                                                                           |
|   |  can local shuffle: true                                                                                                                            |
|   |  cardinality: 49729                                                                                                                                 |
|   |                                                                                                                                                     |
|   |----3:EXCHANGE                                                                                                                                       |
|   |       distribution type: BROADCAST                                                                                                                  |
|   |       cardinality: 362                                                                                                                              |
|   |                                                                                                                                                     |
|   0:OlapScanNode                                                                                                                                        |
|      table: store_returns, rollup: store_returns                                                                                                        |
|      preAggregation: on                                                                                                                                 |
|      Predicates: 3: sr_returned_date_sk IS NOT NULL                                                                                                     |
|      partitionsRatio=1/1, tabletsRatio=10/10                                                                                                            |
|      tabletList=10241,10243,10245,10247,10249,10251,10253,10255,10257,10259                                                                             |
|      actualRows=287514, avgRowSize=20.0                                                                                                                 |
|      cardinality: 277502                                                                                                                                |
|      probe runtime filters:                                                                                                                             |
|      - filter_id = 0, probe_expr = (3: sr_returned_date_sk)                                                                                             |
|                                                                                                                                                         |
| PLAN FRAGMENT 3(F01)                                                                                                                                    |
|   Fragment Cost: 1446.5148514851485                                                                                                                     |
|                                                                                                                                                         |
|   Input Partition: RANDOM                                                                                                                               |
|   OutPut Partition: UNPARTITIONED                                                                                                                       |
|   OutPut Exchange Id: 03                                                                                                                                |
|                                                                                                                                                         |
|   2:Project                                                                                                                                             |
|   |  output columns:                                                                                                                                    |
|   |  21 <-> [21: d_date_sk, INT, false]                                                                                                                 |
|   |  cardinality: 362                                                                                                                                   |
|   |                                                                                                                                                     |
|   1:OlapScanNode                                                                                                                                        |
|      table: date_dim, rollup: date_dim                                                                                                                  |
|      preAggregation: on                                                                                                                                 |
|      Predicates: [27: d_year, INT, false] = 2000                                                                                                        |
|      partitionsRatio=1/1, tabletsRatio=5/5                                                                                                              |
|      tabletList=11543,11545,11547,11549,11551                                                                                                           |
|      actualRows=73049, avgRowSize=8.0                                                                                                                   |
|      cardinality: 362                                                                                                                                   |
+---------------------------------------------------------------------------------------------------------------------------------------------------------+
```

### EXPLAIN COSTS

```Plain
+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| Explain String                                                                                                                                          |
+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| PLAN FRAGMENT 0(F04)                                                                                                                                    |
|   Output Exprs:5: sr_customer_sk | 9: sr_store_sk | 49: sum                                                                                             |
|   Input Partition: UNPARTITIONED                                                                                                                        |
|   RESULT SINK                                                                                                                                           |
|                                                                                                                                                         |
|   9:EXCHANGE                                                                                                                                            |
|      cardinality: 49729                                                                                                                                 |
|                                                                                                                                                         |
| PLAN FRAGMENT 1(F03)                                                                                                                                    |
|                                                                                                                                                         |
|   Input Partition: HASH_PARTITIONED: 5: sr_customer_sk, 9: sr_store_sk                                                                                  |
|   OutPut Partition: UNPARTITIONED                                                                                                                       |
|   OutPut Exchange Id: 09                                                                                                                                |
|                                                                                                                                                         |
|   8:AGGREGATE (merge finalize)                                                                                                                          |
|   |  aggregate: sum[([49: sum, DECIMAL128(38,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]         |
|   |  group by: [5: sr_customer_sk, INT, true], [9: sr_store_sk, INT, true]                                                                              |
|   |  cardinality: 49729                                                                                                                                 |
|   |  column statistics:                                                                                                                                 |
|   |  * sr_customer_sk-->[1.0, 100000.0, 2.010867271218169E-5, 4.0, 49728.786461450894] ESTIMATE                                                         |
|   |  * sr_store_sk-->[1.0, 10.0, 0.14285714285714285, 4.0, 6.0] ESTIMATE                                                                                |
|   |  * sum-->[0.0, 16917.12, 0.034878301578357924, 8.0, 49728.786461450894] ESTIMATE                                                                    |
|   |                                                                                                                                                     |
|   7:EXCHANGE                                                                                                                                            |
|      distribution type: SHUFFLE                                                                                                                         |
|      partition exprs: [5: sr_customer_sk, INT, true], [9: sr_store_sk, INT, true]                                                                       |
|      cardinality: 49729                                                                                                                                 |
|                                                                                                                                                         |
| PLAN FRAGMENT 2(F00)                                                                                                                                    |
|                                                                                                                                                         |
|   Input Partition: RANDOM                                                                                                                               |
|   OutPut Partition: HASH_PARTITIONED: 5: sr_customer_sk, 9: sr_store_sk                                                                                 |
|   OutPut Exchange Id: 07                                                                                                                                |
|                                                                                                                                                         |
|   6:AGGREGATE (update serialize)                                                                                                                        |
|   |  STREAMING                                                                                                                                          |
|   |  aggregate: sum[([12: sr_return_amt, DECIMAL64(7,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true] |
|   |  group by: [5: sr_customer_sk, INT, true], [9: sr_store_sk, INT, true]                                                                              |
|   |  cardinality: 49729                                                                                                                                 |
|   |  column statistics:                                                                                                                                 |
|   |  * sr_customer_sk-->[1.0, 100000.0, 2.010867271218169E-5, 4.0, 49728.786461450894] ESTIMATE                                                         |
|   |  * sr_store_sk-->[1.0, 10.0, 0.14285714285714285, 4.0, 6.0] ESTIMATE                                                                                |
|   |  * sum-->[0.0, 16917.12, 0.034878301578357924, 8.0, 49728.786461450894] ESTIMATE                                                                    |
|   |                                                                                                                                                     |
|   5:Project                                                                                                                                             |
|   |  output columns:                                                                                                                                    |
|   |  5 <-> [5: sr_customer_sk, INT, true]                                                                                                               |
|   |  9 <-> [9: sr_store_sk, INT, true]                                                                                                                  |
|   |  12 <-> [12: sr_return_amt, DECIMAL64(7,2), true]                                                                                                   |
|   |  cardinality: 49729                                                                                                                                 |
|   |  column statistics:                                                                                                                                 |
|   |  * sr_customer_sk-->[1.0, 100000.0, 0.034836564480338346, 4.0, 49728.786461450894] ESTIMATE                                                         |
|   |  * sr_store_sk-->[1.0, 10.0, 0.034996556689413386, 4.0, 6.0] ESTIMATE                                                                               |
|   |  * sr_return_amt-->[0.0, 16917.12, 0.034878301578357924, 8.0, 49728.786461450894] ESTIMATE                                                          |
|   |                                                                                                                                                     |
|   4:HASH JOIN                                                                                                                                           |
|   |  join op: INNER JOIN (BROADCAST)                                                                                                                    |
|   |  equal join conjunct: [3: sr_returned_date_sk, INT, true] = [21: d_date_sk, INT, false]                                                             |
|   |  build runtime filters:                                                                                                                             |
|   |  - filter_id = 0, build_expr = (21: d_date_sk), remote = false                                                                                      |
|   |  output columns: 5, 9, 12                                                                                                                           |
|   |  can local shuffle: true                                                                                                                            |
|   |  cardinality: 49729                                                                                                                                 |
|   |  column statistics:                                                                                                                                 |
|   |  * sr_returned_date_sk-->[2450820.0, 2452822.0, 0.0, 4.0, 361.6287128712871] ESTIMATE                                                               |
|   |  * sr_customer_sk-->[1.0, 100000.0, 0.034836564480338346, 4.0, 49728.786461450894] ESTIMATE                                                         |
|   |  * sr_store_sk-->[1.0, 10.0, 0.034996556689413386, 4.0, 6.0] ESTIMATE                                                                               |
|   |  * sr_return_amt-->[0.0, 16917.12, 0.034878301578357924, 8.0, 49728.786461450894] ESTIMATE                                                          |
|   |  * d_date_sk-->[2450820.0, 2452822.0, 0.0, 4.0, 361.6287128712871] ESTIMATE                                                                         |
|   |                                                                                                                                                     |
|   |----3:EXCHANGE                                                                                                                                       |
|   |       distribution type: BROADCAST                                                                                                                  |
|   |       cardinality: 362                                                                                                                              |
|   |                                                                                                                                                     |
|   0:OlapScanNode                                                                                                                                        |
|      table: store_returns, rollup: store_returns                                                                                                        |
|      preAggregation: on                                                                                                                                 |
|      Predicates: 3: sr_returned_date_sk IS NOT NULL                                                                                                     |
|      partitionsRatio=1/1, tabletsRatio=10/10                                                                                                            |
|      tabletList=10241,10243,10245,10247,10249,10251,10253,10255,10257,10259                                                                             |
|      actualRows=287514, avgRowSize=20.0                                                                                                                 |
|      cardinality: 277502                                                                                                                                |
|      probe runtime filters:                                                                                                                             |
|      - filter_id = 0, probe_expr = (3: sr_returned_date_sk)                                                                                             |
|      column statistics:                                                                                                                                 |
|      * sr_returned_date_sk-->[2450820.0, 2452822.0, 0.0, 4.0, 2018.0] ESTIMATE                                                                          |
|      * sr_customer_sk-->[1.0, 100000.0, 0.034836564480338346, 4.0, 86524.0] ESTIMATE                                                                    |
|      * sr_store_sk-->[1.0, 10.0, 0.034996556689413386, 4.0, 6.0] ESTIMATE                                                                               |
|      * sr_return_amt-->[0.0, 16917.12, 0.034878301578357924, 8.0, 113968.0] ESTIMATE                                                                    |
|                                                                                                                                                         |
| PLAN FRAGMENT 3(F01)                                                                                                                                    |
|                                                                                                                                                         |
|   Input Partition: RANDOM                                                                                                                               |
|   OutPut Partition: UNPARTITIONED                                                                                                                       |
|   OutPut Exchange Id: 03                                                                                                                                |
|                                                                                                                                                         |
|   2:Project                                                                                                                                             |
|   |  output columns:                                                                                                                                    |
|   |  21 <-> [21: d_date_sk, INT, false]                                                                                                                 |
|   |  cardinality: 362                                                                                                                                   |
|   |  column statistics:                                                                                                                                 |
|   |  * d_date_sk-->[2415022.0, 2488070.0, 0.0, 4.0, 361.6287128712871] ESTIMATE                                                                         |
|   |                                                                                                                                                     |
|   1:OlapScanNode                                                                                                                                        |
|      table: date_dim, rollup: date_dim                                                                                                                  |
|      preAggregation: on                                                                                                                                 |
|      Predicates: [27: d_year, INT, false] = 2000                                                                                                        |
|      partitionsRatio=1/1, tabletsRatio=5/5                                                                                                              |
|      tabletList=11543,11545,11547,11549,11551                                                                                                           |
|      actualRows=73049, avgRowSize=8.0                                                                                                                   |
|      cardinality: 362                                                                                                                                   |
|      column statistics:                                                                                                                                 |
|      * d_date_sk-->[2415022.0, 2488070.0, 0.0, 4.0, 361.6287128712871] ESTIMATE                                                                         |
|      * d_year-->[2000.0, 2000.0, 0.0, 4.0, 202.0] ESTIMATE                                                                                              |
+---------------------------------------------------------------------------------------------------------------------------------------------------------+
114 rows in set
```

### EXPLAIN COSTS MOCK

`MOCK` post-processes the rendered plan and the analyzed SQL, replacing every column reference with a stable `mock_col_<N>`. Same column always maps to the same mock name in both the SQL block and the plan, so the two can still be correlated when shared externally.

```Plain
MySQL tpcds> explain costs mock select sr_customer_sk, sum(sr_return_amt)
  from store_returns where sr_store_sk = 1 group by sr_customer_sk;
+------------------------------------------------------------+
| Explain String                                             |
+------------------------------------------------------------+
| Mocked SQL:                                                |
| SELECT `mock_col_1`, sum(`mock_col_2`)                     |
| FROM `store_returns`                                       |
| WHERE `mock_col_3` = 1                                     |
| GROUP BY `mock_col_1`                                      |
|                                                            |
| PLAN FRAGMENT 0(F01)                                       |
|   Output Exprs:5: mock_col_1 | 49: sum                     |
|   ...                                                      |
|   0:OlapScanNode                                           |
|      table: store_returns, rollup: store_returns           |
|      Predicates: [9: mock_col_3, INT, true] = 1            |
|      column statistics:                                    |
|      * mock_col_3-->[1.0, 10.0, ...] ESTIMATE              |
|      * mock_col_1-->[1.0, 100000.0, ...] ESTIMATE          |
|      * mock_col_2-->[0.0, 16917.12, ...] ESTIMATE          |
+------------------------------------------------------------+
```

> **NOTE**
>
> - Mocking only covers column names. Table names, database names, literal values, and function names are left untouched. To also digest literal values, set the session variable `enable_desensitize_explain = true` before issuing the query.
> - The mock mapping lives only for the duration of one EXPLAIN; running the same query twice produces consistent mock names within each run but `mock_col_<N>` numbering is not guaranteed to be identical across runs.
> - Column names that collide with SQL keywords or built-in function names may also be replaced in unrelated positions of the output — this is a known limitation of the whole-word substitution approach.

