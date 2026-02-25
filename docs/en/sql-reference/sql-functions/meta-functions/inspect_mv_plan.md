---
displayed_sidebar: docs
---

# inspect_mv_plan

`inspect_mv_plan(mv_name)`
`inspect_mv_plan(mv_name, use_cache)`

These functions return the logical plan of a materialized view.

## Arguments

`mv_name`: The name of the materialized view (VARCHAR).
`use_cache`: (Optional) A boolean value indicating whether to use the materialized view plan cache. Defaults to `TRUE`.

## Return Value

Returns a VARCHAR string containing the logical plan of the materialized view.

## Examples

Example 1: Inspect a materialized view's logical plan using the plan cache:
```
mysql> select inspect_mv_plan('mv_on_view_1');
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| inspect_mv_plan('mv_on_view_1')                                                                                                                                                                                                                                                                                                                                                                                             |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan 0:
LogicalAggregation {type=GLOBAL ,aggregations={3: sum=sum(2: pv)} ,groupKeys=[1: event_day] ,projection=null ,predicate=null}
->  LogicalOlapScanOperator {table=28673, selectedPartitionId=null, selectedIndexId=28674, outputColumns=[1: event_day, 2: pv], predicate=null, prunedPartitionPredicates=[], limit=-1}
plan 1:
LogicalViewScanOperator {table='view1', outputColumns='[4: event_day, 5: sum_pv]'}
 |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

```

Example 2: Inspect a materialized view's logical plan without using the plan cache:
```
mysql> select inspect_mv_plan('mv_on_view_1', false);
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| inspect_mv_plan('mv_on_view_1', FALSE)                                                                                                                                                                                                                                                                                                                                                                                      |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan 0:
LogicalAggregation {type=GLOBAL ,aggregations={3: sum=sum(2: pv)} ,groupKeys=[1: event_day] ,projection=null ,predicate=null}
->  LogicalOlapScanOperator {table=28673, selectedPartitionId=null, selectedIndexId=28674, outputColumns=[1: event_day, 2: pv], predicate=null, prunedPartitionPredicates=[], limit=-1}
plan 1:
LogicalViewScanOperator {table='view1', outputColumns='[4: event_day, 5: sum_pv]'}
 |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

```