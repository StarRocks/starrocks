---
displayed_sidebar: docs
sidebar_position: 70
---

# Query Hint

Query hints are directives or comments that explicitly suggest the query optimizer on how to execute a query. Currently, StarRocks supports three types of hints: system variable hint (`SET_VAR`), user-defined variable hint (`SET_USER_VARIABLE`), and Join hint. Hints only take effect within a single query.

## System variable hint

You can use a `SET_VAR` hint to set one or more [system variables](../../sql-reference/System_variable.md) in SELECT and SUBMIT TASK statements, and then execute the statements. You can also use a `SET_VAR` hint in the SELECT clause included in other statements, such as CREATE MATERIALIZED VIEW AS SELECT and CREATE VIEW AS SELECT. Note that if the `SET_VAR` hint is used in the SELECT clause of CTE, the `SET_VAR` hint does not take effect even if the statement is executed successfully.

Compared with [the general usage of system variables](../../sql-reference/System_variable.md) which takes effect at the session level, the `SET_VAR` hint takes effect at the statement level and does not impact the entire session.

### Syntax

```SQL
[...] SELECT /*+ SET_VAR(key=value [, key = value]) */ ...
SUBMIT [/*+ SET_VAR(key=value [, key = value]) */] TASK ...
```

### Examples

To specify the aggregation mode for an aggregate query, use the `SET_VAR` hint to set the system variables `streaming_preaggregation_mode` and `new_planner_agg_stage` in the aggregate query.

```SQL
SELECT /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming',new_planner_agg_stage = '2') */ SUM(sales_amount) AS total_sales_amount FROM sales_orders;
```

To specify the execution timeout for a SUBMIT TASK statement, use the `SET_VAR` Hint to set the system variable `insert_timeout` in the SUBMIT TASK statement.

```SQL
SUBMIT /*+ SET_VAR(insert_timeout=3) */ TASK AS CREATE TABLE temp AS SELECT count(*) AS cnt FROM tbl1;
```

To specify the subquery execution timeout for creating a materialized view, use the `SET_VAR` hint to set the system variable `query_timeout` in the SELECT clause.

```SQL
CREATE MATERIALIZED VIEW mv 
PARTITION BY dt 
DISTRIBUTED BY HASH(`key`) 
BUCKETS 10 
REFRESH ASYNC 
AS SELECT /*+ SET_VAR(query_timeout=500) */ * from dual;
```

Specify system variables in a nested query:

```SQL
-- To specify hints in the main query
WITH t AS (SELECT region, sales_amount FROM sales_orders)  
SELECT /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming', new_planner_agg_stage = '2') */  
       SUM(sales_amount) AS total_sales_amount  
FROM t;

-- To specify hints in the subquery
WITH t AS (  
  SELECT /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */  
         region, sales_amount  
  FROM sales_orders  
)  
SELECT SUM(sales_amount) AS total_sales_amount  
FROM t;
```

## User-defined variable hint

You can use a `SET_USER_VARIABLE` hint to set one or more [user-defined variables](../../sql-reference/user_defined_variables.md) in the SELECT statements or INSERT statements. If other statements contain a SELECT clause, you can also use the `SET_USER_VARIABLE` hint in that SELECT clause. Other statements can be SELECT statements and INSERT statements, but cannot be CREATE MATERIALIZED VIEW AS SELECT statements and CREATE VIEW AS SELECT statements. Note that if the `SET_USER_VARIABLE` hint is used in the SELECT clause of CTE, the `SET_USER_VARIABLE` hint does not take effect even if the statement is executed successfully. Since v3.2.4, StarRocks supports the user-defined variable hint.

Compared with [the general usage of user-defined variables](../../sql-reference/user_defined_variables.md) which takes effect at the session level, the `SET_USER_VARIABLE` hint takes effect at the statement level and does not impact the entire session.

### Syntax

```SQL
[...] SELECT /*+ SET_USER_VARIABLE(@var_name = expr [, @var_name = expr]) */ ...
INSERT /*+ SET_USER_VARIABLE(@var_name = expr [, @var_name = expr]) */ ...
```

### Examples

The following SELECT statement references scalar subqueries `select max(age) from users` and `select min(name) from users`, so you can use a `SET_USER_VARIABLE` hint to set these two scalar subqueries as user-defined variables and then run the query.

```SQL
SELECT /*+ SET_USER_VARIABLE (@a = (select max(age) from users), @b = (select min(name) from users)) */ * FROM sales_orders where sales_orders.age = @a and sales_orders.name = @b;
```

## Join hint

For multi-table Join queries, the optimizer usually selects the optimal Join execution method. In special cases, you can use a Join hint to explicitly suggest the Join execution method to the optimizer or disable Join Reorder. Currently, a Join hint supports suggesting Shuffle Join, Broadcast Join, Bucket Shuffle Join, or Colocate Join as a Join execution method. When a Join hint is used, the optimizer does not perform Join Reorder. So you need to select the smaller table as the right table. Additionally, when suggesting [Colocate Join](../../using_starrocks/Colocate_join.md) or Bucket Shuffle Join as the Join execution method, make sure that the data distribution of the joined table meets the requirements of these Join execution methods. Otherwise, the suggested Join execution method cannot take effect.

### Syntax

```SQL
... JOIN { [BROADCAST] | [SHUFFLE] | [BUCKET] | [COLOCATE] | [UNREORDER]} ...
```

:::note
Join Hint is case-insensitive.
:::

### Examples

- Shuffle Join

  If you need to shuffle the data rows with the same bucketing key values from tables A and B onto the same machine before a Join operation is performed, you can hint the Join execution method as Shuffle Join.

  ```SQL
  select k1 from t1 join [SHUFFLE] t2 on t1.k1 = t2.k2 group by t2.k2;
  ```

- Broadcast Join
  
  If table A is a large table and table B is a small table, you can hint the Join execution method as Broadcast Join. The data of the table B is fully broadcasted to the machines on which the data of table A resides, and then the Join operation is performed. Compared to Shuffle Join, Broadcast Join saves the cost of shuffling the data of table A.

  ```SQL
  select k1 from t1 join [BROADCAST] t2 on t1.k1 = t2.k2 group by t2.k2;
  ```

- Bucket Shuffle Join
  
  If the Join equijoin expression in the Join query contains the bucketing key of table A, especially when both tables A and B are large tables, you can hint the Join execution method as Bucket Shuffle Join. The data of table B is shuffled to the machines on which the data of table A resides, according to the data distribution of table A, and then the Join operation is performed. Compared to Broadcast Join, Bucket Shuffle Join significantly reduces data transferring because the data of table B is shuffled only once globally.
  Tables participating in Bucket Shuffle Join must be either non-partitioned or colocated.

  ```SQL
  select k1 from t1 join [BUCKET] t2 on t1.k1 = t2.k2 group by t2.k2;
  ```

- Colocate Join
  
  If tables A and B belong to the same Colocation Group which is specified during table creation, the data rows with the same bucketing key values from tables A and B are distributed on the same BE node. When the Join equijoin expression contains the bucketing key of tables A and B in the Join query, you can hint the Join execution method as Colocate Join. Data with the same key values are directly joined locally, reducing the time spent on data transmission between nodes and improving query performance.

  ```SQL
  select k1 from t1 join [COLOCATE] t2 on t1.k1 = t2.k2 group by t2.k2;
  ```

### View Join execution method

Use the `EXPLAIN` command to view the actual Join execution method. If the returned result shows that the Join execution method matches the Join hint, it means that the Join hint is effective.

```SQL
EXPLAIN select k1 from t1 join [COLOCATE] t2 on t1.k1 = t2.k2 group by t2.k2;
```

![8-9](../../_assets/8-9.png) 
