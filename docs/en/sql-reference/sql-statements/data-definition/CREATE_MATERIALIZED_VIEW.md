# CREATE MATERIALIZED VIEW

## Description

This statement is used to create materialized views.

> **NOTE**
>
> CREATE MATERIALIZED VIEW is an asynchronous statement. Executing this statement successfully only indicates that the task to create the materialized view is successfully submitted. You can check the progress by using SHOW ALTER TABLE ROLLUP statement. After the progress is FINISHED, you can use the DESC table_name ALL statement to check the schema of the materialized view.

## Syntax

```sql
CREATE MATERIALIZED VIEW materialized_view_name AS query
```

## Parameters

1. materialized_view_name

    Name of the materialized view. Required.

    Materialized view names in the same table cannot be duplicated.

2. query

    The query used to construct the materialized view. The result of the query is the data of the materialized view. Currently, the supported query format is:

    ```sql
    SELECT select_expr[, select_expr ...]
    FROM [base_view_name]
    GROUP BY column_name[, column_name ...]
    ORDER BY column_name[, column_name ...]
    ```

    The syntax is the same as the query syntax.

    select_expr: All columns in the materialized view's schema.
    + Only single columns and aggregate columns without expression calculation are supported.  
    + The aggregate function currently only supports SUM, MIN, MAX, and the parameters of the aggregate function can only be a single column without expression calculation.  
    + Contains at least one single column.  
    + All involved columns can only appear once.

    base_view_name: The original table name of the materialized view. Required.
    + Must be a single table and not a subquery

    GROUP BY: Grouped column of materialized view. Optional.
    + The data not filled will not be grouped.

    ORDER BY: sort order of materialized view. Optional.
    + The order of the column sort must be the same as the column declaration order in select_expr.  

    + If ORDER BY is not declared, sort columns are automatically supplemented by rules.

      If the materialized view is of an aggregate type, all grouped columns are automatically supplemented with sort columns.

      If the materialized view is of a non-aggregated type, the first 36 bytes are automatically supplemented as a sort column.

      If the number of sorts automatically supplemented is less than three, the first three are sorted.  

    + If the query contains a grouped column, the sort order must be the same as the grouped column.

## Usage notes

- The current version of StarRocks does not support creating multiple materialized views at the same time. A new materialized view can only be created when the one before is completed.
- Synchronous materialized views only support aggregate functions on a single column. Query statements in the form of `sum(a+b)` are not supported.
- Synchronous materialized views support only one aggregate function for each column of the base table. Query statements such as `select sum(a), min(a) from table` are not supported.
- When creating a synchronous materialized view with an aggregate function, you must specify the GROUP BY clause, and specify at least one GROUP BY column in SELECT.
- Synchronous materialized views do not support clauses such as JOIN, WHERE, and the HAVING clause of GROUP BY.
- When using ALTER TABLE DROP COLUMN to drop a specific column in a base table, you must ensure that all synchronous materialized views of the base table do not contain the dropped column, otherwise the drop operation will fail. Before you drop the column, you must first drop all synchronous materialized views that contain the column.
- Creating too many synchronous materialized views for a table will affect the data load efficiency. When data is being loaded to the base table, the data in synchronous materialized view and base table will be updated synchronously. If a base table contains `n` synchronous materialized views, the efficiency of loading data into the base table is about the same as the efficiency of loading data into `n` tables.

## Example

Base table structure is:

```Plain Text
mysql> desc duplicate_table;
+-------+--------+------+------+---------+-------+
| Field | Type   | Null | Key  | Default | Extra |
+-------+--------+------+------+---------+-------+
| k1    | INT    | Yes  | true | N/A     |       |
| k2    | INT    | Yes  | true | N/A     |       |
| k3    | BIGINT | Yes  | true | N/A     |       |
| k4    | BIGINT | Yes  | true | N/A     |       |
+-------+--------+------+------+---------+-------+
```

1. Create a materialized view containing only the columns of the original table (k1, k2).

    ```sql
    create materialized view k1_k2 as
    select k1, k2 from duplicate_table;
    ```

    The materialized view contains only two columns k1, k2 without any aggregation.

    ```plain text
    +-----------------+-------+--------+------+------+---------+-------+
    | IndexName       | Field | Type   | Null | Key  | Default | Extra |
    +-----------------+-------+--------+------+------+---------+-------+
    | k1_k2           | k1    | INT    | Yes  | true | N/A     |       |
    |                 | k2    | INT    | Yes  | true | N/A     |       |
    +-----------------+-------+--------+------+------+---------+-------+
    ```

2. Create a materialized view sorted by k2.

    ```sql
    create materialized view k2_order as
    select k2, k1 from duplicate_table order by k2;
    ```

    The materialized view's schema is shown below. The materialized view contains only two columns k2, k1, where column k2 is a sort column without any aggregation.

    ```plain text
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k1    | INT    | Yes  | false | N/A     | NONE  |
    +-----------------+-------+--------+------+-------+---------+-------+
    ```

3. Create a materialized view grouped by k1, k2 with k3 as the SUM aggregate.

    ```sql
    create materialized view k1_k2_sumk3 as
    select k1, k2, sum(k3) from duplicate_table group by k1, k2;
    ```

    The materialized view's schema is shown below. The materialized view contains two columns k1, k2 and sum (k3), where k1, k2 are grouped columns, and sum (k3) is the sum of the k3 columns grouped according to k1, k2.

    Because the materialized view does not declare a sort column, and the materialized view has aggregate data, the system supplements the grouped columns k1 and k2 by default.

    ```plain text
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
    |                 | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
    +-----------------+-------+--------+------+-------+---------+-------+
    ```

4. Create a materialized view to remove duplicate rows.

    ```sql
    create materialized view deduplicate as
    select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
    ```

    The materialized view schema is shown below. The materialized view contains k1, k2, k3, and k4 columns, and there are no duplicate rows.

    ```plain text
    +-----------------+-------+--------+------+-------+---------+-------+
    | IndexName       | Field | Type   | Null | Key   | Default | Extra |
    +-----------------+-------+--------+------+-------+---------+-------+
    | deduplicate     | k1    | INT    | Yes  | true  | N/A     |       |
    |                 | k2    | INT    | Yes  | true  | N/A     |       |
    |                 | k3    | BIGINT | Yes  | true  | N/A     |       |
    |                 | k4    | BIGINT | Yes  | true  | N/A     |       |
    +-----------------+-------+--------+------+-------+---------+-------+
    
    ```

5. Create a non-aggregated materialized view that does not declare a sort column.

    The schema of all_type_table is shown below:

    ```plain text
    +-------+--------------+------+-------+---------+-------+
    | Field | Type         | Null | Key   | Default | Extra |
    +-------+--------------+------+-------+---------+-------+
    | k1    | TINYINT      | Yes  | true  | N/A     |       |
    | k2    | SMALLINT     | Yes  | true  | N/A     |       |
    | k3    | INT          | Yes  | true  | N/A     |       |
    | k4    | BIGINT       | Yes  | true  | N/A     |       |
    | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
    | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
    | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
    +-------+--------------+------+-------+---------+-------+
    ```

    The materialized view contains k3, k4, k5, k6, k7 columns, and no sort column is declared. The creation statement is as follows:

    ```sql
    create materialized view mv_1 as
    select k3, k4, k5, k6, k7 from all_type_table;
    ```

    The system's default supplementary sort columns are k3, k4, and k5. The sum of the number of bytes for these three column types is `4 (INT) + 8 (BIGINT) + 16 (DECIMAL) = 28 <36`. So these three columns are added as sort columns.

    The materialized view's schema is as follows. You can see that the key fields of the k3, k4, and k5 columns are true, which is the sort order. The key field of the k6, k7 columns is false, which is the non-sort order.

    ```plain text
    +----------------+-------+--------------+------+-------+---------+-------+
    | IndexName      | Field | Type         | Null | Key   | Default | Extra |
    +----------------+-------+--------------+------+-------+---------+-------+
    | mv_1           | k3    | INT          | Yes  | true  | N/A     |       |
    |                | k4    | BIGINT       | Yes  | true  | N/A     |       |
    |                | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
    |                | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
    |                | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
    +----------------+-------+--------------+------+-------+---------+-------+
    ```

## keyword

CREATE, MATERIALIZED, VIEW
