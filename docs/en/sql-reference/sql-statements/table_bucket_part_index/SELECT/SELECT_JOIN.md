---
displayed_sidebar: docs
sidebar_label: "JOIN"
---

# JOIN

Join operations combine data from two or more tables and then return a result set of some columns from some of them.

StarRocks supports the following joins:
- [Self Join](#self-join)
- [Cross Join](#cross-join)
- [Inner Join](#inner-join)
- [Outer Join](#outer-join) (including Left Join, Right Join, and Full Join)
- [Semi Join](#semi-join)
- [Anti Join](#anti-join)
- [Equi-join and Non-equi-join](#equi-join-and-non-equi-join)
- [Join with the USING clause](#join-with-the-using-clause)
- [ASOF Join](#asof-join)

## Syntax

```sql
SELECT select_list FROM
table_or_subquery1 [INNER] JOIN table_or_subquery2 |
table_or_subquery1 {LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER]} JOIN table_or_subquery2 |
table_or_subquery1 {LEFT | RIGHT} SEMI JOIN table_or_subquery2 |
table_or_subquery1 {LEFT | RIGHT} ANTI JOIN table_or_subquery2 |
[ ON col1 = col2 [AND col3 = col4 ...] |
USING (col1 [, col2 ...]) ]
[other_join_clause ...]
[ WHERE where_clauses ]
```

```sql
SELECT select_list FROM
table_or_subquery1, table_or_subquery2 [, table_or_subquery3 ...]
[other_join_clause ...]
WHERE
col1 = col2 [AND col3 = col4 ...]
```

```sql
SELECT select_list FROM
table_or_subquery1 CROSS JOIN table_or_subquery2
[other_join_clause ...]
[ WHERE where_clauses ]
```

## Self Join

StarRocks supports self-joins. For example, different columns of the same table are joined.

There is actually no special syntax to identify self-join. The conditions on both sides of a join in a self-join come from the same table.

We need to assign them different aliases.

Examples:

```sql
SELECT lhs.id, rhs.parent, lhs.c1, rhs.c2 FROM tree_data lhs, tree_data rhs WHERE lhs.id = rhs.parent;
```

## Cross Join

Cross join can produce a lot of results, so cross join should be used with caution.

Even if you need to use cross join, you need to use filter conditions and ensure that fewer results are returned. Example:

```sql
SELECT * FROM t1, t2;

SELECT * FROM t1 CROSS JOIN t2;
```

## Inner Join

Inner join is the most well-known and commonly used join. Returns results from columns requested by two similar tables, joined if the columns of both tables contain the same value.

If a column name of both tables is the same, we need to use the full name (in the form of table_name.column_name) or alias the column name.

Examples:

The following three queries are equivalent.

```sql
SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
```

## Outer Join

Outer join returns the left or right table or all rows of both. If there is no matching data in another table, set it to NULL. Example:

```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

## Equal and unequal join

Usually, equal joins are the most commonly used joins. It requires the operator of the join condition to be an equal sign.

Unequal join can be used on join conditions `!=`. Unequal joins produce a large number of results and may exceed the memory limit during calculation.

Use with caution. Unequal join only supports Inner Join. Example:

```sql
SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id > t2.id;
```

## Semi Join

Left semi join returns only the rows in the left table that match the data in the right table, regardless of how many rows match the data in the right table.

This row of the left table is returned at most once. Right semi join works similarly, except that the data returned is a right table.

Examples:

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;
```

## Anti Join

Left anti join only returns rows from the left table that do not match the right table.

Right anti join reverses this comparison, returning only rows from the right table that do not match the left table. Example:

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;
```

## Equi-join and Non-equi-join

The various joins supported by StarRocks can be classified as equi-joins and non-equi-joins depending on the join conditions specified in the joins.

| **Join Type**  | **Variants**                                                                  |
| -------------- | ----------------------------------------------------------------------------- |
| Equi-joins     | Self joins, cross joins, inner joins, outer joins, semi joins, and anti joins |
| Non-equi-joins | cross joins, inner joins, left semi joins, left anti joins, and outer joins   |

- Equi-joins
  
  An equi-join uses a join condition in which two join items are combined by the `=` operator. Example: `a JOIN b ON a.id = b.id`.

- Non-equi-joins
  
  A non-equi-join uses a join condition in which two join items are combined by a comparison operator such as `<`, `<=`, `>`, `>=`, or `<>`. Example: `a JOIN b ON a.id < b.id`. Non-equi-joins run slower than equi-joins. We recommend that you exercise caution when you use non-equi-joins.

  The following two examples show how to run non-equi-joins:

  ```SQL
  SELECT t1.id, c1, c2 
  FROM t1 
  INNER JOIN t2 ON t1.id < t2.id;
    
  SELECT t1.id, c1, c2 
  FROM t1 
  LEFT JOIN t2 ON t1.id > t2.id;
  ```

## Join with the USING clause

From v4.0.2 onwards, StarRocks supports specifying join conditions via the `USING` clause in addition to `ON`. It helps to simplify equi-joins with columns of the same name. For example: `SELECT * FROM t1 JOIN t2 USING (id)`.

**Differences between versions:**

- **Versions before v4.0.2**
  
  `USING` is treated as syntactic sugar and internally converted to an `ON` condition. The result would include USING columns from both the left and right tables as separate columns, and table alias qualifiers (for example, `t1.id`) were allowed when referencing USING columns.

  Example:

  ```SQL
  SELECT t1.id, t2.id FROM t1 JOIN t2 USING (id);  -- Returns two separate id columns
  ```

- **v4.0.2 and later**
  
  StarRocks implements SQL-standard `USING` semantics. Key features include:
  
  - All join types are supported, including `FULL OUTER JOIN`.
  - USING columns appear as a single coalesced column in results. For FULL OUTER JOIN, the `COALESCE(left.col, right.col)` semantics is used.
  - Table alias qualifiers (for example, `t1.id`) are no longer supported for USING columns. You must use unqualified column names (for example, `id`).
  - For the result of `SELECT *`, the column order is `[USING columns, left non-USING columns, right non-USING columns]`.

  Example:

  ```SQL
  SELECT t1.id FROM t1 JOIN t2 USING (id);        -- ❌ Error: Column 'id' is ambiguous
  SELECT id FROM t1 JOIN t2 USING (id);           -- ✅ Correct: Returns a single coalesced 'id' column
  SELECT * FROM t1 FULL OUTER JOIN t2 USING (id); -- ✅ FULL OUTER JOIN is supported
  ```

These changes align StarRocks' behavior with SQL-standard compliant databases.

## ASOF Join

An ASOF Join is a type of temporal or range-based join commonly used in time-series analytics. It allows joining two tables based on equality of certain keys and a non-equality condition on time or sequence fields, for example, `t1.time >= t2.time`. The ASOF Join selects the most recent matching row from the right-side table for each row on the left-side table. Supported from v4.0 onwards.

In real-world scenarios, analytics involving time-series data often encounter the following challenges:
- Data collection timing misalignment (for example, different sensor sampling times)
- Small discrepancies between event occurrence and recording times
- Need to find the closest historical record for a given timestamp

Traditional equality joins (INNER Join) often result in significant data loss when handling such data, while inequality joins can lead to performance issues. The ASOF Join was designed to address these specific challenges.

ASOF Joins are commonly used in the following cases:

- **Financial Market Analysis**
  - Matching stock prices with trading volumes
  - Aligning data from different markets
  - Derivative pricing reference data matching
- **IoT Data Processing**
  - Aligning multiple sensor data streams
  - Correlating device state changes
  - Time-series data interpolation
- **Log Analysis**
  - Correlating system events with user actions
  - Matching logs from different services
  - Fault analysis and problem tracking

Syntax:

```SQL
SELECT [select_list]
FROM left_table [AS left_alias]
ASOF LEFT JOIN right_table [AS right_alias]
    ON equality_condition
    AND asof_condition
[WHERE ...]
[ORDER BY ...]
```

- `ASOF LEFT JOIN`: Performs a non-equality join based on the nearest match in time or sequence. ASOF LEFT JOIN returns all rows from the left table, filling unmatched right-side rows with NULL.
- `equality_condition`: A standard equality constraint (for example, matching ticker symbols or IDs).
- `asof_condition`: A range condition typically written as `left.time >= right.time`, indicating to search the most recent `right.time` records that does not exceed `left.time`. 

:::note
Only DATE and DATETIME types are supported in `asof_condition`. And only one `asof_condition` is supported.
:::

Example:

```SQL
SELECT *
FROM holdings h ASOF LEFT JOIN prices p             
ON h.ticker = p.ticker            
AND h.when >= p.when
ORDER BY ALL;
```

Limitations:

- Currently, only Inner Join (default) and Left Outer Join are supported.
- Only DATE and DATETIME types are supported in `asof_condition`.
- Only one `asof_condition` is supported.
