---
displayed_sidebar: docs
sidebar_label: "PIVOT"
---

# PIVOT

This feature is supported from v3.3 onwards.

The PIVOT operation is an advanced feature in SQL that allows you to transform rows into columns in a table, which is particularly useful for creating pivot tables. This comes in handy when dealing with database reports or analytics, especially when you need to summarize or categorize data for presentation.

Actually, the PIVOT is a syntax sugar, which can simplify the writing of the query statement like `sum(case when ... then ... end)`.

## Syntax

```sql
pivot:
SELECT ...
FROM ...
PIVOT (
  aggregate_function(<expr>) [[AS] alias] [, aggregate_function(<expr>) [[AS] alias] ...]
  FOR <pivot_column>
  IN (<pivot_value>)
)

pivot_column:
<column_name> 
| (<column_name> [, <column_name> ...])

pivot_value:
<literal> [, <literal> ...]
| (<literal>, <literal> ...) [, (<literal>, <literal> ...)]
```

## Parameters

In a PIVOT operation, you need to specify several key components:

- aggregate_function(): An aggregate function such as SUM, AVG, COUNT, etc., used to summarize the data.
- alias: An alias for the aggregated result, making the outcome more understandable.
- FOR pivot_column: Specifies the column name on which the row-to-column transformation will be performed.
- IN (pivot_value): Specifies the specific values of the pivot_column that will be transformed into columns.

## Examples

```sql
create table t1 (c0 int, c1 int, c2 int, c3 int);
SELECT * FROM t1 PIVOT (SUM(c1) AS sum_c1, AVG(c2) AS avg_c2 FOR c3 IN (1, 2, 3, 4, 5));
-- The result is equivalent to the following query:
SELECT SUM(CASE WHEN c3 = 1 THEN c1 ELSE NULL END) AS sum_c1_1,
       AVG(CASE WHEN c3 = 1 THEN c2 ELSE NULL END) AS avg_c2_1,
       SUM(CASE WHEN c3 = 2 THEN c1 ELSE NULL END) AS sum_c1_2,
       AVG(CASE WHEN c3 = 2 THEN c2 ELSE NULL END) AS avg_c2_2,
       SUM(CASE WHEN c3 = 3 THEN c1 ELSE NULL END) AS sum_c1_3,
       AVG(CASE WHEN c3 = 3 THEN c2 ELSE NULL END) AS avg_c2_3,
       SUM(CASE WHEN c3 = 4 THEN c1 ELSE NULL END) AS sum_c1_4,
       AVG(CASE WHEN c3 = 4 THEN c2 ELSE NULL END) AS avg_c2_4,
       SUM(CASE WHEN c3 = 5 THEN c1 ELSE NULL END) AS sum_c1_5,
       AVG(CASE WHEN c3 = 5 THEN c2 ELSE NULL END) AS avg_c2_5
FROM t1
GROUP BY c0;
```
