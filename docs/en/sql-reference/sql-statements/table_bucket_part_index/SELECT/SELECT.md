---
displayed_sidebar: docs
---

# SELECT

SELECT queries data from one or more tables, views, or materialized views. SELECT can work as an independent statement or a clause nested in other statements. The output of the SELECT clause can be used as the input of other statements.

StarRocks' query statement basically conforms to the SQL92 standard.

:::note
To query data from tables, views, or materialized views in a StarRocks internal table, you must have the SELECT privilege on these objects. To query data from tables, views, or materialized views in an external data source, you must have the USAGE privilege on the corresponding external catalog.
:::

## Syntax

```SQL
SELECT select_list FROM table_reference [, ...]
```

The SELECT statement generally consists of the following clauses:

- [Common Table Expression (CTE)](./SELECT_CTE.md)
- [JOINs](./SELECT_JOIN.md)
- [ORDER BY](./SELECT_ORDER_BY.md)
- [GROUP BY](./SELECT_GROUP_BY.md)
- [HAVING](./SELECT_HAVING.md)
- [LIMIT](./SELECT_LIMIT.md)
- [OFFSET](./SELECT_OFFSET.md)
- [UNION](./SELECT_UNION.md)
- [INTERSECT](./SELECT_INTERSECT.md)
- [EXCEPT/MINUS](./SELECT_EXCEPT_MINUS.md)
- [DISTINCT](./SELECT_DISTINCT.md)
- [Subquery](./SELECT_subquery.md)
- [WHERE and operators](./SELECT_WHERE_operator.md)
- [Alias](./SELECT_alias.md)
- [PIVOT](./SELECT_PIVOT.md)
- [EXCLUDE](./SELECT_EXCLUDE.md)

For detailed instructions, refer to the corresponding topics.
