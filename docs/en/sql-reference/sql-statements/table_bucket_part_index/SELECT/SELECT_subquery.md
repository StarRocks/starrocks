---
displayed_sidebar: docs
sidebar_label: "Subquery"
---

# Subquery

Subqueries are categorized into two types in terms of relevance:

- A non-correlated subquery obtains its results independently of its outer query.
- A correlated subquery requires values from its outer query.

## Non-correlated subquery

Non-correlated subqueries support [NOT] IN and EXISTS.

Examples:

```sql
SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);

SELECT * FROM t1 WHERE (x,y) [NOT] IN (SELECT x,y FROM t2 LIMIT 2);

SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);
```

From v3.0 onwards, you can specify multiple fields in the WHERE clause of `SELECT... FROM... WHERE... [NOT] IN`, for example, `WHERE (x,y)` in the second SELECT statement.

## Correlated subquery

Related subqueries support [NOT] IN and [NOT] EXISTS.

Examples:

```sql
SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);

SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);
```

Subqueries also support scalar quantum queries. It can be divided into irrelevant scalar quantum query, related scalar quantum query and scalar quantum query as parameters of the general function.

## Examples

1. Uncorrelated scalar quantum query with predicate = sign. For example, output information about the person with the highest wage.

    ```sql
    SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
    ```

2. Uncorrelated scalar quantum queries with predicates `>`, `<` etc. For example, output information about people who are paid more than average.

    ```sql
    SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
    ```

3. Related scalar quantum queries. For example, output the highest salary information for each department.

    ```sql
    SELECT name FROM table a WHERE salary = (SELECT MAX(salary) FROM table b WHERE b.Department= a.Department);
    ```

4. Scalar quantum queries are used as parameters of ordinary functions.

    ```sql
    SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));
    ```
