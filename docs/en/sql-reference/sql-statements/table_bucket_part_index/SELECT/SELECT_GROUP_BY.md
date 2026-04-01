---
displayed_sidebar: docs
sidebar_label: "GROUP BY"
---

# GROUP BY

The GROUP BY clause is often used with aggregate functions. Columns specified in the GROUP BY clause will not participate in the aggregation operation.

## Syntax

```sql
SELECT
...
aggregate_function() [ FILTER ( where boolean_expression ) ]
...
FROM ...
[ ... ]
GROUP BY [
    , ... |
    GROUPING SETS [, ...] (  groupSet [ , groupSet [ , ... ] ] ) |
    ROLLUP(expr  [ , expr [ , ... ] ]) |
    CUBE(expr  [ , expr [ , ... ] ])
    ]
[ ... ]
```

## Parameters

- `FILTER` can be used together with aggregate functions. Only filtered rows will participate in the calculation of the aggregate function.

  > **NOTE**
  >
  > - The FILTER clause is only supported in AVG, COUNT, MAX, MIN, SUM, ARRAY_AGG, and ARRAY_AGG_DISTINCT functions.
  > - The FILTER clause is not supported for COUNT DISTINCT.
  > - When the FILTER clause is specified, ORDER BY clauses are not allowed within ARRAY_AGG and ARRAY_AGG_DISTINCT functions.

- `GROUPING SETS`, `CUBE`, and `ROLLUP` are extensions of the GROUP BY clause. In a GROUP BY clause, they can be used to achieve grouped aggregations of multiple sets. The results are equivalent to that of the UNION of multiple GROUP BY clauses.

## Examples

Example 1: `FILTER`

  The following two queries are equivalent.

  ```sql
  SELECT
    COUNT(*) AS total_users,
    SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male_users,
    SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female_users
  FROM users;
  ```
  
  ```sql
  SELECT
    COUNT(*) AS total_users,
    COUNT(*) FILTER (WHERE gender = 'M') AS male_users,
    COUNT(*) FILTER (WHERE gender = 'F') AS female_users
  FROM users;
  ```

Example 2: `GROUPING SETS`, `CUBE`, and `ROLLUP`

  `ROLLUP(a,b,c)` is equivalent to the following `GROUPING SETS` clause.
  
    ```sql
    GROUPING SETS (
    (a,b,c),
    (a,b  ),
    (a    ),
    (     )
    )
    ```
  
  `CUBE (a, b, c)` is equivalent to the following `GROUPING SETS` clause.
  
    ```sql
    GROUPING SETS (
    ( a, b, c ),
    ( a, b    ),
    ( a,    c ),
    ( a       ),
    (    b, c ),
    (    b    ),
    (       c ),
    (         )
    )
    ```
  
  Test in an actual dataset.
  
    ```sql
    SELECT * FROM t;
    +------+------+------+
    | k1   | k2   | k3   |
    +------+------+------+
    | a    | A    |    1 |
    | a    | A    |    2 |
    | a    | B    |    1 |
    | a    | B    |    3 |
    | b    | A    |    1 |
    | b    | A    |    4 |
    | b    | B    |    1 |
    | b    | B    |    5 |
    +------+------+------+
    8 rows in set (0.01 sec)
  
    SELECT k1, k2, SUM(k3) FROM t
    GROUP BY GROUPING SETS ( (k1, k2), (k2), (k1), ( ) );
    +------+------+-----------+
    | k1   | k2   | sum(`k3`) |
    +------+------+-----------+
    | b    | B    |         6 |
    | a    | B    |         4 |
    | a    | A    |         3 |
    | b    | A    |         5 |
    | NULL | B    |        10 |
    | NULL | A    |         8 |
    | a    | NULL |         7 |
    | b    | NULL |        11 |
    | NULL | NULL |        18 |
    +------+------+-----------+
    9 rows in set (0.06 sec)
  
    SELECT k1, k2, GROUPING_ID(k1,k2), SUM(k3) FROM t
    GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ());
    +------+------+---------------+----------------+
    | k1   | k2   | grouping_id(k1,k2) | sum(`k3`) |
    +------+------+---------------+----------------+
    | a    | A    |             0 |              3 |
    | a    | B    |             0 |              4 |
    | a    | NULL |             1 |              7 |
    | b    | A    |             0 |              5 |
    | b    | B    |             0 |              6 |
    | b    | NULL |             1 |             11 |
    | NULL | A    |             2 |              8 |
    | NULL | B    |             2 |             10 |
    | NULL | NULL |             3 |             18 |
    +------+------+---------------+----------------+
    9 rows in set (0.02 sec)
    ```
