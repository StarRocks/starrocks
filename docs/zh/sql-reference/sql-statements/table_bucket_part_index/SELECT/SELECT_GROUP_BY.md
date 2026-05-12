---
displayed_sidebar: docs
sidebar_label: "GROUP BY"
---

# GROUP BY

GROUP BY 语句通常与聚合函数一起使用。GROUP BY 语句中指定的列不参与聚合运算。

## 语法

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

## 参数

- `FILTER` 可以与聚合函数一起使用。只有经过筛选的行才会参与聚合函数的计算。

  > **注意**
  >
  > - `FILTER` 子句仅支持 AVG、COUNT、MAX、MIN、SUM、ARRAY_AGG 和 ARRAY_AGG_DISTINCT 函数。
  > - `FILTER` 子句不支持 COUNT DISTINCT。
  > - 指定 `FILTER` 子句后，ARRAY_AGG 和 ARRAY_AGG_DISTINCT 函数中不允许使用 ORDER BY 子句。

- `GROUPING SETS`、`CUBE` 和 `ROLLUP` 是 GROUP BY 子句的扩展。在 GROUP BY 子句中，它们可用于实现多组分组聚合。结果等同于多个 GROUP BY 子句的 UNION 结果。

## 示例

示例 1：`FILTER`

  以下两个查询是等效的。

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

示例 2: `GROUPING SETS`、`CUBE` 和 `ROLLUP`

  `ROLLUP(a,b,c)` 等价于以下 `GROUPING SETS` 语句。

    ```sql
    GROUPING SETS (
    (a,b,c),
    (a,b  ),
    (a    ),
    (     )
    )
    ```

`CUBE (a, b, c)` 等价于以下 `GROUPING SETS` 语句。

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

在一个真实的数据集中进行测试。

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
