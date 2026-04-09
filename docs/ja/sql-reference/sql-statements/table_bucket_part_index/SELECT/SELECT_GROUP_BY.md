---
displayed_sidebar: docs
sidebar_label: "GROUP BY"
---

# GROUP BY

GROUP BY句は、集計関数と組み合わせて使用されることがよくあります。GROUP BY句で指定された列は、集計演算には参加しません。

## 構文

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

## パラメータ

- `FILTER` は、集計関数と一緒に使用できます。フィルターされた行のみが集計関数の計算に参加します。

  > **NOTE**
  >
  > - FILTER句は、AVG、COUNT、MAX、MIN、SUM、ARRAY_AGG、およびARRAY_AGG_DISTINCT関数でのみサポートされています。
  > - FILTER句は、COUNT DISTINCTではサポートされていません。
  > - FILTER句が指定されている場合、ARRAY_AGG関数およびARRAY_AGG_DISTINCT関数内ではORDER BY句は許可されません。

- `GROUPING SETS` 、 `CUBE` 、および `ROLLUP` は、GROUP BY句の拡張です。 GROUP BY句では、複数のセットのグループ化された集計を実現するために使用できます。結果は、複数のGROUP BY句のUNIONの結果と同等です。

## 例

例1： `FILTER`

  次の2つのクエリは同等です。

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

例2：`GROUPING SETS`、`CUBE`、および `ROLLUP`

`ROLLUP(a,b,c)` は、次の `GROUPING SETS` 句と同等です。

    ```sql
    GROUPING SETS (
    (a,b,c),
    (a,b  ),
    (a    ),
    (     )
    )
    ```

`CUBE (a, b, c)` は、次の `GROUPING SETS` 句と同等です。

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

実際のデータセットでテストします。

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
