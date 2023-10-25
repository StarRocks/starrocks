# GROUP BY

## 描述

  GROUP BY `GROUPING SETS` ｜ `CUBE` ｜ `ROLLUP` 是对 GROUP BY 子句的扩展，它能够在一个 GROUP BY 子句中实现多个集合的分组的聚合。其结果等价于将多个相应 GROUP BY 子句进行 UNION 操作。

  GROUP BY 子句是只含有一个元素的 GROUP BY GROUPING SETS 的特例。
  例如，GROUPING SETS 语句：

  ```sql
  SELECT a, b, SUM( c ) FROM tab1 GROUP BY GROUPING SETS ( (a, b), (a), (b), ( ) );
  ```

  其查询结果等价于：

  ```sql
  SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b
  UNION
  SELECT a, null, SUM( c ) FROM tab1 GROUP BY a
  UNION
  SELECT null, b, SUM( c ) FROM tab1 GROUP BY b
  UNION
  SELECT null, null, SUM( c ) FROM tab1
  ```

  `GROUPING(expr)` 指示一个列是否为聚合列，如果是聚合列为 0，否则为 1

  `GROUPING_ID(expr  [ , expr [ , ... ] ])` 与 GROUPING 类似，GROUPING_ID 根据指定的 column 顺序，计算出一个列列表的 bitmap 值，每一位为 GROUPING 的值. GROUPING_ID()函数返回位向量的十进制值。

## 语法

  ```sql
  SELECT ...
  FROM ...
  [ ... ]
  GROUP BY [
      , ... |
      GROUPING SETS [, ...] (  groupSet [ , groupSet [ , ... ] ] ) |
      ROLLUP(expr  [ , expr [ , ... ] ]) |
      expr  [ , expr [ , ... ] ] WITH ROLLUP |
      CUBE(expr  [ , expr [ , ... ] ]) |
      expr  [ , expr [ , ... ] ] WITH CUBE
      ]
  [ ... ]
  ```

### Parameters

  `groupSet` 表示 select list 中的列，别名或者表达式组成的集合 `groupSet ::= { ( expr  [ , expr [ , ... ] ] )}`。

  `expr`  表示 select list 中的列，别名或者表达式。

### Note

  starrocks 支持类似 PostgreSQL 语法，语法实例如下：

  ```sql
  SELECT a, b, SUM( c ) FROM tab1 GROUP BY GROUPING SETS ( (a, b), (a), (b), ( ) );
  SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY ROLLUP(a,b,c)
  SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY CUBE(a,b,c)
  ```

  `ROLLUP(a,b,c)` 等价于如下 `GROUPING SETS` 语句。

  ```sql
  GROUPING SETS (
  (a,b,c),
  (a,b  ),
  (a    ),
  (     )
  )
  ```

  `CUBE ( a, b, c )` 等价于如下 `GROUPING SETS` 语句。

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

## 示例

  下面是一个实际数据的例子:

  ```sql
  > SELECT * FROM t;
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

  > SELECT k1, k2, SUM(k3) FROM t GROUP BY GROUPING SETS ( (k1, k2), (k2), (k1), ( ) );
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

  > SELECT k1, k2, GROUPING_ID(k1,k2), SUM(k3) FROM t GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ());
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
