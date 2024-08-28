---
displayed_sidebar: docs
---

# SELECT

## 功能

SELECT 语句用于从单个或多个表，视图，物化视图中读取数据。SELECT 语句一般由以下子句组成：

- [SELECT](#select)
  - [功能](#功能)
    - [WITH](#with)
    - [连接 (Join)](#连接-join)
      - [Self Join](#self-join)
      - [笛卡尔积 (Cross Join)](#笛卡尔积-cross-join)
      - [Inner Join](#inner-join)
      - [Outer Join](#outer-join)
      - [Semi Join](#semi-join)
      - [Anti Join](#anti-join)
      - [等值 Join 和非等值 Join](#等值-join-和非等值-join)
    - [ORDER BY](#order-by)
    - [GROUP BY](#group-by)
  - [语法](#语法)
    - [Parameters](#parameters)
    - [Note](#note)
  - [示例](#示例)
    - [HAVING](#having)
    - [LIMIT](#limit)
      - [OFFSET](#offset)
    - [**UNION**](#union)
    - [**INTERSECT**](#intersect)
    - [**EXCEPT/MINUS**](#exceptminus)
    - [DISTINCT](#distinct)
    - [子查询](#子查询)
      - [不相关子查询](#不相关子查询)
      - [相关子查询](#相关子查询)
    - [WHERE 与操作符](#where-与操作符)
      - [算数操作符](#算数操作符)
      - [BETWEEN 操作符](#between-操作符)
      - [比较操作符](#比较操作符)
      - [In 操作符](#in-操作符)
      - [Like 操作符](#like-操作符)
      - [逻辑操作符](#逻辑操作符)
      - [正则表达式操作符](#正则表达式操作符)
    - [别名 (alias)](#别名-alias)
    - [PIVOT](#pivot)
      - [语法](#语法-1)
      - [参数](#参数)
      - [示例](#示例-1)

SELECT 可以作为独立的语句也可以作为其他语句的子句，其查询结果可以作为另一个语句的输入值。

StarRocks 的查询语句基本符合 SQL-92 标准。下面介绍支持的 SELECT 用法。

> **说明**
>
> 如果要查询 StarRocks 表、视图、或物化视图内的数据，需要有对应对象的 SELECT 权限。如果要查询 External Catalog 里的数据，需要有对应 Catalog 的 USAGE 权限。

### WITH

可以在 SELECT 语句之前添加的子句，用于定义在 SELECT 内部多次引用的复杂表达式的别名。

与 CREATE VIEW 类似，但在子句中定义的表和列名在查询结束后不会持久，也不会与实际表或 VIEW 中的名称冲突。

使用 WITH 子句的好处：

- 方便和易于维护，减少查询内部的重复。

- 通过将查询中最复杂的部分抽象成单独的块，更易于阅读和理解 SQL 代码。

示例：

```sql
-- Define one subquery at the outer level, and another at the inner level as part of the
-- initial stage of the UNION ALL query.

with t1 as (select 1),t2 as (select 2)
select * from t1 union all select * from t2;
```

### 连接 (Join)

连接操作合并 2 个或多个表的数据，然后返回某些表中某些列的结果集。

目前 StarRocks 支持 Self Join、Cross Join、Inner Join、Outer Join、Semi Join 和 Anti Join。其中，Outer Join 包括 Left Join、Right Join 和 Full Join。

Join 的语法定义如下：

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

#### Self Join

StarRocks 支持 Self Join，即自己和自己 Join。例如同一张表的不同列进行 Join。

实际上没有特殊的语法标识 Self Join。Self Join 中 Join 两边的条件都来自同一张表，

您需要给他们分配不同的别名。

例如：

```sql
SELECT lhs.id, rhs.parent, lhs.c1, rhs.c2 FROM tree_data lhs, tree_data rhs WHERE lhs.id = rhs.parent;
```

#### 笛卡尔积 (Cross Join)

Cross join 会产生大量的结果，须慎用 cross join。

即使需要使用 Cross Join 时也需要使用过滤条件并且确保返回结果数较少。例如：

```sql
SELECT * FROM t1, t2;

SELECT * FROM t1 CROSS JOIN t2;
```

#### Inner Join

Inner Inner Join 是大家最熟知，最常用的 Join。返回的结果来自相近的两张表所请求的列，Join 的条件为两个表的列包含有相同的值。

如果两个表的某个列名相同，我们需要使用全名（table_name.column_name 形式）或者给列名起别名。

例如：

下列 3 个查询是等价的。

```sql
SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
```

#### Outer Join

Outer Join 返回左表或者右表或者两者所有的行。如果在另一张表中没有匹配的数据，则将其设置为 `NULL`。例如：

```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

#### Semi Join

Left Semi Join 只返回左表中能匹配右表数据的行，不管能匹配右表多少行数据，

左表的该行最多只返回一次。Right Semi Join 原理相似，只是返回的数据是右表的。

例如：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;
```

#### Anti Join

Left Anti Join 只返回左表中不能匹配右表的行。

Right Anti Join 反转了这个比较，只返回右表中不能匹配左表的行。例如：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;
```

#### 等值 Join 和非等值 Join

根据 Join 条件的不同，StarRocks 支持的上述各种类型的 Join 可以分为等值 Join 和非等值 Join，如下表所示：

| **等值 Join**   | Self Join、Cross Join、Inner Join、Outer Join、Semi Join 和 Anti Join |
| --------------- | ------------------------------------------------------------ |
| **非等值 Join** | Cross Join、Inner Join，LEFT SEMI JOIN， LEFT ANTI JOIN 和 Outer Join  |

- 等值 Join
  
  等值 Join 使用等值条件作为连接条件，例如 `a JOIN b ON a.id = b.id`。

- 非等值 Join
  
  非等值 Join 不使用等值条件，使用 `<`、`<=`、`>`、`>=`、`<>` 等比较操作符，例如 `a JOIN b ON a.id < b.id`。与等值 Join 相比，非等值 Join 目前效率较低，建议您谨慎使用。

  以下为非等值 Join 的两个使用示例：

  ```SQL
  SELECT t1.id, c1, c2 
  FROM t1 
  INNER JOIN t2 ON t1.id < t2.id;
    
  SELECT t1.id, c1, c2 
  FROM t1 
  LEFT JOIN t2 ON t1.id > t2.id;
  ```

### ORDER BY

ORDER BY 通过比较一列或者多列的大小来对结果集进行排序。

ORDER BY 是比较耗时耗资源的操作，因为所有数据都需要发送到 1 个节点后才能排序，需要更多的内存。

如果需要返回前 N 个排序结果，需要使用 LIMIT 子句；为了限制内存的使用，如果您没有指定 LIMIT 子句，则默认返回前 65535 个排序结果。

ORDER BY 语法定义如下：

```sql
ORDER BY col [ASC | DESC]
[ NULLS FIRST | NULLS LAST ]
```

默认排序顺序是 ASC（升序）。示例：

```sql
select * from big_table order by tiny_column, short_column desc;
```

StarRocks 支持在 ORDER BY 后声明 null 值排在最前面还是最后面，语法为 `order by <> [ NULLS FIRST | NULLS LAST ]`。`NULLS FIRST` 表示 null 值的记录将排在最前面，`NULLS LAST` 表示 null 值的记录将排在最后面。

示例：将 null 值始终排在最前面。

```sql
select  *  from  sales_record  order by  employee_id  nulls first;
```

### GROUP BY

GROUP BY 子句通常和聚合函数（例如 COUNT(), SUM(), AVG(), MIN()和 MAX()）一起使用。

GROUP BY 指定的列不会参加聚合操作。GROUP BY 子句可以加入 HAVING 子句来过滤聚合函数产出的结果。例如：

```sql
select tiny_column, sum(short_column)
from small_table 
group by tiny_column;
```

```sql
+-------------+---------------------+
| tiny_column |  sum('short_column')|
+-------------+---------------------+
|      1      |        2            |
|      2      |        1            |
+-------------+---------------------+
```

## 语法

  ```sql
  SELECT ...
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

### Parameters

- `groupSet` 表示 select list 中的列，别名或者表达式组成的集合 `groupSet ::= { ( expr  [ , expr [ , ... ] ] )}`。

- `expr`  表示 select list 中的列，别名或者表达式。

### Note

StarRocks 支持类似 PostgreSQL 语法，语法实例如下：

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

`CUBE (a, b, c)` 等价于如下 `GROUPING SETS` 语句。

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

  `GROUPING(expr)` 指示一个列是否为聚合列，如果是聚合列为 0，否则为 1。

  `GROUPING_ID(expr  [ , expr [ , ... ] ])` 与 GROUPING 类似，GROUPING_ID 根据指定的 column 顺序，计算出一个列列表的 bitmap 值，每一位为 GROUPING 的值. GROUPING_ID()函数返回位向量的十进制值。

### HAVING

HAVING 子句不过滤表中的行数据，而是过滤聚合函数产出的结果。

通常来说 HAVING 要和聚合函数（例如 COUNT(), SUM(), AVG(), MIN(), MAX()）以及 group by 子句一起使用。

示例：

```sql
select tiny_column, sum(short_column) 
from small_table 
group by tiny_column 
having sum(short_column) = 1;
```

```sql
+-------------+---------------------+
|tiny_column  | sum('short_column') |
+-------------+---------------------+
|         2   |        1            |
+-------------+---------------------+

1 row in set (0.07 sec)
```

```sql
select tiny_column, sum(short_column) 
from small_table 
group by tiny_column 
having tiny_column > 1;
```

```sql
+-------------+---------------------+
|tiny_column  | sum('short_column') |
+-------------+---------------------+
|      2      |          1          |
+-------------+---------------------+

1 row in set (0.07 sec)
```

### LIMIT

LIMIT 子句用于限制返回结果的最大行数。设置返回结果的最大行数可以帮助 StarRocks 优化内存的使用。

该子句主要应用如下场景：

1. 返回 top-N 的查询结果。

2. 简单查看表中包含的内容。

3. 表中数据量大，或者 where 子句没有过滤太多的数据，需要限制查询结果集的大小。

使用说明：LIMMIT 子句的值必须是数字型字面常量。

示例：

```sql
mysql> select tiny_column from small_table limit 1;

+-------------+
|tiny_column  |
+-------------+
|     1       |
+-------------+

1 row in set (0.02 sec)
```

```sql
mysql> select tiny_column from small_table limit 10000;

+-------------+
|tiny_column  |
+-------------+
|      1      |
|      2      |
+-------------+

2 rows in set (0.01 sec)
```

#### OFFSET

OFFSET 子句用于跳过结果集的前若干行结果，直接返回后续的结果。

默认从第 0 行开始，因此 OFFSET 0 和不带 OFFSET 返回相同的结果。

通常来说，OFFSET 子句需要与 ORDER BY 子句和 LIMIT 子句一起使用才有效。

示例：

```sql
mysql> select varchar_column from big_table order by varchar_column limit 3;

+----------------+
| varchar_column | 
+----------------+
|    beijing     | 
|    chongqing   | 
|    tianjin     | 
+----------------+

3 rows in set (0.02 sec)
```

```sql
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 0;

+----------------+
|varchar_column  |
+----------------+
|     beijing    |
+----------------+

1 row in set (0.01 sec)
```

```sql
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 1;

+----------------+
|varchar_column  |
+----------------+
|    chongqing   | 
+----------------+

1 row in set (0.01 sec)
```

```sql
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 2;

+----------------+
|varchar_column  |
+----------------+
|     tianjin    |     
+----------------+

1 row in set (0.02 sec)
```

> **注意**
>
> 在没有 ORDER BY 的情况下使用 OFFSET 语法是允许的，但是此时 OFFSET 无意义。这种情况只取 LIMIT 的值，忽略 OFFSET 的值。因此在没有 ORDER BY 的情况下，OFFSET 超过结果集的最大行数依然是有结果的。**建议使用 OFFSET 时一定带上 ORDER BY。**

### **UNION**

UNION 子句用于合并多个查询的结果，即获取并集。

**语法如下：**

```SQL
query_1 UNION [DISTINCT | ALL] query_2
```

- DISTINCT：默认值，返回不重复的结果。UNION 和 UNION DISTINCT 效果相同（见第二个示例）。
- ALL: 返回所有结果的集合，不进行去重。由于去重工作比较耗费内存，因此使用 UNION ALL 查询速度会快一些，内存消耗也会少一些（见第一个示例）。

> **说明**
>
> 每条 SELECT 查询返回的列数必须相同，且列类型必须能够兼容。

**示例：**

以表 `select1` 和 `select2` 示例说明。

```SQL
CREATE TABLE select1(
    id          INT,
    price       INT
    )
DISTRIBUTED BY HASH(id);

INSERT INTO select1 VALUES
    (1,2),
    (1,2),
    (2,3),
    (5,6),
    (5,6);

CREATE TABLE select2(
    id          INT,
    price       INT
    )
DISTRIBUTED BY HASH(id);

INSERT INTO select2 VALUES
    (2,3),
    (3,4),
    (5,6),
    (7,8);
```

示例一：返回两张表中所有 `id` 的并集，不进行去重。

```Plaintext
mysql> (select id from select1) union all (select id from select2) order by id;

+------+
| id   |
+------+
|    1 |
|    1 |
|    2 |
|    2 |
|    3 |
|    5 |
|    5 |
|    5 |
|    7 |
+------+
11 rows in set (0.02 sec)
```

示例二：返回两张表中 `id` 的并集，进行去重。下面两条查询在功能上对等。

```Plaintext
mysql> (select id from select1) union (select id from select2) order by id;

+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
|    5 |
|    7 |
+------+
6 rows in set (0.01 sec)

mysql> (select id from select1) union distinct (select id from select2) order by id;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
|    5 |
|    7 |
+------+
5 rows in set (0.02 sec)
```

示例三：返回两张表中所有 `id` 的并集，进行去重，只返回 3 行结果。下面两条查询在功能上对等。

```SQL
mysql> (select id from select1) union distinct (select id from select2)
order by id
limit 3;
++------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
+------+
4 rows in set (0.11 sec)

mysql> select * from (select id from select1 union distinct select id from select2) as t1
order by id
limit 3;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
+------+
3 rows in set (0.01 sec)
```

### **INTERSECT**

INTERSECT 子句用于返回多个查询结果之间的交集，即每个结果中都有的数据，并对结果集进行去重。

**语法如下：**

```SQL
query_1 INTERSECT [DISTINCT] query_2
```

> **说明**
>
> - INTERSECT 效果等同于 INTERSECT DISTINCT。不支持 ALL 关键字。
> - 每条 SELECT 查询返回的列数必须相同，且列类型能够兼容。

**示例：**

继续使用 UNION 子句里的两张表。返回两张表中 `id` 和 `price` 组合的交集。下面两条查询在功能上对等。

```Plaintext
mysql> (select id, price from select1) intersect (select id, price from select2)
order by id;

+------+-------+
| id   | price |
+------+-------+
|    2 |     3 |
|    5 |     6 |
+------+-------+

mysql> (select id, price from select1) intersect distinct (select id, price from select2)
order by id;

+------+-------+
| id   | price |
+------+-------+
|    2 |     3 |
|    5 |     6 |
+------+-------+
```

### **EXCEPT/MINUS**

EXCEPT/MINUS 子句用于返回多个查询结果之间的补集，即返回左侧查询中在右侧查询中不存在的数据，并对结果集去重。EXCEPT 和 MINUS 功能对等。

**语法如下：**

```SQL
query_1 {EXCEPT | MINUS} [DISTINCT] query_2
```

> **说明**
>
> - EXCEPT 效果等同于 EXCEPT DISTINCT。不支持 ALL 关键字。
> - 每条 SELECT 查询返回的列数必须相同，且列类型能够兼容。

**示例：**

继续使用 UNION 子句里的两张表。返回表 `select1` 中不存在于表 `select2` 的 `(id,price)` 组合。

可以看到在结果中对组合 `(1,2)` 进行了去重。

```Plaintext
mysql> (select id, price from select1) except (select id, price from select2)
order by id;
+------+-------+
| id   | price |
+------+-------+
|    1 |     2 |
+------+-------+

mysql> (select id, price from select1) minus (select id, price from select2)
order by id;
+------+-------+
| id   | price |
+------+-------+
|    1 |     2 |
+------+-------+
```

### DISTINCT

DISTINCT 关键字对结果集进行去重。示例：

```SQL
-- 返回一列中去重后的值。
select distinct tiny_column from big_table limit 2;

-- 返回多列中去重后的组合。
select distinct tiny_column, int_column from big_table limit 2;
```

DISTINCT 可以和聚合函数 (通常是 count) 一同使用，count(distinct) 用于计算出一个列或多个列上包含多少不同的组合。

```SQL
-- 计算一列中不重复值的个数。
select count(distinct tiny_column) from small_table;
```

```sql
+-------------------------------+
| count(DISTINCT 'tiny_column') |
+-------------------------------+
|             2                 |
+-------------------------------+
1 row in set (0.06 sec)
```

```SQL
-- 计算多列中不重复组合的个数。
select count(distinct tiny_column, int_column) from big_table limit 2;
```

StarRocks 支持多个聚合函数同时使用 distinct。

```SQL
-- 单独返回多个聚合函数去重后的个数。

select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;
```

### 子查询

子查询按相关性可以分为不相关子查询和相关子查询。

- 不相关子查询（简单查询）不依赖外层查询的结果。
- 相关子查询需要依赖外层查询的结果才能执行。

#### 不相关子查询

不相关子查询支持 [NOT] IN 和 EXISTS。

示例：

```sql
SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);

SELECT * FROM t1 WHERE (x,y) [NOT] IN (SELECT x,y FROM t2 LIMIT 2);

SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);
```

从 3.0 版本开始，`SELECT... FROM... WHERE... [NOT] IN` 支持在 WHERE 中指定多个字段进行比较，即上面第二个示例中的 `WHERE (x,y)` 用法。

#### 相关子查询

相关子查询支持 [NOT] IN 和 [NOT] EXISTS。

示例：

```sql
SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);

SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);
```

子查询还支持标量子查询。分为不相关标量子查询、相关标量子查询和标量子查询作为普通函数的参数。

示例：

1. 不相关标量子查询，谓词为 = 号。例如输出最高工资的人的信息。

    ```sql
    SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
    ```

2. 不相关标量子查询，谓词为 >,< 等。例如输出比平均工资高的人的信息。

    ```sql
    SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
    ```

3. 相关标量子查询。例如输出各个部门工资最高的信息。

    ```sql
    SELECT name FROM table a WHERE salary = （SELECT MAX(salary) FROM table b WHERE b.部门= a.部门）;
    ```

4. 标量子查询作为普通函数的参数。

    ```sql
    SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));
    ```

### WHERE 与操作符

SQL 操作符是一系列用于比较的函数，这些操作符广泛地用于 SELECT 语句的 WHERE 子句中。

#### 算数操作符

算术操作符通常出现在包含左操作数，操作符和右操作数（大部分情况下）组成的表达式中。

**+和-**：分别代表着加法和减法，可以作为单元或 2 元操作符。当其作为单元操作符时，如+1, -2.5 或者-col_name，表达的意思是该值乘以+1 或者-1。

因此单元操作符+返回的是未发生变化的值，单元操作符-改变了该值的符号位。

用户可以将两个单元操作符叠加起来，比如++5(返回的是正值)，-+2 或者+-2（这两种情况返回的是负值），但是用户不能使用连续的两个-号。

因为--被解释为后面的语句是注释（用户也是可以使用两个-号的，此时需要在两个-号之间加上空格或圆括号，如-(-2)或者- -2，这两种写法实际表达的结果都是+2）。

+或者-作为 2 元操作符时，例如 2+2，3-1.5 或者 col1 + col2，表达的含义是左值加或者减去右值。左值和右值必须都是数字类型。

***和/**：分别代表着乘法和除法操作符。两侧的操作数必须都是数据类型。

当两个数相乘时，类型较小的操作数在需要的情况下类型可能会提升（比如 SMALLINT 提升到 INT 或者 BIGINT 等），表达式的结果被提升到下一个较大的类型，

比如 TINYINT 乘以 INT 产生的结果的类型会是 BIGINT）。当两个数相乘时，为了避免精度丢失，操作数和表达式结果都会被解释成 DOUBLE 类型。

如果用户想把表达式结果转换成其他类型，需要用 CAST 函数转换。

**%**：取模操作符。返回左操作数除以右操作数的余数。左操作数和右操作数都必须是整型。

**&，|和^**：按位操作符返回对两个操作数进行按位与，按位或，按位异或操作的结果。两个操作数都要求是一种整型类型。

如果按位操作符的两个操作数的类型不一致，则类型小的操作数会被提升到类型较大的操作数，然后再做相应的按位操作。

在 1 个表达式中可以出现多个算术操作符，用户可以用小括号将相应的算术表达式括起来。算术操作符通常没有对应的数学函数来表达和算术操作符相同的功能。

比如我们没有 MOD()函数来表示%操作符的功能。反过来，数学函数也没有对应的算术操作符。比如幂函数 POW()并没有相应的 **求幂操作符。

#### BETWEEN 操作符

在 where 子句中，表达式可能同时与上界和下界比较。如果表达式大于等于下界，同时小于等于上界，比较的结果是 true。语法定义如下：

```sql
expression BETWEEN lower_bound AND upper_bound
```

数据类型：通常表达式（expression）的计算结果都是数字类型，该操作符也支持其他数据类型。如果必须要确保下界和上界都是可比较的字符，可以使用 cast()函数。

使用说明：如果操作数是 string 类型应注意，起始部分为上界的长字符串将不会匹配上界，该字符串比上界要大。例如：‘MJ’比‘M’大，所以 `between 'A' and 'M'` 不会匹配‘MJ’。

如果需要确保表达式能够正常工作，可以使用一些函数，如 upper(), lower(), substr(), trim()。

示例：

```sql
select c1 from t1 where month between 1 and 6;
```

#### 比较操作符

比较操作符用来判断列和列是否相等或者对列进行排序。`=`, `!=`, `<=`, `>=` 可以适用所有数据类型。

其中 `<>` 符号是不等于的意思，与 `!=` 的功能一致。IN 和 BETWEEN 操作符提供更简短的表达来描述相等、小于、大于等关系的比较。

#### In 操作符

In 操作符会和 VALUE 集合进行比较，如果可以匹配该集合中任何一元素，则返回 TRUE。

参数和 VALUE 集合必须是可比较的。所有使用 IN 操作符的表达式都可以写成用 OR 连接的等值比较，但是 IN 的语法更简单，更精准，更容易让 StarRocks 进行优化。

示例：

```sql
select * from small_table where tiny_column in (1,2);
```

#### Like 操作符

该操作符用于和字符串进行比较。"_"用来匹配单个字符，"%"用来匹配多个字符。参数必须要匹配完整的字符串。通常，把"%" 放在字符串的尾部更加符合实际用法。

示例：

```sql
mysql> select varchar_column from small_table where varchar_column like 'm%';

+----------------+
|varchar_column  |
+----------------+
|     milan      |
+----------------+

1 row in set (0.02 sec)
```

```sql
mysql> select varchar_column from small_table where varchar_column like 'm____';

+----------------+
| varchar_column | 
+----------------+
|    milan       | 
+----------------+

1 row in set (0.01 sec)
```

#### 逻辑操作符

逻辑操作符返回一个 BOOL 值，逻辑操作符包括单元操作符和多元操作符，每个操作符处理的参数都是返回值为 BOOL 值的表达式。支持的操作符有：

AND: 2 元操作符，如果左侧和右侧的参数的计算结果都是 TRUE，则 AND 操作符返回 TRUE。

OR: 2 元操作符，如果左侧和右侧的参数的计算结果有一个为 TRUE，则 OR 操作符返回 TRUE。如果两个参数都是 FALSE，则 OR 操作符返回 FALSE。

NOT: 单元操作符，反转表达式的结果。如果参数为 TRUE，则该操作符返回 FALSE；如果参数为 FALSE，则该操作符返回 TRUE。

示例：

```sql
mysql> select true and true;

+-------------------+
| (TRUE) AND (TRUE) | 
+-------------------+
|         1         | 
+-------------------+

1 row in set (0.00 sec)
```

```sql
mysql> select true and false;

+--------------------+
| (TRUE) AND (FALSE) | 
+--------------------+
|         0          | 
+--------------------+

1 row in set (0.01 sec)
```

```sql
mysql> select true or false;

+-------------------+
| (TRUE) OR (FALSE) | 
+-------------------+
|        1          | 
+-------------------+

1 row in set (0.01 sec)
```

```sql
mysql> select not true;

+----------+
| NOT TRUE | 
+----------+
|     0    | 
+----------+

1 row in set (0.01 sec)
```

#### 正则表达式操作符

判断是否匹配正则表达式，使用 POSIX 标准的正则表达式。

"^" 用来匹配字符串的首部，"$" 用来匹配字符串的尾部，"." 匹配任何一个单字符，"*"匹配 0 个或多个选项，"+"匹配 1 个多个选项，"?" 表示分贪婪表示等等。正则表达式需要匹配完整的值，并不是仅仅匹配字符串的部分内容。

如果想匹配中间的部分，正则表达式的前面部分可以写成 "^.*" 或者".*"。"^" 和 "$" 通常是可以省略的。RLIKE 操作符和 REGEXP 操作符是同义词。

"|" 操作符是个可选操作符，"|" 两侧的正则表达式只需满足 1 侧条件即可，"|" 操作符和两侧的正则表达式通常需要用()括起来。

示例：

```sql
mysql> select varchar_column from small_table where varchar_column regexp '(mi|MI).*';

+----------------+
| varchar_column | 
+----------------+
|     milan      |       
+----------------+

1 row in set (0.01 sec)
```

```sql
mysql> select varchar_column from small_table where varchar_column regexp 'm.*';

+----------------+
| varchar_column | 
+----------------+
|     milan      |  
+----------------+

1 row in set (0.01 sec)
```

### 别名 (alias)

在查询中书写表名、列名，或者包含列的表达式的名字时，可以通过 AS 给它们分配一个别名。

当需要使用表名、列名时，可以使用别名来访问。别名通常相对原名来说更简短更容易记忆。当需要新建一个别名时，只需在 select list 或者 from list 中的表、列、表达式名称后面加上 AS alias 子句即可。AS 关键词是可选的，用户可以直接在原名后面指定别名。如果别名或者其他标志符和 [StarRocks 内部保留关键字](../keywords.md)同名时，需要在该名称加上反引号，比如 `rank`。**别名对大小写敏感，但是列别名和表达式别名对大小写不敏感**。

示例：

```sql
select tiny_column as name, int_column as sex from big_table;

select sum(tiny_column) as total_count from big_table;

select one.tiny_column, two.int_column from small_table one, big_table two where one.tiny_column = two.tiny_column;
```

### PIVOT

PIVOT操作符是SQL中的一个高级特性，它允许你将表中的行转换为列，通常用于数据透视表的创建。这在处理数据库报表或分析时非常有用，特别是当你需要对数据进行汇总或分类展示时。

实际上，PIVOT 是一种语法糖，它可以简化像 sum(case when ... then ... end) 这样的查询语句的编写。

#### 语法
  
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

#### 参数

在PIVOT操作中，你需要指定以下几个关键部分：
- aggregate_function()：聚合函数，如SUM、AVG、COUNT等，用于对数据进行汇总。
- alias：为聚合结果指定的别名，使得结果更易于理解。
- FOR pivot_column：指定要进行行转列操作的列名。
- IN (pivot_value)：指定pivot_column列中要转换为列的具体值。

#### 示例

```sql
create table t1 (c0 int, c1 int, c2 int, c3 int);
SELECT * FROM t1 PIVOT (SUM(c1) AS sum_c1, AVG(c2) AS avg_c2 FOR c3 IN (1, 2, 3, 4, 5));
-- 结果等同于以下查询：
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
