# SELECT

## 功能

Select 语句由 select，from，where，group by，having，order by，union 等部分组成。

StarRocks 的查询语句基本符合 SQL92 标准，下面简要介绍支持的 select 用法。

### 连接(Join)

连接操作是合并 2 个或多个表的数据，然后返回其中某些表中的某些列的结果集。

目前 StarRocks 支持 inner join，outer join，semi join，anti join，cross join。

在 inner join 条件里除了支持等值 join，还支持不等值 join，为了性能考虑，推荐使用等值 join。

其它 join 只支持等值 join。连接的语法定义如下：

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

#### Self-Join

StarRocks 支持 self-joins，即自己和自己 join。例如同一张表的不同列进行 join。

实际上没有特殊的语法标识 self-join。self-join 中 join 两边的条件都来自同一张表，

我们需要给他们分配不同的别名。

例如：

```sql
SELECT lhs.id, rhs.parent, lhs.c1, rhs.c2 FROM tree_data lhs, tree_data rhs WHERE lhs.id = rhs.parent;
```

#### 笛卡尔积(Cross Join)

Cross join 会产生大量的结果，须慎用 cross join,

即使需要使用 cross join 时也需要使用过滤条件并且确保返回结果数较少。例如：

```sql
SELECT * FROM t1, t2;

SELECT * FROM t1 CROSS JOIN t2;
```

#### Inner join

Inner join 是大家最熟知，最常用的 join。返回的结果来自相近的 2 张表所请求的列，join 的条件为两个表的列包含有相同的值。

如果两个表的某个列名相同，我们需要使用全名（table_name.column_name 形式）或者给列名起别名。

例如：

下列 3 个查询是等价的。

```sql
SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
```

#### Outer join

Outer join 返回左表或者右表或者两者所有的行。如果在另一张表中没有匹配的数据，则将其设置为 NULL。例如：

```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

#### 等值和不等值 join

通常情况下，用户使用等值 join 居多，等值 join 要求 join 条件的操作符是等号。

不等值 join 在 join 条件上可以使用!=，等符号。不等值 join 会产生大量的结果，在计算过程中可能超过内存限额。

因此需要谨慎使用。不等值 join 只支持 inner join。例如：

```sql
SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id > t2.id;
```

#### Semi join

Left semi join 只返回左表中能匹配右表数据的行，不管能匹配右表多少行数据，

左表的该行最多只返回一次。Right semi join 原理相似，只是返回的数据是右表的。

例如：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;
```

#### Anti join

Left anti join 只返回左表中不能匹配右表的行。

Right anti join 反转了这个比较，只返回右表中不能匹配左表的行。例如：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;
```

### Order by

Order by 通过比较一列或者多列的大小来对结果集进行排序。

order by 是比较耗时耗资源的操作，因为所有数据都需要发送到 1 个节点后才能排序，排序操作相比不排序操作需要更多的内存。

如果需要返回前 N 个排序结果，需要使用 LIMIT 从句；为了限制内存的使用，如果用户没有指定 LIMIT 从句，则默认返回前 65535 个排序结果。

Order by 语法定义如下：

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

### Group by

Group by 从句通常和聚合函数（例如 COUNT(), SUM(), AVG(), MIN()和 MAX()）一起使用。

Group by 指定的列不会参加聚合操作。Group by 从句可以加入 Having 从句来过滤聚合函数产出的结果。例如：

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

2 rows in set (0.07 sec)
```

### Having

Having 从句不是过滤表中的行数据，而是过滤聚合函数产出的结果。

通常来说 having 要和聚合函数（例如 COUNT(), SUM(), AVG(), MIN(), MAX()）以及 group by 从句一起使用。

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

### Limit

Limit 从句用于限制返回结果的最大行数。设置返回结果的最大行数可以帮助 StarRocks 优化内存的使用。

该从句主要应用如下场景：

1.返回 top-N 的查询结果。

2.简单查看表中包含的内容。

3.表中数据量大，或者 where 从句没有过滤太多的数据，需要限制查询结果集的大小。

使用说明：limit 从句的值必须是数字型字面常量。

举例：

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

#### Offset

Offset 从句使得结果集跳过前若干行结果后直接返回后续的结果。

结果集默认起始行为第 0 行，因此 offset 0 和不带 offset 返回相同的结果。

通常来说，offset 从句需要与 order by 从句和 limit 从句一起使用才有效。

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

注：在没有 order by 的情况下使用 offset 语法是允许的，但是此时 offset 无意义。

这种情况只取 limit 的值，忽略掉 offset 的值。因此在没有 order by 的情况下。

offset 超过结果集的最大行数依然是有结果的。建议用户使用 offset 时一定要带上 order by。

### Union

Union 从句用于合并多个查询的结果集。语法定义如下：

```sql
query_1 UNION [DISTINCT | ALL] query_2
```

使用说明：

只使用 union 关键词和使用 union disitnct 的效果是相同的。由于去重工作是比较耗费内存的，

因此使用 union all 操作查询速度会快些，耗费内存会少些。如果用户想对返回结果集进行 order by 和 limit 操作，

需要将 union 操作放在子查询中，然后 select from subquery，最后把 subquery 和 order by 放在子查询外面。

举例：

```sql
mysql> (select tiny_column from small_table) union all (select tiny_column from small_table);

+-------------+
|tiny_column  |
+-------------+
|      1      |
|      2      |
|      1      |
|      2      |
+-------------+

4 rows in set (0.10 sec)
```

```sql
mysql> (select tiny_column from small_table) union (select tiny_column from small_table);

+-------------+
|tiny_column  |
+-------------+
|      2      |
|      1      |
+-------------+

2 rows in set (0.11 sec)
```

```sql
mysql> select * from (select tiny_column from small_table union all

select tiny_column from small_table) as t1 

order by tiny_column limit 4;

+-------------+
| tiny_column |
+-------------+
|       1     |
|       1     |
|       2     |
|       2     |
+-------------+

4 rows in set (0.11 sec)
```

### Distinct

Distinct 操作符对结果集进行去重。示例：

```SQL
-- Returns the unique values from one column.
select distinct tiny_column from big_table limit 2;

-- Returns the unique combinations of values from multiple columns.
select distinct tiny_column, int_column from big_table limit 2;
```

distinct 可以和聚合函数(通常是 count 函数)一同使用，count(disitnct)用于计算出一个列或多个列上包含多少不同的组合。

```SQL
-- Counts the unique values from one column.
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
-- Counts the unique combinations of values from multiple columns.
select count(distinct tiny_column, int_column) from big_table limit 2;
```

StarRocks 支持多个聚合函数同时使用 distinct。

```SQL
-- Count the unique value from multiple aggregation function separately.
select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;
```

### 子查询

子查询按相关性分为不相关子查询和相关子查询。

#### 不相关子查询

不相关子查询支持 [NOT] IN 和 EXISTS。

举例：

```sql
SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);
```

```sql
SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);
```

#### 相关子查询

相关子查询支持 [NOT] IN 和 [NOT] EXISTS。

举例：

```sql
SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);

SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);
```

子查询还支持标量子查询。分为不相关标量子查询、相关标量子查询和标量子查询作为普通函数的参数。

举例：

1.不相关标量子查询，谓词为 = 号。例如输出最高工资的人的信息。

```sql
SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
```

2.不相关标量子查询，谓词为 >,< 等。例如输出比平均工资高的人的信息。

```sql
SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
```

3.相关标量子查询。例如输出各个部门工资最高的信息。

```sql
SELECT name FROM table a WHERE salary = （SELECT MAX(salary) FROM table b WHERE b.部门= a.部门）;
```

4.标量子查询作为普通函数的参数。

```sql
SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));
```

### With 子句

可以在 SELECT 语句之前添加的子句，用于定义在 SELECT 内部多次引用的复杂表达式的别名。

与 CREATE VIEW 类似，但在子句中定义的表和列名在查询结束后不会持久，也不会与实际表或 VIEW 中的名称冲突。

用 WITH 子句的好处有：

方便和易于维护，减少查询内部的重复。

通过将查询中最复杂的部分抽象成单独的块，更易于阅读和理解 SQL 代码。

举例：

```sql
-- Define one subquery at the outer level, and another at the inner level as part of the
-- initial stage of the UNION ALL query.

with t1 as (select 1),t2 as (select 2)
select * from t1 union all select * from t2;
```

### Where 与操作符

SQL 操作符是一系列用于比较的函数，这些操作符广泛地用于 select 语句的 where 从句中。

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

#### Between 操作符

在 where 从句中，表达式可能同时与上界和下界比较。如果表达式大于等于下界，同时小于等于上界，比较的结果是 true。语法定义如下：

```sql
expression BETWEEN lower_bound AND upper_bound
```

数据类型：通常表达式（expression）的计算结果都是数字类型，该操作符也支持其他数据类型。如果必须要确保下界和上界都是可比较的字符，可以使用 cast()函数。

使用说明：如果操作数是 string 类型应注意，起始部分为上界的长字符串将不会匹配上界，该字符串比上界要大。例如：‘MJ’比‘M’大，所以 `between 'A' and 'M'` 不会匹配‘MJ’。

如果需要确保表达式能够正常工作，可以使用一些函数，如 upper(), lower(), substr(), trim()。

举例：

```sql
select c1 from t1 where month between 1 and 6;
```

#### 比较操作符

比较操作符用来判断列和列是否相等或者对列进行排序。`=, !=, <=, > =` 可以适用所有数据类型。

其中 `<>` 符号是不等于的意思，与!= 的功能一致。IN 和 BETWEEN 操作符提供更简短的表达来描述相等、小于、大于等关系的比较。

In 操作符

In 操作符会和 VALUE 集合进行比较，如果可以匹配该集合中任何一元素，则返回 TRUE。

参数和 VALUE 集合必须是可比较的。所有使用 IN 操作符的表达式都可以写成用 OR 连接的等值比较，但是 IN 的语法更简单，更精准，更容易让 StarRocks 进行优化。

举例：

```sql
select * from small_table where tiny_column in (1,2);
```

#### Like 操作符

该操作符用于和字符串进行比较。"_"用来匹配单个字符，"%"用来匹配多个字符。参数必须要匹配完整的字符串。通常，把"%" 放在字符串的尾部更加符合实际用法。

举例：

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

举例：

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

如果想匹配中间的部分，正则表达式的前面部分可以写成 "^.*" 或者".*"。"^" 和 "$" 通常是可以省略的。RLKIE 操作符和 REGEXP 操作符是同义词。

"|" 操作符是个可选操作符，"|" 两侧的正则表达式只需满足 1 侧条件即可，"|" 操作符和两侧的正则表达式通常需要用()括起来。

举例：

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

### 别名

当在查询中书写表、列，或者包含列的表达式的名字时，可以同时给它们分配一个别名。

当需要使用表名、列名时，可以使用别名来访问。别名通常相对原名来说更简短更好记。当需要新建一个别名时，只需在 select list 或者 from list 中的表、列、表达式名称后面加上 AS alias 从句即可。

AS 关键词是可选的，用户可以直接在原名后面指定别名。如果别名或者其他标志符和内部关键词同名时，需要在该名称加上``符号。别名对大小写是敏感的。

举例：

```sql
select tiny_column as name, int_column as sex from big_table;

select sum(tiny_column) as total_count from big_table;

select one.tiny_column, two.int_column from small_table one, <br/> big_table two where one.tiny_column = two.tiny_column;
```

## 关键字(keywords)

SELECT
