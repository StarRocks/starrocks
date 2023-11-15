# SELECT

## Description

The SELECT statement consists of select, from, where, group by, having, order by, union, and so on.

StarRocks' query statement basically conforms to the SQL92 standard. Here is a brief description of the supported select usage.

### Join

Join operations combine data from two or more tables and then return a result set of some columns from some of them.

StarRocks currently supports inner join, outer join, semi join, anti join, cross join.

In addition to equal join, unequal join is also supported in inner join condition. For performance reasons, equal join is recommended.

Other joins only support equal join. The syntax of the connection is defined as follows:

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

StarRocks supports self-joins, which are self-joins and self-joins. For example, different columns of the same table are joined.

There is actually no special syntax to identify self-join. The conditions on both sides of a join in a self-join come from the same table.

We need to assign them different aliases.

Example：

```sql
SELECT lhs.id, rhs.parent, lhs.c1, rhs.c2 FROM tree_data lhs, tree_data rhs WHERE lhs.id = rhs.parent;
```

#### Cross Join

Cross join can produce a lot of results, so cross join should be used with caution.

Even if you need to use cross join, you need to use filter conditions and ensure that fewer results are returned. For example:

```sql
SELECT * FROM t1, t2;

SELECT * FROM t1 CROSS JOIN t2;
```

#### Inner join

Inner join is the most well-known and commonly used join. Returns results from columns requested by two similar tables, joined if the columns of both tables contain the same value.

If a column name of both tables is the same, we need to use the full name (in the form of table_name.column_name) or alias the column name.

For example:

The following three queries are equivalent.

```sql
SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
```

#### Outer join

Outer join returns the left or right table or all rows of both. If there is no matching data in another table, set it to NULL. For example:

```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

#### Equivalent and unequal join

Usually, users use the most equal join, which requires the operator of the join condition to be an equal sign.

Unequal join can be used on join conditions`!=`, Equal sign. Unequal joins produce a large number of results and may exceed the memory limit during calculation.

Use with caution. Unequal join only supports inner join. For example:

```sql
SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id > t2.id;
```

#### Semi join

Left semi join returns only the rows in the left table that match the data in the right table, regardless of how many rows match the data in the right table.

This row of the left table is returned at most once. Right semi join works similarly, except that the data returned is a right table.

For example:

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;
```

#### Anti join

Left anti join only returns rows from the left table that do not match the right table.

Right anti join reverses this comparison, returning only rows from the right table that do not match the left table. For example:

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;
```

### Order by

Order by sorts the result set by comparing the sizes of one or more columns.

Order by is a time-consuming and resource-consuming operation because all data needs to be sent to one node before it can be sorted, and sorting requires more memory than unsorted operations.

If you need to return the first N sort results, you need to use the LIMIT clause; To limit memory usage, if the user does not specify a LIMIT clause, the first 65535 sort results are returned by default.

The Order by syntax is defined as follows:

```sql
ORDER BY col [ASC | DESC]
```

The default sort order is ASC (ascending). Example:

```sql
select * from big_table order by tiny_column, short_column desc;
```

### Group by

Group by clauses are often used with aggregate functions such as COUNT(), SUM(), AVG(), MIN(), and MAX().

The column specified by group by will not participate in the aggregation operation. The group by clause can be added with the Having clause to filter the results produced by the aggregate function. for example:

```sql
select tiny_column, sum(short_column)
from small_table 
group by tiny_column;
```

```plain text
+-------------+---------------------+
| tiny_column |  sum('short_column')|
+-------------+---------------------+
|      1      |        2            |
|      2      |        1            |
+-------------+---------------------+

2 rows in set (0.07 sec)
```

### Having

The Having clause does not filter row data in a table, but filters the results of aggregate functions.

Generally speaking, having is used with aggregate functions (such as COUNT(), SUM(), AVG(), MIN(), MAX()) and group by clauses.

Example：

```sql
select tiny_column, sum(short_column) 
from small_table 
group by tiny_column 
having sum(short_column) = 1;
```

```plain text
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

```plain text
+-------------+---------------------+
|tiny_column  | sum('short_column') |
+-------------+---------------------+
|      2      |          1          |
+-------------+---------------------+

1 row in set (0.07 sec)
```

### Limit

Limit clauses are used to limit the maximum number of rows returned. Setting the maximum number of rows returned can help StarRocks optimize memory usage.

This clause is mainly used in the following scenarios:

Returns the result of the top-N query.

Think about what's included in the table below.

The size of the query result set needs to be limited because of the large amount of data in the table or because the where clause does not filter too much data.

Instructions for use: The value of the limit clause must be a numeric literal constant.

Example：

```plain text
mysql> select tiny_column from small_table limit 1;

+-------------+
|tiny_column  |
+-------------+
|     1       |
+-------------+

1 row in set (0.02 sec)
```

```plain text
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

Offset

The Offset clause causes the result set to skip the first few rows and return the following results directly.

The result set defaults to start at line 0, so offset 0 and no offset return the same results.

Generally speaking, offset clauses need to be used with order by clauses and limit clauses to be valid.

Example：

```plain text
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

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 0;

+----------------+
|varchar_column  |
+----------------+
|     beijing    |
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 1;

+----------------+
|varchar_column  |
+----------------+
|    chongqing   | 
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 2;

+----------------+
|varchar_column  |
+----------------+
|     tianjin    |     
+----------------+

1 row in set (0.02 sec)
```

Note: It is allowed to use offset syntax without order by, but offset does not make sense at this time.

In this case, only the limit value is taken, and the offset value is ignored. So without order by.

Offset exceeds the maximum number of rows in the result set and is still a result. It is recommended that users use offset with order by.

### Union

Union clauses are used to merge the result sets of multiple queries. The syntax is defined as follows:

```sql
query_1 UNION [DISTINCT | ALL] query_2
```

Instructions:

Using only union keywords and union distinct works the same way. Since de-duplication is memory intensive,

As a result, queries using union all operations are faster and consume less memory. If the user wants to order by and limit the returned result set,

You need to place the union operation in the subquery, then select from the subquery, and finally place the subquery and order by outside the subquery.

Example：

```plain text
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

```plain text
mysql> (select tiny_column from small_table) union (select tiny_column from small_table);

+-------------+
|tiny_column  |
+-------------+
|      2      |
|      1      |
+-------------+

2 rows in set (0.11 sec)
```

```plain text
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

The distinct operator deduplicates the result set. Example:

```SQL
-- Returns the unique values from one column.
select distinct tiny_column from big_table limit 2;

-- Returns the unique combinations of values from multiple columns.
select distinct tiny_column, int_column from big_table limit 2;
```

distinct can be used with aggregate functions (usually count functions), and count (distinct) is used to calculate how many different combinations are contained on one or more columns.

```SQL
-- Counts the unique values from one column.
select count(distinct tiny_column) from small_table;
```

```plain text
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

StarRocks supports multiple aggregate functions using distinct at the same time.

```SQL
-- Count the unique value from multiple aggregation function separately.
select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;
```

### Subquery

Subqueries are divided into irrelevant subqueries and related subqueries by relevance.

#### Irrelevant subquery

Uncorrelated subqueries support [NOT] IN and EXISTS.

Example：

```sql
SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);
```

```sql
SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);
```

#### Related subquery

Related subqueries support [NOT] IN and [NOT] EXISTS.

Example：

```sql
SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);

SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);
```

Subqueries also support scalar quantum queries. It can be divided into irrelevant scalar quantum query, related scalar quantum query and scalar quantum query as parameters of the general function.

For example：

1. Uncorrelated scalar quantum query with predicate = sign. For example, output information about the person with the highest wage.

    ```sql
    SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
    ```

2. Uncorrelated scalar quantum queries with predicates `>, <` etc. For example, output information about people who are paid more than average.

    ```sql
    SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
    ```

3. Related scalar quantum queries. For example, output the highest salary information for each department.

    ```sql
    SELECT name FROM table a WHERE salary = （SELECT MAX(salary) FROM table b WHERE b.Department= a.Department）;
    ```

4. Scalar quantum queries are used as parameters of ordinary functions.

    ```sql
    SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));
    ```

### With clause

A clause that can be added before a SELECT statement to define an alias for a complex expression that is referenced multiple times inside SELECT.

Similar to CRATE VIEW, but the table and column names defined in the clause do not persist after the query ends and do not conflict with names in the actual table or VIEW.

The benefits of using a WITH clause are:

Convenient and easy to maintain, reducing duplication within queries.

 It is easier to read and understand SQL code by abstracting the most complex parts of a query into separate blocks.

Example：

```sql
-- Define one subquery at the outer level, and another at the inner level as part of the
-- initial stage of the UNION ALL query.
with t1 as (select 1) (with t2 as (select 2)

select * from t2) union all select * from t1;
```

### Where and Operator

SQL operators are a series of functions used for comparison and are widely used in where clauses of select statements.

#### Arithmetic operator

Arithmetic operators usually appear in expressions that contain left, right, and most often left operands

**+and-**：can be used as a unit or as a 2-ary operator. When used as a unit operator, such as +1, -2.5 or -col_ name, which means the value is multiplied by +1 or -1.

So the cell operator + returns an unchanged value, and the cell operator - changes the symbol bits of that value.

Users can overlay two cell operators, such as +5 (returning a positive value), -+2 or +2 (returning a negative value), but users cannot use two consecutive - signs.

Because--is interpreted as a comment in the following statement (when a user can use two-signs, a space or parenthesis is required between the two-signs, such as-(-2) or - -2, which actually results in + 2).

When + or - is a binary operator, such as 2+2, 3+1.5, or col1+col2, it means that the left value is added or subtracted from the right value. Both left and right values must be numeric types.

**and/**：represent multiplication and division, respectively. The operands on both sides must be data types. When two numbers are multiplied.

Smaller operands may be promoted if needed (e.g., SMALLINT to INT or BIGINT), and the result of the expression will be promoted to the next larger type.

For example, TINYINT multiplied by INT will produce a BIGINT type of result. When two numbers are multiplied, both operands and expression results are interpreted as DOUBLE types to avoid loss of precision.

If the user wants to convert the result of the expression to another type, it needs to be converted using the CAST function.

**%**：Modulation operator. Returns the remainder of the left operand divided by the right operand. Both left and right operands must be integers.

**&，|and ^**：The bitwise operator returns the result of bitwise AND, bitwise OR, bitwise XOR operations on two operands. Both operands require an integer type.

If the types of the two operands of a bitwise operator are inconsistent, the operands of a smaller type are promoted to the operands of a larger type, and the corresponding bitwise operations are performed.

Multiple arithmetic operators can appear in an expression, and the user can enclose the corresponding arithmetic expression in parentheses. Arithmetic operators usually do not have corresponding mathematical functions to express the same functions as arithmetic operators.

For example, we don't have the MOD() function to represent the% operator. Conversely, mathematical functions do not have corresponding arithmetic operators. For example, the power function POW() does not have a corresponding ** exponentiation operator.

 Users can find out which arithmetic functions we support through the Mathematical Functions section.

#### Between Operator

In a where clause, expressions may be compared with both upper and lower bounds. If the expression is greater than or equal to the lower bound and less than or equal to the upper bound, the result of the comparison is true. The syntax is defined as follows:

```sql
expression BETWEEN lower_bound AND upper_bound
```

Data type: Usually an expression evaluates to a numeric type, which also supports other data types. If you must ensure that both the lower and upper bounds are comparable characters, you can use the cast() function.

 Instructions for use: If the operand is of type string, note that a long string starting with an upper bound will not match the upper bound, which is larger than the upper bound. For example, "between'A'and'M' will not match'MJ'.

 If you need to make sure the expression works correctly, you can use functions such as upper (), lower (), substr (), trim ().

Example：

```sql
select c1 from t1 where month between 1 and 6;
```

#### Comparison operator

The comparison operator is used to determine whether columns and columns are equal or to sort them. =,!=, >= All data types are available.

The `<>` and `!=` operators indicate that value `a` is not equal to value `b`.

In Operator

The In operator compares to the VALUE collection and returns TRUE if it can match any of the elements in the collection.

Parameters and VALUE collections must be comparable. All expressions using the IN operator can be written as equivalent comparisons connected with OR, but the syntax of IN is simpler, more precise, and easier for StarRocks to optimize.

For example:

```sql
select * from small_table where tiny_column in (1,2);
```

#### Like Operator

This operator is used to compare to a string. ''matches a single character,'%' matches multiple characters. The parameter must match the complete string. Typically, placing'%'at the end of a string is more practical.

Example：

```plain text
mysql> select varchar_column from small_table where varchar_column like 'm%';

+----------------+
|varchar_column  |
+----------------+
|     milan      |
+----------------+

1 row in set (0.02 sec)
```

```plain
mysql> select varchar_column from small_table where varchar_column like 'm____';

+----------------+
| varchar_column | 
+----------------+
|    milan       | 
+----------------+

1 row in set (0.01 sec)
```

#### Logical Operator

Logical operators return a BOOL value, including unit and multiple operators, each of which handles parameters that are expressions that return BOOL values. Supported operators are:

AND: 2-ary operator, the AND operator returns TRUE if the parameters on the left and right are both calculated as TRUE.

OR: 2-ary operator that returns TRUE if one of the parameters on the left and right is calculated as TRUE. If both parameters are FALSE, the OR operator returns FALSE.

NOT: Unit operator, the result of inverting an expression. If the parameter is TRUE, the operator returns FALSE; If the parameter is FALSE, the operator returns TRUE.

Example：

```plain text
mysql> select true and true;

+-------------------+
| (TRUE) AND (TRUE) | 
+-------------------+
|         1         | 
+-------------------+

1 row in set (0.00 sec)
```

```plain text
mysql> select true and false;

+--------------------+
| (TRUE) AND (FALSE) | 
+--------------------+
|         0          | 
+--------------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select true or false;

+-------------------+
| (TRUE) OR (FALSE) | 
+-------------------+
|        1          | 
+-------------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select not true;

+----------+
| NOT TRUE | 
+----------+
|     0    | 
+----------+

1 row in set (0.01 sec)
```

#### Regular Expression Operator

Determines whether the regular expression is matched. Using POSIX standard regular expressions,'^'matches the first part of the string,'$' matches the end of the string.

"." matches any single character, "*" matches zero or more options, "+" matches one or more options, "?" means greedy representation, and so on. Regular expressions need to match complete values, not just parts of strings.

If you want to match the middle part, the front part of the regular expression can be written as'^. 'or'.'. '^'and'$' are usually omitted. The RLIKE operator and the REGEXP operator are synonyms.

The'|'operator is an optional operator. Regular expressions on either side of'|' only need to satisfy one side condition. The'|'operator and regular expressions on both sides usually need to be enclosed in ().

Example：

```plain text
mysql> select varchar_column from small_table where varchar_column regexp '(mi|MI).*';

+----------------+
| varchar_column | 
+----------------+
|     milan      |       
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from small_table where varchar_column regexp 'm.*';

+----------------+
| varchar_column | 
+----------------+
|     milan      |  
+----------------+

1 row in set (0.01 sec)
```

### Alias

When you write the names of tables, columns, or expressions that contain columns in a query, you can assign them an alias at the same time.

Aliases can be used to access tables and columns when they are needed. Aliases are usually shorter and better remembered than their original names. When a new alias is needed,

Simply add an AS alias clause after the table, column, and expression names in the select list or from list.

AS keywords are optional and users can specify aliases directly after the original name. If an alias or other identifier has the same name as an internal keyword, you need to add a ``symbol to the name. Aliases are case sensitive.

Example：

```sql
select tiny_column as name, int_column as sex from big_table;

select sum(tiny_column) as total_count from big_table;

select one.tiny_column, two.int_column from small_table one, <br/> big_table two where one.tiny_column = two.tiny_column;
```

## Keyword

SELECT
