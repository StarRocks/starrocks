---
displayed_sidebar: "English"
---

# DELETE

Deletes data rows from a table based on the specified conditions. The table can be a partitioned or non-partitioned table.

For Duplicate Key tables, Aggregate tables, and Unique Key tables, you can delete data from specified partitions. However, Primary Key tables do not allow you to do so. From v2.3, Primary Key tables support complete `DELETE...WHERE` semantics, which allows you to delete data rows based on the primary key, any column, or the results of a subquery. From v3.0, StarRocks enriches the `DELETE...WHERE` semantics with multi-table joins and common table expressions (CTEs). If you need to join Primary Key tables with other tables in the database, you can reference these other tables in the USING clause or CTE.

## Usage notes

- You must have privileges on the table and database you want to perform DELETE.
- Frequent DELETE operations are not recommended. If needed, perform such operations during off-peak hours.
- The DELETE operation only deletes data in the table. The table remains. To drop the table, run [DROP TABLE](../data-definition/DROP_TABLE.md).
- To prevent misoperations from deleting data in the entire table, you must specify the WHERE clause in the DELETE statement.
- The deleted rows are not immediately cleaned. They are marked as "deleted" and will be temporarily saved in Segment. Physically, the rows are removed only after data version merge (compaction) is completed.
- This operation also deletes data of the materialized views that reference this table.

## Duplicate Key tables, Aggregate tables, and Unique Key tables

### Syntax

```SQL
DELETE FROM  [<db_name>.]<table_name> [PARTITION <partition_name>]
WHERE
column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...]
```

### Parameters

| **Parameter**         | **Required** | **Description**                                                     |
| :--------------- | :------- | :----------------------------------------------------------- |
| `db_name`        | No       | The database to which the destination table belongs. If this parameter is not specified, the current database is used by default.      |
| `table_name`     | Yes      | The table from which you want to delete data.   |
| `partition_name` | No       | The partition from which you want to delete data.  |
| `column_name`    | Yes      | The column you want to use as the DELETE condition. You can specify one or more columns.   |
| `op`             | Yes      | The operator used in the DELETE condition. The supported operators are `=`, `>`, `<`, `>=`, `<=`, `!=`, `IN`, and `NOT IN`. |

### Limits

- For Duplicate Key tables, you can use **any column** as the DELETE condition. For Aggregate tables and Unique Key tables, only **key columns** can be used as the DELETE condition.

- The conditions that you specify must be in the AND relation. If you want to specify conditions in OR relation, you must specify the conditions in separate DELETE statements.

- For Duplicate Key tables, Aggregate tables, and Unique Key tables, the DELETE statement does not support using subquery results as conditions.

### Impacts

After you execute the DELETE statement, the query performance of your cluster may deteriorate for a period of time (before compaction is completed). The degree of deterioration varies based on the number of conditions that you specify. A larger number of conditions indicates a higher degree of deterioration.

### Examples

#### Create a table and insert data

The following example creates a partitioned Duplicate Key table.

```SQL
CREATE TABLE `my_table` (
    `date` date NOT NULL,
    `k1` int(11) NOT NULL COMMENT "",
    `k2` varchar(65533) NULL DEFAULT "" COMMENT "")
DUPLICATE KEY(`date`)
PARTITION BY RANGE(`date`)
(
    PARTITION p1 VALUES [('2022-03-11'), ('2022-03-12')),
    PARTITION p2 VALUES [('2022-03-12'), ('2022-03-13'))
)
DISTRIBUTED BY HASH(`date`)
PROPERTIES
("replication_num" = "3");

INSERT INTO `my_table` VALUES
('2022-03-11', 3, 'abc'),
('2022-03-11', 2, 'acb'),
('2022-03-11', 4, 'abc'),
('2022-03-12', 2, 'bca'),
('2022-03-12', 4, 'cba'),
('2022-03-12', 5, 'cba');
```

#### Query data

```plain
select * from my_table order by date;
+------------+------+------+
| date       | k1   | k2   |
+------------+------+------+
| 2022-03-11 |    3 | abc  |
| 2022-03-11 |    2 | acb  |
| 2022-03-11 |    4 | abc  |
| 2022-03-12 |    2 | bca  |
| 2022-03-12 |    4 | cba  |
| 2022-03-12 |    5 | cba  |
+------------+------+------+
```

#### Delete data

##### Delete data from a specified partition

Delete rows whose `k1` values are `3` from the `p1` partition.

```plain
DELETE FROM my_table PARTITION p1
WHERE k1 = 3;

-- The query result shows rows whose `k1` values are `3` are deleted.

select * from my_table partition (p1);
+------------+------+------+
| date       | k1   | k2   |
+------------+------+------+
| 2022-03-11 |    2 | acb  |
| 2022-03-11 |    4 | abc  |
+------------+------+------+
```

##### Delete data from a specified partition using AND

Delete rows whose `k1` values are greater than or equal to `3` and whose `k2` values are `"abc"` from the `p1` partition.

```plain
DELETE FROM my_table PARTITION p1
WHERE k1 >= 3 AND k2 = "abc";

select * from my_table partition (p1);
+------------+------+------+
| date       | k1   | k2   |
+------------+------+------+
| 2022-03-11 |    2 | acb  |
+------------+------+------+
```

##### Delete data from all partitions

Delete rows whose `k2` values are `"abc"` or `"cba"` from all partitions.

```plain
DELETE FROM my_table
WHERE  k2 in ("abc", "cba");

select * from my_table order by date;
+------------+------+------+
| date       | k1   | k2   |
+------------+------+------+
| 2022-03-11 |    2 | acb  |
| 2022-03-12 |    2 | bca  |
+------------+------+------+
```

## Primary Key tables

From v2.3, Primary Key tables support complete `DELETE...WHERE` semantics, which allows you to delete data rows based on the primary key, any column, or a subquery.

### Syntax

```SQL
[ WITH <with_query> [, ...] ]
DELETE FROM <table_name>
[ USING <from_item> [, ...] ]
[ WHERE <where_condition> ]
```

### Parameters

|   **Parameter**   | **Required** | **Description**                                              |
| :---------------: | :----------- | :----------------------------------------------------------- |
|   `with_query`    | No           | One or more CTEs that can be referenced by name in a DELETE statement. CTEs are temporary result sets that can improve the readability of complex statements. |
|   `table_name`    | Yes          | The table from which you want to delete data.                |
|    `from_item`    | No           | One or more other tables in the database. These tables can be joined with the table being operated based on the condition specified in the WHERE clause. Based on the result set of the join query, StarRocks deletes the matched rows from the table being operated. For example, if the USING clause is `USING t1 WHERE t0.pk = t1.pk;`, StarRocks converts the table expression in the USING clause to `t0 JOIN t1 ON t0.pk=t1.pk;` when executing the DELETE statement. |
| `where_condition` | Yes          | The condition based on which you want to delete rows. Only rows that meet the WHERE condition can be deleted. This parameter is required, because it helps prevent you from accidentally deleting the entire table. If you want to delete the entire table, you can use 'WHERE true'. |

### Limits

- The following comparison operators are supported: `=`, `>`, `<`, `>=`, `<=`, `!=`, `IN`, `NOT IN`.

- The following logical operators are supported: `AND` and `OR`.

- You cannot use the DELETE statement to run concurrent DELETE operations or to delete data at data loading. If you perform such operations, the atomicity, consistency, isolation, and durability (ACID) of transactions may not be ensured.

### Examples

#### Create a table and insert data

Create a Primary Key table named `score_board`:

```sql
CREATE TABLE `score_board` (
  `id` int(11) NOT NULL COMMENT "",
  `name` varchar(65533) NULL DEFAULT "" COMMENT "",
  `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
"replication_num" = "3",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false"
);

INSERT INTO score_board VALUES
(0, 'Jack', 21),
(1, 'Bob', 21),
(2, 'Stan', 21),
(3, 'Sam', 22);
```

#### Query data

Execute the following statement to insert data into the `score_board` table:

```Plain
select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    0 | Jack |   21 |
|    1 | Bob  |   21 |
|    2 | Stan |   21 |
|    3 | Sam  |   22 |
+------+------+------+
4 rows in set (0.00 sec)
```

#### Delete data

##### Delete data by primary key

You can specify the primary key in the DELETE statement, so StarRocks does not need to scan the entire table.

Delete rows whose `id` values are `0` from the `score_board` table.

```Plain
DELETE FROM score_board WHERE id = 0;

select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    1 | Bob  |   21 |
|    2 | Stan |   21 |
|    3 | Sam  |   22 |
+------+------+------+
```

##### Delete data by condition

Example 1: Delete rows whose `score` values are `22` from the `score_board` table.

```Plain
DELETE FROM score_board WHERE score = 22;

select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    0 | Jack |   21 |
|    1 | Bob  |   21 |
|    2 | Stan |   21 |
+------+------+------+
```

Example 2: Delete rows whose `score` values are less than `22` from the `score_board` table.

```Plain
DELETE FROM score_board WHERE score < 22;

select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    3 | Sam  |   22 |
+------+------+------+
```

Example 3: Delete rows whose `score` values are less than `22` and whose `name` values are not `Bob` from the `score_board` table.

```Plain
DELETE FROM score_board WHERE score < 22 and name != "Bob";

select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    1 | Bob  |   21 |
|    3 | Sam  |   22 |
+------+------+------+
2 rows in set (0.00 sec)
```

##### Delete data by subquery result

You can nest one or more subqueries in the `DELETE` statement and use the subquery results as conditions.

Before you delete data, execute the following statement to create another table named `users`:

```SQL
CREATE TABLE `users` (
  `uid` int(11) NOT NULL COMMENT "",
  `name` varchar(65533) NOT NULL COMMENT "",
  `country` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`uid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`uid`)
PROPERTIES (
"replication_num" = "3",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false"
);
```

Insert data into the `users` table:

```SQL
INSERT INTO users VALUES
(0, "Jack", "China"),
(2, "Stan", "USA"),
(1, "Bob", "China"),
(3, "Sam", "USA");
```

```plain
select * from users;
+------+------+---------+
| uid  | name | country |
+------+------+---------+
|    0 | Jack | China   |
|    1 | Bob  | China   |
|    2 | Stan | USA     |
|    3 | Sam  | USA     |
+------+------+---------+
4 rows in set (0.00 sec)
```

Nest a subquery to find the rows whose `country` values are `China` from the `users` table, and delete the rows, which have the same `name` values as the rows returned from the subquery, from the `score_board` table. You can use one of the following methods to fulfill the purpose:

- Method 1

```plain
DELETE FROM score_board
WHERE name IN (select name from users where country = "China");
    
select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    2 | Stan |   21 |
|    3 | Sam  |   22 |
+------+------+------+
```

- Method 2

```plain
DELETE FROM score_board
WHERE EXISTS (select name from users
              where score_board.name = users.name and country = "China");
    
select * from score_board;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|    2 | Stan |    21 |
|    3 | Sam  |    22 |
+------+------+-------+
2 rows in set (0.00 sec)
```

##### Delete data by using multi-table join or CTE

To delete all movies produced by the producer "foo", you can execute the following statement:

```SQL
DELETE FROM films USING producers
WHERE producer_id = producers.id
    AND producers.name = 'foo';
```

You can also use a CTE to rewrite the above statement to improve readability.

```SQL
WITH foo_producers as (
    SELECT * from producers
    where producers.name = 'foo'
)
DELETE FROM films USING foo_producers
WHERE producer_id = foo_producers.id;
```
