# DELETE

Deletes data from a partition of a table based on specified conditions and deletes the data about the materialized views that reference the table.

## DELETE and Duplicate Key model, Aggregate Key model, and Unique Key model

### Syntax

```SQL
DELETE FROM <table_name> [PARTITION <partition_name>]
WHERE
column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...]
```

### Parameters

- `table_name`: the name of the table from which you want to delete data. This parameter is required.

- `PARTITION`: the name of the partition from which you want to delete data. This parameter is required.

- `column_name`: the names of the columns that you want to specify as conditions. You can specify one or more columns.

- `op`: the operator that you want to use. The supported operators are `=`, `>`, `<`, `>=`, `<=`, `!=`, `IN`, and `NOT IN`.

> Note: The parameters enclosed in square brackets ([]) are optional. You can leave these parameters unspecified.

### Usage notes

- If a table uses any data model except the Duplicate Key model, you can specify only the primary key columns of the table as conditions.

- If a primary key column that you specify as a condition for data deletion from a table cannot be found in the associated materialized view, the DELETE statement cannot be executed on the table.

- The conditions that you specify are in AND relations. If you want to specify conditions that are in OR relations, you must specify each condition by using one DELETE statement.

### Limits

The DELETE statement does not support subquery results as conditions.

### Impacts

After you execute the DELETE statement, the query performance of your cluster may deteriorate and remains poor for a period of time. The degree of deterioration varies based on the number of conditions that you specify. A larger number of conditions indicates a higher degree of deterioration.

### Examples

Example 1: Delete the rows whose `k1` values are `3` from the `p1` partition of the `my_table` table.

```SQL
DELETE FROM my_table PARTITION p1
WHERE k1 = 3;
```

Example 2: Delete the rows whose `k1` values are greater than or equal to `3` and whose `k2` values are `"abc"` from the `p1` partition of the `my_table` table.

```SQL
DELETE FROM my_table PARTITION p1
WHERE k1 >= 3 AND k2 = "abc";
```

Example 3: Delete the rows whose `k2` values are `"abc"` or `"cba"` from the `my_table` table.

```SQL
DELETE FROM my_table
WHERE  k2 in ("abc", "cba");
```

## DELETE and Primary Key model

### Syntax

```SQL
DELETE FROM table_name WHERE condition;
```

### Parameters

- `table_name`: the name of the table from which you want to delete data. This parameter is required.

- `condition`: the conditions based on which you want to delete data. You can specify one or more conditions. This parameter is required, so you cannot delete the entire table.

### Usage notes

- The following comparison operators are supported: `=`, `>`, `<`, `>=`, `<=`, `!=`, `IN`, and `NOT IN`.

- The following logical operators are supported: `AND` and `OR`.

### Limits

You cannot use the DELETE statement to concurrently run delete operations or to delete data at data loading. If you perform these operations, the atomicity, consistency, isolation, and durability (ACID) of transactions may not be ensured.

### Examples

#### Create a table

Execute the following statement to create a table named `score_board` in the `test` database:

```Plain
MySQL [test]> CREATE TABLE `score_board` (
  `id` int(11) NOT NULL COMMENT "",
  `name` varchar(65533) NULL DEFAULT "" COMMENT "",
  `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false"
);
```

#### Insert data

Execute the following statement to insert data into the `score_board` table:

```Plain
MySQL [test]> insert into score_board values(0, 'Jack', 21), (1, 'Bob', 21), (2, 'Stan', 21), (3, 'Sam', 22);

MySQL [test]> select * from score_board;
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

Example: Delete the rows whose `id` values are `0` from the `score_board` table.

```Plain
MySQL [test]> delete from score_board where id = 0;

MySQL [test]> select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    1 | Bob  |   21 |
|    2 | Stan |   21 |
|    3 | Sam  |   22 |
+------+------+------+
3 rows in set (0.01 sec)
```

##### Delete data by condition

Example 1: Delete the rows whose `score` values are `22` from the `score_board` table.

```Plain
MySQL [test]> delete from score_board where score = 22;

MySQL [test]> select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    0 | Jack |   21 |
|    1 | Bob  |   21 |
|    2 | Stan |   21 |
+------+------+------+
3 rows in set (0.01 sec)
```

Example 2: Delete the rows whose `score` values are less than `22` from the `score_board` table.

```Plain
MySQL [test]> delete from score_board where score < 22;

MySQL [test]> select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    3 | Sam  |   22 |
+------+------+------+
1 row in set (0.01 sec)
```

Example 3: Delete the rows whose `score` values are less than `22` and whose `name` values are not `Bob` from the `score_board` table.

```Plain
MySQL [test]> delete from score_board where score < 22 and name != "Bob";

MySQL [test]> select * from score_board;
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

Before you start to delete data, execute the following statement to create another table named `users` in the `test` database:

```SQL
MySQL [test]> CREATE TABLE `users` (
  `uid` int(11) NOT NULL COMMENT "",
  `name` varchar(65533) NOT NULL COMMENT "",
  `country` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`uid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`uid`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false"
);
```

Then, execute the following statement to insert data into the `users` table:

```SQL
MySQL [test]> insert into users values (0, "Jack", "China"), (2, "Stan", "USA"), (1, "Bob", "China"), (3, "Sam", "USA");

MySQL [test]> select * from users;
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

```SQL
MySQL [test]> delete from score_board where name in (select name from users where country = "China");
    
MySQL [test]> select * from score_board;
+------+------+------+
| id   | name | score|
+------+------+------+
|    2 | Stan |   21 |
|    3 | Sam  |   22 |
+------+------+------+
2 rows in set (0.00 sec)
```

- Method 2

```SQL
MySQL [test]> delete from score_board where exists (select name from users where score_board.name = users.name and country = "China");
    
MySQL [test]> select * from score_board;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|    2 | Stan |    21 |
|    3 | Sam  |    22 |
+------+------+-------+
2 rows in set (0.00 sec)
```
