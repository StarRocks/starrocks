# DELETE

该语句用于按条件删除指定表的某一分区中的数据。

该操作会同时删除和该表相关的物化视图的数据。

## DELETE 与明细模型、聚合模型和更新模型

### 语法

```SQL
DELETE FROM table_name [PARTITION partition_name]
WHERE
column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...];
```

### 参数说明

- `table_name`：指定要操作的表名。该参数为必选。

- `PARTITION`：指定要操作的分区名。该参数为必选。

- `column_name`：指定作为删除条件的列。可以指定一个或多个列。

- `op`：指定算子。支持 `=`、`>`、`<`、`>=`、`<=`、`!=`、`IN` 和 `NOT IN`。

> 说明：方括号 ([]) 中参数如无需指定的话可省略不写。

### 注意事项

- 非明细模型表只能指定 Key 列上的条件。

- 当选定的 Key 列不存在于某个物化视图中时，无法执行 DELETE 语句。

- 条件之间只能是“与”的关系。 若希望达成“或”的关系，需要将条件分别写在两个 DELETE 语句中。

### 使用限制

DELETE 语句目前不支持以子查询结果作为删除条件。

### 影响

执行 DELETE 语句后，可能会导致接下来一段时间内的查询效率降低。影响程度取决于语句中指定的删除条件的数量。指定的条件越多，影响越大。

### 示例

示例 1：删除 `my_table` 表 `p1` 分区中 `k1` 列值为 `3` 的数据行。

```SQL
DELETE FROM my_table PARTITION p1
WHERE k1 = 3;
```

示例 2：删除 `my_table` 表 `p1` 分区中 `k1` 列值大于等于 `3` 且 `k2` 列值为 `"abc"` 的数据行。

```SQL
DELETE FROM my_table PARTITION p1
WHERE k1 >= 3 AND k2 = "abc";
```

示例 3：删除 `my_table` 表所有分区中 `k2` 列值为 `"abc"` 或 `"cba"` 的数据行。

```SQL
DELETE FROM my_table
WHERE  k2 in ("abc", "cba");
```

## DELETE 与主键模型

### 语法

```SQL
DELETE FROM table_name WHERE condition;
```

### 参数说明

- `table_name`：指定要操作的表名。该参数为必选。

- `condition`：指定删除条件。可以指定一个或多个条件。该参数为必选，可以防止删除整张表。

### 注意事项

- 支持如下比较运算符：`=`、`>`、`<`、`>=`、`<=`、`!=`、`IN` 和 `NOT IN`。

- 支持如下逻辑运算符：`AND` 和 `OR`。

### 使用限制

不支持并发删除或导入数据时删除，因为有可能会导致事务性无法保证的问题。

### 示例

#### 创建表

在数据库 `test` 中创建一张名为 `score_board` 的表，如下所示：

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

#### 插入数据

向 `score_board` 表插入数据，如下所示：

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

#### 删除数据

##### 按主键删除数据

通过指定主键，可以避免全表扫描。

示例：删除 `score_board` 表中 `id` 列值为 `0` 的数据行。

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

##### 按条件删除数据

示例 1：删除 `score_board` 表中 `score` 列值等于 `22` 的所有数据行。

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

示例 2：删除 `score_board` 表中 `score` 列值小于 `22` 的所有数据行。

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

示例 3：删除 `score_board` 表中 `score` 列值小于 `22`、且 `name` 列值不为 `Bob` 的所有数据行。

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

##### 按子查询结果删除数据

可以在 `DELETE` 语句中嵌套一个或多个子查询，并使用子查询结果作为删除条件。

开始删除操作之前，先在数据库 `test` 中再创建一张名为 `users` 的表，如下所示：

```SQL
CREATE TABLE `users` (
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

然后，向 `users` 表中插入数据，如下所示：

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

查询 `users` 表中 `country` 列值为 `China` 的数据行，然后删除 `score_board` 表中与 `users` 表中查询到的数据行具有相同 `name` 列值的所有数据行。有两种实现方法，如下所示：

- 方法 1

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

- 方法 2

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
