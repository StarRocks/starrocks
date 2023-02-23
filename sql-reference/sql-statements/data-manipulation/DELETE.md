# DELETE

该语句用于从表中删除满足条件的数据行。表可以是分区表或者非分区表。

对于明细类型 (Duplicate Key)、聚合类型 (Aggregate Key)，以及更新类型表 (Unique Key)，支持删除表中**指定分区**的数据。主键类型表 (Primary Key) 目前还**不支持**删除指定分区中的数据。从 2.3 版本开始，主键类型表支持完整的 DELETE...WHERE 语义，即支持按主键、任意列、以及子查询结果删除数据。

## 注意事项

- 执行 DELETE 操作需要有对应数据库和表的操作权限。
- 不建议您执行高频的 DELETE 操作。如需要，请在业务低峰期进行。
- DELETE 删除的是表中数据，表依然存在。如果要删除表，参见 [DROP TABLE](../data-definition/DROP%20TABLE.md)。
- 该操作会同时删除和表相关的物化视图的数据。
- 删除的数据不可恢复。为防止误删整表，必须指定 WHERE 子句。
- 删除的数据暂时保留在 segment 中，compaction （数据版本合并）之后会被回收。

## DELETE 与明细模型、聚合模型和更新模型

### 语法

```SQL
DELETE FROM table_name [PARTITION partition_name]
WHERE
column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...]
```

### 参数说明

- `table_name`：要操作的表名，必选。

- `partition_name`：要操作的分区名，可选。

- `column_name`：作为删除条件的列。可以指定一个或多个列，必须指定 WHERE 子句中的过滤条件。

- `op`：指定算子。支持 `=`、`>`、`<`、`>=`、`<=`、`!=`、`IN` 和 `NOT IN`。

> 说明：方括号 ([]) 中参数如无需指定的话可省略不写。

### 注意事项

- 聚合模型和更新模型表仅支持 Key 列作为删除条件。明细模型表支持任意列作为删除条件。

- 当指定的 Key 列不存在于某个物化视图中时，无法执行 DELETE 语句。

- 条件之间只能是“与”的关系。 若希望达成“或”的关系，需要将条件分别写在两个 DELETE 语句中。

### 使用限制

明细模型、聚合模型和更新模型下，DELETE 语句目前不支持以子查询结果作为删除条件。

### 影响

执行 DELETE 语句后，可能会导致接下来一段时间内（compaction 操作执行完成之前）的查询效率降低。影响程度取决于语句中指定的删除条件的数量。指定的条件越多，影响越大。

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

从 2.3 版本开始，主键模型支持完整的 DELETE WHERE 语义。

### 语法

```SQL
DELETE FROM table_name WHERE condition;
```

### 参数说明

- `table_name`：要操作的表名，必选。

- `condition`：删除条件，可以指定一个或多个条件。该参数为必选，防止删除整张表。

### 注意事项

- 支持如下比较运算符：`=`、`>`、`<`、`>=`、`<=`、`!=`、`IN` 和 `NOT IN`。

- 支持如下逻辑运算符：`AND` 和 `OR`。

### 使用限制

不支持并发删除或导入数据时删除，因为可能无法保证导入的事务性。

### 示例

#### 创建表

在数据库 `test` 中创建一张名为 `score_board` 的主键模型表，如下所示：

```SQL
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

```sql
MySQL [test]> insert into score_board VALUES
  (0, 'Jack', 21),
  (1, 'Bob', 21),
  (2, 'Stan', 21),
  (3, 'Sam', 22);
```

查询表数据：

```Plain
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

条件中的列，可以为任意列。

示例一：删除 `score_board` 表中 `score` 列值等于 `22` 的所有数据行。

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

示例二：删除 `score_board` 表中 `score` 列值小于 `22` 的所有数据行。

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

示例三：删除 `score_board` 表中 `score` 列值小于 `22`、且 `name` 列值不为 `Bob` 的所有数据行。

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

向 `users` 表中插入数据，如下所示：

```SQL
MySQL [test]> insert into users VALUES
(0, "Jack", "China"),
(2, "Stan", "USA"),
(1, "Bob", "China"),
(3, "Sam", "USA");
```

```Plain
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
MySQL [test]> delete from score_board
WHERE name in (select name from users where country = "China");
 ```

```Plain
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
MySQL [test]> delete from score_board
WHERE exists (select name from users
              where score_board.name = users.name and country = "China");
 ```

```Plain
MySQL [test]> select * from score_board;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|    2 | Stan |    21 |
|    3 | Sam  |    22 |
+------+------+-------+
2 rows in set (0.00 sec)
```
