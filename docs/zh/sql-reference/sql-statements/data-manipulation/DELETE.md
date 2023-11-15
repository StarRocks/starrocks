# DELETE

## 功能

该语句用于按条件删除表中的数据。注意该操作会同时删除和该表相关的物化视图的数据。

对于明细模型、聚合模型、更新模型表，支持删除表中指定分区的数据。主键模型目前还不支持删除指定分区中的数据。

```SQL
DELETE FROM table_name [PARTITION partition_name]
WHERE
column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...];
```

### 参数说明

- `table_name`：要操作的表名，必选。

- `PARTITION`：要操作的分区名，必选。

- `column_name`：作为删除条件的列。可以指定一个或多个列。

- `op`：指定算子。支持 `=`、`>`、`<`、`>=`、`<=`、`!=`、`IN` 和 `NOT IN`。

> 说明：方括号 ([]) 中参数如无需指定的话可省略不写。

### 注意事项

- 聚合模型和更新模型表仅支持 Key 列作为删除条件。明细模型表支持任意列作为删除条件。

- 当指定的 Key 列不存在于某个物化视图中时，无法执行 DELETE 语句。

- 条件之间只能是“与”的关系。 若希望达成“或”的关系，需要将条件分别写在两个 DELETE 语句中。

### 使用限制

明细模型、聚合模型和更新模型下，DELETE 语句目前不支持以子查询结果作为删除条件。

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

- `table_name`：要操作的表名，必选。

- `condition`：删除条件，可以指定一个或多个条件。该参数为必选，防止删除整张表。

### 注意事项

- 支持如下比较运算符：`=`、`>`、`<`、`>=`、`<=`、`!=`、`IN` 和 `NOT IN`。

- 支持如下逻辑运算符：`AND` 和 `OR`。

### 使用限制

注：方括号 [] 中内容如无需指定可省略不写。

```sql
DELETE FROM table_name [PARTITION partition_name]
WHERE
column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...];
```

说明：

1. op 的可选类型包括：`=, >, <, > =, <=, !=, in, not in`
2. 非 Duplicate 表只能指定 key 列上的条件。
3. 当选定的 key 列不存在于某个 rollup 中时，无法进行 delete。
4. 条件之间只能是“与”的关系。
若希望达成“或”的关系，需要将条件分写在两个 DELETE 语句中。

注意：

1.该语句可能会降低执行后一段时间内的查询效率。
影响程度取决于语句中指定的删除条件的数量。
指定的条件越多，影响越大。
2.delete 语句删除条件目前不支持是子查询结果。

## 示例

1. 删除 my_table partition p1 中 k1 列值为 3 的数据行

    ```sql
    DELETE FROM my_table PARTITION p1
    WHERE k1 = 3;
    ```

2. 删除 my_table partition p1 中 k1 列值大于等于 3 且 k2 列值为 "abc" 的数据行

    ```sql
    DELETE FROM my_table PARTITION p1
    WHERE k1 >= 3 AND k2 = "abc";
    ```

3. 删除 my\_table 所有分区中 k2 列值为 "abc" 或者 "cba" 的数据行

    ```sql
    DELETE FROM my_table
    WHERE  k2 in ("abc", "cba");
    ```

## 关键字(keywords)

DELETE
