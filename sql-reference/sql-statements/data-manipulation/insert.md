# INSERT

## 功能

向 StarRocks 表中插入或覆盖写入数据。关于该种导入数据方式适用的场景请参考 [INSERT INTO 导入](../../../loading/InsertInto.md)。

## 语法

```SQL
INSERT {INTO | OVERWRITE} table_name
[ PARTITION (p1, ...) ]
[ WITH LABEL label]
[ (column [, ...]) ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

## 参数说明

| **参数**    | **说明**                                                     |
| ----------- | ------------------------------------------------------------ |
| INTO        | 将数据追加写入目标表。                                       |
| OVERWRITE   | 将数据覆盖写入目标表。                                       |
| table_name  | 导入数据的目标表。可以为 `db_name.table_name` 形式。         |
| partitions  | 导入的目标分区。此参数必须是目标表中存在的分区，多个分区名称用逗号（,）分隔。如果指定该参数，数据只会被导入相应分区内。如果未指定，则默认将数据导入至目标表的所有分区。 |
| label       | 导入作业的标识，数据库内唯一。如果未指定，StarRocks 会自动为作业生成一个 Label。建议您指定 Label。否则，如果当前导入作业因网络错误无法返回结果，您将无法得知该导入操作是否成功。如果指定了 Label，可以通过 SQL 命令 `SHOW LOAD WHERE label="label";` 查看任务结果。关于 Label 命名限制，参考[系统限制](/reference/System_limit.md)。 |
| column_name | 导入的目标列，必须是目标表中存在的列。该参数的对应关系与列名无关，但与其顺序一一对应。如果不指定目标列，默认为目标表中的所有列。如果源表中的某个列在目标列不存在，则写入默认值。如果当前列没有默认值，导入作业会失败。如果查询语句的结果列类型与目标列的类型不一致，会进行隐式转化，如果不能进行转化，那么 INSERT INTO 语句会报语法解析错误。 |
| expression  | 表达式，用以为对应列赋值。                                   |
| DEFAULT     | 为对应列赋予默认值。                                         |
| query       | 查询语句，查询的结果会导入至目标表中。查询语句支持任意 StarRocks 支持的 SQL 查询语法。 |

## 注意事项

- 当前版本中，StarRocks 在执行 INSERT 语句时，如果有数据不符合目标表格式的数据（例如字符串超长等情况），INSERT 操作默认执行失败。您可以通过设置会话变量 `enable_insert_strict` 为 `false` 以确保 INSERT 操作过滤不符合目标表格式的数据，并继续执行。

- 执行 INSERT OVERWRITE 语句后，系统将为目标分区创建相应的临时分区，并将数据写入临时分区，最后使用临时分区原子替换目标分区来实现覆盖写入。其所有过程均在在 Leader FE 节点执行。因此，如果 Leader FE 节点在覆盖写入过程中发生宕机，将会导致该次 INSERT OVERWRITE 导入失败，其过程中所创建的临时分区也会被删除。

## 示例

以下示例基于表 `test`，其中包含两个列 `c1` 和 `c2`。`c2` 列有默认值 DEFAULT。

### 示例一：向 test 表中导入一行数据

```SQL
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

- 在不指定目标列时，使用表中的列顺序来作为默认的目标列导入顺序。因此以上示例中，第一条、第二条 SQL 语句导入效果相同。
- 如果有目标列未插入数据或使用 DEFAULT 作为值插入数据，该列将使用默认值作为导入数据。因此以上示例中，第三条、第四条语句导入效果相同。

### 示例二：向 test 表中一次性导入多行数据

```SQL
INSERT INTO test VALUES (1, 2), (3, 2 + 2);
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2);
INSERT INTO test (c1) VALUES (1), (3);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
```

- 因表达式结果相同，以上示例中，第一条、第二条 SQL 语句导入效果相同。
- 第三条、第四条语句使用 DEFAULT 作为值插入数据，因此导入效果相同。

### 示例三：向 test 表中导入一个查询语句结果

```SQL
INSERT INTO test SELECT * FROM test2;
INSERT INTO test (c1, c2) SELECT * from test2;
```

### 示例四：向 test 表中导入一个查询语句结果，并指定分区和 Label

```SQL
INSERT INTO test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test2;
INSERT INTO test WITH LABEL `label1` (c1, c2) SELECT * from test2;
```

### 示例五：向 test 表中覆盖写入一个查询语句结果，并指定分区和 Label

```SQL
INSERT OVERWRITE test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test3;
INSERT OVERWRITE test WITH LABEL `label1` (c1, c2) SELECT * from test3;
```
