---
displayed_sidebar: docs
---

# INSERT

## 功能

向 StarRocks 表中插入或覆盖写入数据。关于该种导入数据方式适用的场景请参考 [INSERT INTO 导入](../../../loading/InsertInto.md)。

您可以通过 [SUBMIT TASK](ETL/SUBMIT_TASK.md) 创建异步 INSERT 任务。

## 语法

- **导入**:

  ```sql
  INSERT { INTO | OVERWRITE } [db_name.]<table_name>
  [ PARTITION (<partition_name> [, ...] ) ]
  [ TEMPORARY PARTITION (<temporary_partition_name> [, ...] ) ]
  [ WITH LABEL <label>]
  [ (<column_name>[, ...]) ]
  [ PROPERTIES ("key"="value", ...) ]
  { VALUES ( { <expression> | DEFAULT } [, ...] ) | <query> }
  ```

- **导出**:

  ```sql
  INSERT INTO FILES()
  [ WITH LABEL <label> ]
  { VALUES ( { <expression> | DEFAULT } [, ...] ) | <query> }
  ```

## 参数说明

| **参数**    | **说明**                                                     |
| ----------- | ------------------------------------------------------------ |
| INTO        | 将数据追加写入目标表。                                       |
| OVERWRITE   | 将数据覆盖写入目标表。                                       |
| table_name  | 导入数据的目标表。可以为 `db_name.table_name` 形式。         |
| PARTITION  | 导入的目标分区。此参数必须是目标表中存在的分区，多个分区名称用逗号（,）分隔。如果指定该参数，数据只会被导入相应分区内。如果未指定，则默认将数据导入至目标表的所有分区。 |
| TEMPORARY PARTITION | 指定要把数据导入哪些[临时分区](../../../table_design/data_distribution/Temporary_partition.md)。|
| label       | 导入作业的标识，数据库内唯一。如果未指定，StarRocks 会自动为作业生成一个 Label。建议您指定 Label。否则，如果当前导入作业因网络错误无法返回结果，您将无法得知该导入操作是否成功。如果指定了 Label，可以通过 SQL 命令 `SHOW LOAD WHERE label="label";` 查看任务结果。关于 Label 命名要求，参见[系统限制](../../System_limit.md)。 |
| column_name | 导入的目标列，必须是目标表中存在的列。**不可同时指定 `column_name` 和 `BY NAME`。**<ul><li>如果未指定 `BY NAME`，则目标列的对应关系与列名无关，但与其顺序一一对应。</li><li>如果指定了 `BY NAME`，则目标列的对应关系与顺序无关，系统将根据同名列匹配。</li><li>如果不指定目标列，默认为目标表中的所有列。</li><li>如果源表中的某个列在目标列不存在，则写入默认值。</li><li>如果当前列没有默认值，导入作业会失败。</li><li>如果查询语句的结果列类型与目标列的类型不一致，会进行隐式转化，如果不能进行转化，那么 INSERT INTO 语句会报语法解析错误。</li></ul>**说明**<br />自 v3.3.1 起，INSERT INTO 导入主键表时指定 Column List 会执行部分列更新（而在先前版本中，指定 Column List 仍然导致 Full Upsert）。如不指定 Column List，系统执行 Full Upsert。 |
| BY NAME     | 系统根据列名匹配相同名称的列。**不可同时指定 `column_name` 和 `BY NAME`。**如果未指定 `BY NAME`，则目标列的对应关系与列名无关，但与其顺序一一对应。|
| PROPERTIES  | INSERT 作业的属性Properties of the INSERT job. 每个属性必须为一对键值。关于支持的属性，参考 [PROPERTIES](#properties)。 |
| expression  | 表达式，用以为对应列赋值。                                   |
| DEFAULT     | 为对应列赋予默认值。                                         |
| query       | 查询语句，查询的结果会导入至目标表中。查询语句支持任意 StarRocks 支持的 SQL 查询语法。 |
| FILES()       | 表函数 [FILES()](../../sql-functions/table-functions/files.md)。您可以通过该函数将数据导出至远端存储。更多信息，请参考 [使用 INSERT INTO FILES() 导出数据](../../../unloading/unload_using_insert_into_files.md). |

### PROPERTIES

INSERT statements support configuring PROPERTIES from v3.4.0 onwards.

| 属性              | 说明                                                         |
| ---------------- | ------------------------------------------------------------ |
| timeout          | INSERT 作业的超时时间。单位：秒。您也可以通过变量 `insert_timeout` 在当前 Session 中或全局设置 INSERT 的超时时间。 |
| strict_mode      | 是否在使用 INSERT from FILES() 导入数据时启用严格模式。有效值：`true` 和 `false`（默认值）。启用严格模式时，系统仅导入合格的数据行，过滤掉不合格的行，并返回不合格行的详细信息。更多信息请参见 [严格模式](../../../loading/load_concept/strict_mode.md)。您也可以通过变量 `enable_insert_strict` 在当前 Session 中或全局启用 INSERT 的严格模式。 |
| max_filter_ratio | INSERT from FILES() 导入作业的最大容忍率，即导入作业能够容忍的因数据质量不合格而过滤掉的数据行所占的最大比例。当不合格行数比例超过该限制时，导入作业失败。默认值：`0`。范围：[0, 1]。您也可以通过变量 `insert_max_filter_ratio` 在当前 Session 中或全局设置 INSERT 的最大错误容忍度。 |

:::note

- `strict_mode` 和 `max_filter_ratio` 仅支持 INSERT from FILES() 导入方式。INSERT from Table 导入方式不支持以上属性。
- 从 v3.4.0 起，当 `enable_insert_strict` 设置为 `true` 时，系统只导入合格的数据行，过滤掉不合格行，并返回不合格行的详细信息。在早于 v3.4.0 的版本中，当 `enable_insert_strict` 设置为 `true` 时，INSERT 作业会在出现不合格行时失败。

:::

## 注意事项

- 当前版本中，StarRocks 在执行 INSERT 语句时，如果有数据不符合目标表格式的数据（例如字符串超长等情况），INSERT 操作默认执行失败。您可以通过设置会话变量 `enable_insert_strict` 为 `false` 以确保 INSERT 操作过滤不符合目标表格式的数据，并继续执行。

- 执行 INSERT OVERWRITE 语句后，系统将为目标分区创建相应的临时分区，并将数据写入临时分区，最后使用临时分区原子替换目标分区来实现覆盖写入。其所有过程均在在 Leader FE 节点执行。因此，如果 Leader FE 节点在覆盖写入过程中发生宕机，将会导致该次 INSERT OVERWRITE 导入失败，其过程中所创建的临时分区也会被删除。

### Dynamic Overwrite

从 v3.4.0 开始，StarRocks 支持分区表的 INSERT OVERWRITE 操作的新语义 — Dynamic Overwrite。

当前 INSERT OVERWRITE 默认行为如下：

- 当覆盖整个分区表（即未指定 PARTITION 子句）时，新数据会替换对应分区中的数据。如果存在表中已有分区未涉及覆盖操作，系统会清空该分区数据。
- 当覆盖空的分区表（即其中没有任何分区）但指定了 PARTITION 子句时，系统会报错 `ERROR 1064 (HY000): Getting analyzing error. Detail message: Unknown partition 'xxx' in table 'yyy'`。
- 当覆盖分区表时指定了不存在的分区，系统会报错 `ERROR 1064 (HY000): Getting analyzing error. Detail message: Unknown partition 'xxx' in table 'yyy'`。
- 当覆盖分区表的数据与指定的分区不匹配时，如果开启严格模式，系统会报错 `ERROR 1064 (HY000): Insert has filtered data in strict mode`；如果未开启严格模式，系统会过滤不合格的数据。

新的 Dynamic Overwrite 语义的行为与上述默认行为有很大不同：

当覆盖整个分区表时，新数据会替换对应分区中的数据。但未涉及的分区会保留，而不会被清空或删除。如果新数据对应不存在的分区，系统会自动创建该分区。

Dynamic Overwrite 语义默认禁用。如需启用，需要将系统变量 `dynamic_overwrite` 设置为 `true`。

在当前 Session 中启用 Dynamic Overwrite:

```SQL
SET dynamic_overwrite = true;
```

您也可以在 INSERT OVERWRITE 语句中通过 Hint 启用 Dynamic Overwrite，仅对该语句生效：

示例：

```SQL
INSERT OVERWRITE /*+set_var(set dynamic_overwrite = false)*/ insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

## 示例

### 示例一：一般用法

以下示例基于表 `test`，其中包含两个列 `c1` 和 `c2`。`c2` 列有默认值 DEFAULT。

向 test 表中导入一行数据

```SQL
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

- 在不指定目标列时，使用表中的列顺序来作为默认的目标列导入顺序。因此以上示例中，第一条、第二条 SQL 语句导入效果相同。
- 如果有目标列未插入数据或使用 DEFAULT 作为值插入数据，该列将使用默认值作为导入数据。因此以上示例中，第三条、第四条语句导入效果相同。

向 test 表中一次性导入多行数据

```SQL
INSERT INTO test VALUES (1, 2), (3, 2 + 2);
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2);
INSERT INTO test (c1) VALUES (1), (3);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
```

- 因表达式结果相同，以上示例中，第一条、第二条 SQL 语句导入效果相同。
- 第三条、第四条语句使用 DEFAULT 作为值插入数据，因此导入效果相同。

向 test 表中导入一个查询语句结果

```SQL
INSERT INTO test SELECT * FROM test2;
INSERT INTO test (c1, c2) SELECT * from test2;
```

向 test 表中导入一个查询语句结果，并指定分区和 Label

```SQL
INSERT INTO test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test2;
INSERT INTO test WITH LABEL `label1` (c1, c2) SELECT * from test2;
```

向 test 表中覆盖写入一个查询语句结果，并指定分区和 Label

```SQL
INSERT OVERWRITE test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test3;
INSERT OVERWRITE test WITH LABEL `label1` (c1, c2) SELECT * from test3;
```

### 示例二：通过 INSERT from FILES() 从 AWS S3 中导入 Parquet 数据文件

以下示例将 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/insert_wiki_edit_append.parquet** 中的数据插入至表 `insert_wiki_edit` 中：

```Plain
INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "xxxxxxxxxx",
        "aws.s3.secret_key" = "yyyyyyyyyy",
        "aws.s3.region" = "aa-bbbb-c"
);
```

### 示例三：INSERT 超时设置

以下示例将源表 `source_wiki_edit` 中的数据插入到目标表 `insert_wiki_edit`，并将超时时间设置为 `2` 秒：

```SQL
INSERT INTO insert_wiki_edit
PROPERTIES(
    "timeout" = "2"
)
SELECT * FROM source_wiki_edit;
```

如果要导入大量数据，可以为 `timeout` 或会话变量 `insert_timeout` 设置较大的值。

### 示例四：INSERT 严格模式和 max filter ratio

以下示例将 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/insert_wiki_edit_append.parquet** 中的数据插入至表 `insert_wiki_edit` 中，启用严格模式以过滤不合格的数据行，并且设置最大容错比为 10%：

```SQL
INSERT INTO insert_wiki_edit
PROPERTIES(
    "strict_mode" = "true",
    "max_filter_ratio" = "0.1"
)
SELECT * FROM FILES(
    "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
    "format" = "parquet",
    "aws.s3.access_key" = "XXXXXXXXXX",
    "aws.s3.secret_key" = "YYYYYYYYYY",
    "aws.s3.region" = "us-west-2"
);
```

### 示例五：INSERT 按名称匹配列

以下示例通过列名匹配源表和目标表中的列：

```SQL
INSERT INTO insert_wiki_edit BY NAME
SELECT event_time, user, channel FROM source_wiki_edit;
```

在这种情况下，改变 `channel` 和 `user` 的顺序不会改变列的映射关系。
