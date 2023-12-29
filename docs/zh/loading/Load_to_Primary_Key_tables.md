---
displayed_sidebar: "Chinese"
---

# 通过导入实现数据变更

StarRocks 的[主键模型](../table_design/table_types/primary_key_table.md)支持通过 [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)、[Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 或 [Routine Load](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md) 导入作业，对 StarRocks 表进行数据变更，包括插入、更新和删除数据。不支持通过 [Spark Load](../sql-reference/sql-statements/data-manipulation/SPARK_LOAD.md) 导入作业或 [INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md) 语句对 StarRocks 表进行数据变更。

StarRocks 还支持部分更新 (Partial Update) 和条件更新 (Conditional Update)。

> **注意**
>
> 导入操作需要目标表的 INSERT 权限。如果您的用户账号没有 INSERT 权限，请参考 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 给用户赋权。

本文以 CSV 格式的数据文件为例介绍如何通过导入实现数据变更。具体支持的数据文件类型，跟您选择的导入方式有关。

> **说明**
>
> 对于 CSV 格式的数据，StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (|)。

## 内部实现

StarRocks 的主键模型目前支持 UPSERT 和 DELETE 操作，不支持区分 INSERT 和 UPDATE 操作。

在创建导入作业时，StarRocks 支持在导入作业的创建语句或命令中添加 `__op` 字段，用于指定操作类型。

> **说明**
>
> 不需要在创建 StarRocks 表时添加 `__op` 列。

不同的导入方式，定义 `__op` 字段的方法也不相同：

- 如果使用 Stream Load 导入方式，需要通过 `columns` 参数来定义 `__op` 字段。

- 如果使用 Broker Load 导入方式，需要通过 SET 子句来定义 `__op` 字段。

- 如果使用 Routine Load 导入方式，需要通过 `COLUMNS` 参数来定义 `__op` 字段。

根据要做的数据变更操作，您可以选择添加或者不添加 `__op` 字段。不添加 `__op` 字段的话，默认为 UPSERT 操作。主要涉及的数据变更操作场景如下：

- 当数据文件只涉及 UPSERT 操作时，可以不添加 `__op` 字段。

- 当数据文件只涉及 DELETE 操作时，必须添加 `__op` 字段，并且指定操作类型为 DELETE。

- 当数据文件中同时包含 UPSERT 和 DELETE 操作时，必须添加 `__op` 字段，并且确保数据文件中包含一个代表操作类型的列，取值为 `0` 或 `1`。其中，取值为 `0` 时代表 UPSERT 操作，取值为 `1` 时代表 DELETE 操作。

## 使用说明

- 必须确保待导入的数据文件中每一行的列数都相同。

- 所更新的列必须包含主键列。

## 前提条件

### Broker Load

参见[从 HDFS 导入](../loading/hdfs_load.md)或[从云存储导入](../loading/cloud_storage_load.md)中的“背景信息”小节。

### Routine Load

如果使用 Routine Load 导入数据，必须确保您的 Apache Kafka® 集群已创建 Topic。本文假设您已部署四个 Topic，分别为 `topic1`、`topic2`、`topic3` 和 `topic4`。

## 基本操作

下面通过几个示例来展示具体的导入操作。有关使用 Stream Load、Broker Load 和 Routine Load 导入数据的详细语法和参数介绍，请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)、[BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 和 [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md)。

### UPSERT

当数据文件只涉及 UPSERT 操作时，可以不添加 `__op` 字段。

如果您添加 `__op` 字段：

- 可以指定 `__op` 为 UPSERT 操作。

- 也可以不做任何指定，StarRocks 默认导入为 UPSERT 操作。

#### 数据样例

1. 准备数据文件。

   a. 在本地文件系统创建一个 CSV 格式的数据文件 `example1.csv`。文件包含三列，分别代表用户 ID、用户姓名和用户得分，如下所示：

      ```Plain
      101,Lily,100
      102,Rose,100
      ```

   b. 把 `example1.csv` 文件中的数据上传到 Kafka 集群的 `topic1` 中。

2. 准备 StarRocks 表。

   a. 在数据库 `test_db` 中创建一张名为 `table1` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户名称和用户得分，主键为 `id` 列，如下所示：

      ```SQL
      CREATE TABLE `table1`
      (
          `id` int(11) NOT NULL COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `score` int(11) NOT NULL COMMENT "用户得分"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **说明**
      >
      > 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

   b. 向 `table1` 表中插入一条数据，如下所示：

      ```SQL
      INSERT INTO table1 VALUES
          (101, 'Lily',80);
      ```

#### 导入数据

通过导入，把 `example1.csv` 文件中 `id` 为 `101` 的数据更新到 `table1` 表中，并且把 `example1.csv` 文件中 `id` 为 `102` 的数据插入到 `table1` 表中。

- 通过 Stream Load 导入：
  - 不添加 `__op` 字段：

    ```Bash
    curl --location-trusted -u <username>:<password> \
        -H "Expect:100-continue" \
        -H "label:label1" \
        -H "column_separator:," \
        -T example1.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
    ```

  - 添加 `__op` 字段：

    ```Bash
    curl --location-trusted -u <username>:<password> \
        -H "Expect:100-continue" \
        -H "label:label2" \
        -H "column_separator:," \
        -H "columns:__op ='upsert'" \
        -T example1.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
    ```

- 通过 Broker Load 导入：

  - 不添加 `__op` 字段：

    ```SQL
    LOAD LABEL test_db.label1
    (
        data infile("hdfs://<hdfs_host>:<hdfs_port>/example1.csv")
        into table table1
        columns terminated by ","
        format as "csv"
    )
    with broker
    ```

  - 添加 `__op` 字段：

    ```SQL
    LOAD LABEL test_db.label2
    (
        data infile("hdfs://<hdfs_host>:<hdfs_port>/example1.csv")
        into table table1
        columns terminated by ","
        format as "csv"
        set (__op = 'upsert')
    )
    with broker
    ```

- 通过 Routine Load 导入：

  - 不添加 `__op` 字段：

    ```SQL
    CREATE ROUTINE LOAD test_db.table1 ON table1
    COLUMNS TERMINATED BY ",",
    COLUMNS (id, name, score)
    PROPERTIES
    (
        "desired_concurrent_number" = "3",
        "max_batch_interval" = "20",
        "max_batch_rows"= "250000",
        "max_error_number" = "1000"
    )
    FROM KAFKA
    (
        "kafka_broker_list" ="<kafka_broker_host>:<kafka_broker_port>",
        "kafka_topic" = "test1",
        "property.kafka_default_offsets" ="OFFSET_BEGINNING"
    );
    ```

  - 添加 `__op` 字段：

    ```SQL
    CREATE ROUTINE LOAD test_db.table1 ON table1
    COLUMNS TERMINATED BY ",",
    COLUMNS (id, name, score, __op ='upsert')
    PROPERTIES
    (
        "desired_concurrent_number" = "3",
        "max_batch_interval" = "20",
        "max_batch_rows"= "250000",
        "max_error_number" = "1000"
    )
    FROM KAFKA
    (
        "kafka_broker_list" ="<kafka_broker_host>:<kafka_broker_port>",
        "kafka_topic" = "test1",
        "property.kafka_default_offsets" ="OFFSET_BEGINNING"
    );
    ```

#### 查询数据

导入完成后，查询 `table1` 表的数据，如下所示：

```SQL
SELECT * FROM table1;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|  101 | Lily |   100 |
|  102 | Rose |   100 |
+------+------+-------+
2 rows in set (0.02 sec)
```

从查询结果可以看到，`example1.csv` 文件中 `id` 为 `101` 的数据已经更新到 `table1` 表中，并且 `example1.csv` 文件中 `id` 为 `102` 的数据已经插入到 `table1` 表中。

### DELETE

当数据文件只涉及 DELETE 操作时，必须添加 `__op` 字段，并且指定操作类型为 DELETE。

#### 数据样例

1. 准备数据文件。

   a. 在本地文件系统创建一个 CSV 格式的数据文件 `example2.csv`。文件包含三列，分别代表用户 ID、用户姓名和用户得分，如下所示：

      ```Plain
      101,Jack,100
      ```

   b. 把 `example2.csv` 文件中的数据上传到 Kafka 集群的 `topic2` 中。

2. 准备 StarRocks 表。

   a. 在数据库 `test_db` 中创建一张名为 `table2` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户名称和用户得分，主键为 `id` 列，如下所示：

      ```SQL
      CREATE TABLE `table2`
      (
          `id` int(11) NOT NULL COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `score` int(11) NOT NULL COMMENT "用户得分"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **说明**
      >
      > 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

   b. 向 `table2` 表中插入数据，如下所示：

      ```SQL
      INSERT INTO table2 VALUES
          (101, 'Jack', 100),
          (102, 'Bob', 90);
      ```

#### 导入数据

通过导入，把 `example2.csv` 文件中 `id` 为 `101` 的数据从 `table2` 表中删除。

- 通过 Stream Load 导入：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label3" \
      -H "column_separator:," \
      -H "columns:__op='delete'" \
      -T example2.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
  ```

- 通过 Broker Load 导入：

  ```SQL
  LOAD LABEL test_db.label3
  (
      data infile("hdfs://<hdfs_host>:<hdfs_port>/example2.csv")
      into table table2
      columns terminated by ","
      format as "csv"
      set (__op = 'delete')
  )
  with broker  
  ```

- 通过 Routine Load 导入：

  ```SQL
  CREATE ROUTINE LOAD test_db.table2 ON table2
  COLUMNS(id, name, score, __op = 'delete')
  PROPERTIES
  (
      "desired_concurrent_number" = "3",
      "max_batch_interval" = "20",
      "max_batch_rows"= "250000",
      "max_error_number" = "1000"
  )
  FROM KAFKA
  (
      "kafka_broker_list" ="<kafka_broker_host>:<kafka_broker_port>",
      "kafka_topic" = "test2",
      "property.kafka_default_offsets" ="OFFSET_BEGINNING"
  );
  ```

#### 查询数据

导入完成后，查询 `table2` 表的数据，如下所示：

```SQL
SELECT * FROM table2;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|  102 | Bob  |    90 |
+------+------+-------+
1 row in set (0.00 sec)
```

从查询结果可以看到，`example2.csv` 文件中 `id` 为 `101` 的数据已经从 `table2` 表中删除。

### UPSERT 和 DELETE

当数据文件中同时包含 UPSERT 和 DELETE 操作时，必须添加 `__op` 字段，并且确保数据文件中包含一个代表操作类型的列，取值为 `0` 或 `1`。其中，取值为 `0` 时代表 UPSERT 操作，取值为 `1` 时代表 DELETE 操作。

#### 数据样例

1. 准备数据文件。

   a. 在本地文件系统创建一个 CSV 格式的数据文件 `example3.csv`。文件包含四列，分别代表用户 ID、用户姓名、用户得分和操作类型，如下所示：

      ```Plain
      101,Tom,100,1
      102,Sam,70,0
      103,Stan,80,0
      ```

   b. 把 `example3.csv` 文件中的数据上传到 Kafka 集群的 `topic3` 中。

2. 准备 StarRocks 表。

   a. 在数据库 `test_db` 中创建一张名为 `table3` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户名称和用户得分，主键为 `id` 列，如下所示：

      ```SQL
      CREATE TABLE `table3`
      (
          `id` int(11) NOT NULL COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `score` int(11) NOT NULL COMMENT "用户得分"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **说明**
      >
      > 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

   b. 向 `table3` 表中插入数据，如下所示：

      ```SQL
      INSERT INTO table3 VALUES
          (101, 'Tom', 100),
          (102, 'Sam', 90);
      ```

#### 导入数据

通过导入，把 `example3.csv` 文件中 `id` 为 `101` 的数据从 `table3` 表中删除，把 `example3.csv` 文件中 `id` 为 `102` 的数据更新到 `table3` 表，并且把 `example3.csv` 文件中 `id` 为 `103` 的数据插入到 `table3` 表：

- 通过 Stream Load 导入：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label4" \
      -H "column_separator:," \
      -H "columns: id, name, score, temp, __op = temp" \
      -T example3.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table3/_stream_load
  ```

  > **说明**
  >
  > 上述示例中，通过 `columns` 参数把 `example3.csv` 文件中代表组别代码的第四列临时命名为 `temp`，然后定义 `__op` 字段等于临时命名的 `temp` 列。这样，StarRocks 可以根据 `example3.csv` 文件中第四列的取值是 `0` 还是 `1` 来确定执行 UPSERT 还是 DELETE 操作。

- 通过 Broker Load 导入：

  ```Bash
  LOAD LABEL test_db.label4
  (
      data infile("hdfs://<hdfs_host>:<hdfs_port>/example1.csv")
      into table table1
      columns terminated by ","
      format as "csv"
      (id, name, score, temp)
      set (__op=temp)
  )
  with broker
  ```

- 通过 Routine Load 导入：

  ```SQL
  CREATE ROUTINE LOAD test_db.table3 ON table3
  COLUMNS(id, name, score, temp, __op = temp)
  PROPERTIES
  (
      "desired_concurrent_number" = "3",
      "max_batch_interval" = "20",
      "max_batch_rows"= "250000",
      "max_error_number" = "1000"
  )
  FROM KAFKA
  (
      "kafka_broker_list" = "<kafka_broker_host>:<kafka_broker_port>",
      "kafka_topic" = "test3",
      "property.kafka_default_offsets" = "OFFSET_BEGINNING"
  );
  ```

#### 查询数据

导入完成后，查询 `table3` 表的数据，如下所示：

```SQL
SELECT * FROM table3;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|  102 | Sam  |    70 |
|  103 | Stan |    80 |
+------+------+-------+
2 rows in set (0.01 sec)
```

从查询结果可以看到，`example3.csv` 文件中 `id` 为 `101` 的数据已经从 `table3` 表中删除，`example3.csv` 文件中 `id` 为 `102` 的数据已经更新到 `table3` 表中，并且 `example3.csv` 文件中 `id` 为 `103` 的数据已经插入到 `table3` 表中。

## 部分更新

自 StarRocks v2.2 起，主键模型表支持部分更新 (Partial Update)，您可以选择只更新部分指定的列。这里以 CSV 格式的数据文件为例进行说明。

> **注意**
>
> 在部分更新模式下，如果要更新的行不存在，那么 StarRocks 会插入新的一行，并自动对缺失的列填充默认值。

### 数据样例

1. 准备数据文件。

   a. 在本地文件系统创建一个 CSV 格式的数据文件 `example4.csv`。文件包含两列，分别代表用户 ID 和用户姓名，如下所示：

      ```Plain
      101,Lily
      102,Rose
      103,Alice
      ```

   b. 把 `example4.csv` 文件中的数据上传到 Kafka 集群的 `topic4` 中。

2. 准备 StarRocks 表。

   a. 在数据库 `test_db` 中创建一张名为 `table4` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户名称和用户得分，主键为 `id` 列，如下所示：

      ```SQL
      CREATE TABLE `table4`
      (
          `id` int(11) NOT NULL COMMENT "用户 ID",
          `name` varchar(65533) NOT NULL COMMENT "用户姓名",
          `score` int(11) NOT NULL COMMENT "用户得分"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **说明**
      >
      > 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

   b. 向 `table4` 表中插入一条数据，如下所示：

      ```SQL
      INSERT INTO table4 VALUES
          (101, 'Tom',80);
      ```

### 导入数据

通过导入，把 `example4.csv` 里的两列数据更新到 `table4` 表的 `id` 和 `name` 两列。

- 通过 Stream Load 导入：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label7" -H "column_separator:," \
      -H "partial_update:true" \
      -H "columns:id,name" \
      -T example4.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table4/_stream_load
  ```

  > **说明**
  >
  > 使用 Stream Load 导入数据时，需要设置 `partial_update` 为 `true`，以开启部分更新特性。另外，还需要在 `columns` 中声明待更新数据的列的名称。

- 通过 Broker Load 导入：

  ```SQL
  LOAD LABEL test_db.table4
  (
      data infile("hdfs://<hdfs_host>:<hdfs_port>/example4.csv")
      into table table4
      format as "csv"
      (id, name)
  )
  WTIH BROKER
  PROPERTIES
  (
      "partial_update" = "true"
  );
  ```

  > **说明**
  >
  > 使用 Broker Load 导入数据时，需要设置 `partial_update` 为 `true`，以开启部分更新特性。另外，还需要在 `column_list` 中声明待更新数据的列的名称。

- 通过 Routine Load 导入：

  ```SQL
  CREATE ROUTINE LOAD test_db.table4 on table4
  COLUMNS (id, name),
  COLUMNS TERMINATED BY ','
  PROPERTIES
  (
      "partial_update" = "true"
  )
  FROM KAFKA
  (
      "kafka_broker_list" ="<kafka_broker_host>:<kafka_broker_port>",
      "kafka_topic" = "test4",
      "property.kafka_default_offsets" ="OFFSET_BEGINNING"
  );
  ```

  > **说明**
  >
  > 使用 Routine Load 导入数据时，需要设置 `partial_update` 为 `true`，以开启部分更新特性。另外，还需要在 `COLUMNS` 中声明待更新数据的列的名称。

### 查询数据

导入完成后，查询 `table4` 表的数据，如下所示：

```SQL
SELECT * FROM table4;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|  102 | Rose  |     0 |
|  101 | Lily  |    80 |
|  103 | Alice |     0 |
+------+-------+-------+
3 rows in set (0.01 sec)
```

从查询结果可以看到，`example4.csv` 文件中 `id` 为 `101` 的数据已经更新到 `table4` 表中，并且 `example4.csv` 文件中 `id` 为 `102` 和 `103` 的数据已经插入到 `table4` 表中。

## 条件更新

自 StarRocks v2.5 起，主键模型表支持条件更新 (Conditional Update)。您可以指定某一非主键列为更新条件，这样只有当导入的数据中该列的值大于等于当前值的时候，更新才会生效。

条件更新功能用于解决数据乱序的问题。如果上游数据发生乱序，可以使用条件更新功能保证新的数据不被老的数据覆盖。

> **说明**
>
> - 不支持给同一批导入的数据指定不同的条件。
>
> - 不支持删除操作。
>
> - 在 3.1.3 版本及以前，StarRocks 不支持条件更新同部分更新一并使用。自 3.1.3 版本起，StarRocks 才支持条件更新同部分更新一并使用。
>

### 数据样例

1. 准备数据文件。

   a. 在本地文件系统创建一个 CSV 格式的数据文件 `example5.csv`。文件包含三列，分别代表用户 ID、版本号和用户得分，如下所示：

      ```Plain
      101,1,100
      102,3,100
      ```

   b. 把 `example5.csv` 文件中的数据上传到 Kafka 集群的 `topic5` 中。

2. 准备 StarRocks 表。

   a. 在数据库 `test_db` 中创建一张名为 `table5` 的主键模型表。表包含 `id`、`version` 和 `score` 三列，分别代表用户 ID、版本号和用户得分，主键为 `id` 列，如下所示：

      ```SQL
      CREATE TABLE `table5`
      (
          `id` int(11) NOT NULL COMMENT "用户 ID", 
          `version` int NOT NULL COMMENT "版本号",
          `score` int(11) NOT NULL COMMENT "用户得分"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`) DISTRIBUTED BY HASH(`id`);
      ```

      > **说明**
      >
      > 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

   b. 向 `table5` 表中插入两条数据，如下所示：

      ```SQL
      INSERT INTO table5 VALUES
          (101, 2, 80),
          (102, 2, 90);
      ```

### 导入数据

通过导入，把 `example5.csv` 文件中 `id` 为 `101`、`102` 的数据更新到 `table5` 表中，指定 `merge_condition` 为 `version` 列，表示只有当导入的数据中 `verion` 大于等于 `table5` 中对应行的`version` 值时，更新才会生效。

- 通过 Stream Load 导入：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label10" \
      -H "column_separator:," \
      -H "merge_condition:version" \
      -T example5.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table5/_stream_load
  ```

- 通过 Routine Load 导入：

  ```SQL
  CREATE ROUTINE LOAD test_db.table5 on table5
  COLUMNS (id, version, score),
  COLUMNS TERMINATED BY ','
  PROPERTIES
  (
      "merge_condition" = "version"
  )
  FROM KAFKA
  (
      "kafka_broker_list" ="<kafka_broker_host>:<kafka_broker_port>",
      "kafka_topic" = "topic5",
      "property.kafka_default_offsets" ="OFFSET_BEGINNING"
  );
  ```

- 通过 Broker Load 导入：

  ```SQL
  LOAD LABEL test_db.table5
  ( DATA INFILE ("s3://xxx.csv")
    INTO TABLE table5 COLUMNS TERMINATED BY "," FORMAT AS "CSV"
  )
  WITH BROKER
  PROPERTIES
  (
      "merge_condition" = "version"
  );
  ```

### 查询数据

导入完成后，查询 `table5` 表的数据，如下所示：

```SQL
SELECT * FROM table5;
+------+------+-------+
| id   | version | score |
+------+------+-------+
|  101 |       2 |   80 |
|  102 |       3 |  100 |
+------+------+-------+
2 rows in set (0.02 sec)
```

从查询结果可以看到，`example5.csv` 文件中 `id` 为 `101` 的数据并没有被更新，而 `id` 为 `102` 已经被更新。
