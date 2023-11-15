# 通过导入实现数据变更

StarRocks 的[主键模型](../table_design/Data_model.md#主键模型)支持通过 [Stream Load](./StreamLoad.md)、[Broker Load](./BrokerLoad.md) 或 [Routine Load](./RoutineLoad.md) 导入作业，对 StarRocks 表进行数据变更，包括插入、更新和删除数据。不支持通过 [Spark Load](../loading/SparkLoad.md) 导入作业或 [INSERT](../loading/InsertInto.md) 语句对 StarRocks 表进行数据变更。

StarRocks 还支持部分更新 (Partial Update)。

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

如果使用 Broker Load 导入数据，必须确保您的 StarRocks 集群中已部署 Broker。您可以通过 [SHOW BROKER](../sql-reference/sql-statements/Administration/SHOW_BROKER.md) 语句来查看集群中已经部署的 Broker。如果集群中没有部署 Broker，请参见[部署 Broker 节点](../administration/deploy_broker.md)完成 Broker 部署。本文假设您已部署一个名为 `broker1` 的 Broker。

如果使用 Routine Load 导入数据，必须确保您的 Apache Kafka® 集群已创建 Topic。本文假设您已部署四个 Topic，分别为 `topic1`、`topic2`、`topic3` 和 `topic4`。

## 操作示例

下面通过几个示例来展示具体的导入操作。有关使用 Stream Load、Broker Load 和 Routine Load 导入数据的详细语法和参数介绍，请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)、[BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 和 [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md)。

### UPSERT

当数据文件只涉及 UPSERT 操作时，可以不添加 `__op` 字段。

如果您添加 `__op` 字段：

- 可以指定 `__op` 为 UPSERT 操作。

- 也可以不做任何指定，StarRocks 默认导入为 UPSERT 操作。

#### 数据样例

1. 准备 StarRocks 表。

   a. 在数据库 `test_db` 中创建一张名为 `table1` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户名称和用户得分，主键为 `id` 列，如下所示：

    ```SQL
    MySQL [test_db]> CREATE TABLE `table1`
    (
        `id` int(11) NOT NULL COMMENT "用户 ID",
        `name` varchar(65533) NOT NULL COMMENT "用户姓名",
        `score` int(11) NOT NULL COMMENT "用户得分"
    )
    ENGINE=OLAP
    PRIMARY KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 10;
    ```

   b. 向 `table1` 表中插入一条数据，如下所示：

    ```SQL
    MySQL [test_db]> INSERT INTO table1 VALUES
        (101, 'Lily',80);
    ```

2. 准备数据文件。

   a. 在本地文件系统创建一个 CSV 格式的数据文件 `example1.csv`。文件包含三列，分别代表用户 ID、用户姓名和用户得分，如下所示：

    ```Plain
    101,Lily,100
    102,Rose,100
    ```

   b. 把 `example1.csv` 文件中的数据上传到 Kafka 集群的 `topic1` 中。

#### 导入数据

通过导入，把 `example1.csv` 文件中 `id` 为 `101` 的数据更新到 `table1` 表中，并且把 `example1.csv` 文件中 `id` 为 `102` 的数据插入到 `table1` 表中。

- 通过 Stream Load 导入：
  - 不添加 `__op` 字段：

    ```Bash
    curl --location-trusted -u <username>:<password> \
        -H "label:label1" \
        -H "column_separator:," \
        -T example1.csv -XPUT\
        http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
    ```

  - 添加 `__op` 字段：

    ```Bash
    curl --location-trusted -u <username>:<password> \
        -H "label:label2" \
        -H "column_separator:," \
        -H "columns:__op ='upsert'" \
        -T example1.csv -XPUT\
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
    with broker "broker1";
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
    with broker "broker1";
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
MySQL [test_db]> SELECT * FROM table1;
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

1. 准备 StarRocks 表。

   a. 在数据库 `test_db` 中创建一张名为 `table2` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户名称和用户得分，主键为 `id` 列，如下所示：

   ```SQL
   MySQL [test_db]> CREATE TABLE `table2`
   (
       `id` int(11) NOT NULL COMMENT "用户 ID",
       `name` varchar(65533) NOT NULL COMMENT "用户姓名",
       `score` int(11) NOT NULL COMMENT "用户得分"
   )
   ENGINE=OLAP
   PRIMARY KEY(`id`)
   DISTRIBUTED BY HASH(`id`) BUCKETS 10;
   ```

   b. 向 `table2` 表中插入数据，如下所示：

   ```SQL
   MySQL [test_db]> INSERT INTO table2 VALUES
       (101, 'Jack', 100),
       (102, 'Bob', 90);
   ```

2. 准备数据文件。

   a. 在本地文件系统创建一个 CSV 格式的数据文件 `example2.csv`。文件包含三列，分别代表用户 ID、用户姓名和用户得分，如下所示：

   ```Plain
   101,Jack,100
   ```

   b. 把 `example2.csv` 文件中的数据上传到 Kafka 集群的 `topic2` 中。

#### 导入数据

通过导入，把 `example2.csv` 文件中 `id` 为 `101` 的数据从 `table2` 表中删除。

- 通过 Stream Load 导入：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "label:label3" \
      -H "column_separator:," \
      -H "columns:__op='delete'" \
      -T example2.csv -XPUT\
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
  with broker "broker1";  
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
MySQL [test_db]> SELECT * FROM table2;
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

1. 准备 StarRocks 表。

   a. 在数据库 `test_db` 中创建一张名为 `table3` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户名称和用户得分，主键为 `id` 列，如下所示：

   ```SQL
   MySQL [test_db]> CREATE TABLE `table3`
   (
       `id` int(11) NOT NULL COMMENT "用户 ID",
       `name` varchar(65533) NOT NULL COMMENT "用户姓名",
       `score` int(11) NOT NULL COMMENT "用户得分"
   )
   ENGINE=OLAP
   PRIMARY KEY(`id`)
   DISTRIBUTED BY HASH(`id`) BUCKETS 10;
   ```

   b. 向 `table3` 表中插入数据，如下所示：

   ```SQL
   MySQL [test_db]> INSERT INTO table3 VALUES
       (101, 'Tom', 100),
       (102, 'Sam', 90);
   ```

2. 准备数据文件。

   a. 在本地文件系统创建一个 CSV 格式的数据文件 `example3.csv`。文件包含四列，分别代表用户 ID、用户姓名、用户得分和操作类型，如下所示：

   ```Plain
   101,Tom,100,1
   102,Sam,70,0
   103,Stan,80,0
   ```

   b. 把 `example3.csv` 文件中的数据上传到 Kafka 集群的 `topic3` 中。

#### 导入数据

通过导入，把 `example3.csv` 文件中 `id` 为 `101` 的数据从 `table3` 表中删除，把 `example3.csv` 文件中 `id` 为 `102` 的数据更新到 `table3` 表，并且把 `example3.csv` 文件中 `id` 为 `103` 的数据插入到 `table3` 表：

- 通过 Stream Load 导入：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "label:label4" \
      -H "column_separator:," \
      -H "columns: id, name, score, temp, __op = temp" \
      -T example3.csv -XPUT\
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
  with broker "broker1";
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
MySQL [test_db]> SELECT * FROM table3;
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

### 数据样例

1. 准备 StarRocks 表。

   a. 在数据库 `test_db` 中创建一张名为 `table4` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户名称和用户得分，主键为 `id` 列，如下所示：

   ```SQL
   MySQL [test_db]> CREATE TABLE `table4`
   (
       `id` int(11) NOT NULL COMMENT "用户 ID",
       `name` varchar(65533) NOT NULL COMMENT "用户姓名",
       `score` int(11) NOT NULL COMMENT "用户得分"
   )
   ENGINE=OLAP
   PRIMARY KEY(`id`)
   DISTRIBUTED BY HASH(`id`) BUCKETS 10;
   ```

   b. 向 `table4` 表中插入一条数据，如下所示：

   ```SQL
   MySQL [test_db]> INSERT INTO table4 VALUES
       (101, 'Tom',80);
   ```

2. 准备数据文件。

   a. 在本地文件系统创建一个 CSV 格式的数据文件 `example4.csv`。文件包含两列，分别代表用户 ID 和用户姓名，如下所示：

   ```Plain
   101,Lily
   102,Rose
   103,Alice
   ```

   b. 把 `example4.csv` 文件中的数据上传到 Kafka 集群的 `topic4` 中。

### 导入数据

通过导入，把 `example4.csv` 里的两列数据更新到 `table4` 表的 `id` 和 `name` 两列。

- 通过 Stream Load 导入：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "label:label7" -H "column_separator:," \
      -H "partial_update:true" \
      -H "columns:id,name" \
      -T example4.csv -XPUT\
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
  with broker "broker1"
  properties
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
MySQL [test_db]> SELECT * FROM table4;
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
