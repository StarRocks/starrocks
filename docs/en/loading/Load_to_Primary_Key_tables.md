---
displayed_sidebar: "English"
---

# Change data through loading

[Primary Key tables](../table_design/table_types/primary_key_table.md) provided by StarRocks allows you to make data changes to StarRocks tables by running [Stream Load](../loading/StreamLoad.md), [Broker Load](../loading/BrokerLoad.md), or [Routine Load](../loading/RoutineLoad.md) jobs. These data changes include inserts, updates, and deletions. However, Primary Key tables do not support changing data by using [Spark Load](../loading/SparkLoad.md) or [INSERT](../loading/InsertInto.md).

StarRocks also supports partial updates and conditional updates.

This topic uses CSV data as an example to describe how to make data changes to a StarRocks table through loading. The data file formats that are supported vary depending on the loading method of your choice.

> **NOTE**
>
> For CSV data, you can use a UTF-8 string, such as a comma (,), tab, or pipe (|), whose length does not exceed 50 bytes as a text delimiter.

## Implementation

Primary Key tables provided by StarRocks support UPSERT and DELETE operations and does not distinguish INSERT operations from UPDATE operations.

When you create a load job, StarRocks supports adding a field named `__op` to the job creation statement or command. The `__op` field is used to specify the type of operation you want to perform.

> **NOTE**
>
> When you create a table, you do not need to add a column named `__op` to that table.

The method of defining the `__op` field varies depending on the loading method of your choice:

- If you choose Stream Load, define the `__op` field by using the `columns` parameter.

- If you choose Broker Load, define the `__op` field by using the SET clause.

- If you choose Routine Load, define the `__op` field by using the `COLUMNS` column.

You can decide whether to add the `__op` field based on the data changes you want to make. If you do not add the `__op` field, the operation type defaults to UPSERT. The major data change scenarios are as follows:

- If the data file you want to load involves only UPSERT operations, you do not need to add the `__op` field.

- If the data file you want to load involves only DELETE operations, you must add the `__op` field and specify the operation type as DELETE.

- If the data file you want to load involves both UPSERT and DELETE operations, you must add the `__op` field and make sure that the data file contains a column whose values are `0` or `1`. A value of `0` indicates an UPSERT operation, and a value of `1` indicates a DELETE operation.

## Usage notes

- Make sure that each row in your data file has the same number of columns.

- The columns that involve data changes must include the primary key column.

## Prerequisites

### Broker Load

See the "Background information" section in [Load data from HDFS or cloud storage](../loading/BrokerLoad.md).

### Routine load

If you choose Routine Load, make sure that topics are created in your Apache Kafka® cluster. Assume that you have created four topics: `topic1`, `topic2`, `topic3`, and `topic4`.

## Basic operations

This section provides examples of how to make data changes to a StarRocks table through loading. For detailed syntax and parameter descriptions, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md), [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md), and [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md).

### UPSERT

If the data file you want to load involves only UPSERT operations, you do not need to add the `__op` field.

> **NOTE**
>
> If you add the `__op` field:
>
> - You can specify the operation type as UPSERT.
>
> - You can leave the `__op` field empty, because the operation type defaults to UPSERT.

#### Data examples

1. Prepare a data file.

   a. Create a CSV file named `example1.csv` in your local file system. The file consists of three columns, which represent user ID, user name, and user score in sequence.

      ```Plain
      101,Lily,100
      102,Rose,100
      ```

   b. Publish the data of `example1.csv` to `topic1` of your Kafka cluster.

2. Prepare a StarRocks table.

   a. Create a Primary Key table named `table1` in your StarRocks database `test_db`. The table consists of three columns: `id`, `name`, and `score`, of which `id` is the primary key.

      ```SQL
      CREATE TABLE `table1`
      (
          `id` int(11) NOT NULL COMMENT "user ID",
          `name` varchar(65533) NOT NULL COMMENT "user name",
          `score` int(11) NOT NULL COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **NOTE**
      >
      > Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [determine the number of buckets](../table_design/Data_distribution.md#determine-the-number-of-buckets).

   b. Insert a record into `table1`.

      ```SQL
      INSERT INTO table1 VALUES
          (101, 'Lily',80);
      ```

#### Load data

Run a load job to update the record whose `id` is `101` in `example1.csv` to `table1` and insert the record whose `id` is `102` in `example1.csv` into `table1`.

- Run a Stream Load job.
  
  - If you do not want to include the `__op` field, run the following command:

    ```Bash
    curl --location-trusted -u <username>:<password> \
        -H "Expect:100-continue" \
        -H "label:label1" \
        -H "column_separator:," \
        -T example1.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
    ```

  - If you want to include the `__op` field, run the following command:

    ```Bash
    curl --location-trusted -u <username>:<password> \
        -H "Expect:100-continue" \
        -H "label:label2" \
        -H "column_separator:," \
        -H "columns:__op ='upsert'" \
        -T example1.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
    ```

- Run a Broker Load job.

  - If you do not want to include the `__op` field, run the following command:

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

  - If you want to include the `__op` field, run the following command:

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

- Run a Routine Load job.

  - If you do not want to include the `__op` field, run the following command:

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

  - If you want to include the `__op` field, run the following command:

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

#### Query data

After the load is complete, query the data of `table1` to verify that the load is successful:

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

As shown in the preceding query result, the record whose `id` is `101` in `example1.csv` has been updated to `table1`, and the record whose `id` is `102` in `example1.csv` has been inserted into `table1`.

### DELETE

If the data file you want to load involves only DELETE operations, you must add the `__op` field and specify the operation type as DELETE.

#### Data examples

1. Prepare a data file.

   a. Create a CSV file named `example2.csv` in your local file system. The file consists of three columns, which represent user ID, user name, and user score in sequence.

      ```Plain
      101,Jack,100
      ```

   b. Publish the data of `example2.csv` to `topic2` of your Kafka cluster.

2. Prepare a StarRocks table.

   a. Create a Primary Key table named `table2` in your StarRocks table `test_db`. The table consists of three columns: `id`, `name`, and `score`, of which `id` is the primary key.

      ```SQL
      CREATE TABLE `table2`
            (
          `id` int(11) NOT NULL COMMENT "user ID",
          `name` varchar(65533) NOT NULL COMMENT "user name",
          `score` int(11) NOT NULL COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **NOTE**
      >
      > Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [determine the number of buckets](../table_design/Data_distribution.md#determine-the-number-of-buckets).

   b. Insert two records into `table2`.

      ```SQL
      INSERT INTO table2 VALUES
      (101, 'Jack', 100),
      (102, 'Bob', 90);
      ```

#### Load data

Run a load job to delete the record whose `id` is `101` in `example2.csv` from `table2`.

- Run a Stream Load job.

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label3" \
      -H "column_separator:," \
      -H "columns:__op='delete'" \
      -T example2.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
  ```

- Run a Broker Load job.

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

- Run a Routine Load job.

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

#### Query data

After the load is complete, query the data of `table2` to verify that the load is successful:

```SQL
SELECT * FROM table2;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|  102 | Bob  |    90 |
+------+------+-------+
1 row in set (0.00 sec)
```

As shown in the preceding query result, the record whose `id` is `101` in `example2.csv` has been deleted from `table2`.

### UPSERT and DELETE

If the data file you want to load involves both UPSERT and DELETE operations, you must add the `__op` field and make sure that the data file contains a column whose values are `0` or `1`. A value of `0` indicates an UPSERT operation, and a value of `1` indicates a DELETE operation.

#### Data examples

1. Prepare a data file.

   a. Create a CSV file named `example3.csv` in your local file system. The file consists of four columns, which represent user ID, user name, user score, and operation type in sequence.

      ```Plain
      101,Tom,100,1
      102,Sam,70,0
      103,Stan,80,0
      ```

   b. Publish the data of `example3.csv` to `topic3` of your Kafka cluster.

2. Prepare a StarRocks table.

   a. Create a Primary Key table named `table3` in your StarRocks database `test_db`. The table consists of three columns: `id`, `name`, and `score`, of which `id` is the primary key.

      ```SQL
      CREATE TABLE `table3`
      (
          `id` int(11) NOT NULL COMMENT "user ID",
          `name` varchar(65533) NOT NULL COMMENT "user name",
          `score` int(11) NOT NULL COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **NOTE**
      >
      > Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [determine the number of buckets](../table_design/Data_distribution.md#determine-the-number-of-buckets).

   b. Insert two records into `table3`.

      ```SQL
      INSERT INTO table3 VALUES
          (101, 'Tom', 100),
          (102, 'Sam', 90);
      ```

#### Load data

Run a load job to delete the record whose `id` is `101` in `example3.csv` from `table3`, update the record whose `id` is `102` in `example3.csv` to `table3`, and insert the record whose `id` is `103` in `example3.csv` into `table3`.

- Run a Stream Load job:

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label4" \
      -H "column_separator:," \
      -H "columns: id, name, score, temp, __op = temp" \
      -T example3.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table3/_stream_load
  ```

  > **NOTE**
  >
  > In the preceding example, the fourth column that represents the operation type in `example3.csv` is temporarily named as `temp` and the `__op` field is mapped onto the `temp` column by using the `columns` parameter. As such, StarRocks can decide whether to perform an UPSERT or DELETE operation depending on the value in the fourth column of `example3.csv` is `0` or `1`.

- Run a Broker Load job:

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

- Run a Routine Load job:

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

#### Query data

After the load is complete, query the data of  `table3` to verify that the load is successful:

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

As shown in the preceding query result, the record whose `id` is `101` in `example3.csv` has been deleted from `table3`, the record whose `id` is `102` in `example3.csv` has been updated to `table3`, and the record whose `id` is `103` in `example3.csv` has been inserted into `table3`.

## Partial updates

Since v2.2, StarRocks supports updating only the specified columns of a table that uses the Primary Key table. This section uses CSV as an example to describe how to perform partial updates.

> **NOTICE**
>
> When you perform a partial update, if the row to be updated does not exist, StarRocks inserts a new row, and fills default values in fields that are empty because no data updates are inserted into them.

### Data examples

1. Prepare a data file.

   a. Create a CSV file named `example4.csv` in your local file system. The file consists of two columns, which represent user ID and user name in sequence.

      ```Plain
      101,Lily
      102,Rose
      103,Alice
      ```

   b. Publish the data of `example4.csv` to `topic4` of your Kafka cluster.

2. Prepare a StarRocks table.

   a. Create a Primary Key table named `table4` in your StarRocks database `test_db`. The table consists of three columns: `id`, `name`, and `score`, of which `id` is the primary key.

      ```SQL
      CREATE TABLE `table4`
      (
          `id` int(11) NOT NULL COMMENT "user ID",
          `name` varchar(65533) NOT NULL COMMENT "user name",
          `score` int(11) NOT NULL COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **NOTE**
      >
      > Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [determine the number of buckets](../table_design/Data_distribution.md#determine-the-number-of-buckets).

   b. Insert a record into `table4`.

      ```SQL
      INSERT INTO table4 VALUES
          (101, 'Tom',80);
      ```

### Load data

Run a load to update the data in the two columns of `example4.csv` to the `id` and `name` columns of `table4`.

- Run a Stream Load job:

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label7" -H "column_separator:," \
      -H "partial_update:true" \
      -H "columns:id,name" \
      -T example4.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table4/_stream_load
  ```

  > **NOTE**
  >
  > If you choose Stream Load, you must set the `partial_update` parameter to `true` to enable the partial update feature. Additionally, you must use the `columns` parameter to specify the columns you want to update.

- Run a Broker Load job:

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

  > **NOTE**
  >
  > If you choose Broker Load, you must set the `partial_update` parameter to `true` to enable the partial update feature. Additionally, you must use the `column_list` parameter to specify the columns you want to update.

- Run a Routine Load job:

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

  > **NOTE**
  >
  > If you choose Broker Load, you must set the `partial_update` parameter to `true` to enable the partial update feature. Additionally, you must use the `COLUMNS` parameter to specify the columns you want to update.

### Query data

After the load is complete, query the data of `table4` to verify that the load is successful:

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

As shown in the preceding query result, the record whose `id` is `101` in `example4.csv` has been updated to `table4`, and the records whose `id` are `102` and `103` in `example4.csv` have been Inserted into `table4`.

## Conditional updates

From StarRocks v2.5 onwards, Primary Key tables support conditional updates. You can specify a non-primary key column as the condition to determine whether updates can take effect. As such, the update from a source record to a destination record takes effect only when the source data record has a greater or equal value than the destination data record in the specified column.

The conditional update feature is designed to resolve data disorder. If the source data is disordered, you can use this feature to ensure that new data will not be overwritten by old data.

> **NOTICE**
>
> - You cannot specify different columns as update conditions for the same batch of data.
> - DELETE operations do not support conditional updates.
> - Partial updates and conditional updates cannot be used simultaneously.
> - Only Stream Load and Routine Load support conditional updates.

### Data examples

1. Prepare a data file.

   a. Create a CSV file named `example5.csv` in your local file system. The file consists of three columns, which represent user ID, version, and user score in sequence.

      ```Plain
      101,1,100
      102,3,100
      ```

   b. Publish the data of `example5.csv` to `topic5` of your Kafka cluster.

2. Prepare a StarRocks table.

   a. Create a Primary Key table named `table5` in your StarRocks database `test_db`. The table consists of three columns: `id`, `version`, and `score`, of which `id` is the primary key.

      ```SQL
      CREATE TABLE `table5`
      (
          `id` int(11) NOT NULL COMMENT "user ID", 
          `version` int NOT NULL COMMENT "version",
          `score` int(11) NOT NULL COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 10;
      ```

      > **NOTE**
      >
      > Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [determine the number of buckets](../table_design/Data_distribution.md#determine-the-number-of-buckets).

   b. Insert a record into `table5`.

      ```SQL
      INSERT INTO table5 VALUES
          (101, 2, 80),
          (102, 2, 90);
      ```

### Load data

Run a load to update the records whose `id` values are `101` and `102`, respectively, from `example5.csv` into `table5`, and specify that the updates take effect only when the `verion` value in each of the two records is greater or equal to their current `version` values.

- Run a Stream Load job:

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label10" \
      -H "column_separator:," \
      -H "merge_condition:version" \
      -T example5.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table5/_stream_load
  ```

- Run a Routine Load job:

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

### Query data

After the load is complete, query the data of `table5` to verify that the load is successful:

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

As shown in the preceding query result, the record whose `id` is `101` in `example5.csv` is not updated to `table5`, and the record whose `id` is `102` in `example5.csv` has been Inserted into `table5`.
