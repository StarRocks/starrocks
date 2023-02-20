# Transform data at loading

StarRocks supports data transformation at loading.

This feature supports [Stream Load](../loading/StreamLoad.md), [Broker Load](../loading/BrokerLoad.md), and [Routine Load](../loading/RoutineLoad.md) but does not support [Spark Load](../loading/SparkLoad.md).

This topic uses CSV data as an example to describe how to extract and transform data at loading. The data file formats that are supported vary depending on the loading method of your choice.

> **NOTE**
>
> For CSV data, you can use a UTF-8 string, such as a comma (,), tab, or pipe (|), whose length does not exceed 50 bytes as a text delimiter.

## Scenarios

When you load a data file into a StarRocks table, the data of the data file may not be completely mapped onto the data of the StarRocks table. In this situation, you do not need to extract or transform the data before you load it into the StarRocks table. StarRocks can help you extract and transform the data during loading:

- Skip columns that do not need to be loaded.
  
  You can skip the columns that do not need to be loaded. Additionally, if the columns of the data file are in a different order than the columns of the StarRocks table, you can create a column mapping between the data file and the StarRocks table.

- Filter out rows you do not want to load.
  
  You can specify filter conditions based on which StarRocks filters out the rows that you do not want to load.

- Generate new columns from original columns.
  
  Generated columns are special columns that are computed from the original columns of the data file. You can map the generated columns onto the columns of the StarRocks table.

- Extract partition field values from a file path.
  
  If the data file is generated from Apache Hive™, you can extract partition field values from the file path.

## Prerequisites

### Broker Load

See the "Background information" section in [Load data from HDFS or cloud storage](../loading/BrokerLoad.md).

### Routine load

If you choose Routine Load, make sure that topics are created in your Apache Kafka® cluster. Assume that you have created two topics: `topic1` and `topic2`.

## Data examples

1. Create tables in your StarRocks database `test_db`.

   a. Create a table named `table1`, which consists of three columns: `event_date`, `event_type`, and `user_id`.

   ```SQL
   MySQL [test_db]> CREATE TABLE table1
   (
       `event_date` DATE COMMENT "event date",
       `event_type` TINYINT COMMENT "event type",
       `user_id` BIGINT COMMENT "user ID"
   )
   DISTRIBUTED BY HASH(user_id) BUCKETS 10;
   ```

   b. Create a table named `table2`, which consists of four columns: `date`, `year`, `month`, and `day`.

   ```SQL
   MySQL [test_db]> CREATE TABLE table2
   (
       `date` DATE COMMENT "date",
       `year` INT COMMENT "year",
       `month` TINYINT COMMENT "month",
       `day` TINYINT COMMENT "day"
   )
   DISTRIBUTED BY HASH(date) BUCKETS 10;
   ```

2. Create data files in your local file system.

   a. Create a data file named `file1.csv`. The file consists of four columns, which represent user ID, user gender, event date, and event type in sequence.

   ```Plain
   354,female,2020-05-20,1
   465,male,2020-05-21,2
   576,female,2020-05-22,1
   687,male,2020-05-23,2
   ```

   b. Create a data file named `file2.csv`. The file consists of only one column, which represents date.

   ```Plain
   2020-05-20
   2020-05-21
   2020-05-22
   2020-05-23
   ```

3. Upload `file1.csv` and `file2.csv` to the `/user/starrocks/data/input/` path of your HDFS cluster, publish the data of `file1.csv` to `topic1` of your Kafka cluster, and publish the data of `file2.csv` to `topic2` of your Kafka cluster.

## Skip columns that do not need to be loaded

The data file that you want to load into a StarRocks table may contain some columns that cannot be mapped to any columns of the StarRocks table. In this situation, StarRocks supports loading only the columns that can be mapped from the data file onto the columns of the StarRocks table.

This feature supports loading data from the following data sources:

- Local file system

- HDFS and cloud storage
  
  > **NOTE**
  >
  > This section uses HDFS as an example.

- Kafka

In most cases, the columns of a CSV file are not named. For some CSV files, the first row is composed of column names, but StarRocks processes the content of the first row as common data rather than column names. Therefore, when you load a CSV file, you must temporarily name the columns of the CSV file **in sequence** in the job creation statement or command. These temporarily named columns are mapped **by name** onto the columns of the StarRocks table. Take note of the following points about the columns of the data file:

- The data of the columns that can be mapped onto and are temporarily named by using the names of the columns in the StarRocks table is directly loaded.

- The columns that cannot be mapped onto the columns of the StarRocks table are ignored, the data of these columns are not loaded.

- If some columns can be mapped onto the columns of the StarRocks table but are not temporarily named in the job creation statement or command, the load job reports errors.

This section uses `file1.csv` and `table1` as an example. The four columns of `file1.csv` are temporarily named as `user_id`, `user_gender`, `event_date`, and `event_type` in sequence. Among the temporarily named columns of `file1.csv`, `user_id`, `event_date`, and `event_type` can be mapped onto specific columns of `table1`, whereas `user_gender` cannot be mapped onto any column of `table1`. Therefore, `user_id`, `event_date`, and `event_type` are loaded into `table1`, but `user_gender` is not.

### Load data

#### Load data from a local file system

If `file1.csv` is stored in your local file system, run the following command to create a [Stream Load](../loading/StreamLoad.md) job:

```Bash
curl --location-trusted -u root: \
    -H "column_separator:," \
    -H "columns: user_id, user_gender, event_date, event_type" \
    -T file1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

> **NOTE**
>
> If you choose Stream Load, you must use the `columns` parameter to temporarily name the columns of the data file to create a column mapping between the data file and the StarRocks table.

For detailed syntax and parameter descriptions, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md).

#### Load data from an HDFS cluster

If `file1.csv` is stored in your HDFS cluster, execute the following statement to create a [Broker Load](../loading/BrokerLoad.md) job:

```SQL
LOAD LABEL test_db.label1
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/file1.csv")
    INTO TABLE `table1`
    FORMAT AS "csv"
    COLUMNS TERMINATED BY ","
    (user_id, user_gender, event_date, event_type)
)
WITH BROKER "broker1";
```

> **NOTE**
>
> If you choose Broker Load, you must use the `column_list` parameter to temporarily name the columns of the data file to create a column mapping between the data file and the StarRocks table.

For detailed syntax and parameter descriptions, see [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md).

#### Load data from a Kafka cluster

If the data of `file1.csv` is published to `topic1` of your Kafka cluster, execute the following statement to create a [Routine Load](../loading/RoutineLoad.md) job:

```SQL
CREATE ROUTINE LOAD test_db.table101 ON table1
    COLUMNS TERMINATED BY ",",
    COLUMNS(user_id, user_gender, event_date, event_type)
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker_host>:<kafka_broker_port>",
    "kafka_topic" = "topic1",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

> **NOTE**
>
> If you choose Routine Load, you must use the `COLUMNS` parameter to temporarily name the columns of the data file to create a column mapping between the data file and the StarRocks table.

For detailed syntax and parameter descriptions, see [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/ROUTINE%20LOAD.md).

### Query data

After the load of data from your local file system, HDFS cluster, or Kafka cluster is complete, query the data of `table1` to verify that the load is successful:

```SQL
MySQL [test_db]> SELECT * FROM table1;
+------------+------------+---------+
| event_date | event_type | user_id |
+------------+------------+---------+
| 2020-05-22 |          1 |     576 |
| 2020-05-20 |          1 |     354 |
| 2020-05-21 |          2 |     465 |
| 2020-05-23 |          2 |     687 |
+------------+------------+---------+
4 rows in set (0.01 sec)
```

## Filter out rows that you do not want to load

When you load a data file into a StarRocks table, you may not want to load specific rows of the data file. In this situation, you can use the WHERE clause to specify the rows that you want to load. StarRocks filters out all rows that do not meet the filter conditions specified in the WHERE clause.

This feature supports loading data from the following data sources:

- Local file system

- HDFS and cloud storage
  > **NOTE**
  >
  > This section uses HDFS as an example.

- Kafka

This section uses `file1.csv` and `table1` as an example. If you want to load only the rows whose event type is `1` from `file1.csv` into `table1`, you can use the WHERE clause to specify a filter condition `event_type = 1`.

### Load data

#### Load data from a local file system

If `file1.csv` is stored in your local file system, run the following command to create a [Stream Load](../loading/StreamLoad.md) job:

```Bash
curl --location-trusted -u root: \
    -H "column_separator:," \
    -H "columns: user_id, user_gender, event_date, event_type" \
    -H "where: event_type=1" \
    -T file1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

For detailed syntax and parameter descriptions, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md).

#### Load data from an HDFS cluster

If `file1.csv` is stored in your HDFS cluster, execute the following statement to create a [Broker Load](../loading/BrokerLoad.md) job:

```SQL
LOAD LABEL test_db.label2
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/file1.csv")
    INTO TABLE `table1`
    FORMAT AS "csv"
    COLUMNS TERMINATED BY ","
    (user_id, user_gender, event_date, event_type)
    WHERE event_type = 1
)
WITH BROKER "broker1";
```

For detailed syntax and parameter descriptions, see [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md).

#### Load data from a Kafka cluster

If the data of `file1.csv` is published to `topic1` of your Kafka cluster, execute the following statement to create a [Routine Load](../loading/RoutineLoad.md) job:

```SQL
CREATE ROUTINE LOAD test_db.table102 ON table1
COLUMNS TERMINATED BY ",",
COLUMNS (user_id, user_gender, event_date, event_type)
WHERE event_type = 1
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker_host>:<kafka_broker_port>",
    "kafka_topic" = "topic1",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

For detailed syntax and parameter descriptions, see [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/ROUTINE%20LOAD.md).

### Query data

After the load of data from your local file system, HDFS cluster, or Kafka cluster is complete, query the data of `table1` to verify that the load is successful:

```SQL
MySQL [test_db]> SELECT * FROM table1;
+------------+------------+---------+
| event_date | event_type | user_id |
+------------+------------+---------+
| 2020-05-20 |          1 |     354 |
| 2020-05-22 |          1 |     576 |
+------------+------------+---------+
2 rows in set (0.01 sec)
```

## Generate new columns from original columns

When you load a data file into a StarRocks table, some data of the data file may require conversions before the data can be loaded into the StarRocks table. In this situation, you can use functions or expressions in the job creation command or statement to implement data conversions.

This feature supports loading data from the following data sources:

- Local file system

- HDFS and cloud storage
  > **NOTE**
  >
  > This section uses HDFS as an example.

- Kafka

This section uses `file2.csv` and `table2` as an example. `file2.csv` consists of only one column that represents date. You can use the [year](../sql-reference/sql-functions/date-time-functions/year.md), [month](../sql-reference/sql-functions/date-time-functions/month.md), and [day](../sql-reference/sql-functions/date-time-functions/day.md) functions to extract the year, month, and day in each date from `file2.csv` and load the extracted data into the `year`, `month`, and `day` columns of `table2`.

### Load data

#### Load data from a local file system

If `file2.csv` is stored in your local file system, run the following command to create a [Stream Load](../loading/StreamLoad.md) job:

```Bash
curl --location-trusted -u root: \
    -H "column_separator:," \
    -H "columns:date,year=year(date),month=month(date),day=day(date)" \
    -T file2.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
```

> **NOTE**
>
> - In the `columns` parameter, you must first temporarily name **all columns** of the data file, and then temporarily name the new columns that you want to generate from the original columns of the data file. As shown in the preceding example, the only column of `file2.csv` is temporarily named as `date`, and then the `year=year(date)`, `month=month(date)`, and `day=day(date)` functions are invoked to generate three new columns, which are temporarily named as `year`, `month`, and `day`.
>
> - Stream Load does not support `column_name = function(column_name)` but supports `column_name = function(column_name)`.

For detailed syntax and parameter descriptions, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md).

#### Load data from an HDFS cluster

If `file2.csv` is stored in your HDFS cluster, execute the following statement to create a [Broker Load](../loading/BrokerLoad.md) job:

```SQL
LOAD LABEL test_db.label3
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/file2.csv")
    INTO TABLE `table2`
    FORMAT AS "csv"
    COLUMNS TERMINATED BY ","
    (date)
    SET(year=year(date), month=month(date), day=day(date))
)
WITH BROKER "broker1";
```

> **NOTE**
>
> You must first use the `column_list` parameter to temporarily name **all columns** of the data file, and then use the SET clause to temporarily name the new columns that you want to generate from the original columns of the data file. As shown in the preceding example, the only column of `file2.csv` is temporarily named as `date` in the `column_list` parameter, and then the `year=year(date)`, `month=month(date)`, and `day=day(date)` functions are invoked in the SET clause to generate three new columns, which are temporarily named as `year`, `month`, and `day`.

For detailed syntax and parameter descriptions, see [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md).

#### Load data from a Kafka cluster

If the data of `file2.csv` is published to `topic2` of your Kafka cluster, execute the following statement to create a [Routine Load](../loading/RoutineLoad.md) job:

```SQL
CREATE ROUTINE LOAD test_db.table201 ON table2
    COLUMNS TERMINATED BY ",",
    COLUMNS(date,year=year(date),month=month(date),day=day(date))
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker_host>:<kafka_broker_port>",
    "kafka_topic" = "topic2",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

> **NOTE**
>
> In the `COLUMNS` parameter, you must first temporarily name **all columns** of the data file, and then temporarily name the new columns that you want to generate from the original columns of the data file. As shown in the preceding example, the only column of `file2.csv` is temporarily named as `date`, and then the `year=year(date)`, `month=month(date)`, and `day=day(date)` functions are invoked to generate three new columns, which are temporarily named as `year`, `month`, and `day`.

For detailed syntax and parameter descriptions, see [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/ROUTINE%20LOAD.md).

### Query data

After the load of data from your local file system, HDFS cluster, or Kafka cluster is complete, query the data of `table2` to verify that the load is successful:

```SQL
MySQL [test_db]> SELECT * FROM table2;
+------------+------+-------+------+
| date       | year | month | day  |
+------------+------+-------+------+
| 2020-05-20 | 2020 |  5    | 20   |
| 2020-05-21 | 2020 |  5    | 21   |
| 2020-05-22 | 2020 |  5    | 22   |
| 2020-05-23 | 2020 |  5    | 23   |
+------------+------+-------+------+
4 rows in set (0.01 sec)
```

## Extract partition field values from a file path

If the file path that you specify contains partition fields, you can use the `COLUMNS FROM PATH AS` parameter to specify the partition fields you want to extract from the file paths. The partition fields in file paths are equivalent to the columns in data files. The `COLUMNS FROM PATH AS` parameter is supported only when you load data from an HDFS cluster.

For example, you want to load the following four data files generated from Hive:

```Plain
/user/starrocks/data/input/date=2020-05-20/data
1,354
/user/starrocks/data/input/date=2020-05-21/data
2,465
/user/starrocks/data/input/date=2020-05-22/data
1,576
/user/starrocks/data/input/date=2020-05-23/data
2,687
```

The four data files are stored in the `/user/starrocks/data/input/` path of your HDFS cluster. Each of these data files is partitioned by partition field `date` and consists of two columns, which represent event type and user ID in sequence.

### Load data from an HDFS cluster

Execute the following statement to create a [Broker Load](../loading/BrokerLoad.md) job, which enables you to extract the `date` partition field values from the `/user/starrocks/data/input/` file path and use a wildcard (*) to specify that you want to load all data files in the file path to `table1`:

```SQL
LOAD LABEL test_db.label4
(
    DATA INFILE("hdfs://<fe_host>:<fe_http_port>/user/starrocks/data/input/date=*/*")
    INTO TABLE `table1`
    FORMAT AS "csv"
    COLUMNS TERMINATED BY ","
    (event_type, user_id)
    COLUMNS FROM PATH AS (date)
    SET(event_date = date)
)
WITH BROKER "broker1";
```

> **NOTE**
>
> In the preceding example, the `date` partition field in the specified file path is equivalent to the `event_date` column of `table1`. Therefore, you need to use the SET clause to map the `date` partition field onto the `event_date` column. If the partition field in the specified file path has the same name as a column of the StarRocks table, you do not need to use the SET clause to create a mapping.

For detailed syntax and parameter descriptions, see [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md).

### Query data

After the load of data from your HDFS cluster is complete, query the data of `table1` to verify that the load is successful:

```SQL
MySQL [test_db]> SELECT * FROM table1;
+------------+------------+---------+
| event_date | event_type | user_id |
+------------+------------+---------+
| 2020-05-22 |          1 |     576 |
| 2020-05-20 |          1 |     354 |
| 2020-05-21 |          2 |     465 |
| 2020-05-23 |          2 |     687 |
+------------+------------+---------+
4 rows in set (0.01 sec)
```
