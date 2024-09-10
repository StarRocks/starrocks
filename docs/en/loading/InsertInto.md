---
displayed_sidebar: docs
---

# Load data using INSERT

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.md'

This topic describes how to load data into StarRocks by using a SQL statement - INSERT.

Similar to MySQL and many other database management systems, StarRocks supports loading data to an internal table with INSERT. You can insert one or more rows directly with the VALUES clause to test a function or a DEMO. You can also insert data defined by the results of a query into an internal table from an [external table](../data_source/External_table.md). From StarRocks v3.1 onwards, you can directly load data from files on cloud storage using the INSERT command and the table function [FILES()](../sql-reference/sql-functions/table-functions/files.md).

StarRocks v2.4 further supports overwriting data into a table by using INSERT OVERWRITE. The INSERT OVERWRITE statement integrates the following operations to implement the overwriting function:

1. Creates temporary partitions according to the partitions that store the original data.
2. Inserts data into the temporary partitions.
3. Swaps the original partitions with the temporary partitions.

> **NOTE**
>
> If you need to verify the data before overwriting it, instead of using INSERT OVERWRITE, you can follow the above procedures to overwrite your data and verify it before swapping the partitions.

## Precautions

- You can cancel a synchronous INSERT transaction only by pressing the **Ctrl** and **C** keys from your MySQL client.
- You can submit an asynchronous INSERT task using [SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md).
- As for the current version of StarRocks, the INSERT transaction fails by default if the data of any rows does not comply with the schema of the table. For example, the INSERT transaction fails if the length of a field in any row exceeds the length limit for the mapping field in the table. You can set the session variable `enable_insert_strict` to `false` to allow the transaction to continue by filtering out the rows that mismatch the table.
- If you execute the INSERT statement frequently to load small batches of data into StarRocks, excessive data versions are generated. It severely affects query performance. We recommend that, in production, you should not load data with the INSERT command too often or use it as a routine for data loading on a daily basis. If your application or analytic scenario demand solutions to loading streaming data or small data batches separately, we recommend you use Apache Kafka® as your data source and load the data via [Routine Load](../loading/RoutineLoad.md).
- If you execute the INSERT OVERWRITE statement, StarRocks creates temporary partitions for the partitions which store the original data, inserts new data into the temporary partitions, and [swaps the original partitions with the temporary partitions](../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md#use-a-temporary-partition-to-replace-the-current-partition). All these operations are executed in the FE Leader node. Hence, if the FE Leader node crashes while executing INSERT OVERWRITE command, the whole load transaction will fail, and the temporary partitions will be truncated.

## Preparation

### Check privileges

<InsertPrivNote />

### Create objects

Create a database named `load_test`, and create a table `insert_wiki_edit` as the destination table and a table `source_wiki_edit` as the source table.

> **NOTE**
>
> Examples demonstrated in this topic are based on the table `insert_wiki_edit` and the table `source_wiki_edit`. If you prefer working with your own tables and data, you can skip the preparation and move on to the next step.

```SQL
CREATE DATABASE IF NOT EXISTS load_test;
USE load_test;
CREATE TABLE insert_wiki_edit
(
    event_time      DATETIME,
    channel         VARCHAR(32)      DEFAULT '',
    user            VARCHAR(128)     DEFAULT '',
    is_anonymous    TINYINT          DEFAULT '0',
    is_minor        TINYINT          DEFAULT '0',
    is_new          TINYINT          DEFAULT '0',
    is_robot        TINYINT          DEFAULT '0',
    is_unpatrolled  TINYINT          DEFAULT '0',
    delta           INT              DEFAULT '0',
    added           INT              DEFAULT '0',
    deleted         INT              DEFAULT '0'
)
DUPLICATE KEY(
    event_time,
    channel,
    user,
    is_anonymous,
    is_minor,
    is_new,
    is_robot,
    is_unpatrolled
)
PARTITION BY RANGE(event_time)(
    PARTITION p06 VALUES LESS THAN ('2015-09-12 06:00:00'),
    PARTITION p12 VALUES LESS THAN ('2015-09-12 12:00:00'),
    PARTITION p18 VALUES LESS THAN ('2015-09-12 18:00:00'),
    PARTITION p24 VALUES LESS THAN ('2015-09-13 00:00:00')
)
DISTRIBUTED BY HASH(user);

CREATE TABLE source_wiki_edit
(
    event_time      DATETIME,
    channel         VARCHAR(32)      DEFAULT '',
    user            VARCHAR(128)     DEFAULT '',
    is_anonymous    TINYINT          DEFAULT '0',
    is_minor        TINYINT          DEFAULT '0',
    is_new          TINYINT          DEFAULT '0',
    is_robot        TINYINT          DEFAULT '0',
    is_unpatrolled  TINYINT          DEFAULT '0',
    delta           INT              DEFAULT '0',
    added           INT              DEFAULT '0',
    deleted         INT              DEFAULT '0'
)
DUPLICATE KEY(
    event_time,
    channel,user,
    is_anonymous,
    is_minor,
    is_new,
    is_robot,
    is_unpatrolled
)
PARTITION BY RANGE(event_time)(
    PARTITION p06 VALUES LESS THAN ('2015-09-12 06:00:00'),
    PARTITION p12 VALUES LESS THAN ('2015-09-12 12:00:00'),
    PARTITION p18 VALUES LESS THAN ('2015-09-12 18:00:00'),
    PARTITION p24 VALUES LESS THAN ('2015-09-13 00:00:00')
)
DISTRIBUTED BY HASH(user);
```

> **NOTICE**
>
> Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [set the number of buckets](../table_design/Data_distribution.md#set-the-number-of-buckets).

## Insert data via INSERT INTO VALUES

You can append one or more rows to a specific table by using INSERT INTO VALUES command. Multiple rows are separated by comma (,). For detailed instructions and parameter references, see [SQL Reference - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md).

> **CAUTION**
>
> Inserting data via INSERT INTO VALUES merely applies to the situation when you need to verify a DEMO with a small dataset. It is not recommended for a massive testing or production environment. To load mass data into StarRocks, see [Loading options](./loading_introduction/Loading_intro.md) for other options that suit your scenarios.

The following example inserts two rows into the data source table `source_wiki_edit` with the label `insert_load_wikipedia`. Label is the unique identification label for each data load transaction within the database.

```SQL
INSERT INTO source_wiki_edit
WITH LABEL insert_load_wikipedia
VALUES
    ("2015-09-12 00:00:00","#en.wikipedia","AustinFF",0,0,0,0,0,21,5,0),
    ("2015-09-12 00:00:00","#ca.wikipedia","helloSR",0,1,0,1,0,3,23,0);
```

## Insert data via INSERT INTO SELECT

You can load the result of a query on a data source table into the target table via INSERT INTO SELECT command. INSERT INTO SELECT command performs ETL operations on the data from the data source table, and loads the data into an internal table in StarRocks. The data source can be one or more internal or external tables, or even data files on cloud storage. The target table MUST be an internal table in StarRocks. For detailed instructions and parameter references, see [SQL Reference - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md).

### Insert data from an internal or external table into an internal table

> **NOTE**
>
> Inserting data from an external table is identical to inserting data from an internal table. For simplicity, we only demonstrate how to insert data from an internal table in the following examples.

- The following example inserts the data from the source table to the target table `insert_wiki_edit`.

```SQL
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_1
SELECT * FROM source_wiki_edit;
```

- The following example inserts the data from the source table to the `p06` and `p12` partitions of the target table `insert_wiki_edit`. If no partition is specified, the data will be inserted into all partitions. Otherwise, the data will be inserted only into the specified partition(s).

```SQL
INSERT INTO insert_wiki_edit PARTITION(p06, p12)
WITH LABEL insert_load_wikipedia_2
SELECT * FROM source_wiki_edit;
```

Query the target table to make sure there is data in them.

```Plain text
MySQL > select * from insert_wiki_edit;
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user     | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #en.wikipedia | AustinFF |            0 |        0 |      0 |        0 |              0 |    21 |     5 |       0 |
| 2015-09-12 00:00:00 | #ca.wikipedia | helloSR  |            0 |        1 |      0 |        1 |              0 |     3 |    23 |       0 |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.00 sec)
```

If you truncate the `p06` and `p12` partitions, the data will not be returned in a query.

```Plain
MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

- The following example inserts the `event_time` and `channel` columns from the source table to the target table `insert_wiki_edit`. Default values are used in the columns that are not specified here.

```SQL
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_3 
(
    event_time, 
    channel
)
SELECT event_time, channel FROM source_wiki_edit;
```

:::note
From v3.3.1, specifying a column list in the INSERT INTO statement on a Primary Key table will perform Partial Updates (instead of Full Upsert in earlier versions). If the column list is not specified, the system will perform Full Upsert.
:::

### Insert data directly from files in an external source using FILES()

From v3.1 onwards, StarRocks supports directly loading data from files on cloud storage using the INSERT command and the [FILES()](../sql-reference/sql-functions/table-functions/files.md) function, thereby you do not need to create an external catalog or file external table first. Besides, FILES() can automatically infer the table schema of the files, greatly simplifying the process of data loading.

The following example inserts data rows from the Parquet file **parquet/insert_wiki_edit_append.parquet** within the AWS S3 bucket `inserttest` into the table `insert_wiki_edit`:

```Plain
INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "us-west-2"
);
```

## Overwrite data via INSERT OVERWRITE VALUES

You can overwrite a specific table with one or more rows by using INSERT OVERWRITE VALUES command. Multiple rows are separated by comma (,). For detailed instructions and parameter references, see [SQL Reference - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md).

> **CAUTION**
>
> Overwriting data via INSERT OVERWRITE VALUES merely applies to the situation when you need to verify a DEMO with a small dataset. It is not recommended for a massive testing or production environment. To load mass data into StarRocks, see [Loading options](./loading_introduction/Loading_intro.md) for other options that suit your scenarios.

Query the source table and the target table to make sure there is data in them.

```Plain
MySQL > SELECT * FROM source_wiki_edit;
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user     | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #ca.wikipedia | helloSR  |            0 |        1 |      0 |        1 |              0 |     3 |    23 |       0 |
| 2015-09-12 00:00:00 | #en.wikipedia | AustinFF |            0 |        0 |      0 |        0 |              0 |    21 |     5 |       0 |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.02 sec)
 
MySQL > SELECT * FROM insert_wiki_edit;
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user     | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #ca.wikipedia | helloSR  |            0 |        1 |      0 |        1 |              0 |     3 |    23 |       0 |
| 2015-09-12 00:00:00 | #en.wikipedia | AustinFF |            0 |        0 |      0 |        0 |              0 |    21 |     5 |       0 |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.01 sec)
```

The following example overwrites the source table `source_wiki_edit` with two new rows.

```SQL
INSERT OVERWRITE source_wiki_edit
WITH LABEL insert_load_wikipedia_ow
VALUES
    ("2015-09-12 00:00:00","#cn.wikipedia","GELongstreet",0,0,0,0,0,36,36,0),
    ("2015-09-12 00:00:00","#fr.wikipedia","PereBot",0,1,0,1,0,17,17,0);
```

## Overwrite data via INSERT OVERWRITE SELECT

You can overwrite a table with the result of a query on a data source table via INSERT OVERWRITE SELECT command. INSERT OVERWRITE SELECT statement performs ETL operations on the data from one or more internal or external tables, and overwrites an internal table with the data For detailed instructions and parameter references, see [SQL Reference - INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md).

> **NOTE**
>
> Loading data from an external table is identical to loading data from an internal table. For simplicity, we only demonstrate how to overwrite the target table with the data from an internal table in the following examples.

Query the source table and the target table to make sure that they hold different rows of data.

```Plain
MySQL > SELECT * FROM source_wiki_edit;
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user         | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #cn.wikipedia | GELongstreet |            0 |        0 |      0 |        0 |              0 |    36 |    36 |       0 |
| 2015-09-12 00:00:00 | #fr.wikipedia | PereBot      |            0 |        1 |      0 |        1 |              0 |    17 |    17 |       0 |
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.02 sec)
 
MySQL > SELECT * FROM insert_wiki_edit;
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user     | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #en.wikipedia | AustinFF |            0 |        0 |      0 |        0 |              0 |    21 |     5 |       0 |
| 2015-09-12 00:00:00 | #ca.wikipedia | helloSR  |            0 |        1 |      0 |        1 |              0 |     3 |    23 |       0 |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.01 sec)
```

- The following example overwrites the table `insert_wiki_edit` with the data from the source table.

```SQL
INSERT OVERWRITE insert_wiki_edit
WITH LABEL insert_load_wikipedia_ow_1
SELECT * FROM source_wiki_edit;
```

- The following example overwrites the `p06` and `p12` partitions of the table `insert_wiki_edit` with the data from the source table.

```SQL
INSERT OVERWRITE insert_wiki_edit PARTITION(p06, p12)
WITH LABEL insert_load_wikipedia_ow_2
SELECT * FROM source_wiki_edit;
```

Query the target table to make sure there is data in them.

```plain text
MySQL > select * from insert_wiki_edit;
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user         | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #fr.wikipedia | PereBot      |            0 |        1 |      0 |        1 |              0 |    17 |    17 |       0 |
| 2015-09-12 00:00:00 | #cn.wikipedia | GELongstreet |            0 |        0 |      0 |        0 |              0 |    36 |    36 |       0 |
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.01 sec)
```

If you truncate the `p06` and `p12` partitions, the data will not be returned in a query.

```Plain
MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

:::note
For tables that use the `PARTITION BY column` strategy, INSERT OVERWRITE supports creating new partitions in the destination table by specifying the value of the partition key. Existing partitions are overwritten as usual.

The following example creates the partitioned table `activity`, and creates a new partition in the table while inserting data into it:

```SQL
CREATE TABLE activity (
id INT          NOT NULL,
dt VARCHAR(10)  NOT NULL
) ENGINE=OLAP 
DUPLICATE KEY(`id`)
PARTITION BY (`id`, `dt`)
DISTRIBUTED BY HASH(`id`);

INSERT OVERWRITE activity
PARTITION(id='4', dt='2022-01-01')
WITH LABEL insert_activity_auto_partition
VALUES ('4', '2022-01-01');
```

:::

- The following example overwrites the target table `insert_wiki_edit` with the `event_time` and `channel` columns from the source table. The default value is assigned to the columns into which no data is overwritten.

```SQL
INSERT OVERWRITE insert_wiki_edit
WITH LABEL insert_load_wikipedia_ow_3 
(
    event_time, 
    channel
)
SELECT event_time, channel FROM source_wiki_edit;
```

## Insert data into a table with generated columns

A generated column is a special column whose value is derived from a pre-defined expression or evaluation based on other columns. Generated columns are especially useful when your query requests involve evaluations of expensive expressions, for example, querying a certain field from a JSON value, or calculating ARRAY data. StarRocks evaluates the expression and stores the results in the generated columns while data is being loaded into the table, thereby avoiding the expression evaluation during queries and improving the query performance.

You can load data into a table with generated columns using INSERT.

The following example creates a table `insert_generated_columns` and inserts a row into it. The table contains two generated columns: `avg_array` and `get_string`. `avg_array` calculates the average value of ARRAY data in `data_array`, and `get_string` extracts the strings from the JSON path `a` in `data_json`.

```SQL
CREATE TABLE insert_generated_columns (
  id           INT(11)           NOT NULL    COMMENT "ID",
  data_array   ARRAY<INT(11)>    NOT NULL    COMMENT "ARRAY",
  data_json    JSON              NOT NULL    COMMENT "JSON",
  avg_array    DOUBLE            NULL 
      AS array_avg(data_array)               COMMENT "Get the average of ARRAY",
  get_string   VARCHAR(65533)    NULL 
      AS get_json_string(json_string(data_json), '$.a') COMMENT "Extract JSON string"
) ENGINE=OLAP 
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id);

INSERT INTO insert_generated_columns 
VALUES (1, [1,2], parse_json('{"a" : 1, "b" : 2}'));
```

> **NOTE**
>
> Directly loading data into generated columns is not supported.

You can query the table to check the data within it.

```Plain
mysql> SELECT * FROM insert_generated_columns;
+------+------------+------------------+-----------+------------+
| id   | data_array | data_json        | avg_array | get_string |
+------+------------+------------------+-----------+------------+
|    1 | [1,2]      | {"a": 1, "b": 2} |       1.5 | 1          |
+------+------------+------------------+-----------+------------+
1 row in set (0.02 sec)
```

## Load data asynchronously using INSERT

Loading data with INSERT submits a synchronous transaction, which may fail because of session interruption or timeout. You can submit an asynchronous INSERT transaction using [SUBMIT TASK](../sql-reference/sql-statements/loading_unloading/ETL/SUBMIT_TASK.md). This feature is supported since StarRocks v2.5.

- The following example asynchronously inserts the data from the source table to the target table `insert_wiki_edit`.

```SQL
SUBMIT TASK AS INSERT INTO insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

- The following example asynchronously overwrites the table `insert_wiki_edit` with the data from the source table.

```SQL
SUBMIT TASK AS INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

- The following example asynchronously overwrites the table `insert_wiki_edit` with the data from the source table, and extends the query timeout to `100000` seconds using hint.

```SQL
SUBMIT /*+set_var(query_timeout=100000)*/ TASK AS
INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

- The following example asynchronously overwrites the table `insert_wiki_edit` with the data from the source table, and specifies the task name as `async`.

```SQL
SUBMIT TASK async
AS INSERT OVERWRITE insert_wiki_edit
SELECT * FROM source_wiki_edit;
```

You can check the status of an asynchronous INSERT task by querying the metadata view `task_runs` in Information Schema.

The following example checks the status of the INSERT task `async`.

```SQL
SELECT * FROM information_schema.task_runs WHERE task_name = 'async';
```

## Check the INSERT job status

### Check via the result

A synchronous INSERT transaction returns different status in accordance with the result of the transaction.

- **Transaction succeeds**

StarRocks returns the following if the transaction succeeds:

```Plain
Query OK, 2 rows affected (0.05 sec)
{'label':'insert_load_wikipedia', 'status':'VISIBLE', 'txnId':'1006'}
```

- **Transaction fails**

If all rows of data fail to be loaded into the target table, the INSERT transaction fails. StarRocks returns the following if the transaction fails:

```Plain
ERROR 1064 (HY000): Insert has filtered data in strict mode, tracking_url=http://x.x.x.x:yyyy/api/_load_error_log?file=error_log_9f0a4fd0b64e11ec_906bbede076e9d08
```

You can locate the problem by checking the log with `tracking_url`.

### Check via Information Schema

You can use the [SELECT](../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) statement to query the results of one or more load jobs from the `loads` table in the `information_schema` database. This feature is supported from v3.1 onwards.

Example 1: Query the results of load jobs executed on the `load_test` database, sort the results by creation time (`CREATE_TIME`) in descending order, and only return the top result.

```SQL
SELECT * FROM information_schema.loads
WHERE database_name = 'load_test'
ORDER BY create_time DESC
LIMIT 1\G
```

Example 2: Query the result of the load job (whose label is `insert_load_wikipedia`) executed on the `load_test` database:

```SQL
SELECT * FROM information_schema.loads
WHERE database_name = 'load_test' and label = 'insert_load_wikipedia'\G
```

The return is as follows:

```Plain
*************************** 1. row ***************************
              JOB_ID: 21319
               LABEL: insert_load_wikipedia
       DATABASE_NAME: load_test
               STATE: FINISHED
            PROGRESS: ETL:100%; LOAD:100%
                TYPE: INSERT
            PRIORITY: NORMAL
           SCAN_ROWS: 0
       FILTERED_ROWS: 0
     UNSELECTED_ROWS: 0
           SINK_ROWS: 2
            ETL_INFO: 
           TASK_INFO: resource:N/A; timeout(s):300; max_filter_ratio:0.0
         CREATE_TIME: 2023-08-09 10:42:23
      ETL_START_TIME: 2023-08-09 10:42:23
     ETL_FINISH_TIME: 2023-08-09 10:42:23
     LOAD_START_TIME: 2023-08-09 10:42:23
    LOAD_FINISH_TIME: 2023-08-09 10:42:24
         JOB_DETAILS: {"All backends":{"5ebf11b5-365e-11ee-9e4a-7a563fb695da":[10006]},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":175,"InternalTableLoadRows":2,"ScanBytes":0,"ScanRows":0,"TaskNumber":1,"Unfinished backends":{"5ebf11b5-365e-11ee-9e4a-7a563fb695da":[]}}
           ERROR_MSG: NULL
        TRACKING_URL: NULL
        TRACKING_SQL: NULL
REJECTED_RECORD_PATH: NULL
1 row in set (0.01 sec)
```

For information about the fields in the return results, see [Information Schema > loads](../sql-reference/information_schema/loads.md).

### Check via curl command

You can check the INSERT transaction status by using curl command.

Launch a terminal, and execute the following command:

```Bash
curl --location-trusted -u <username>:<password> \
  http://<fe_address>:<fe_http_port>/api/<db_name>/_load_info?label=<label_name>
```

The following example checks the status of the transaction with label `insert_load_wikipedia`.

```Bash
curl --location-trusted -u <username>:<password> \
  http://x.x.x.x:8030/api/load_test/_load_info?label=insert_load_wikipedia
```

> **NOTE**
>
> If you use an account for which no password is set, you need to input only `<username>:`.

The return is as follows:

```Plain
{
   "jobInfo":{
      "dbName":"load_test",
      "tblNames":[
         "source_wiki_edit"
      ],
      "label":"insert_load_wikipedia",
      "state":"FINISHED",
      "failMsg":"",
      "trackingUrl":""
   },
   "status":"OK",
   "msg":"Success"
}
```

## Configuration

You can set the following configuration items for INSERT transaction:

- **FE configuration**

| FE configuration                   | Description                                                  |
| ---------------------------------- | ------------------------------------------------------------ |
| insert_load_default_timeout_second | Default timeout for INSERT transaction. Unit: second. If the current INSERT transaction is not completed within the time set by this parameter, it will be canceled by the system and the status will be CANCELLED. As for current version of StarRocks, you can only specify a uniform timeout for all INSERT transactions using this parameter, and you cannot set a different timeout for a specific INSERT transaction. The default is 3600 seconds (1 hour). If the INSERT transaction cannot be completed within the specified time, you can extend the timeout by adjusting this parameter. |

- **Session variables**

| Session variable     | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| enable_insert_strict | Switch value to control if the INSERT transaction is tolerant of invalid data rows. When it is set to `true`, the transaction fails if any of the data rows is invalid. When it is set to `false`, the transaction succeeds when at least one row of data has been loaded correctly, and the label will be returned. The default is `true`. You can set this variable with `SET enable_insert_strict = {true or false};` command. |
| query_timeout        | Timeout for SQL commands. Unit: second. INSERT, as a SQL command, is also restricted by this session variable. You can set this variable with the `SET query_timeout = xxx;` command. |
