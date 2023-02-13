# Load data using INSERT

This topic describes how to load data into StarRocks by using a SQL statement - INSERT.

Similar to MySQL and many other database management systems, StarRocks supports loading data to an internal table with INSERT. You can insert one or more rows directly with the VALUES clause to test a function or a DEMO. You can also insert data defined by the results of a query into an internal table from an [external table](../data_source/External_table.md).

StarRocks v2.4 further supports overwriting data into a table by using INSERT OVERWRITE. The INSERT OVERWRITE statement integrates the following operations to implement the overwriting function:

1. Creates temporary partitions according to the partitions that store the original data.
2. Inserts data into the temporary partitions.
3. Swaps the original partitions with the temporary partitions.

> **NOTE**
>
> If you need to verify the data before overwriting it, instead of using INSERT OVERWRITE, you can follow the above procedures to overwrite your data and verify it before swapping the partitions.

## Precautions

- Because INSERT transaction is synchronous, you can cancel it only by pressing the **Ctrl** and **C** keys from your MySQL client.

- As for the current version of StarRocks, the INSERT transaction fails by default if the data of any rows does not comply with the schema of the table. For example, the INSERT transaction fails if the length of a field in any row exceeds the length limit for the mapping field in the table. You can set the session variable `enable_insert_strict` to `false` to allow the transaction to continue by filtering out the rows that mismatch the table.

- If you execute the INSERT statement frequently to load small batches of data into StarRocks, excessive data versions are generated. It severely affects query performance. We recommend that, in production, you should not load data with the INSERT command too often or use it as a routine for data loading on a daily basis. Should your application or analytic scenario demand solutions to loading streaming data or small data batches separately, we recommend you use Apache Kafka® as your data source and load the data via [Routine Load](../loading/RoutineLoad.md).

- If you execute the INSERT OVERWRITE statement, StarRocks creates temporary partitions for the partitions which store the original data, inserts new data into the temporary partitions, and swaps the original partitions with the temporary partitions. All these operations are executed in the FE Leader node. Hence, if the FE Leader node crashes while executing INSERT OVERWRITE command, the whole load transaction will fail, and the temporary partitions will be truncated.

## Preparation

Create a database named `load_test`, and create a table `insert_wiki_edit` as the destination table and a table `source_wiki_edit` as the source table.

> **NOTE**
>
> Examples demonstrated in this topic are based on the table `insert_wiki_edit` and the table `source_wiki_edit`. Should you prefer working with your own tables and data, you can skip the preparation and move on to the next step.

```SQL
CREATE DATABASE IF NOT EXISTS load_test;
USE load_test;
CREATE TABLE insert_wiki_edit
(
    event_time DATETIME,
    channel VARCHAR(32) DEFAULT '',
    user VARCHAR(128) DEFAULT '',
    is_anonymous TINYINT DEFAULT '0',
    is_minor TINYINT DEFAULT '0',
    is_new TINYINT DEFAULT '0',
    is_robot TINYINT DEFAULT '0',
    is_unpatrolled TINYINT DEFAULT '0',
    delta INT DEFAULT '0',
    added INT DEFAULT '0',
    deleted INT DEFAULT '0'
)
DUPLICATE KEY
(
    event_time,
    channel,
    user,
    is_anonymous,
    is_minor,
    is_new,
    is_robot,
    is_unpatrolled
)
PARTITION BY RANGE(event_time)
(
    PARTITION p06 VALUES LESS THAN ('2015-09-12 06:00:00'),
    PARTITION p12 VALUES LESS THAN ('2015-09-12 12:00:00'),
    PARTITION p18 VALUES LESS THAN ('2015-09-12 18:00:00'),
    PARTITION p24 VALUES LESS THAN ('2015-09-13 00:00:00')
)
DISTRIBUTED BY HASH(user) BUCKETS 3;

CREATE TABLE source_wiki_edit
(
    event_time DATETIME,
    channel VARCHAR(32) DEFAULT '',
    user VARCHAR(128) DEFAULT '',
    is_anonymous TINYINT DEFAULT '0',
    is_minor TINYINT DEFAULT '0',
    is_new TINYINT DEFAULT '0',
    is_robot TINYINT DEFAULT '0',
    is_unpatrolled TINYINT DEFAULT '0',
    delta INT DEFAULT '0',
    added INT DEFAULT '0',
    deleted INT DEFAULT '0'
)
DUPLICATE KEY
(
    event_time,
    channel,
    user,
    is_anonymous,
    is_minor,
    is_new,
    is_robot,
    is_unpatrolled
)
PARTITION BY RANGE(event_time)
(
    PARTITION p06 VALUES LESS THAN ('2015-09-12 06:00:00'),
    PARTITION p12 VALUES LESS THAN ('2015-09-12 12:00:00'),
    PARTITION p18 VALUES LESS THAN ('2015-09-12 18:00:00'),
    PARTITION p24 VALUES LESS THAN ('2015-09-13 00:00:00')
)
DISTRIBUTED BY HASH(user) BUCKETS 3;
```

## Insert data via INSERT INTO VALUES

You can append one or more rows to a specific table by using INSERT INTO VALUES command. Multiple rows are separated by comma (,). For detailed instructions and parameter references, see [SQL Reference - INSERT](../sql-reference/sql-statements/data-manipulation/insert.md).

> **CAUTION**
>
> Inserting data via INSERT INTO VALUES merely applies to the situation when you need to verify a DEMO with a small dataset. It is not recommended for a massive testing or production environment. To load mass data into StarRocks, see [Ingestion Overview](../loading/Loading_intro.md) for other options that suit your scenarios.

The following example insert two rows into the data source table `source_wiki_edit` with the label `insert_load_wikipedia`. Label is the unique identification label for each data load transaction within the database.

```SQL
INSERT INTO source_wiki_edit
WITH LABEL insert_load_wikipedia
VALUES
    ("2015-09-12 00:00:00","#en.wikipedia","AustinFF",0,0,0,0,0,21,5,0),
    ("2015-09-12 00:00:00","#ca.wikipedia","helloSR",0,1,0,1,0,3,23,0);
```

## Insert data via INSERT INTO SELECT

You can load the result of a query on a data source table into the target table via INSERT INTO SELECT command. INSERT INTO SELECT command performs ETL operations on the data from the data source table, and loads the data into an internal table in StarRocks. The data source can be one or more internal or external tables. The target table MUST be an internal table in StarRocks. For detailed instructions and parameter references, see [SQL Reference - INSERT](../sql-reference/sql-statements/data-manipulation/insert.md).

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

If you [truncate](../sql-reference/sql-functions/math-functions/truncate.md) the `p06` and `p12` partitions, the data will not be returned in a query.

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

## Overwrite data via INSERT OVERWRITE VALUES

You can overwrite a specific table with one or more rows by using INSERT OVERWRITE VALUES command. Multiple rows are separated by comma (,). For detailed instructions and parameter references, see [SQL Reference - INSERT](../sql-reference/sql-statements/data-manipulation/insert.md).

> **CAUTION**
>
> Overwriting data via INSERT OVERWRITE VALUES merely applies to the situation when you need to verify a DEMO with a small dataset. It is not recommended for a massive testing or production environment. To load mass data into StarRocks, see [Ingestion Overview](../loading/Loading_intro.md) for other options that suit your scenarios.

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

You can overwrite a table with the result of a query on a data source table via INSERT OVERWRITE SELECT command. INSERT OVERWRITE SELECT statement performs ETL operations on the data from one or more internal or external tables, and overwrites an internal table with the data For detailed instructions and parameter references, see [SQL Reference - INSERT](../sql-reference/sql-statements/data-manipulation/insert.md).

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

If you [truncate](../sql-reference/sql-functions/math-functions/truncate.md) the `p06` and `p12` partitions, the data will not be returned in a query.

```Plain
MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

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

## Check the INSERT transaction status

### Check via the result

The INSERT transaction returns different status in accordance with the result of the transaction.

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

### Check via SHOW LOAD

You can check the INSERT transaction status by using SHOW LOAD command.

The following example checks the status of the transaction with label `insert_load_wikipedia`.

```SQL
SHOW LOAD WHERE label="insert_load_wikipedia"\G
```

The return is as follows:

```Plain
*************************** 1. row ***************************
         JobId: 13525
         Label: insert_load_wikipedia
         State: FINISHED
      Progress: ETL:100%; LOAD:100%
          Type: INSERT
       EtlInfo: NULL
      TaskInfo: cluster:N/A; timeout(s):3600; max_filter_ratio:0.0
      ErrorMsg: NULL
    CreateTime: 2022-08-02 11:41:26
  EtlStartTime: 2022-08-02 11:41:26
 EtlFinishTime: 2022-08-02 11:41:26
 LoadStartTime: 2022-08-02 11:41:26
LoadFinishTime: 2022-08-02 11:41:26
           URL: 
    JobDetails: {"Unfinished backends":{},"ScannedRows":0,"TaskNumber":0,"All backends":{},"FileNumber":0,"FileSize":0}
```

### Check via curl command

You can check the INSERT transaction status by using curl command.

The following example checks the status of the transaction with label `insert_load_wikipedia`.

```Bash
curl --location-trusted -u root: \
  http://x.x.x.x:8030/api/load_test/_load_info?label=insert_load_wikipedia
```

The return is as follows:

```Plain
{
  "jobInfo": {
    "dbName": "default_cluster:load_test",
    "tblNames": [
      "source_wiki_edit"
    ],
    "label": "insert_load_wikipedia",
    "clusterName": "default_cluster",
    "state": "FINISHED",
    "failMsg": "",
    "trackingUrl": ""
  },
  "status": "OK",
  "msg": "Success"
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
| enable_insert_strict | Switch value to control if the INSERT transaction is tolerant of invalid data rows. When it is set to `true`, the transaction fails if any of the data rows is invalid. When it is set to `false`, the transaction succeeds when at least one row of data has been loaded correctly, and the label will be returned. The default is `true`. You can set this variable with `SET enable_insert_strict = false/ture;` command. |
| query_timeout        | Timeout for SQL commands. Unit: second. INSERT, as a SQL command, is also restricted by this session variable. You can set this variable with the `SET query_timeout = xxx;` command. |
