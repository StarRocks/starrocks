---
displayed_sidebar: "Chinese"
---

# 通过 INSERT 语句导入数据

本文介绍如何使用 INSERT 语句向 StarRocks 导入数据。

与 MySQL 等数据库系统类似，StarRocks 支持通过 INSERT 语句导入数据。您可以使用 INSERT INTO VALUES 语句直接向表中插入数据，您还可以通过 INSERT INTO SELECT 语句将其他 StarRocks 表中的数据导入到新的 StarRocks 表中，或者将其他数据源的数据通过[外部表功能](../data_source/External_table.md)导入至 StarRocks 内部表中。

2.4 版本中，StarRocks 进一步支持通过 INSERT OVERWRITE 语句批量**覆盖写入**目标表。INSERT OVERWRITE 语句通过整合以下三部分操作来实现覆盖写入：

1. 为目标分区[创建临时分区](../table_design/Temporary_partition.md#创建临时分区)
2. [写入数据至临时分区](../table_design/Temporary_partition.md#导入数据至临时分区)
3. [使用临时分区原子替换目标分区](../table_design/Temporary_partition.md#使用临时分区进行替换)

如果您希望在替换前验证数据，可以根据以上步骤自行实现覆盖写入数据。

## 注意事项

- 您只能在 MySQL 客户端通过 `Ctrl` + `C` 按键强制取消同步 INSERT 导入任务。
- 当前版本中，StarRocks 在执行 INSERT 语句时，如果有数据不符合目标表格式（例如字符串超长等情况），INSERT 操作默认执行失败。您可以通过设置会话变量 `enable_insert_strict` 为 `false` 以确保 INSERT 操作过滤不符合目标表格式的数据，并继续执行。
- 频繁使用 INSERT 语句导入小批量数据会产生过多的数据版本，从而影响查询性能，因此不建议您频繁使用 INSERT 语句导入数据或将其作为生产环境的日常例行导入作业。如果您的业务场景需要流式导入或者小批量多次导入数据，建议使用 Apache Kafka® 作为数据源并通过 [Routine Load](../loading/RoutineLoad.md) 方式进行导入作业。
- 执行 INSERT OVERWRITE 语句后，系统将为目标分区创建相应的临时分区，并将数据写入临时分区，最后使用临时分区[原子替换](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md#使用临时分区替换原分区)目标分区来实现覆盖写入。其所有过程均在 Leader FE 节点执行。因此，如果 Leader FE 节点在覆盖写入过程中发生宕机，将会导致该次 INSERT OVERWRITE 导入失败，其过程中所创建的临时分区也会被删除。

## 准备工作

在 StarRocks 中创建数据库 `load_test`，并在其中创建导入目标表 `insert_wiki_edit` 以及数据源表 `source_wiki_edit`。

> **说明**
>
> 本文中演示的操作示例均基于表 `insert_wiki_edit` 和数据源表 `source_wiki_edit`。如果您选择使用自己的表以及数据，请跳过当前步骤，并根据使用场景修改需要导入的数据。

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

## 通过 INSERT INTO VALUES 语句导入数据

您可以通过 INSERT INTO VALUES 语句向指定的表中直接导入数据。此导入方式中，多条数据用逗号（,）分隔。详细使用方式，参考 [SQL 参考 - INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md)。详细参数信息，参考 [INSERT 参数说明](../sql-reference/sql-statements/data-manipulation/INSERT.md#参数说明)。

> **注意**
>
> INSERT INTO VALUES 语句导入方式仅适用于导入少量数据作为验证 DEMO 用途，不适用于大规模测试或生产环境。如需大规模导入数据，请选择其他导入方式。

以下示例以 `insert_load_wikipedia` 为 Label 向源表 `source_wiki_edit` 中导入两条数据。Label 是导入作业的标识，数据库内唯一。

```SQL
INSERT INTO source_wiki_edit
WITH LABEL insert_load_wikipedia
VALUES
    ("2015-09-12 00:00:00","#en.wikipedia","AustinFF",0,0,0,0,0,21,5,0),
    ("2015-09-12 00:00:00","#ca.wikipedia","helloSR",0,1,0,1,0,3,23,0);
```

| 参数       | 说明                                                         |
| ---------- | ------------------------------------------------------------ |
| table_name | 导入数据的目标表。可以使用 `db_name.table_name` 形式。       |
| label      | 导入作业的标识，数据库内唯一。如果未指定，StarRocks 会自动为作业生成一个 Label。建议您指定 Label。否则，如果当前导入作业因网络错误无法返回结果，您将无法得知该导入操作是否成功。如果指定了 Label，可以通过 SQL 命令 `SHOW LOAD WHERE label="label";` 查看作业结果。 |
| values     | 通过 VALUES 语法插入一条或者多条数据，多条数据用逗号（,）分隔。 |

## 通过 INSERT INTO SELECT 语句导入数据

您可以通过 INSERT INTO SELECT 语句将源表中的数据导入至目标表中。INSERT INTO SELECT 将源表中的数据进行 ETL 转换之后，导入到 StarRocks 内表中。源表可以是一张或多张内部表或者外部表。目标表必须是 StarRocks 的内表。执行该语句之后，系统将 SELECT 语句结果导入目标表。详细使用方式，参考 [INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md)。详细参数信息，参考 [INSERT 参数](../sql-reference/sql-statements/data-manipulation/INSERT.md#参数说明)。

> 说明
>
> 以下示例仅展示导入内部表数据，其操作过程与导入外部表数据相同，故不重复演示导入外部表数据过程。

- 以下示例以 `insert_load_wikipedia_1` 为 Label 将源表中的数据导入至目标表中。

```SQL
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_1
SELECT * FROM source_wiki_edit;
```

- 以下示例以 `insert_load_wikipedia_2` 为 Label 将源表中的数据导入至目标表的 `p06` 和 `p12` 分区中。如果不指定目标分区，数据将会导入全表；如果指定目标分区，数据只会导入指定的分区。

```SQL
INSERT INTO insert_wiki_edit PARTITION(p06, p12)
WITH LABEL insert_load_wikipedia_2
SELECT * FROM source_wiki_edit;
```

如果清空 `p06` 和 `p12` 分区，则查询不到先前插入至对应分区的数据。

```Plain
MySQL > select * from insert_wiki_edit;
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user     | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #en.wikipedia | AustinFF |            0 |        0 |      0 |        0 |              0 |    21 |     5 |       0 |
| 2015-09-12 00:00:00 | #ca.wikipedia | helloSR  |            0 |        1 |      0 |        1 |              0 |     3 |    23 |       0 |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.00 sec)

MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

- 以下示例以 `insert_load_wikipedia_3` 为 Label 将源表中 `event_time` 和 `channel` 列的数据导入至目标表的对应列中。未被导入的列将被赋予默认值。

```SQL
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_3 
(
    event_time, 
    channel
)
SELECT event_time, channel FROM source_wiki_edit;
```

| 参数        | 说明                                                         |
| ----------- | ------------------------------------------------------------ |
| table_name  | 导入数据的目标表。可以为 `db_name.table_name` 形式。         |
| partitions  | 导入的目标分区。此参数必须是目标表中存在的分区，多个分区名称用逗号（`,`）分隔。如果指定该参数，数据只会被导入相应分区内。如果未指定，则默认将数据导入至目标表的所有分区。 |
| label       | 导入作业的标识，数据库内唯一。如果未指定，StarRocks 会自动为作业生成一个 Label。建议您指定 Label。否则，如果当前导入作业因网络错误无法返回结果，您将无法得知该导入操作是否成功。如果指定了 Label，可以通过 SQL 命令 `SHOW LOAD WHERE label="label"` 查看作业结果。 |
| column_name | 导入的目标列，必须是目标表中存在的列。该参数与导入数据的列的名称可以不同，但顺序需一一对应。如果不指定目标列，默认为目标表中的所有列。如果源表中的某个列在目标列不存在，则写入默认值。如果当前列没有默认值，导入作业会失败。如果查询语句的结果列类型与目标列的类型不一致，会进行隐式转化，如果不能进行转化，那么 INSERT INTO 语句会报语法解析错误。 |
| query       | 查询语句，查询的结果会导入至目标表中。查询语句支持任意 StarRocks 支持的 SQL 查询语法。 |

## 通过 INSERT OVERWRITE VALUES 语句覆盖写入数据

您可以通过 INSERT OVERWRITE VALUES 语句向指定的表中覆盖写入数据。此导入方式中，多条数据用逗号（,）分隔。详细使用方式，参考 [INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md)。详细参数信息，参考 [INSERT 参数说明](../sql-reference/sql-statements/data-manipulation/INSERT.md#参数说明)。

> **注意**
>
> INSERT OVERWRITE VALUES 语句导入方式仅适用于导入少量数据作为验证 DEMO 用途，不适用于大规模测试或生产环境。如需大规模导入数据，请选择其他导入方式。

查询源表以及目标表确认其中已有数据。

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

以下示例以 `insert_load_wikipedia_ow` 为 Label 向源表 `source_wiki_edit` 中覆盖写入两条数据。

```SQL
INSERT OVERWRITE source_wiki_edit
WITH LABEL insert_load_wikipedia_ow
VALUES
    ("2015-09-12 00:00:00","#cn.wikipedia","GELongstreet",0,0,0,0,0,36,36,0),
    ("2015-09-12 00:00:00","#fr.wikipedia","PereBot",0,1,0,1,0,17,17,0);
```

## 通过 INSERT OVERWRITE SELECT 语句覆盖写入数据

您可以通过 INSERT OVERWRITE SELECT 语句将源表中的数据覆盖写入至目标表中。INSERT OVERWRITE SELECT 将源表中的数据进行 ETL 转换之后，覆盖写入到 StarRocks 内表中。源表可以是一张或多张内部表或者外部表。目标表必须是 StarRocks 的内表。执行该语句之后，系统使用 SELECT 语句结果覆盖目标表的数据。详细使用方式，参考 [INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md)。详细参数信息，参考 [INSERT 参数](../sql-reference/sql-statements/data-manipulation/INSERT.md#参数说明)。

> 说明
>
> 以下示例仅展示导入内部表数据，其操作过程与导入外部表数据相同，故不重复演示导入外部表数据过程。

- 以下示例以 `insert_load_wikipedia_ow_1` 为 Label 将源表中的数据覆盖写入至目标表中。

```SQL
INSERT OVERWRITE insert_wiki_edit
WITH LABEL insert_load_wikipedia_ow_1
SELECT * FROM source_wiki_edit;
```

- 以下示例以 `insert_load_wikipedia_2` 为 Label 将源表中的数据覆盖写入至目标表的 `p06` 和 `p12` 分区中。如果不指定目标分区，数据将会覆盖写入入全表；如果指定目标分区，数据只会覆盖写入指定的分区。

```SQL
INSERT OVERWRITE insert_wiki_edit PARTITION(p06, p12)
WITH LABEL insert_load_wikipedia_ow_2
SELECT * FROM source_wiki_edit;
```

如果清空 `p06` 和 `p12` 分区，则查询不到先前覆盖写入至对应分区的数据。

```Plain
MySQL > select * from insert_wiki_edit;
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user         | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #fr.wikipedia | PereBot      |            0 |        1 |      0 |        1 |              0 |    17 |    17 |       0 |
| 2015-09-12 00:00:00 | #cn.wikipedia | GELongstreet |            0 |        0 |      0 |        0 |              0 |    36 |    36 |       0 |
+---------------------+---------------+--------------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.01 sec)

MySQL > TRUNCATE TABLE insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL > select * from insert_wiki_edit;
Empty set (0.00 sec)
```

- 以下示例以 `insert_load_wikipedia_ow_3` 为 Label 将源表中 `event_time` 和 `channel` 列的数据覆盖写入至目标表的对应列中。未被导入的列将被赋予默认值。

```SQL
INSERT OVERWRITE insert_wiki_edit
WITH LABEL insert_load_wikipedia_ow_3 
(
    event_time, 
    channel
)
SELECT event_time, channel FROM source_wiki_edit;
```

## 查看导入作业状态

### 通过返回结果查看

INSERT 导入作业会根据执行结果的不同，返回以下两种作业状态：

- **执行成功**

如果导入执行成功，StarRocks 的返回如下：

```Plain
Query OK, 2 rows affected, 2 warnings (0.05 sec)
{'label':'insert_load_wikipedia', 'status':'VISIBLE', 'txnId':'1006'}
```

| 返回          | 说明                                                         |
| ------------- | ------------------------------------------------------------ |
| rows affected | 表示总共有多少行数据被导入。`warnings` 表示被过滤的行数。    |
| label         | 用户指定或自动生成的 Label。Label 是该 INSERT 导入作业的标识，当前数据库内唯一。 |
| status        | 表示导入数据是否可见。VISIBLE 表示可见，COMMITTED 表示已提交但暂不可见。 |
| txnId         | 该 INSERT 导入对应的导入事务 ID。                            |

- **执行失败**

如果所有数据都无法被导入，则导入执行失败，StarRocks 将返回相应错误以及 `tracking_url`。您可以通过 `tracking_url` 查看错误相关的日志信息并排查问题。

```Plain
ERROR 1064 (HY000): Insert has filtered data in strict mode, tracking_url=http://x.x.x.x:yyyy/api/_load_error_log?file=error_log_9f0a4fd0b64e11ec_906bbede076e9d08
```

### 通过 SHOW LOAD 语句查看

您可以通过 [SHOW LOAD](../sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) 语句查看 INSERT 导入作业状态。

以下示例通过 SHOW LOAD 语句查看 Label 为 `insert_load_wikipedia` 的导入作业状态。

```SQL
SHOW LOAD WHERE label="insert_load_wikipedia"\G
```

返回如下：

```Plain
*************************** 1. row ***************************
         JobId: 10278
         Label: insert_load_wikipedia
         State: FINISHED
      Progress: ETL:100%; LOAD:100%
          Type: INSERT
      Priority: NORMAL
      ScanRows: 0
  FilteredRows: 0
UnselectedRows: 0
      SinkRows: 2
       EtlInfo: NULL
      TaskInfo: resource:N/A; timeout(s):300; max_filter_ratio:0.0
      ErrorMsg: NULL
    CreateTime: 2023-06-12 18:31:07
  EtlStartTime: 2023-06-12 18:31:07
 EtlFinishTime: 2023-06-12 18:31:07
 LoadStartTime: 2023-06-12 18:31:07
LoadFinishTime: 2023-06-12 18:31:08
   TrackingSQL: 
    JobDetails: {"All backends":{"3d96e21a-090c-11ee-9083-00163e0e2cf9":[10142]},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":175,"InternalTableLoadRows":2,"ScanBytes":0,"ScanRows":0,"TaskNumber":1,"Unfinished backends":{"3d96e21a-090c-11ee-9083-00163e0e2cf9":[]}}
1 row in set (0.00 sec)
```

### 通过 curl 命令查看

您可以通过 curl 命令查看 INSERT 导入作业状态。

启动终端，并运行以下命令：

```Plain
curl --location-trusted -u <username>:<password> \
  http://<fe_address>:<fe_http_port>/api/<db_name>/_load_info?label=<label_name>
```

以下示例通过 curl 命令查看 Label 为 `insert_load_wikipedia` 的导入作业状态。

```Plain
curl --location-trusted -u <username>:<password> \
  http://x.x.x.x:8030/api/load_test/_load_info?label=insert_load_wikipedia
```

> **说明**
>
> 如果账号没有设置密码，这里只需要传入 `<username>:`。

返回如下：

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

## 相关配置项

你可以为 INSERT 导入作业设定以下配置项：

- **FE 配置项**

| FE 配置项                          | 说明                                                         |
| ---------------------------------- | ------------------------------------------------------------ |
| insert_load_default_timeout_second | INSERT 导入作业的超时时间，单位为秒。如果当前 INSERT 导入作业在该参数设定的时间内未完成则会被系统取消，状态为 CANCELLED。目前仅支持通过该参数为所有 INSERT 导入作业统一设定超时时间，不支持为单独的导入作业设置超时时间。默认为 3600 秒（1 小时）。如果导入作业无法在规定时间内完成，您可以通过调整该参数延长超时时间。 |

- **Session 变量**

| Session 变量         | 说明                                                         |
| -------------------- | ------------------------------------------------------------ |
| enable_insert_strict | INSERT 导入是否容忍错误数据行。设置为 `true` 时，如果有一条数据错误，则返回导入失败。设置为 `false` 时，如果至少有一条数据被正确导入，则返回导入成功，并会返回一个 Label。该参数默认为 `true`。您可以通过 `SET enable_insert_strict = {true or false};` 命令来设定该参数。 |
| query_timeout        | SQL 命令的超时时间，单位为秒。INSERT 语句作为 SQL 命令，同样受到该 Session 变量的限制。您可以通过 `SET query_timeout = xxx;` 命令来设定该参数。 |
