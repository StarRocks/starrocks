# 通过 INSERT INTO 语句导入数据

本文介绍如何使用 INSERT INTO 语句向 StarRocks 导入数据。

与 MySQL 等数据库系统类似，StarRocks 支持通过 INSERT INTO 语句导入数据。您可以使用 INSERT INTO VALUES 语句直接向表中插入数据，您还可以通过 INSERT INTO SELECT 语句将其他 StarRocks 表中的数据导入到新的 StarRocks 表中，或者将其他数据源的数据通过[外部表功能](../data_source/External_table.md)导入至 StarRocks 内部表中。

> 注意
>
> - 当前版本中，INSERT INTO 语句导入方式仅支持在 MySQL 客户端通过 **ctrl** + **c** 按键强制取消。
> - 当前版本中，StarRocks 在执行 INSERT INTO 语句时，如果有数据不符合目标表格式的数据（例如字符串超长等情况），INSERT 操作默认执行失败。您可以通过设置会话变量 `enable_insert_strict` 为 `false` 以确保 INSERT 操作仅过滤不符合目标表格式的数据，并继续执行。
> - 频繁使用 INSERT INTO 语句导入小批量数据会产生过多的数据版本，从而影响查询性能，因此不建议您频繁使用 INSERT INTO 语句导入数据或将其作为生产环境的日常例行导入任务。如果您的业务场景需要流式导入或者小批量多次导入数据，建议使用 Apache Kafka® 作为数据源并通过 [Routine Load](../loading/RoutineLoad.md) 方式进行导入作业。

## 准备工作

在 StarRocks 中创建数据库 `load_test`，并在其中创建聚合模型表 `insert_wiki_edit` 以及数据源表 `source_wiki_edit`。

> 说明
>
> 本文中演示的操作示例均基于表 `insert_wiki_edit` 和数据源表 `source_wiki_edit`。如果您选择使用自己的表以及数据，请跳过当前步骤，并根据使用场景修改需要导入的数据。

```sql
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
    delta INT SUM DEFAULT '0',
    added INT SUM DEFAULT '0',
    deleted INT SUM DEFAULT '0'
)
AGGREGATE KEY
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
    delta INT SUM DEFAULT '0',
    added INT SUM DEFAULT '0',
    deleted INT SUM DEFAULT '0'
)
AGGREGATE KEY
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

您可以通过 INSERT INTO VALUES 语句向指定的表中直接导入数据。此导入方式中，多条数据用逗号（,）分隔。详细使用方式，参考 [INSERT](../sql-reference/sql-statements/data-manipulation/insert.md)。详细参数信息，参考 [INSERT 参数说明](../sql-reference/sql-statements/data-manipulation/insert.md#参数说明)。

> 注意
>
> INSERT INTO VALUES 语句导入方式仅适用于导入少量数据作为验证 DEMO 用途，不适用于大规模测试或生产环境。如需大规模导入数据，请选择其他导入方式。

以下示例以 `insert_load_wikipedia` 为 Label 向源表 `source_wiki_edit` 中导入两条数据。

```sql
INSERT INTO source_wiki_edit
WITH LABEL insert_load_wikipedia
VALUES
    ("2015-09-12 00:00:00","#en.wikipedia","AustinFF",0,0,0,0,0,21,5,0),
    ("2015-09-12 00:00:00","#ca.wikipedia","helloSR",0,1,0,1,0,3,23,0);
```

## 通过 INSERT INTO SELECT 语句导入数据

您可以通过 INSERT INTO SELECT 语句将源表中的数据导入至目标表中。详细使用方式，参考 [INSERT](../sql-reference/sql-statements/data-manipulation/insert.md)。详细参数信息，参考 [INSERT 参数](../sql-reference/sql-statements/data-manipulation/insert.md#参数说明)。

- 以下示例以 `insert_load_wikipedia_1` 为 Label 将源表中的数据导入至目标表中。

```sql
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_1
SELECT * FROM source_wiki_edit;
```

- 以下示例以 `insert_load_wikipedia_2` 为 Label 将源表中的数据导入至目标表的 `p06` 和 `p12` 分区中。

```sql
INSERT INTO insert_wiki_edit PARTITION(p06, p12)
WITH LABEL insert_load_wikipedia_2
SELECT * FROM source_wiki_edit;
```

- 以下示例将源表中指定列的数据导入至目标表中。

```sql
INSERT INTO insert_wiki_edit
    WITH LABEL insert_load_wikipedia_3 (event_time, channel)
    SELECT event_time, channel FROM routine_wiki_edit;
```

如果清空 `p06` 和 `p12` 分区，则查询不到先前插入至对应分区的数据。

```plain text
MySQL [load_test]> select * from insert_wiki_edit;
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| event_time          | channel       | user     | is_anonymous | is_minor | is_new | is_robot | is_unpatrolled | delta | added | deleted |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
| 2015-09-12 00:00:00 | #en.wikipedia | AustinFF |            0 |        0 |      0 |        0 |              0 |    21 |     5 |       0 |
| 2015-09-12 00:00:00 | #ca.wikipedia | helloSR  |            0 |        1 |      0 |        1 |              0 |     3 |    23 |       0 |
+---------------------+---------------+----------+--------------+----------+--------+----------+----------------+-------+-------+---------+
2 rows in set (0.00 sec)

MySQL [load_test]> truncate table insert_wiki_edit PARTITION(p06, p12);
Query OK, 0 rows affected (0.01 sec)

MySQL [load_test]> select * from insert_wiki_edit;
Empty set (0.00 sec)
```

- 以下示例以 `insert_load_wikipedia_3` 为 Label 将源表中 `event_time` 和 `channel` 列的数据导入至目标表的对应列中。

```sql
INSERT INTO insert_wiki_edit
WITH LABEL insert_load_wikipedia_3 
(
    event_time, 
    channel
)
SELECT event_time, channel FROM source_wiki_edit;
```

## 查看导入任务状态

### 通过结果返回查看

INSERT INTO 导入任务会根据执行结果的不同，返回以下两种任务状态：

- **执行成功**

如果导入执行成功，StarRocks 的返回如下：

```plain text
Query OK, 2 rows affected, 2 warnings (0.05 sec)
{'label':'insert_load_wikipedia', 'status':'VISIBLE', 'txnId':'1006'}
```

|返回|说明|
|---|----|
|rows affected|表示总共有多少行数据被导入。`warnings` 表示被过滤的行数。|
|label|用户指定或自动生成的 Label。Label 是该 INSERT INTO 导入作业的标识，当前数据库内唯一。|
|status|表示导入数据是否可见。VISIBLE 表示可见，COMMITTED 表示已提交但暂不可见。|
|txnId|该 INSERT INTO 导入对应的导入事务 ID。|

- **执行失败**

如果所有数据都无法被导入，则导入执行失败，StarRocks 将返回相应错误以及 `tracking_url`。您可以通过 `tracking_url` 查看错误相关的日志信息并排查问题。

```plain text
ERROR 1064 (HY000): Insert has filtered data in strict mode, tracking_url=http://x.x.x.x:yyyy/api/_load_error_log?file=error_log_9f0a4fd0b64e11ec_906bbede076e9d08
```

### 通过 SHOW LOAD 语句查看

您可以通过 SHOW LOAD 语句查看 INSERT INTO 导入作业状态。

以下示例通过 SHOW LOAD 语句查看 Label 为 `insert_load_wikipedia` 的导入作业状态。

```sql
SHOW LOAD WHERE label="insert_load_wikipedia"\G
```

返回如下：

```plain text
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

### 通过 curl 命令查看

您可以通过 curl 命令查看 INSERT INTO 导入作业状态。

```shell
curl --location-trusted -u {user}:{passwd} \
  http://{hostname}:{port}/api/{database}/_load_info?label={labelname}
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

```plain text
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

## 相关配置项

你可以为 INSERT INTO 导入作业设定以下配置项：

- **FE 配置项**

|FE 配置项|说明|
|-----------|----|
|insert_load_default_timeout_second|INSERT INTO 导入任务的超时时间，单位为秒。如果当前 INSERT INTO 导入任务在该参数设定的时间内未完成则会被系统取消，状态为 CANCELLED。目前仅支持通过该参数为所有 INSERT INTO 导入作业统一设定超时时间，不支持为单独的导入作业设置超时时间。默认为 3600 秒（1 小时）。如果导入任务无法在规定时间内完成，您可以通过调整该参数延长超时时间。|

- **Session 变量**

| Session 变量         | 说明                                                         |
| -------------------- | ------------------------------------------------------------ |
| enable_insert_strict | INSERT 导入是否容忍错误数据行。设置为 `true` 时，如果有一条数据错误，则返回导入失败。设置为 `false` 时，如果至少有一条数据被正确导入，则返回导入成功，并会返回一个 Label。该参数默认为 `true`。您可以通过 `SET enable_insert_strict = true;` 命令来设定该参数。 |
| query_timeout        | SQL 命令的超时时间，单位为秒。INSERT 语句作为 SQL 命令，同样受到该 Session 变量的限制。您可以通过 `SET query_timeout = xxx;` 命令来设定该参数。 |
