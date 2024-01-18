---
displayed_sidebar: "Chinese"
---

# 导入过程中实现数据转换

StarRocks 支持在导入数据的过程中实现数据转换。

目前支持的导入方式有 [Stream Load](./StreamLoad.md)、[Broker Load](./BrokerLoad.md) 和 [Routine Load](./RoutineLoad.md)。暂不支持 [Spark Load](./SparkLoad.md) 导入方式。

本文以 CSV 格式的数据文件为例介绍如何在导入过程中实现数据转换。具体支持哪些格式的数据文件的转换，跟您选择的导入方式有关。

> **说明**
>
> 对于 CSV 格式的数据，StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (|)。

## 应用场景

在向 StarRocks 表中导入数据时，有时候 StarRocks 表中的内容与源数据文件中的内容不完全一致。在以下几种情况下，您不需要进行任何外部的 ETL 工作，StarRocks 就可以帮助您在导入过程中完成数据的提取和转化：

- 跳过不需要导入的列。
  
  一方面，该功能使您可以跳过不需要导入的列；另一方面，当 StarRocks 表与源数据文件的列顺序不一致时，您可以通过该功能建立两者之间的列映射关系。

- 过滤掉不需要导入的行。
  
  在导入时，您可以通过指定过滤条件，跳过不需要导入的行，只导入必要的行。

- 生成衍生列。
  
  衍生列是指对源数据文件中的列进行计算之后产生的新列。该功能使您可以将计算后产生的新列落入到 StarRocks 表中。

- 从文件路径中获取分区字段的内容。
  
  支持 Apache Hive™ 分区路径命名方式，使 StarRocks 能够从文件路径中获取分区列的内容。

## 前提条件

### Broker Load

参见[从 HDFS 或外部云存储系统导入数据](../loading/BrokerLoad.md)中的“背景信息”小节。

### Routine Load

如果使用 [Routine Load](./RoutineLoad.md) 导入数据，必须确保您的 Apache Kafka® 集群已创建 Topic。本文假设您已部署两个 Topic，分别为 `topic1` 和 `topic2`。

## 数据样例

1. 在本地文件系统中创建数据文件。

   a. 创建一个名为 `file1.csv` 的数据文件，文件一共包含四列，分别代表用户 ID、用户性别、事件日期和事件类型，如下所示：

   ```Plain
   354,female,2020-05-20,1
   465,male,2020-05-21,2
   576,female,2020-05-22,1
   687,male,2020-05-23,2
   ```

   b. 创建一个 `file2.csv` 的数据文件，文件只包含一个时间戳格式的列，代表日期，如下所示：

   ```Plain
   2020-05-20
   2020-05-21
   2020-05-22
   2020-05-23
   ```

2. 在 `test_db` 的数据库中创建 StarRocks 表。

   > **说明**
   >
   > 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

   a. 创建一张名为 `table1` 的表，包含 `user_id`、`event_date` 和 `event_type` 三列，如下所示：

      ```SQL
      CREATE TABLE table1
      (
          `user_id` BIGINT COMMENT "用户 ID",
          `event_date` DATE COMMENT "事件日期",
          `event_type` TINYINT COMMENT "事件类型"
      )
      DISTRIBUTED BY HASH(user_id);
      ```

   b. 创建一张名为 `table2` 的表，包含 `date`、`year`、`month` 和 `day` 四列，如下所示：

      ```SQL
      CREATE TABLE table2
      (
          `date` DATE COMMENT "日期",
          `year` INT COMMENT "年",
          `month` TINYINT COMMENT "月",
          `day` TINYINT COMMENT "日"
      )
      DISTRIBUTED BY HASH(date);
      ```

3. 把 `file1.csv` 和 `file2.csv` 文件上传到 HDFS 集群的 `/user/starrocks/data/input/` 路径下，并把 `file1.csv` 和 `file2.csv` 文件中的数据分别上传到 Apache Kafka® 集群的 `topic1` 和 `topic2` 中。

## 跳过不需要导入的列

源数据文件中可能包含一些 StarRocks 表中不存在的列。StarRocks 支持您只导入 StarRocks 表中存在的列，而忽略掉不需要的列。

该特性支持如下数据源：

- 本地文件系统

- HDFS 和外部云存储系统
  > **说明**
  >
  > 这里以 HDFS 为例进行介绍。

- Kafka

通常情况下，CSV 文件中源数据文件中的列，是没有命名的。有些 CSV 文件中，会在首行给出列名，但其实 StarRocks 仍然是不感知的，会当做普通数据处理。因此，在导入 CSV 格式的数据时，您需要在导入命令或者语句中对源数据文件中的列**按顺序**依次临时命名。这些临时命名的列，会和 StarRocks 表中的列**按名称**进行对应。这里需要注意以下几点：

- 源数据文件中与 StarRocks 表中都存在、且命名相同的列，其数据会直接导入。

- 源数据文件中存在、但是 StarRocks 表中不存在的列，其数据会在导入过程中忽略掉。

- 如果有 StarRocks 表中存在、但是未声明的列，会报错。

本小节以 `file1.csv` 文件和 `table1` 表为例。假设 `file1.csv` 文件中的四列按顺序依次临时命名为 `user_id` 、`user_gender`、`event_date` 和 `event_type`。其中，`file1.csv` 文件中临时命名的 `user_id` 、`event_date` 和 `event_type` 这些列都能在 `table1` 表中找到对应的列，所以这些列对应的数据都会被导入到 `table1` 表中；而 `file1.csv` 文件中临时命名的 `user_gender` 列，在 `table1` 表中并不存在，所以导入时会被直接忽略掉。

### 导入数据

#### 从本地文件系统导入

如果 `file1.csv` 文件存储在本地文件系统，可以通过如下语句，创建 [Stream Load](./StreamLoad.md) 导入作业来实现数据导入：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns: user_id, user_gender, event_date, event_type" \
    -T file1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

> **说明**
>
> `columns` 参数用于对数据文件中的列进行临时命名，从而映射到 StarRocks 表的列。

有关详细的语法和参数介绍，请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。

#### 从 HDFS 导入

如果 `file1.csv` 文件存储在 HDFS 上，可以通过如下语句，创建 [Broker Load](./BrokerLoad.md) 导入作业来实现数据导入：

```SQL
LOAD LABEL test_db.label1
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/file1.csv")
    INTO TABLE `table1`
    FORMAT AS "csv"
    COLUMNS TERMINATED BY ","
    (user_id, user_gender, event_date, event_type)
)
WITH BROKER
```

> **说明**
>
> `column_list` 参数用于对数据文件中的列进行临时命名，从而映射到 StarRocks 表的列。

有关详细的语法和参数介绍，请参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

#### 从 Kafka 导入

如果 `file1.csv` 文件里的数据存储在 Kafka 集群的 `topic1` 中，可以通过如下语句，创建 [Routine Load](./RoutineLoad.md) 导入作业来实现数据导入：

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

> **说明**
>
> `COLUMNS` 参数用于对数据中的列进行临时命名，从而映射到 StarRocks 表的列。

有关详细的语法和参数介绍，请参见 [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md)。

### 查询数据

从本地文件系统、HDFS、或者 Kafka 导入上述数据后，查询 `table1` 表，如下所示：

```SQL
SELECT * FROM table1;
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

## 过滤掉不需要导入的行

源数据文件中可能包含一些 StarRocks 表中不需要的行。StarRocks 支持您通过 WHERE 子句来指定要导入哪些行，不符合条件的数据就会被过滤掉。

该特性支持如下数据源：

- 本地文件系统

- HDFS 和外部云存储系统
  > **说明**
  >
  > 这里以 HDFS 为例进行介绍。

- Kafka

本小节以 `file1.csv` 文件和 `table1` 表为例。如果您只想把 `file1.csv` 文件中代表事件类型的列中取值为 `1` 的行导入到 `table1` 表，可以通过 WHERE 子句指定过滤条件 `event_type = 1`。

### 导入数据

#### 从本地文件系统导入

如果 `file1.csv` 文件存储在本地文件系统，可以通过如下语句，创建 [Stream Load](./StreamLoad.md) 导入作业来实现数据导入：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns: user_id, user_gender, event_date, event_type" \
    -H "where: event_type=1" \
    -T file1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

有关详细的语法和参数介绍，请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。

#### 从 HDFS 导入

如果 `file1.csv` 文件存储在 HDFS 上，可以通过如下语句，创建 [Broker Load](./BrokerLoad.md) 导入作业来实现数据导入：

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
WITH BROKER
```

有关详细的语法和参数介绍，请参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

#### 从 Kafka 导入

如果 `file1.csv` 文件的数据存储在 Kafka 集群的 `topic1` 中，可以通过如下语句，创建 [Routine Load](./RoutineLoad.md) 导入作业来实现导入：

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

有关详细的语法和参数介绍，请参见 [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md)。

### 查询数据

从本地文件系统、HDFS、或者 Kafka 导入上述数据后，查询 `table1` 表，如下所示：

```SQL
SELECT * FROM table1;
+------------+------------+---------+
| event_date | event_type | user_id |
+------------+------------+---------+
| 2020-05-20 |          1 |     354 |
| 2020-05-22 |          1 |     576 |
+------------+------------+---------+
2 rows in set (0.01 sec)
```

## 生成衍生列

源数据文件中的数据可能需要进行一些转化工作后，才能导入到 StarRocks 表中。StarRocks 支持您在导入命令或者语句中通过函数实现数据转化。

该特性支持如下数据源：

- 本地文件系统

- HDFS 和外部云存储系统
  > **说明**
  >
  > 这里以 HDFS 为例进行介绍。

- Kafka

本小节以 `file2.csv` 文件和 `table2` 表为例。`file2.csv` 文件中只包含一列代表日期的时间戳格式的数据，您可以通过 [year](../sql-reference/sql-functions/date-time-functions/year.md)、[month](../sql-reference/sql-functions/date-time-functions/month.md)、[day](../sql-reference/sql-functions/date-time-functions/day.md) 函数提取 `file2.csv` 文件中的数据，分别落入到 `table2` 表的 `year`、`month`、`day` 列。

### 导入数据

#### 从本地文件系统导入

如果 `file2.csv` 文件存储在本地文件系统，可以通过如下语句，创建 [Stream Load](./StreamLoad.md) 导入作业来实现数据导入：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns:date,year=year(date),month=month(date),day=day(date)" \
    -T file2.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
```

> **说明**
>
> - 必须通过 `columns` 参数先声明源数据文件中包含的**所有列**，然后再声明衍生列。如上述示例中，`columns` 参数中先声明 `file2.csv` 文件中包含的仅有的一列临时命名为 `date`，然后再声明需要调用函数经过转化才能生成的衍生列：`year=year(date)`、`month=month(date)` 和 `day=day(date)`。
>
> - 不支持 `column_name = function(column_name)` 的形式，需要时可以重命名衍生列之前的列，比如为 `column_name = func(``tempcolumn_name``)`。

有关详细的语法和参数介绍，请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。

#### 从 HDFS 导入

如果 `file2.csv` 文存储在 HDFS 上，可以通过如下语句，创建 [Broker Load](./BrokerLoad.md) 导入作业来实现数据导入：

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
WITH BROKER
```

> **说明**
>
> 必须先通过 `column_list` 参数声明源数据文件中包含的所有列，然后再通过 SET 子句声明衍生列。如上述示例中，先通过 `column_list` 参数声明 `file2.csv` 文件中包含的仅有的一列临时命名为 `date`，然后再通过 SET 子句声明需要调用函数经过转化才能生成的衍生列：`year=year(date)`、`month=month(date)` 和 `day=day(date)`。

有关详细的语法和参数介绍，请参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

#### 从 Kafka 导入

如果 `file2.csv` 文件的数据存储在 Kafka 集群的 `topic2` 中，可以通过如下语句，创建 [Routine Load](./RoutineLoad.md) 导入作业来实现导入：

```SQL
CREATE ROUTINE LOAD test_db.table2 ON table2
    COLUMNS TERMINATED BY ",",
    COLUMNS(date,year=year(date),month=month(date),day=day(date))
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker_host>:<kafka_broker_port>",
    "kafka_topic" = "topic2",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

> **说明**
>
> 必须通过 `COLUMNS` 参数先声明源数据文件中包含的所有列，然后再声明衍生列。如上述示例中，`COLUMNS` 参数中先声明 `file2.csv` 文件中包含的仅有的一列临时命名为 `date`，然后再声明需要调用函数经过转化才能生成的衍生列：`year=year(date)`、`month=month(date)` 和 `day=day(date)`。

有关详细的语法和参数介绍，请参见 [CREATE ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md)。

### 查询数据

从本地文件系统、HDFS、或者 Kafka 导入上述数据后，查询 `table2` 表，如下所示：

```SQL
SELECT * FROM table2;
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

## 从文件路径中获取分区字段的内容

当指定的文件路径中存在分区字段时，StarRocks 支持您使用 `COLUMNS FROM PATH AS` 参数指定要提取文件路径中哪些分区字段的信息，相当于源数据文件中的列。该参数只有在从 HDFS 导入数据时可用。

例如，要导入 Hive 生成的四个数据文件，这些文件存储在 HDFS 上的 `/user/starrocks/data/input/` 路径下，每个数据文件都按照 `date` 分区字段进行分区，并且每个数据文件都只包含两列，分别代表事件类型和用户 ID，如下所示：

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

### 导入数据

可以通过如下语句，创建 [Broker Load](./BrokerLoad.md) 导入作业，获取文件路径 `/user/starrocks/data/input/` 中分区字段 `date` 的信息，并通过使用通配符 (*) 指定将该文件路径下所有数据文件都导入到 `table1` 表中：

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
WITH BROKER
```

> **说明**
>
> 上述示例中，指定的文件路径中的分区字段 `date` 对应 `table1` 表中的 `event_date` 列，因此需要通过 SET 子句完成 `date` 到 `event_date` 的映射。如果指定的文件路径中的分区字段与其对应的 StarRocks 表中的列名称一样，则不需要通过 SET 子句来指定映射关系。

有关详细的语法和参数介绍，请参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

### 查询数据

从 HDFS 导入上述数据后，查询 `table1` 表，如下所示：

```SQL
SELECT * FROM table1;
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
