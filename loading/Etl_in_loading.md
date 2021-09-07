# ETL when loading

用户在向StarRocks表中导入数据时，有时候目标表中的内容与数据源中的内容**不完全一**样。比如：

* 场景一：数据源中包含一些目标表中不需要的内容，可能是存在**多余的行**，也可能是**多余的列**。
* 场景二：数据源中的内容并不能够直接导入到StarRocks中，可能**需要进行部分转化**工作后，才能够导入到StarRocks中。比如，原始文件中的数据是时间戳格式，目标表中的数据类型是Datetime，需要在数据导入时完成类型转化。

StarRocks能够在数据导入时完成数据转化的操作。这样在数据源与目标表中内容不一致的情况下，用户不需要外部的ETL工作，可以直接通过StarRocks提供的能力在导入时就完成数据转化。

通过StarRocks提供的能力，用户可以在数据导入时实现以下目标：

1. 选择需要导入的列。一方面通过此功能可以跳过不需要导入的列；另一方面当表中列的顺序与文件中字段顺序不一致时，可以通过此功能建立两者的字段映射，从而导入文件。
2. 过滤不需要的行。在导入时可以通过指定表达式，从而跳过不需要导入的行，只导入必要的行内容。
3. 导入时生成衍生列（即通过计算处理产生新的列）导入到StarRocks目标表中。
4. 支持Hive分区路径命名方式，StarRocks能够从文件路径中获取分区列的内容。

---

## 选择需要导入的列

### 样例数据

假如需要向下面的表中导入一份数据：

~~~sql
CREATE TABLE event (
    `event_date` DATE,
    `event_type` TINYINT,
    `user_id` BIGINT
)
DISTRIBUTED BY HASH(user_id) BUCKETS 3;
~~~

但是数据文件中却包含了四个字段“user_id, user_gender, event_date, event_type”，样例数据如下所示：

~~~text
354,female,2020-05-20,1
465,male,2020-05-21,2
576,female,2020-05-22,1
687,male,2020-05-23,2
~~~

### 本地文件导入

通过下面的命令能够将本地数据导入到对应表中：

~~~bash
curl --location-trusted -u root -H "column_separator:," \
    -H "columns: user_id, user_gender, event_date, event_type" -T load-columns.txt \
    http://{FE_HOST}:{FE_HTTP_PORT}/api/test/event/_stream_load
~~~

CSV 格式的文件中的列，本来是没有命名的，通过 **columns**，可以按顺序对其命名（一些 CSV 中，会在首行给出列名，但其实系统是不感知的，会当做普通数据处理）。在这个 case 中，通过 **columns** 字段，描述了文件中**按顺序**的字段名字分别是 user_id, user_gender, event_date, event_type。然后，columns 的字段，会和系统中导入表的字段做**列名对应**，并将数据导入到表中：

* 与导入表中字段相同的名字，就会直接导入
* 导入表中不存在的字段，会在导入过程中忽略掉
* 导入表中存在，但 columns 中未指定的字段，会报错

针对这个例子，字段"user_id, event_date, event_type"都能够在表中找到对应的字段，所以对应的内容都会被导入到 StarRocks 表中。而"user_gender"这个字段在表中并不存在，所以导入时会直接忽略掉这个字段。

### HDFS导入

通过下面的命令能够将HDFS的数据导入到对应的表中：

~~~sql
LOAD LABEL test.label_load (
    DATA INFILE("hdfs://{HDFS_HOST}:{HDFS_PORT}/tmp/zc/starrocks/data/date=*/*")
    INTO TABLE `event`
    COLUMNS TERMINATED BY ","
    FORMAT AS "csv"
    (user_id, user_gender, event_date, event_type)
)
WITH BROKER hdfs;
~~~

通过"(user_id, user_gender, event_date, event_type)"部分指定文件中的字段名字。StarRocks导入过程中的行为与本地文件导入行为一致。需要的字段会被导入到StarRocks中，不需要的字段会被忽略掉。

### Kafka导入

通过下面的命令能够将Kafka中的数据导入到对应表中：

~~~sql
CREATE ROUTINE LOAD test.event_load ON event
    COLUMNS TERMINATED BY ",",
    COLUMNS(user_id, user_gender, event_date, event_type),
WHERE event_type = 1
FROM KAFKA (
    "kafka_broker_list" = "{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}",
    "kafka_topic" = "event"
);
~~~

通过"COLUMNS(user_id, user_gender, event_date, event_type)"字段指示Kafka流message所包含的字段名字。StarRocks导入过程中的行为与本地文件一致。需要的字段会被导入到StarRocks中，不需要的字段会被忽略掉。

### 查询内容

~~~SQL
> select * from event;
+------------+------------+---------+
| event_date | event_type | user_id |
+------------+------------+---------+
| 2020-05-22 |          1 |     576 |
| 2020-05-20 |          1 |     354 |
| 2020-05-21 |          2 |     465 |
| 2020-05-23 |          2 |     687 |
+------------+------------+---------+
~~~

---

## 跳过不需要导入的行

### 样例数据

假如需要向下面的表中导入一份数据：

~~~sql
CREATE TABLE event (
    `event_date` DATE,
    `event_type` TINYINT,
    `user_id` BIGINT
)
DISTRIBUTED BY HASH(user_id) BUCKETS 3;
~~~

假设数据文件中包含三列，样例数据如下所示：

~~~text
2020-05-20,1,354
2020-05-21,2,465
2020-05-22,1,576
2020-05-23,2,687
~~~

现在由于业务需要，目的表中只需要分析 ***event_type*** 为 1 的数据。

### 本地文件导入

在导入本地文件的时候，可以通过下面的命令实现导入event_type=1的数据。具体是通过指定HTTP请求中的Header "where:event_type=1"来过滤数据：

~~~bash
curl --location-trusted -u root -H "column_separator:," \
    -H "where:event_type=1" -T load-rows.txt \
    http://{FE_HOST}:{FE_HTTP_PORT}/test/event/_stream_load
~~~

### HDFS导入

通过下面的命令，能够实现只将HDFS文件中event_type为1的数据导入到StarRocks中。具体方法是通过"WHERE event_type = 1"选项来过滤要导入的数据：

~~~sql
LOAD LABEL test.label_load (
    DATA INFILE("hdfs://{HDFS_HOST}:{HDFS_PORT}/tmp/zc/starrocks/data/date=*/*")
    INTO TABLE `event`
    COLUMNS TERMINATED BY ","
    FORMAT AS "csv"
    WHERE event_type = 1
)
WITH BROKER hdfs;
~~~

### Kafka导入

通过下面的命令，能够将Kafka中event_type为1的数据导入到StarRocks的表中。具体方法是通过指定"WHERE event_type = 1"来过滤要导入的数据：

~~~sql
CREATE ROUTINE LOAD test.event_load ON event
COLUMNS TERMINATED BY ",",
WHERE event_type = 1
FROM KAFKA (
    "kafka_broker_list" = "{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}",
    "kafka_topic" = "event"
);
~~~

### 查询内容

~~~SQL
> select * from event;
+------------+------------+---------+
| event_date | event_type | user_id |
+------------+------------+---------+
| 2020-05-20 |          1 |     354 |
| 2020-05-22 |          1 |     576 |
+------------+------------+---------+
~~~

---

## 生成衍生列

假如需要向下面的表中导入一份数据：

~~~sql
CREATE TABLE dim_date (
    `date` DATE,
    `year` INT,
    `month` TINYINT,
    `day` TINYINT
)
DISTRIBUTED BY HASH(date) BUCKETS 1;
~~~

但是原始数据文件中只有一个列的内容，具体的数据内容如下：

~~~text
2020-05-20
2020-05-21
2020-05-22
2020-05-23
~~~

在导入时，通过下面的命令实现数据转化。

### 本地文件导入3

通过下面的命令，能够在导入本地文件的同时，生成对应的衍生列。方法是指定HTTP请求中的`Header "columns:date, year=year(date), month=month(date), day=day(date)"`，让StarRocks在导入过程中根据文件内容计算生成对应的列。

~~~bash
curl --location-trusted -u root -H "column_separator:," \
    -H "columns:date,year=year(date),month=month(date),day=day(date)" -T load-date.txt \
    http://127.0.0.1:8431/api/test/dim_date/_stream_load
~~~

这里需要注意：

* 需要在衍生列之前，先列出 CSV 格式文件中的所有列
* 然后再列出各种衍生列
* 不能有 `col_name = func(col_name)` 的形式，需要时重命名衍生列之前的列名，比如为 `col_name = func(col_name0)`

### HDFS导入

与前述本地文件导入方式类似，通过下面的命令能够实现HDFS文件导入：

~~~sql
LOAD LABEL test.label_load (
    DATA INFILE("hdfs://{HDFS_HOST}:{HDFS_PORT}/tmp/zc/starrocks/data/date=*/*")
    INTO TABLE `event`
    COLUMNS TERMINATED BY ","
    FORMAT AS "csv"
    (date)
    SET(year=year(date), month=month(date), day=day(date))
)
WITH BROKER hdfs;
~~~

### Kafka导入

类似的，通过下面的命令能够实现从Kafka导入相应数据：

~~~sql
CREATE ROUTINE LOAD test.event_load ON event
    COLUMNS TERMINATED BY ",",
    COLUMNS(date,year=year(date),month=month(date),day=day(date))
FROM KAFKA (
    "kafka_broker_list" = "{KAFKA_BROKER_HOST}:{KAFKA_BROKER_PORT}",
    "kafka_topic" = "event"
);
~~~

### 查询内容

~~~SQL
> SELECT * FROM dim_date;
+------------+------+-------+------+
| date       | year | month | day  |
+------------+------+-------+------+
| 2020-05-20 | 2020 |  5    | 20   |
| 2020-05-21 | 2020 |  5    | 21   |
| 2020-05-22 | 2020 |  5    | 22   |
| 2020-05-23 | 2020 |  5    | 23   |
+------------+------+-------+------+
~~~

---

## 从文件路径中获取字段内容

### 样例数据

假设我们要向下面的表中导入数据：

~~~sql
CREATE TABLE event (
    `event_date` DATE,
    `event_type` TINYINT,
    `user_id` BIGINT
)
DISTRIBUTED BY HASH(user_id) BUCKETS 3;
~~~

要导入的数据是Hive生成的数据，数据按照event_date进行分区，每个文件中只包含"event_type", "user_id"两列。具体的数据内容如下所示：

~~~text
/tmp/starrocks/data/date=2020-05-20/data
1,354
/tmp/starrocks/data/date=2020-05-21/data
2,465
/tmp/starrocks/data/date=2020-05-22/data
1,576
/tmp/starrocks/data/date=2020-05-23/data
2,687
~~~

通过下面的命令可以将数据导入到表"event"中，并且从文件路径中获取"**event_date**"的信息。

### HDFS导入

~~~SQL
LOAD LABEL test.label_load (
    DATA INFILE("hdfs://{HDFS_HOST}:{HDFS_PORT}/tmp/starrocks/data/date=*/*")
    INTO TABLE `event`
    COLUMNS TERMINATED BY ","
    FORMAT AS "csv"
    (event_type, user_id)
    COLUMNS FROM PATH AS (date)
    SET(event_date = date)
)
WITH BROKER hdfs;
~~~

上述的命令是将匹配路径通配符所有的文件导入到表"event"中。其中文件都为CSV格式，各个列的内容通过“,”进行分割。文件中包含“event_type”，“user_id”两个列。并且能够**通过文件路径中获取 “date” 列的信息**，因为date列在表中对应的名字是"**event_date**"，所以通过SET语句完成映射。

### 查询内容

~~~SQL
> select * from event;
+------------+------------+---------+
| event_date | event_type | user_id |
+------------+------------+---------+
| 2020-05-22 |          1 |     576 |
| 2020-05-20 |          1 |     354 |
| 2020-05-21 |          2 |     465 |
| 2020-05-23 |          2 |     687 |
+------------+------------+---------+
~~~
