# BROKER LOAD

## description

Broker Load 通过随 StarRocks 集群一同部署的 broker 进行，访问对应数据源的数据，进行数据导入。

可以通过 show broker 命令查看已经部署的 broker。

目前支持以下5种数据源：

1. Apache HDFS：社区版本 hdfs。
2. Amazon S3：Amazon对象存储。
3. 阿里云 OSS：阿里云对象存储。
4. 腾讯COS：腾讯云对象存储。
5. 百度BOS：百度对象存储。
语法：

```sql
LOAD LABEL load_label
(
data_desc1[, data_desc2, ...]
)
WITH BROKER broker_name
[broker_properties]
[opt_properties];
```

1. load_label

    当前导入批次的标签。在一个 database 内唯一。
    语法：

<<<<<<< HEAD
    ```sql
    [database_name.]your_label
=======
`label_name` 指定导入作业的标签。

`database_name` 为可选，指定目标 StarRocks 表所在的数据库。

每个导入作业都对应一个在该数据库内唯一的标签。通过标签，可以查看对应导入作业的执行情况，并防止导入相同的数据。导入作业的状态为 **FINISHED** 时，其标签不可再复用给其他导入作业。导入作业的状态为 **CANCELLED** 时，其标签可以复用给其他导入作业，但通常都是用来重试同一个导入作业（即使用同一个标签导入相同的数据）以实现数据“精确一次 (Exactly-Once)”语义。

有关标签的命名规范，请参见[系统限制](../../../reference/System_limit.md)。

### `data_desc`

用于描述一批次待导入的数据。每个 `data_desc` 声明了本批次待导入数据所属的数据源地址、ETL 函数、StarRocks 表和分区等信息。

Broker Load 支持一次导入多个数据文件。在一个导入作业中，您可以使用多个 `data_desc` 来声明导入多个数据文件，也可以使用一个 `data_desc` 来声明导入一个路径下的所有数据文件。Broker Load 还支持保证单次导入事务的原子性，即单次导入的多个数据文件都成功或者都失败，而不会出现部分导入成功、部分导入失败的情况。

`data_desc` 语法如下：

```SQL
DATA INFILE ("<file_path>"[, "<file_path>" ...])
[NEGATIVE]
INTO TABLE <table_name>
[PARTITION (<partition_name>[, <partition_name> ...])]
[FORMAT AS "CSV | Parquet | ORC"]
[COLUMNS TERMINATED BY "<column_separator>"]
[(column_list)]
[COLUMNS FROM PATH AS (<partition_field_name>[, <partition_field_name> ...])]
[SET <k1=f1(v1)>[, <k2=f2(v2)> ...]]
[WHERE predicate]
```

`data_desc` 中的必选参数如下：

- `file_path`

  用于指定待导入数据文件所在的路径。

  您可以指定导入一个具体的数据文件。例如，通过指定 `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/20210411"` 可以匹配 HDFS 服务器上 `/user/data/tablename` 目录下名为 `20210411` 的数据文件。

  您也可以用通配符指定导入某个路径下所有的数据文件。Broker Load 支持如下通配符：`?`、`*`、`[]`、`{}` 和 `^`。具体请参见[通配符使用规则参考](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html#globStatus-org.apache.hadoop.fs.Path-)。例如， 通过指定 `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/*/*"` 路径可以匹配 HDFS 服务器上 `/user/data/tablename` 目录下所有分区内的数据文件，通过 `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/dt=202104*/*"` 路径可以匹配 HDFS 服务器上 `/user/data/tablename` 目录下所有 `202104` 分区内的数据文件。

  > **说明**
  >
  > 中间的目录也可以使用通配符匹配。

  以 HDFS 数据源为例，文件路径中的 `hdfs_host` 和 `hdfs_port` 参数说明如下：

  - `hdfs_host`：HDFS 集群中 NameNode 所在主机的 IP 地址。
  - `hdfs_port`：HDFS 集群中 NameNode 所在主机的 FS 端口。默认端口号为 `9000`。

- `INTO TABLE`

  用于指定目标 StarRocks 表的名称。

`data_desc` 中的可选参数如下：

- `NEGATIVE`

  用于撤销某一批已经成功导入的数据。如果想要撤销某一批已经成功导入的数据，可以通过指定 `NEGATIVE` 关键字来导入同一批数据。

  > **说明**
  >
  > 该参数仅适用于目标 StarRocks 表使用聚合模型、并且所有 Value 列的聚合函数均为 `sum` 的情况。

- `PARTITION`

  指定要把数据导入哪些分区。如果不指定该参数，则默认导入到 StarRocks 表所在的所有分区中。

- `FORMAT AS`

  用于指定待导入数据文件的格式。取值包括 `CSV`、`Parquet` 和 `ORC`。如果不指定该参数，则默认通过 `file_path` 参数中指定的文件扩展名（**.csv**、**.parquet**、和 **.orc**）来判断文件格式。

- `COLUMNS TERMINATED BY`

  用于指定待导入数据文件中的列分隔符。如果不指定该参数，则默认列分隔符为 `\t`，即 Tab。必须确保这里指定的列分隔符与待导入数据文件中的列分隔符一致；否则，导入作业会因数据质量错误而失败，作业状态 (`State`) 会显示为 `CANCELLED`。

  需要注意的是，Broker Load 通过 MySQL 协议提交导入请求，除了 StarRocks 会做转义处理以外，MySQL 协议也会做转义处理。因此，如果列分隔符是 Tab 等不可见字符，则需要在列分隔字符前面多加一个反斜线 (\\)。例如，如果列分隔符是 `\t`，这里必须输入 `\\t`；如果列分隔符是 `\n`，这里必须输入 `\\n`。Apache Hive™ 文件的列分隔符为 `\x01`，因此，如果待导入数据文件是 Hive 文件，这里必须传入 `\\x01`。

  > **说明**
  >
  > StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (|)。

- `column_list`

  用于指定待导入数据文件和 StarRocks 表之间的列对应关系。语法如下：`(<column_name>[, <column_name> ...])`。`column_list` 中声明的列与 StarRocks 表中的列按名称一一对应。

  > **说明**
  >
  > 如果待导入数据文件的列和 StarRocks 表中的列按顺序一一对应，则不需要指定 `column_list` 参数。

  如果要跳过待导入数据文件中的某一列，只需要在 `column_list` 参数中将该列命名为 StarRocks 表中不存在的列名即可。具体请参见[导入过程中实现数据转换](../../../loading/Etl_in_loading.md)。

- `COLUMNS FROM PATH AS`

  用于从指定的文件路径中提取一个或多个分区字段的信息。该参数仅当指定的文件路径中存在分区字段时有效。

  例如，待导入数据文件所在的路径为 `/path/col_name=col_value/file1`，其中 `col_name` 可以对应到 StarRocks 表中的列。这时候，您可以设置参数为 `col_name`。导入时，StarRocks 会将 `col_value` 落入 `col_name` 对应的列中。

  > **说明**
  >
  > 该参数只有在从 HDFS 导入数据时可用。

- `SET`

  用于将待导入数据文件的某一列按照指定的函数进行转化，然后将转化后的结果落入 StarRocks 表中。语法如下：`column_name = expression`。以下为两个示例：

  - StarRocks 表中有三列，按顺序依次为 `col1`、`col2` 和 `col3`；待导入数据文件中有四列，前两列按顺序依次对应 StarRocks 表中的 `col1`、`col2` 列，后两列之和对应 StarRocks 表中的 `col3` 列。这种情况下，需要通过 `column_list` 参数声明 `(col1,col2,tmp_col3,tmp_col4)`，并使用 SET 子句指定 `SET (col3=tmp_col3+tmp_col4)` 来实现数据转换。
  - StarRocks 表中有三列，按顺序依次为 `year`、`month` 和 `day`；待导入数据文件中只有一个包含时间数据的列，格式为 `yyyy-mm-dd hh:mm:ss`。这种情况下，需要通过 `column_list` 参数声明 `(tmp_time)`、并使用 SET 子句指定 `SET (year = year(tmp_time), month=month(tmp_time), day=day(tmp_time))` 来实现数据转换。

- `WHERE`

  用于指定过滤条件，对做完转换的数据进行过滤。只有符合 WHERE 子句中指定的过滤条件的数据才会导入到 StarRocks 表中。

### `WITH BROKER`

用于指定 Broker 的名称。

### `broker_properties`

用于提供访问数据源的鉴权信息。数据源不同，需要提供的鉴权信息也不同。

#### HDFS

社区版本的 HDFS，支持简单认证和 Kerberos 认证两种认证方式（Broker Load 默认使用简单认证），并且支持 NameNode 节点的 HA 配置。如果数据源为社区版本的 HDFS，可以提供如下配置信息：

- 认证方式

  - 如果使用简单认证，需要指定如下配置：

    ```Plain
    "hadoop.security.authentication" = "simple"
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))
    ```

2. data_desc

    用于描述一批导入数据。

    语法：

    ```sql
    DATA INFILE
    (
        "file_path1"[, file_path2, ...]
    )
    [NEGATIVE]
    INTO TABLE `table_name`
    [PARTITION (p1, p2)]
    [COLUMNS TERMINATED BY "column_separator"]
    [FORMAT AS "file_type"]
    [(column_list)]
    [COLUMNS FROM PATH AS (column_list)]
    [SET (k1 = func(k2))]
    [WHERE predicate]
    ```

    说明：

    ```plain text
    file_path:

    文件路径，可以指定到一个文件，也可以用 * 通配符指定某个目录下的所有文件。通配符必须匹配到文件，而不能是目录。

    PARTITION:

    如果指定此参数，则只会导入指定的分区，导入分区以外的数据会被过滤掉。
    如果不指定，默认导入table的所有分区。

    NEGATIVE：
    如果指定此参数，则相当于导入一批“负”数据。用于抵消之前导入的同一批数据。
    该参数仅适用于存在 value 列，并且 value 列的聚合类型仅为 SUM 的情况。

    column_separator：

    用于指定导入文件中的列分隔符。默认为 \t
    如果是不可见字符，则需要加\\x作为前缀，使用十六进制来表示分隔符。
    如hive文件的分隔符\x01，指定为"\\x01"

    file_type：

    用于指定导入文件的类型，例如：parquet、orc、csv。默认值通过文件后缀名判断。

    column_list：

    用于指定导入文件中的列和 table 中的列的对应关系。
    当需要跳过导入文件中的某一列时，将该列指定为 table 中不存在的列名即可。
    语法：
    (col_name1, col_name2, ...)

    COLUMNS FROM PATH AS:

    提取文件路径中的分区字段。
    例: 导入文件为/path/col_name=col_value/file1，col_name为表中的列，则导入时会将col_value导入到col_name对应的列中。

    SET:

    如果指定此参数，可以将源文件某一列按照函数进行转化，然后将转化后的结果导入到table中。语法为 `column_name` = expression。举几个例子帮助理解。
    例1: 表中有3个列“c1, c2, c3", 源文件中前两列依次对应(c1,c2)，后两列之和对应c3；那么需要指定 columns (c1,c2,tmp_c3,tmp_c4) SET (c3=tmp_c3+tmp_c4);
    例2: 表中有3个列“year, month, day"三个列，源文件中只有一个时间列，为”2018-06-01 01:02:03“格式。
    那么可以指定 columns(tmp_time) set (year = year(tmp_time), month=month(tmp_time), day=day(tmp_time)) 完成导入。

    WHERE:

    对做完 transform 的数据进行过滤，符合 where 条件的数据才能被导入。WHERE 语句中只可引用表中列名。
    ```

3. broker_name

    所使用的 broker 名称，可以通过 show broker 命令查看。

4. broker_properties

    用于提供通过 broker 访问数据源的信息。不同的 broker，以及不同的访问方式，需要提供的信息不同。

    1. Apache HDFS

        社区版本的 hdfs，支持简单认证、kerberos 认证。以及支持 HA 配置。

        简单认证：

        hadoop.security.authentication = simple (默认)

        username：hdfs 用户名

        password：hdfs 密码

        kerberos 认证：

        hadoop.security.authentication = kerberos

        kerberos_principal：指定 kerberos 的 principal

        kerberos_keytab：指定 kerberos 的 keytab 文件路径。该文件必须为 broker 进程所在服务器上的文件。

        kerberos_keytab_content：指定 kerberos 中 keytab 文件内容经过 base64 编码之后的内容。这个跟 kerberos_keytab 配置二选一就可以。

        namenode HA：

        通过配置 namenode HA，可以在 namenode 切换时，自动识别到新的 namenode

        dfs.nameservices: 指定 hdfs 服务的名字，自定义，如："dfs.nameservices" = "my_ha"

        dfs.ha.namenodes.xxx：自定义 namenode 的名字,多个名字以逗号分隔。其中 xxx 为 dfs.nameservices 中自定义的名字，如 "dfs.ha.namenodes.my_ha" = "my_nn"

        dfs.namenode.rpc-address.xxx.nn：指定 namenode 的rpc地址信息。其中 nn 表示 dfs.ha.namenodes.xxx 中配置的 namenode 的名字，如："dfs.namenode.rpc-address.my_ha.my_nn" = "host:port"

        dfs.client.failover.proxy.provider：指定 client 连接 namenode 的 provider，默认为：org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider

    2. Amazon S3

        需提供：
        fs.s3a.access.key：AmazonS3的access key

        fs.s3a.secret.key：AmazonS3的secret key

        fs.s3a.endpoint：AmazonS3的endpoint

    3. 阿里云 OSS

        需提供：
        fs.oss.accessKeyId：Aliyun OSS的access key

        fs.oss.accessKeySecret：Aliyun OSS的secret key

        fs.oss.endpoint：Aliyun OSS的endpoint

    4. 百度 BOS

       需提供：
       bos_endpoint：BOS 的endpoint

       bos_accesskey：公有云用户的 accesskey

       bos_secret_accesskey：公有云用户的 secret_accesskey

    5. opt_properties

        用于指定一些特殊参数。

        语法：

        [PROPERTIES ("key"="value", ...)]

        可以指定如下参数：
        timeout：         指定导入操作的超时时间。默认超时为4小时。单位秒。

        max_filter_ratio：最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。

<<<<<<< HEAD
        exec_mem_limit：  导入内存限制。默认为 2GB。单位为字节。
=======
    > **说明**
    >
    > - “平均导入速度”是指目前 StarRocks 集群的平均导入速度。由于每个 StarRocks 集群的机器环境不同、且集群允许的并发查询任务数也不同，因此，StarRocks 集群的平均导入速度需要根据历史导入速度进行推测。
    >
    > - “导入并发数”可以通过 `max_broker_concurrency` 参数设置，具体请参见“从 HDFS 或外部云存储系统导入数据”文档中的“[作业拆分与并行执行](../../../loading/BrokerLoad.md#作业拆分与并行执行)”章节。
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))

        strict mode：     是否对数据进行严格限制。默认为 false。

        timezone:         指定某些受时区影响的函数的时区，如 strftime/alignment_timestamp/from_unixtime 等等，具体请查阅 [时区] 文档。如果不指定，则使用 "Asia/Shanghai" 时区。

    6. 导入数据格式样例

        整型类（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）：1, 1000, 1234

        浮点类（FLOAT/DOUBLE/DECIMAL）：1.1, 0.23, .356

        日期类（DATE/DATETIME）：2017-10-03, 2017-06-13 12:34:03。
        （注：如果是其他日期格式，可以在导入命令中，使用 strftime 或者 time_format 函数进行转换）

<<<<<<< HEAD
        字符串类（CHAR/VARCHAR）："I am a student", "a"
=======
  如果因为设置最大容忍率为 `0` 而导致作业失败，可以通过 [SHOW LOAD](./SHOW_LOAD.md) 语句来查看导入作业的结果信息。然后，判断错误的数据行是否可以被过滤掉。如果可以被过滤掉，则可以根据结果信息中的 `dpp.abnorm.ALL` 和 `dpp.norm.ALL` 来计算导入作业的最大容忍率，然后调整后重新提交导入作业。计算公式如下：
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))

        NULL值：\N

### Syntax

1. 从 HDFS 导入一批数据，指定超时时间和过滤比例。使用铭文 my_hdfs_broker 的 broker。简单认证。

    ```sql
    LOAD LABEL example_db.label1
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    )
    WITH BROKER my_hdfs_broker
    (
        "username" = "hdfs_user",
        "password" = "hdfs_passwd"
    )
    PROPERTIES
    (
        "timeout" = "3600",
        "max_filter_ratio" = "0.1"
    );
    ```

    其中 hdfs_host 为 namenode 的 host，hdfs_port 为 fs.defaultFS 端口（默认9000）

2. 从 HDFS 导入一批数据，指定hive的默认分隔符\x01，并使用通配符*指定目录下的所有文件。

    使用简单认证，同时配置 namenode HA

<<<<<<< HEAD
    ```sql
    LOAD LABEL example_db.label3
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/*")
    INTO TABLE `my_table`
=======
  指定导入作业所使用的时区。默认为 `Asia/Shanghai` 时区。该参数会影响所有导入涉及的、跟时区设置有关的函数所返回的结果。受时区影响的函数有 strftime、alignment_timestamp 和 from_unixtime 等，具体请参见[设置时区](../../../administration/timezone.md)。导入参数 `timezone` 设置的时区对应“[设置时区](../../../administration/timezone.md)”中所述的会话级时区。

## 列映射

在导入数据时，可以通过 `column_list` 参数来指定待导入数据文件和 StarRocks 表之间的列映射关系。如果待导入数据文件中的列与 StarRocks 表中的列按顺序一一对应，则不需要指定该参数；否则，必须通过该参数来配置列映射关系，一般包括如下两种场景：

- 待导入数据文件中的列与 StarRocks 表中的列一一对应，并且数据不需要通过函数计算、可以直接落入 StarRocks 表中对应的列。

  您需要在 `column_list` 参数中按照待导入数据文件中的列顺序、使用 StarRocks 表中对应的列名来配置列映射关系。

  例如，StarRocks 表中有三列，按顺序依次为 `col1`、`col2` 和 `col3`；待导入数据文件中也有三列，按顺序依次对应 StarRocks 表中的 `col3`、`col2` 和 `col1`。这种情况下，需要指定 `(col3, col2, col1)`。

- 待导入数据文件中的列与 StarRocks 表中的列不一一对应，或者某些列的数据需要通过函数计算以后才能落入 StarRocks 表中对应的列。

  您不仅需要在 `column_list` 参数中按照待导入数据文件中的列顺序、使用 StarRocks 表中对应的列名来配置列映射关系，还需要指定参与数据计算的函数。以下为两个示例：

  - StarRocks 表中有三列，按顺序依次为 `col1`、`col2` 和 `col3` ；待导入数据文件中有四列，前三列按顺序依次对应 StarRocks 表中的 `col1`、`col2` 和 `col3`，第四列在 StarRocks 表中无对应的列。这种情况下，需要指定 `(col1, col2, col3, temp)`，其中，最后一列可随意指定一个名称（如 `temp`）用于占位即可。
  - StarRocks 表中有三列，按顺序依次为 `year`、`month` 和 `day`。待导入数据文件中只有一个包含时间数据的列，格式为 `yyyy-mm-dd hh:mm:ss`。这种情况下，可以指定 `(col, year = year(col), month=month(col), day=day(col))`。其中，`col` 是待导入数据文件中所包含的列的临时命名，`year = year(col)`、`month=month(col)` 和 `day=day(col)` 用于指定从待导入数据文件中的 `col` 列提取对应的数据并落入 StarRocks 表中对应的列，如 `year = year(col)` 表示通过 `year` 函数提取待导入数据文件中 `col` 列的 `yyyy` 部分的数据并落入 StarRocks 表中的 `year` 列。

## 示例

本文以 HDFS 数据源为例，介绍各种导入配置。

### 导入 CSV 格式的数据

本小节以 CSV 格式的数据为例，重点阐述在创建导入作业的时候，如何运用各种参数配置来满足不同业务场景下的各种导入要求。

#### 设置超时时间

StarRocks 数据库 `test_db` 里的表 `table1` 包含三列，按顺序依次为 `col1`、`col2`、`col3`。

数据文件 `example1.csv` 也包含三列，按顺序一一对应 `table1` 中的三列。

如果要把 `example1.csv` 中所有的数据都导入到 `table1` 中，并且要求超时时间最大不超过 3600 秒，可以执行如下语句：

```SQL
LOAD LABEL test_db.label1
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example1.csv")
    INTO TABLE table1
)
WITH BROKER "mybroker"
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

#### 设置最大容错率

StarRocks 数据库 `test_db` 里的表 `table2` 包含三列，按顺序依次为 `col1`、`col2`、`col3`。

数据文件 `example2.csv` 也包含三列，按顺序一一对应 `table2` 中的三列。

如果要把 `example2.csv` 中所有的数据都导入到 `table2` 中，并且要求容错率最大不超过 `0.1`，可以执行如下语句：

```SQL
LOAD LABEL test_db.label2
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example2.csv")
    INTO TABLE table2
)
WITH BROKER "mybroker"
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "max_filter_ratio" = "0.1"
);
```

#### 导入指定路径下所有数据文件

StarRocks 数据库 `test_db` 里的表 `table3` 包含三列，按顺序依次为 `col1`、`col2`、`col3`。

HDFS 集群的 `/user/starrocks/data/input/` 路径下所有数据文件也包含三列，按顺序一一对应 `table3` 中的三列，并且列分隔符为 Hive 文件的默认列分隔符 `\x01`。

如果要把 `hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/` 路径下所有数据文件的数据都导入到 `table3` 中，可以执行如下语句：

```SQL
LOAD LABEL test_db.label3
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/*")
    INTO TABLE table3
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))
    COLUMNS TERMINATED BY "\\x01"
    )
    WITH BROKER my_hdfs_broker
    (
        "username" = "hdfs_user",
        "password" = "hdfs_passwd",
        "dfs.nameservices" = "my_ha",
        "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
        "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
        "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
        "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    )
    ```

3. 从 HDFS 导入一批“负”数据。同时使用 kerberos 认证方式。提供 keytab 文件路径。

    ```sql
    LOAD LABEL example_db.label4
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/old_file)
    NEGATIVE
    INTO TABLE `my_table`
    COLUMNS TERMINATED BY "\t"
    )
    WITH BROKER my_hdfs_broker
    (
        "hadoop.security.authentication" = "kerberos",
        "kerberos_principal"="starrocks@YOUR.COM",
        "kerberos_keytab"="/home/starRocks/starRocks.keytab"
    )
    ````

4. 从 HDFS 导入一批数据，指定分区。同时使用 kerberos 认证方式。提供 base64 编码后的 keytab 文件内容。

    ```sql
    LOAD LABEL example_db.label5
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    PARTITION (p1, p2)
    COLUMNS TERMINATED BY ","
    (k1, k3, k2, v1, v2)
    )
    WITH BROKER my_hdfs_broker
    (
        "hadoop.security.authentication"="kerberos",
        "kerberos_principal"="starrocks@YOUR.COM",
        "kerberos_keytab_content"="BQIAAABEAAEACUJBSURVLkNPTQAEcGFsbw"
    )
    ```

5. 从 BOS 导入一批数据，指定分区, 并对导入文件的列做一些转化，如下：

<<<<<<< HEAD
    表结构为：
    k1 varchar(20)
    k2 int
=======
有关 `hll_hash` 函数和 `hll_empty` 函数的用法，请参见 [HLL](../data-definition/HLL.md)。
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))

    假设数据文件只有一行数据：

    Adele,1,1

    数据文件中各列，对应导入语句中指定的各列：
    k1,tmp_k2,tmp_k3

    转换如下：
    1. k1: 不变换
    2. k2：是 tmp_k2 和 tmp_k3 数据之和

    ```sql
    LOAD LABEL example_db.label6
    (
    DATA INFILE("bos://my_bucket/input/file")
    INTO TABLE `my_table`
    PARTITION (p1, p2)
    COLUMNS TERMINATED BY ","
    (k1, tmp_k2, tmp_k3)
    SET (
    k2 = tmp_k2 + tmp_k3
    )
    )
    WITH BROKER my_bos_broker
    (
        "bos_endpoint" = "http://bj.bcebos.com",
        "bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "bos_secret_accesskey"="yyyyyyyyyyyyyyyyyyyy"
    )
    ```

6. 导入数据到含有HLL列的表，可以是表中的列或者数据里面的列

    如果表中有三列分别是（id,v1,v2,v3）。其中v1和v2列是hll列。导入的源文件有3列。则（column_list）中声明第一列为id，第二三列为一个临时命名的k1,k2。

    在SET中必须给表中的hll列特殊声明 hll_hash。表中的v1列等于原始数据中的hll_hash(k1)列, 表中的v3列在原始数据中并没有对应的值，使用empty_hll补充默认值。

    ```SQL
    LOAD LABEL example_db.label7
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    PARTITION (p1, p2)
    COLUMNS TERMINATED BY ","
    (id, k1, k2)
    SET (
    v1 = hll_hash(k1),
    v2 = hll_hash(k2),
    v3 = empty_hll()
    )
    )
    WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

    LOAD LABEL example_db.label8
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    PARTITION (p1, p2)
    COLUMNS TERMINATED BY ","
    (k1, k2, tmp_k3, tmp_k4, v1, v2)
    SET (
    v1 = hll_hash(tmp_k3),
    v2 = hll_hash(tmp_k4)
    )
    )
    WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");
    ```

7. 导入Parquet文件中数据  指定FORMAT 为parquet， 默认是通过文件后缀判断

    ```SQL
    LOAD LABEL example_db.label9
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    FORMAT AS "parquet"
    (k1, k2, k3)
    )
    WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");
    ```

8. 提取文件路径中的分区字段

    如果需要，则会根据表中定义的字段类型解析文件路径中的分区字段（partitioned fields），类似Spark中Partition Discovery的功能

    ```SQL
    LOAD LABEL example_db.label10
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/*/*")
    INTO TABLE `my_table`
    FORMAT AS "csv"
    (k1, k2, k3)
    COLUMNS FROM PATH AS (city, utc_date)
    SET (uniq_id = md5sum(k1, city))
    )
    WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");
    ```

    `hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing`目录下包括如下文件：

    `[hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/utc_date=2019-06-26/0000.csv, hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/utc_date=2019-06-26/0001.csv, ...]`

    则提取文件路径的中的city和utc_date字段

9. 对待导入数据进行过滤，k1 值大于 k2 值的列才能被导入

    ```sql
    LOAD LABEL example_db.label10
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    where k1 > k2
    )
    ```

10. 提取文件路径中的时间分区字段，并且时间包含 %3A (在 hdfs 路径中，不允许有 ':'，所有 ':' 会由 %3A 替换)

    假设有如下文件：

    /user/data/data_time=2020-02-17 00%3A00%3A00/test.txt

    /user/data/data_time=2020-02-18 00%3A00%3A00/test.txt

    ```PLAIN TEXT
    表结构为：
    data_time DATETIME,
    k2        INT,
    k3        INT
    ```

    ```SQL
    LOAD LABEL example_db.label11
    (
    DATA INFILE("hdfs://host:port/user/data/*/test.txt")
    INTO TABLE `tbl12`
    COLUMNS TERMINATED BY ","
    (k2,k3)
    COLUMNS FROM PATH AS (data_time)
    SET (data_time=str_to_date(data_time, '%Y-%m-%d %H%%3A%i%%3A%s'))
    )
    WITH BROKER "hdfs" ("username"="user", "password"="pass");
    ```

11. 从 阿里云 OSS 导入 csv 格式的数据

    ```SQL
    LOAD LABEL example_db.label12
    (
    DATA INFILE("oss://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
    )
    WITH BROKER my_broker
    (
        "fs.oss.accessKeyId" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "fs.oss.accessKeySecret" = "yyyyyyyyyyyyyyyyyyyy",
        "fs.oss.endpoint" = "oss-cn-zhangjiakou-internal.aliyuncs.com"
    )
    ```

12. 从腾讯云 COS 导入 csv 格式的数据

    ```SQL
    LOAD LABEL example_db.label13
    (
    DATA INFILE("cosn://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
    )
    WITH BROKER my_broker
    (
        "fs.cosn.userinfo.secretId" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "fs.cosn.userinfo.secretKey" = "yyyyyyyyyyyyyyyyyyyy",
        "fs.cosn.bucket.endpoint_suffix" = "cos.ap-beijing.myqcloud.com"
    )
    ```

13. 从 Amazon S3 导入 csv 格式的数据

    ```SQL
    LOAD LABEL example_db.label14
    (
    DATA INFILE("s3a://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
    )
    WITH BROKER my_broker
    (
        "fs.s3a.access.key" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "fs.s3a.secret.key" = "yyyyyyyyyyyyyyyyyyyy",
        "fs.s3a.endpoint" = "s3-ap-northeast-1.amazonaws.com"
    )
    ```

## keyword

BROKER,LOAD
