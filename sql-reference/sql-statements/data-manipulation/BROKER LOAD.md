# BROKER LOAD

## 功能

Broker Load 通过随 StarRocks 集群一同部署的 broker 进行，访问对应数据源的数据，进行数据导入。该导入方式使用的场景详见 [Broker load](/loading/BrokerLoad.md) 章节。

可以通过 `show broker` 命令查看已经部署的 broker。

目前支持以下 4 种数据源：

1. HDFS：社区版本 hdfs。
2. Amazon S3：Amazon 对象存储。
3. 阿里云 OSS：阿里云对象存储。
4. 腾讯COS：腾讯云对象存储。

## 语法

注：方括号 [] 中内容可省略不写。

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

    ```sql
    [database_name.]your_label
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

    如果指定此参数，则只会导入指定StarRocks表的分区，导入分区以外的数据会被过滤掉。
    如果不指定，默认导入table的所有分区。

    NEGATIVE：
    如果指定此参数，则相当于导入一批"负"数据。用于抵消之前导入的同一批数据。
    该参数仅适用于存在 value 列，并且 value 列的聚合类型仅为 SUM 的情况。

    column_separator：

    用于指定导入文件中的列分隔符。默认为 \t
    如果是不可见字符，则需要加\\x作为前缀，使用十六进制来表示分隔符。
    如hive文件的分隔符\x01，指定为"\\x01"

    file_type：

    用于指定导入文件的类型，例如：parquet、orc、csv。默认值通过文件后缀名判断。

    column_list：

    用于指定导入文件中的列和 table 中的列的对应关系。
    当需要跳过导入文件中的某一列时，将该列指定为 table 中不存在的列名即可。list中的字段名称与StarRocks表中字段值对应。
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

    1. HDFS

        社区版本的 hdfs，支持简单认证、kerberos 认证。以及支持 HA 配置。

        简单认证：

        ```plain text
        hadoop.security.authentication = simple (默认)

        username：hdfs 用户名

        password：hdfs 密码  
        ```

        kerberos 认证：

        ```plain text
        hadoop.security.authentication = kerberos

        kerberos_principal：指定 kerberos 的 principal

        kerberos_keytab：指定 kerberos 的 keytab 文件路径。该文件必须为 broker 进程所在服务器上的文件。

        kerberos_keytab_content：指定 kerberos 中 keytab 文件内容经过 base64 编码之后的内容。这个跟 kerberos_keytab 配置二选一就可以。
        ```

        namenode HA：

        ```plain text
        通过配置 namenode HA，可以在 namenode 切换时，自动识别到新的 namenode

        dfs.nameservices: 指定 hdfs 服务的名字，自定义，如："dfs.nameservices" = "my_ha"

        dfs.ha.namenodes.xxx：自定义 namenode 的名字, 多个名字以逗号分隔。其中 xxx 为 dfs.nameservices 中自定义的名字，如 "dfs.ha.namenodes.my_ha" = "my_nn"

        dfs.namenode.rpc-address.xxx.nn：指定 namenode 的 rpc 地址信息。其中 nn 表示 dfs.ha.namenodes.xxx 中配置的 namenode 的名字，如："dfs.namenode.rpc-address.my_ha.my_nn" = "host: port"

        dfs.client.failover.proxy.provider：指定 client 连接 namenode 的 provider，默认为：org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
        ```

    2. Amazon S3

        需提供：
        fs.s3a.access.key：AmazonS3 的 access key

        fs.s3a.secret.key：AmazonS3 的 secret key

        fs.s3a.endpoint：AmazonS3 的 endpoint

    3. 阿里云 OSS

        需提供：
        fs.oss.accessKeyId：Aliyun OSS 的 access key

        fs.oss.accessKeySecret：Aliyun OSS 的 secret key

        fs.oss.endpoint：Aliyun OSS 的 endpoint

    4. opt_properties

        用于指定一些特殊参数。

        语法：

        [PROPERTIES ("key" = "value", ...)]

        可以指定如下参数：
        timeout：         指定导入操作的超时时间。默认超时为 4 小时，单位秒。

        max_filter_ratio：最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。

        exec_mem_limit：  导入内存限制。默认为 2GB。单位为字节。

        strict_mode：     是否对数据进行严格限制。默认为 false。

        timezone:         指定某些受时区影响的函数的时区，如 strftime/alignment_timestamp/from_unixtime 等等，具体请查阅 [时区](/using_starrocks/timezone.md) 文档。如果不指定，则使用 "Asia/Shanghai" 时区。

    5. 导入数据格式样例

        整型类（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）：1, 1000, 1234

        浮点类（FLOAT/DOUBLE/DECIMAL）：1.1, 0.23, .356

        日期类（DATE/DATETIME）：2017-10-03, 2017-06-13 12: 34: 03。
        （注：如果是其他日期格式，可以在导入命令中，使用 strftime 或者 time_format 函数进行转换）

        字符串类（CHAR/VARCHAR）："I am a student", "a"

        NULL 值：\N

## 示例

### 指定超时时间和过滤比例

从 HDFS 导入一批数据，指定超时时间和过滤比例。使用铭文 my_hdfs_broker 的 broker。简单认证。

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

其中 hdfs_host 为 namenode 的 host，hdfs_port 为 fs.defaultFS 端口（默认 9000）

### 设置分隔符及使用通配符目录下所有文件

从 HDFS 导入一批数据，指定 hive 的默认分隔符\x01，并使用通配符*指定目录下的所有文件。

使用简单认证，同时配置 namenode HA

```sql
LOAD LABEL example_db.label3
(
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/*")
    INTO TABLE `my_table`
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

### 导入“负”数据并设置 kerberos 认证

从 HDFS 导入一批 "负" 数据，"负" 数据详细含义见上文语法参数介绍部分。同时使用 kerberos 认证方式。提供 keytab 文件路径。

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
```

### 导入数据到指定分区

从 HDFS 导入一批数据，指定分区。同时使用 kerberos 认证方式。提供 base64 编码后的 keytab 文件内容。

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

### 导入 HLL 列数据

导入数据到含有 HLL 列的表，可以是表中的列或者数据里面的列

如果表中有三列分别是（id, v1, v2, v3）。其中 v1 和 v2 列是 hll 列。导入的源文件有 3 列。则（column_list）中声明第一列为 id，第二三列为一个临时命名的 k1, k2。

在 SET 中必须给表中的 hll 列特殊声明 hll_hash。表中的 v1 列等于原始数据中的 hll_hash(k1)列, 表中的 v3 列在原始数据中并没有对应的值，使用 empty_hll 补充默认值。

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

### 导入 Parquet 格式数据

导入 Parquet 文件中数据指定 FORMAT 为 parquet， 默认是通过文件后缀判断。

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

### 提取文件路径中的分区字段

如果用户需要，则可以根据表中定义的字段类型来解析文件路径中的分区字段（partitioned fields），该功能类似Spark中Partition Discovery的功能

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

hdfs://hdfs_host: hdfs_port/user/starRocks/data/input/dir/city = beijing 目录下包括如下文件：

[hdfs://hdfs_host: hdfs_port/user/starRocks/data/input/dir/city = beijing/utc_date = 2019-06-26/0000.csv, hdfs://hdfs_host: hdfs_port/user/starRocks/data/input/dir/city = beijing/utc_date = 2019-06-26/0001.csv, ...]

则提取文件路径的中的 city 和 utc_date 字段

### 对导入数据进行过滤

对待导入数据进行过滤，k1 值大于 k2 值的列才能被导入

```sql
LOAD LABEL example_db.label10
(
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    where k1 > k2
)
```

### 提取文件路径中包含%3A 的时间分区字段

在 hdfs 路径中，不允许有 ':'，所有 ':' 会由 %3A 替换。

假设有如下文件：

- `/user/data/data_time = 2020-02-17 00%3A00%3A00/test.txt`
- `/user/data/data_time = 2020-02-18 00%3A00%3A00/test.txt`

表结构为：

```PLAIN TEXT
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

### 从阿里云 OSS 导入 CSV 格式的数据

```SQL
LOAD LABEL example_db.label12
(
    DATA INFILE("oss://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
)
WITH BROKER my_broker
(
    "fs.oss.accessKeyId" = "xxxxxxxxxxxxxxxxxxxxxx",
    "fs.oss.accessKeySecret" = "yyyyyyyyyyyyyyyyyyyy",
    "fs.oss.endpoint" = "oss-cn-zhangjiakou-internal.aliyuncs.com"
)
```

### 从腾讯云 COS 导入 CSV 格式的数据

```SQL
LOAD LABEL example_db.label13
(
    DATA INFILE("cosn://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
)
WITH BROKER my_broker
(
    "fs.cosn.userinfo.secretId" = "xxxxxxxxxxxxxxxxx",
    "fs.cosn.userinfo.secretKey" = "yyyyyyyyyyyyyyyy",
    "fs.cosn.bucket.endpoint_suffix" = "cos.ap-beijing.myqcloud.com"
)
```

### 从 Amazon S3 导入 CSV 格式的数据

```SQL
LOAD LABEL example_db.label14
(
    DATA INFILE("s3a://my_bucket/input/file.csv")
    INTO TABLE `my_table`
    (k1, k2, k3)
)
WITH BROKER my_broker
(
    "fs.s3a.access.key" = "xxxxxxxxxxxxxxxxxxxx",
    "fs.s3a.secret.key" = "yyyyyyyyyyyyyyyyyyyy",
    "fs.s3a.endpoint" = "s3-ap-northeast-1.amazonaws.com"
)
```
