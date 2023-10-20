# BROKER LOAD

## description

Broker Load is performed via brokers deployed with the StarRocks cluster. It will access data from corresponding data source and load data into StarRocks via Broker.

Use show broker command to check the deployed broker.

It supports the following six data sources:

1. Apache HDFS： hdfs of community version.
2. Amazon S3：Amazon object storage.
3. 阿里云 OSS：Aliyun object storage.
4. 腾讯COS：Tencent cloud object stroage.
5. 百度BOS：Baidu object storage.
Syntax:

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

    Label of the current load. Unique load label within a database.

    Syntax:

    ```sql
    [database_name.]your_label
    ```

2. data_desc

    To describe the data source.

    Syntax:

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

    Note:

    ```plain text
    file_path:
    
    File path, which can direct to a file and also to all files in a directory with the * wildcard. Wildcards must match to files, not the directory.
    
    PARTITION:
    
    If the parameter is specified, data will only be loaded to specified partitions and data out of partition's range will be filtered. If not specified, all partitions will be loaded. 
    
    NEGATIVE：
    If this parameter is specified, it is equivalent to importing a batch of "negative" data to offset the same batch of data loaded before. The parameter applies only to the case where value column exists and the aggregation type of value column is SUM. 
    
    column_separator：
    
    It is used to specify the column separator in the loaded file. Default is \t. 
    If the character is invisible, it needs to be prefixed with \\x, using hexadecimal to represent the separator. 
    For example, the separator \x01 of the hive file is specified as \\ x01 
    
    file_type：
    
    It is used to specify the type of loaded file, such as parquet, orc, csv. Default values are determined by the file suffix name. 
    
    column_list：
    
    Used to specify the correspondence between columns in the imported file and columns in the table. 
    To skip a column in the imported file, specify it as a column name that does not exist in the table. 
    Syntax: 
    (col_name1, col_name2, ...)
    
    COLUMNS FROM PATH AS:
    
    Extract the partition field from the file path. For example, the loaded file is /path/col_name=col_value/file1 and col_name is a column in the table, and then col_value will be imported to the column corresponding to col_name.
    
    SET:
    
    If this parameter is specified, a column of the source file can be converted based on a function, and then the transformed result can be loaded into the table. The syntax is column_name = expression. Some examples are given to facilitate understanding. 
    Example 1: There are three columns "c1, c2, c3" in the table. The first two columns in the source file correspond in turn (c1, c2), and the sum of the last two columns correspond to c3. Then, column (c1, c2, tmp_c3, tmp_c4) SET (c3 = tmp_c3 + tmp_c4) should be specified. 
    Example 2: There are three columns "year, month, day" in the table. There is only one time column in the source file, in the format of "2018-06-01:02:03". Then you can specify columns (tmp_time) set (year = year (tmp_time), month = month (tmp_time), day = day (tmp_time)) to complete the loading. 
    
    WHERE:
    
    Filter the data under "transform" d, and data that meets "where" predicates can be loaded. Only column names in tables can be referenced in WHERE statements.
    ```

3. broker_name

    The name of the broker being used and can be viewed through the `show broker` command.

4. broker_properties

    It is used to provide information about accessing data source via broker. For different brokers, and different access methods, different information needs to be provided.

    1. Apache HDFS

        hdfs of community version, which supports simple authentication, Kerberos authentication, and HA configuration.

        **Simple authentication:**

        hadoop.security.authentication = simple (default)

        username: hdfs username

        password: hdfs password

        **kerberos authentication:**

        hadoop.security.authentication = kerberos

        kerberos_principal: principal of specified kerberos

        kerberos_keytab: keytab file path of specified kerberos. This file must be on the server where broker process resides.

        kerberos_keytab_content: the contents of the KeyTab file in specified Kerberos after base64 encoding, which is optional from the kerberos_keytab configuration.  

        **namenode HA:**  

        By configuring namenode HA, new namenode can be automatically identified when the namenode is switched.

        dfs.nameservices: specify hdfs service name, custom, eg: "dfs.nameservices" = "my_ha"  

        dfs.ha.namenodes.xxx: customize the name of a namenode, separated by commas. XXX is a custom name in dfs. name services, such as "dfs. ha. namenodes. my_ha" = "my_nn"

        dfs.namenode.rpc-address.xxx.nn: specify rpc address information for namenode, where nn denotes the name of the namenode configured in dfs.ha.namenodes.xxxx, such as: "dfs.namenode.rpc-address.my_ha.my_nn"= "host:port"

        dfs.client.failover.proxy.provider: specify the provider that client connects to namenode by default: org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider.

    2. Amazon S3

        The following need to be provided:

        access key of fs.s3a.access.key：AmazonS3

        secret key of fs.s3a.secret.key：AmazonS3

        endpoint of fs.s3a.endpoint：AmazonS3

    3. Aliyun OSS

        The following need to be provided:

        access key of fs.oss.accessKeyId：Aliyun OSS

        secret key of fs.oss.accessKeySecret：Aliyun OSS

        endpoint of fs.oss.endpoint：Aliyun OSS

    4. Baidu BOS

       The following need to be provided:

       Endpoint of bos_endpoint：BOS

       accesskey of bos_ accesskey: public cloud user

       secret_accesskey of bos_secret_accesskey: public cloud user

    5. opt_properties

        It is used to specify some special parameters.

        Syntax:

        [PROPERTIES ("key"="value", …)]

        You can specify the following parameters:

        timeout: Specifies the timeout time for the import operation. The default timeout is 4 hours and the unit is second.

        max_filter_ratio: Data ratio of maximum tolerance filterable (data irregularity, etc.). Zero tolerance by default.

        exc_mem_limit: Memory limit. 2GB by default. The unit is byte.

        strict_mode: Whether the data is strictly restricted. The default is false.

        timezone: Specify time zones for functions affected by time zones, such as strftime/alignment_timestamp/from_unixtime, etc. See the [Time Zone] documentation for details. If not specified, use the "Asia/Shanghai" time zone.

    6. Format sample of loaded data

        Integer（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）: 1, 1000, 1234

        Float（FLOAT/DOUBLE/DECIMAL）: 1.1, 0.23, .356

        Date（DATE/DATETIME）: 2017-10-03, 2017-06-13 12:34:03. (Note: If it's in other date formats, use strftime or time_format functions to convert in the loading command)

    String（CHAR/VARCHAR）: "I am a student", "a"

    NULL value: \N

### Syntax

1. Load a batch of data from HDFS, specify timeout and filtering ratio. Use the broker with the plaintext my_hdfs_broker. Simple authentication.

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

    Where hdfs_host is the host of the namenode and hdfs_port is the fs.defaultFS port (default 9000)

2. Load a batch of data from HDFS, specify hive's default delimiter \x01, and use wildcard * to specify all files under the directory. Use simple authentication and configure namenode HA.

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

3. Load a batch of "negative" data from HDFS and use Kerberos authentication to provide KeyTab file path.

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

4. Load a batch of data from HDFS, specify partition. At the same time, use Kerberos authentication mode. Provide the KeyTab file content encoded by base64.

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

5. Load a batch of data from BOS, specify partitions, and make some conversions to the columns of the imported files, as follows:

    Table schema:
    k1 varchar(20)
    k2 int

    Assuming that the data file has only one row of data:

    Adele,1,1

    The columns in the data file correspond to the columns specified in the loaded statement:
    k1,tmp_k2,tmp_k3

    Conduct the following conversion:
    1. k1: unchanged
    2. k2：sum of tmp_k2 and tmp_k3

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

6. Load data into tables containing HLL columns, which can be columns in tables or columns in data.

    If there are three columns in the table are (id, v1, v2, v3). The v1 and v2 columns are hll columns. The imported source file has 3 columns.Then declare the first column is id in (column_list) and the second and the third columns are temporarily named k1 and k2.

    In SET, the HLL column in the table must be specifically declared hll_hash. The v1 column in the table is equal to the hll_hash (k1) column in the original data. The v3 column in the table does not have a corresponding value in the original data, and use empty_hll to supplement the default value.

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

7. Load data in Parquet file and specify FORMAT as parquet. The default is determined by file suffix.

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

8. Extract partitioned fields in file paths.

    If necessary, partitioned fields in the file path are resolved based on the field type defined in the table, similar to the Partition Discovery function in Spark.

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

    `hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing` contains following files:

    `[hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/utc_date=2019-06-26/0000.csv, hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/utc_date=2019-06-26/0001.csv, ...]`

    Extract city and utc_date fields in the file path.

9. Filter the loaded data: columns whose k1 value is bigger than k2 value can be imported.

    ```sql
    LOAD LABEL example_db.label10
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    where k1 > k2
    )
    ```

10. Extract time partitioned fields in file paths, and the time includes %3A (in hdfs path, all ':' will be replaced by '%3A')

    Assume we have files:

    /user/data/data_time=2020-02-17 00%3A00%3A00/test.txt

    /user/data/data_time=2020-02-18 00%3A00%3A00/test.txt

    ```PLAIN TEXT
    Table structure：
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

11. Load data in csv format from Aliyun OSS.

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

12. Load data in csv format from Tencent Cloud COS.

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

13. Load data in csv format from Amazon S3.

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
