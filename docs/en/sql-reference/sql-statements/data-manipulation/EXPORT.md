# EXPORT

## description

This statement is used to export the data in a specified table to a specified location.

This function is implemented by broker process. For different purpose storage systems, different brokers need to be deployed. Deployed brokers can be viewed through SHOW BROKER.

This is an asynchronous operation, which returns if the task is submitted successfully. After execution, you can use the SHOW EXPORT command to check progress.

Syntax:

```sql
EXPORT TABLE table_name
[PARTITION (p1[,p2])]
TO export_path
[opt_properties]
broker;
```

1. table_name

    The name of the table to be exported. Currently, this system only supports the export of tables with engine as OLAP and mysql.

2. partition

    You can export certain specified partitions of the specified table.

3. export_path

    The export path. Currently, you cannot export to local, but to broker.

    If you need to export a directory, it must end with a slash. Otherwise, the part after the last slash will be identified as the prefix to the exported file.

4. opt_properties

     It is used to specify some special parameters.

     Syntax:

    ```sql
    [PROPERTIES ("key"="value", ...)]
    ```

     The following parameters can be specified:

    ```plain text
    column_separator: Specify the exported column separator, defaulting to t. 
    line_delimiter: Specify the exported line separator, defaulting to\n. 
    exec_mem_limit: Export the upper limit of memory usage for a single BE node, defaulting to 2GB in bytes.
    timeout：The time-out for importing jobs, defaulting to 1 day in seconds.
    include_query_id: Whether the exported file name contains query id, defaulting to true.
    ```

5. broker

     It is used to specify the broker to be used for the export.

     Syntax:

    ```sql
    WITH BROKER broker_name ("key"="value"[,...])
    ```

    Here you need to specify the specific broker name and the required broker properties.

    For brokers corresponding to different storage systems, the input parameters are different. Specific parameters can be referred to: the required properties of broker in`help broker load`

## example

1. Export all data from the testTbl table to HDFS

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    ```

2. Export partitions p1 and p2 from the testTbl table to HDFS

    ```sql
    EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c/" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    ```

3. Export all data in the testTbl table to hdfs, using "," as column separator

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/" PROPERTIES ("column_separator"=",") WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    ```

4. Export all data in the testTbl table to hdfs, using Hive custom separator "\x01" as column separator

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/" PROPERTIES ("column_separator"="\\x01") WITH BROKER "broker_name";
    ```

5. Export all data in the testTbl table to hdfs, specifying the exported file prefix as testTbl_

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/testTbl_" WITH BROKER "broker_name";
    ```

6. Export all data in the testTbl table to OSS

    ```sql
    EXPORT TABLE testTbl TO "oss://oss-package/export/"
    WITH BROKER "broker_name"
    (
    "fs.oss.accessKeyId" = "xxx",
    "fs.oss.accessKeySecret" = "yyy",
    "fs.oss.endpoint" = "oss-cn-zhangjiakou-internal.aliyuncs.com"
    );
    ```

7. Export all data in the testTbl table to COS

    ```sql
    EXPORT TABLE testTbl TO "cosn://cos-package/export/"
    WITH BROKER "broker_name"
    (
    "fs.cosn.userinfo.secretId" = "xxx",
    "fs.cosn.userinfo.secretKey" = "yyy",
    "fs.cosn.bucket.endpoint_suffix" = "cos.ap-beijing.myqcloud.com"
    );
    ```

8. Export all data in the testTbl table to S3

    ```sql
    EXPORT TABLE testTbl TO "s3a://s3-package/export/"
    WITH BROKER "broker_name"
    (
    "fs.s3a.access.key" = "xxx",
    "fs.s3a.secret.key" = "yyy",
    "fs.s3a.endpoint" = "s3-ap-northeast-1.amazonaws.com"
    );
    ```

## keyword

EXPORT
