# EXPORT

## Description

This statement is used to export the data in a specified table to a specified location.

This is an asynchronous operation, which returns if the task is submitted successfully. After execution, you can use the SHOW EXPORT command to check progress.

Syntax:

```sql
EXPORT TABLE <table_name>
[PARTITION (p1[,p2])]
TO export_path
[opt_properties]
WITH BROKER
```

1. `table_name`

    The name of the table to be exported. Currently, this system only supports the export of tables with engine as OLAP and mysql.

2. `partition`

    You can export certain specified partitions of the specified table.

3. `export_path`

    The export path.

    If you need to export a directory, it must end with a slash. Otherwise, the part after the last slash will be identified as the prefix to the exported file.

4. `opt_properties`

     It is used to specify some special parameters.

     Syntax:

    ```sql
    [PROPERTIES ("key"="value", ...)]
    ```

    The following parameters can be specified:

    - `column_separator`: specifies the exported column separator, defaulting to `t`.
    - `line_delimiter`: specifies the exported line separator, defaulting to `\n`.
    - `exec_mem_limit`: specifies the upper limit of memory usage for export jobs on a single BE node, defaulting to 2 GB in bytes.
    - `timeout`：specifies the time-out period for export jobs, defaulting to 1 day in seconds.
    - `include_query_id`: specifies whether the exported file name contains query ID, defaulting to `true`.

5. `WITH BROKER`

    In StarRocks v2.4 and earlier, input `WITH BROKER "<broker_name>"` to specify the broker group you want to use. From StarRocks v2.5 onwards, you no longer need to specify a broker group, but you still need to retain the `WITH BROKER` keyword.

## Examples

1. Export all data from the testTbl table to HDFS

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/" WITH BROKER ("username"="xxx", "password"="yyy");
    ```

2. Export partitions p1 and p2 from the testTbl table to HDFS

    ```sql
    EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c/" WITH BROKER ("username"="xxx", "password"="yyy");
    ```

3. Export all data in the testTbl table to hdfs, using "," as column separator

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/" PROPERTIES ("column_separator"=",") WITH BROKER ("username"="xxx", "password"="yyy");
    ```

4. Export all data in the testTbl table to hdfs, using Hive custom separator "\x01" as column separator

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/" PROPERTIES ("column_separator"="\\x01") WITH BROKER;
    ```

5. Export all data in the testTbl table to hdfs, specifying the exported file prefix as testTbl_

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/testTbl_" WITH BROKE;
    ```

6. Export all data in the testTbl table to OSS

    ```sql
    EXPORT TABLE testTbl TO "oss://oss-package/export/"
    WITH BROKER
    (
    "fs.oss.accessKeyId" = "xxx",
    "fs.oss.accessKeySecret" = "yyy",
    "fs.oss.endpoint" = "oss-cn-zhangjiakou-internal.aliyuncs.com"
    );
    ```

7. Export all data in the testTbl table to COS

    ```sql
    EXPORT TABLE testTbl TO "cosn://cos-package/export/"
    WITH BROKER
    (
    "fs.cosn.userinfo.secretId" = "xxx",
    "fs.cosn.userinfo.secretKey" = "yyy",
    "fs.cosn.bucket.endpoint_suffix" = "cos.ap-beijing.myqcloud.com"
    );
    ```

8. Export all data in the testTbl table to S3

    ```sql
    EXPORT TABLE testTbl TO "s3a://s3-package/export/"
    WITH BROKER
    (
    "aws.s3.access_key" = "xxx",
    "aws.s3.secret_key" = "yyy",
    "aws.s3.endpoint" = "s3-ap-northeast-1.amazonaws.com"
    );
    ```
