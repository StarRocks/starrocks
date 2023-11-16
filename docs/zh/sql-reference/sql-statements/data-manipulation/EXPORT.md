---
displayed_sidebar: "Chinese"
---

# EXPORT

## description

该语句用于将指定表的数据导出到指定位置。

该功能通过 broker 进程实现。对于不同的目的存储系统，需要部署不同的 broker。可以通过 SHOW BROKER 查看已部署的 broker。

这是一个异步操作，任务提交成功则返回。执行后可使用 SHOW EXPORT 命令查看进度。

语法：

```sql
EXPORT TABLE table_name
[PARTITION (p1[,p2])]
TO export_path
[opt_properties]
broker;
```

1. table_name

    当前要导出的表的表名，目前支持engine为olap和mysql的表的导出。

2. partition

    可以只导出指定表的某些指定分区

3. export_path

    导出的路径。目前不能导出到本地，需要导出到 broker。如果是目录，需要以斜线结尾。否则最后一个斜线后面的部分会作为导出文件的前缀。如不指定文件名前缀，文件名前缀默认为 **data_**。

4. opt_properties

    用于指定一些特殊参数。

    语法：

    ```sql
    [PROPERTIES ("key"="value", ...)]
    ```

    可以指定如下参数：

    ```plain text
    column_separator: 指定导出的列分隔符，默认为\t。
    line_delimiter: 指定导出的行分隔符，默认为\n。
    load_mem_limit: 导出在单个 BE 节点的内存使用上限，默认为 2GB，单位为字节。
    timeout：导入作业的超时时间，默认为1天，单位是「秒」。
    include_query_id: 导出文件名中是否包含 query id，默认为 true。
    ```

5. broker

    用于指定导出使用的broker

    语法：

    ```sql
    WITH BROKER broker_name ("key"="value"[,...])
    ```

    这里需要指定具体的broker name, 以及所需的broker属性

    对于不同存储系统对应的 broker，这里需要输入的参数不同。具体参数可以参阅：`help broker load` 中 broker 所需属性。

## example

1. 将 testTbl 表中的所有数据导出到 hdfs 上

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    ```

2. 将 testTbl 表中的分区p1,p2导出到 hdfs 上

    ```sql
    EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c/" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    ```

3. 将 testTbl 表中的所有数据导出到 hdfs 上，以","作为列分隔符

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/" PROPERTIES ("column_separator"=",") WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
    ```

4. 将 testTbl 表中的所有数据导出到 hdfs 上，以 Hive 默认分隔符"\x01"作为列分隔符

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/" PROPERTIES ("column_separator"="\\x01") WITH BROKER "broker_name";
    ```

5. 将 testTbl 表中的所有数据导出到 hdfs 上，指定导出文件前缀为 testTbl_

    ```sql
    EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c/testTbl_" WITH BROKER "broker_name";
    ```

6. 将 testTbl 表中的所有数据导出到 OSS 上

    ```sql
    EXPORT TABLE testTbl TO "oss://oss-package/export/"
    WITH BROKER "broker_name"
    (
    "fs.oss.accessKeyId" = "xxx",
    "fs.oss.accessKeySecret" = "yyy",
    "fs.oss.endpoint" = "oss-cn-zhangjiakou-internal.aliyuncs.com"
    );
    ```

7. 将 testTbl 表中的所有数据导出到 COS 上

    ```sql
    EXPORT TABLE testTbl TO "cosn://cos-package/export/"
    WITH BROKER "broker_name"
    (
    "fs.cosn.userinfo.secretId" = "xxx",
    "fs.cosn.userinfo.secretKey" = "yyy",
    "fs.cosn.bucket.endpoint_suffix" = "cos.ap-beijing.myqcloud.com"
    );
    ```

8. 将 testTbl 表中的所有数据导出到 S3 上

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
