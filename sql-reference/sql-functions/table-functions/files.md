# FILES

## 功能

从外部数据源读取数据文件以进行直接查询或通过 [INSERT](../../sql-statements/data-manipulation/insert.md) 进行数据导入。该功能自 v3.1.0 起支持。

目前 FILES() 函数支持以下数据源和文件格式：

- **数据源：**
  - AWS S3
  - HDFS
- **文件格式：**
  - Parquet
  - ORC

## 语法

```SQL
FILES("key" = "value"...)
```

## 参数说明

所有参数均为 `"key" = "value"` 形式的参数对。

| **参数**                       | **说明**                                                     |
| ------------------------------ | ------------------------------------------------------------ |
| path                           | 用于访问数据文件的 URI。示例：如使用 AWS S3：`s3://testbucket/parquet/test.parquet`如使用 HDFS：`hdfs:///test/parquet/test.orc` |
| format                         | 数据文件的格式。有效值：`parquet` 和 `orc`。                 |
| aws.s3.access_key              | 用于指定访问 AWS S3 存储空间的 Access Key。                  |
| aws.s3.secret_key              | 用于指定访问 AWS S3 存储空间的 Secret Key。                  |
| aws.s3.region                  | 用于指定需访问的 AWS S3 存储空间的地区，如 `us-west-1`。     |
| hadoop.security.authentication | 用于指定待访问 HDFS 集群的认证方式。有效值：`simple` 和 `kerberos`。默认值：`simple`。`simple` 表示简单认证，即无认证。`kerberos` 表示 Kerberos 认证。 |
| username                       | 用于访问 HDFS 集群中 NameNode 节点的用户名。                 |
| password                       | 用于访问 HDFS 集群中 NameNode 节点的密码。                   |
| kerberos_principal             | 用于指定 Kerberos 的用户或服务 (Principal)。每个 Principal 在 HDFS 集群内唯一，由如下三部分组成：<ul><li>`username` 或 `servicename`：HDFS 集群中用户或服务的名称。</li><li>`instance`：HDFS 集群要认证的节点所在服务器的名称，用来保证用户或服务全局唯一。比如，HDFS 集群中有多个 DataNode 节点，各节点需要各自独立认证。</li><li>`realm`：域，必须全大写。</li></ul>示例：`nn/zelda1@ZELDA.COM`。 |
| kerberos_keytab                | 用于指定 Kerberos 的 Key Table（简称为“keytab”）文件的路径。 |
| kerberos_keytab_content        | 用于指定 Kerberos 中 keytab 文件的内容经过 Base64 编码之后的内容。该参数与 `kerberos_keytab` 参数二选一配置。 |

## 示例

示例一：查询 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet/par-dup.parquet** 中的数据

```Plain
MySQL > SELECT * FROM FILES(
     "path" = "s3://inserttest/parquet/par-dup.parquet",
     "format" = "parquet",
     "aws.s3.access_key" = "XXXXXXXXXX",
     "aws.s3.secret_key" = "YYYYYYYYYY",
     "aws.s3.region" = "ap-southeast-1"
);
+------+---------------------------------------------------------+
| c1   | c2                                                      |
+------+---------------------------------------------------------+
|    1 | {"1": "key", "1": "1", "111": "1111", "111": "aaaa"}    |
|    2 | {"2": "key", "2": "NULL", "222": "2222", "222": "bbbb"} |
+------+---------------------------------------------------------+
2 rows in set (22.335 sec)
```

示例二：将 AWS S3 存储桶 `inserttest` 内 Parquet 文件 **parquet****/insert_wiki_edit_append.parquet** 中的数据插入至表 `insert_wiki_edit` 中：

```Plain
MySQL > INSERT INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "ap-southeast-1"
);
Query OK, 2 rows affected (23.03 sec)
{'label':'insert_d8d4b2ee-ac5c-11ed-a2cf-4e1110a8f63b', 'status':'VISIBLE', 'txnId':'2440'}
```
