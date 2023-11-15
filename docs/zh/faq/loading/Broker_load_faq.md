# Broker Load 常见问题

## 1. Broker Load 是否支持再次执行已经执行成功、处于 FINISHED 状态的导入作业？

Broker Load 不支持再次执行已经执行成功、处于 FINISHED 状态的导入作业。而且，为了保证数据不丢不重，每个执行成功的导入作业的标签 (Label) 均不可复用。可以使用 [SHOW LOAD](/sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) 语句查看历史的导入记录，找到想要再次执行的导入作业，复制作业信息，并修改作业标签后，重新创建一个导入作业并执行。

## 2. 通过 Broker Load 导入 HDFS 数据时，为什么数据的导入日期字段会出现异常，比正确的日期时间多加了 8 小时？这种情况应该怎么处理？

StarRocks 表在建表时设置的 `timezone` 为中国时区，创建 Broker Load 导入作业时设置的 `timezone` 也是中国时区，而服务器设置的是 UTC 时区。因此，日期字段在导入时，比正确的日期时间多加了 8 小时。为避免该问题，需要在建表时去掉 `timezone` 参数。

## 3. 通过 Broker Load 导入 ORC 格式的数据时，发生 "ErrorMsg: type:ETL_RUN_FAIL; msg:Cannot cast '<slot 6>' from VARCHAR to ARRAY<VARCHAR(30)>" 错误应该如何处理？

源数据文件和 Starrocks 表两侧的列名不一致，执行 `SET` 子句的时候系统内部会有一个类型推断，但是在调用 [cast](../../sql-reference/sql-functions/cast.md) 函数执行数据类型转换的时候失败了。解决办法是确保两侧的列名一致，这样就不需要 `SET` 子句，也就不会调用 cast 函数执行数据类型转换，导入就可以成功了。

## 4. 为什么 Broker Load 导入作业没报错，但是却查询不到数据？

Broker Load 是一种异步的导入方式，创建导入作业的语句没报错，不代表导入作业成功了。可以通过 [SHOW LOAD](/sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) 语句来查看导入作业的结果状态和 `errmsg` 信息，然后修改导入作业的参数配置后，再重试导入作业。

## 5. 导入报 "failed to send batch" 或 "TabletWriter add batch with unknown id" 错误应该如何处理？

该错误由数据写入超时而引起。需要修改[系统变量](/reference/System_variable.md) `query_timeout` 和 [BE 配置项](/administration/Configuration.md#配置-be-静态参数) `streaming_load_rpc_max_alive_time_sec` 的配置。

## 6. 导入报 "LOAD-RUN-FAIL; msg:OrcScannerAdapter::init_include_columns. col name = xxx not found" 错误应该如何处理？

如果导入的是 Parquet 或 ORC 格式的数据，检查文件头的列名是否与 StarRocks 表中的列名一致，例如:

```SQL
(tmp_c1,tmp_c2)
SET
(
   id=tmp_c2,
   name=tmp_c1
)
```

上述示例，表示将 Parquet 或 ORC 文件中以 `tmp_c1` 和 `tmp_c2` 为列名的列，分别映射到 StarRocks 表中的 `name` 和 `id` 列。如果没有使用 `SET` 子句，则以 `column_list` 参数中指定的列作为映射。具体请参见 [BROKER LOAD](/sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

> **注意**
>
> 如果导入的是 Apache Hive™ 版本直接生成的 ORC 文件，并且 ORC 文件中的表头是 `(_col0, _col1, _col2, ...)`，可能导致 "Invalid Column Name" 错误。这时候需要使用 `SET` 子句设置列转换规则。

## 7. 发生其他比如导入作业长时间没有结束等问题应该如何处理？

在 FE 上的日志文件 **fe.log** 中，根据导入作业的标签来搜索导入作业的 ID。然后，在 BE 上的日志文件 **be.INFO** 文件中，根据导入作业的 ID 来搜索上下文日志，进而查看具体原因。

## 8. 如何配置访问高可用 (HA) 模式下的 Apache HDFS 集群？

按照如下配置：

- `dfs.nameservices`：自定义 HDFS 集群的名称，如 `"dfs.nameservices" = "my_ha"`。

- `dfs.ha.namenodes.xxx`：自定义 NameNode 的名字，多个名称以逗号 (,) 分隔。其中 `xxx` 为 `dfs.nameservices` 中配置的 HDFS 集群的名称，如 `"dfs.ha.namenodes.my_ha" = "my_nn"`。

- `dfs.namenode.rpc-address.xxx.nn`：指定 NameNode 的 RPC 地址信息。其中 `nn` 表示 `dfs.ha.namenodes.xxx` 中自定义 NameNode 的名称，如 `"dfs.namenode.rpc-address.my_ha.my_nn" = "host:port"`。

- `dfs.client.failover.proxy.provider`：指定客户端连接 NameNode 的提供者，默认为 `org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider`。

示例如下：

```SQL
(
    "dfs.nameservices" = "my-ha",
    "dfs.ha.namenodes.my-ha" = "my-namenode1, my-namenode2",
    "dfs.namenode.rpc-address.my-ha.my-namenode1" = "nn1-host:rpc_port",
    "dfs.namenode.rpc-address.my-ha.my-namenode2" = "nn2-host:rpc_port",
    "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
)
```

高可用模式可以和简单认证、Kerberos 认证两种认证方式组合，进行 HDFS 集群访问。例如，通过简单认证访问高可用模式部署的 HDFS 集群，需要指定如下配置：

```SQL
(
    "username"="user",
    "password"="passwd",
    "dfs.nameservices" = "my-ha",
    "dfs.ha.namenodes.my-ha" = "my_namenode1, my_namenode2",
    "dfs.namenode.rpc-address.my-ha.my-namenode1" = "nn1-host:rpc_port",
    "dfs.namenode.rpc-address.my-ha.my-namenode2" = "nn2-host:rpc_port",
    "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
)
```

关于 HDFS 集群的配置可以写入 **hdfs-site.xml** 文件中。这样您在使用 Broker 程序读取 HDFS 集群的数据时，只需要填写集群的文件路径名和认证信息即可。

## 9. 如何配置 Hadoop ViewFS Federation？

需要将 ViewFs 相关的配置文件 `core-site.xml` 和 `hdfs-site.xml` 拷贝到 **broker/conf** 目录中。

如果有自定义的文件系统，需要将文件系统相关的 **.jar** 文件拷贝到 **broker/lib** 目录中。

## 10. 通过 Kerberos 认证访问 HDFS 集群时，报 "Can't get Kerberos realm" 错误应该如何处理？

首先检查是否所有的 Broker 所在的机器都配置了 **/etc/krb5.conf** 文件。

如果配置了仍然报错，需要在 Broker 的启动脚本中 `JAVA_OPTS` 变量的最后，加上 `-Djava.security.krb5.conf:/etc/krb5.conf`。
