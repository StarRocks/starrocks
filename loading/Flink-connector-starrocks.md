# 设计背景

flink的用户想要将数据sink到StarRocks当中，但是flink官方只提供了flink-connector-jdbc, 不足以满足导入性能要求，为此我们新增了一个flink-connector-starrocks，内部实现是通过缓存并批量由stream load导入。

## 使用方式

[源码地址](https://github.com/StarRocks/flink-connector-starrocks)

将以下内容加入`pom.xml`:

点击 [版本信息](https://search.maven.org/search?q=g:com.starrocks) 查看页面Latest Version信息，替换下面x.x.x内容

```xml
<dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>flink-connector-starrocks</artifactId>
    <!-- for flink-1.14 -->
    <version>x.x.x_flink-1.14_2.11</version>
    <version>x.x.x_flink-1.14_2.12</version>
    <!-- for flink-1.13 -->
    <version>x.x.x_flink-1.13_2.11</version>
    <version>x.x.x_flink-1.13_2.12</version>
    <!-- for flink-1.12 -->
    <version>x.x.x_flink-1.12_2.11</version>
    <version>x.x.x_flink-1.12_2.12</version>
    <!-- for flink-1.11 -->
    <version>x.x.x_flink-1.11_2.11</version>
    <version>x.x.x_flink-1.11_2.12</version>
</dependency>
```

使用方式如下：

```scala
// -------- sink with raw json string stream --------
fromElements(new String[]{
    "{\"score\": \"99\", \"name\": \"stephen\"}",
    "{\"score\": \"100\", \"name\": \"lebron\"}"
}).addSink(
    StarRocksSink.sink(
        // the sink options
        StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", "jdbc:mysql://fe1_ip:query_port,fe2_ip:query_port,fe3_ip:query_port?xxxxx")
            .withProperty("load-url", "fe1_ip:http_port;fe2_ip:http_port;fe3_ip:http_port")
            .withProperty("username", "xxx")
            .withProperty("password", "xxx")
            .withProperty("table-name", "xxx")
            .withProperty("database-name", "xxx")
            .withProperty("sink.properties.format", "json")
            .withProperty("sink.properties.strip_outer_array", "true")
            .build()
    )
);


// -------- sink with stream transformation --------
class RowData {
    public int score;
    public String name;
    public RowData(int score, String name) {
        ......
    }
}
fromElements(
    new RowData[]{
        new RowData(99, "stephen"),
        new RowData(100, "lebron")
    }
).addSink(
    StarRocksSink.sink(
        // the table structure
        TableSchema.builder()
            .field("score", DataTypes.INT())
            .field("name", DataTypes.VARCHAR(20))
            .build(),
        // the sink options
        StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", "jdbc:mysql://fe1_ip:query_port,fe2_ip:query_port,fe3_ip:query_port?xxxxx")
            .withProperty("load-url", "fe1_ip:http_port;fe2_ip:http_port;fe3_ip:http_port")
            .withProperty("username", "xxx")
            .withProperty("password", "xxx")
            .withProperty("table-name", "xxx")
            .withProperty("database-name", "xxx")
            .withProperty("sink.properties.column_separator", "\\x01")
            .withProperty("sink.properties.row_delimiter", "\\x02")
            .build(),
        // set the slots with streamRowData
        (slots, streamRowData) -> {
            slots[0] = streamRowData.score;
            slots[1] = streamRowData.name;
        }
    )
);
```

或者：

```scala
// create a table with `structure` and `properties`
// Needed: Add `com.starrocks.connector.flink.table.StarRocksDynamicTableSinkFactory` to: `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`
tEnv.executeSql(
    "CREATE TABLE USER_RESULT(" +
        "name VARCHAR," +
        "score BIGINT" +
    ") WITH ( " +
        "'connector' = 'starrocks'," +
        "'jdbc-url'='jdbc:mysql://fe1_ip:query_port,fe2_ip:query_port,fe3_ip:query_port?xxxxx'," +
        "'load-url'='fe1_ip:http_port;fe2_ip:http_port;fe3_ip:http_port'," +
        "'database-name' = 'xxx'," +
        "'table-name' = 'xxx'," +
        "'username' = 'xxx'," +
        "'password' = 'xxx'," +
        "'sink.buffer-flush.max-rows' = '1000000'," +
        "'sink.buffer-flush.max-bytes' = '300000000'," +
        "'sink.buffer-flush.interval-ms' = '5000'," +
        "'sink.properties.column_separator' = '\\x01'," +
        "'sink.properties.row_delimiter' = '\\x02'," +
        "'sink.max-retries' = '3'" +
        "'sink.properties.*' = 'xxx'" + // stream load properties like `'sink.properties.columns' = 'k1, v1'`
    ")"
);
```

其中Sink选项如下：

| Option | Required | Default | Type | Description |
|  :-----:  | :-----:  | :-----:  | :-----:  | :-----:  |
| connector | YES | NONE | String |**starrocks**|
| jdbc-url | YES | NONE | String | this will be used to execute queries in starrocks. |
| load-url | YES | NONE | String | **fe_ip:http_port;fe_ip:http_port** separated with '**;**', which would be used to do the batch sinking. |
| database-name | YES | NONE | String | starrocks database name |
| table-name | YES | NONE | String | starrocks table name |
| username | YES | NONE | String | starrocks connecting username |
| password | YES | NONE | String | starrocks connecting password |
| sink.semantic | NO | **at-least-once** | String | **at-least-once** or **exactly-once**(**flush at checkpoint only** and options like **sink.buffer-flush.*** won't work either). |
| sink.buffer-flush.max-bytes | NO | 94371840(90M) | String | the max batching size of the serialized data, range: **[64MB, 10GB]**. |
| sink.buffer-flush.max-rows | NO | 500000 | String | the max batching rows, range: **[64,000, 5000,000]**. |
| sink.buffer-flush.interval-ms | NO | 300000 | String | the flushing time interval, range: **[1000ms, 3600000ms]**. |
| sink.max-retries | NO | 1 | String | max retry times of the stream load request, range: **[0, 10]**. |
| sink.connect.timeout-ms | NO | 1000 | String | Timeout in millisecond for connecting to the `load-url`, range: **[100, 60000]**. |
| sink.properties.* | NO | NONE | String | the stream load properties like **'sink.properties.columns' = 'k1, k2, k3'**. |

### 注意事项

- 支持exactly-once的数据sink保证，需要外部系统的 two phase commit 机制。由于 StarRocks 无此机制，我们需要依赖flink的checkpoint-interval在每次checkpoint时保存批数据以及其label，在checkpoint完成后的第一次invoke中阻塞flush所有缓存在state当中的数据，以此达到精准一次。但如果StarRocks挂掉了，会导致用户的flink sink stream 算子长时间阻塞，并引起flink的监控报警或强制kill。

- 默认使用csv格式进行导入，用户可以通过指定`'sink.properties.row_delimiter' = '\\x02'`（此参数自 StarRocks-1.15.0 开始支持）与`'sink.properties.column_separator' = '\\x01'`来自定义行分隔符与列分隔符。

- 如果遇到导入停止的 情况，请尝试增加flink任务的内存。

- 如果代码运行正常且能接收到数据，但是写入不成功时请确认当前机器能访问BE的http_port端口，这里指能ping通集群show backends显示的ip:port。举个例子：如果一台机器有外网和内网ip，且FE/BE的http_port均可通过外网ip:port访问，集群里绑定的ip为内网ip，任务里loadurl写的FE外网ip:http_port,FE会将写入任务转发给BE内网ip:port,这时如果Client机器ping不通BE的内网ip就会写入失败。

## 使用 Flink-connector 写入实现 MySQL 数据同步

### 基本原理

通过Flink-cdc和StarRocks-migrate-tools（简称smt）可以实现MySQL数据的秒级同步。

![MySQL同步](../assets/4.9.2.png)

如图所示，Smt可以根据MySQL和StarRocks的集群信息和表结构自动生成source table和sink table的建表语句。  
通过Flink-cdc-connector消费MySQL的binlog，然后通过Flink-connector-starrocks写入StarRocks。

### 使用说明

1. 下载 [Flink](https://flink.apache.org/downloads.html), 推荐使用1.13，最低支持版本1.11。
2. 下载 [Flink CDC connector](https://github.com/ververica/flink-cdc-connectors/releases)，请注意下载对应Flink版本的Flink-MySQL-CDC。
3. 下载 [Flink StarRocks connector](https://github.com/StarRocks/flink-connector-starrocks)，请注意1.13版本和1.11/1.12版本使用不同的connector.
4. 复制 `flink-sql-connector-mysql-cdc-xxx.jar`, `flink-connector-starrocks-xxx.jar` 到 `flink-xxx/lib/`
5. 下载 [smt.tar.gz](https://www.starrocks.com/en-US/download/community)
6. 解压并修改配置文件
  `Db` 需要修改成MySQL的连接信息。  
  `be_num` 需要配置成StarRocks集群的节点数（这个能帮助更合理的设置bucket数量）。  
  `[table-rule.1]` 是匹配规则，可以根据正则表达式匹配数据库和表名生成建表的SQL，也可以配置多个规则。  
  `flink.starrocks.*` 是StarRocks的集群配置信息，参考Flink.  
  
    ```bash
    [db]
    host = 192.168.1.1
    port = 3306
    user = root
    password =  

    [other]
    # number of backends in StarRocks
    be_num = 3
    # `decimal_v3` is supported since StarRocks-1.18.1
    use_decimal_v3 = false
    # file to save the converted DDL SQL
    output_dir = ./result


    [table-rule.1]
    # pattern to match databases for setting properties
    database = ^console_19321.*$
    # pattern to match tables for setting properties
    table = ^.*$

    ############################################
    ### flink sink configurations
    ### DO NOT set `connector`, `table-name`, `database-name`, they are auto-generated
    ############################################
    flink.starrocks.jdbc-url=jdbc:mysql://192.168.1.1:9030
    flink.starrocks.load-url= 192.168.1.1:8030
    flink.starrocks.username=root
    flink.starrocks.password=
    flink.starrocks.sink.properties.column_separator=\x01
    flink.starrocks.sink.properties.row_delimiter=\x02
    flink.starrocks.sink.buffer-flush.interval-ms=15000
    ```

7. 执行starrocks-migrate-tool，所有建表语句都生成在result目录下

    ```bash
    $./starrocks-migrate-tool
    $ls result
    flink-create.1.sql    smt.tar.gz              starrocks-create.all.sql
    flink-create.all.sql  starrocks-create.1.sql
    ```

8. 生成StarRocks的表结构

    ```bash
    Mysql -hxx.xx.xx.x -P9030 -uroot -p < starrocks-create.1.sql
    ```

9. 生成Flink table并开始同步

    ```bash
    bin/sql-client.sh -f flink-create.1.sql
    ```

    这个执行以后同步任务会持续执行
    > 如果是Flink 1.13之前的版本可能无法直接执行脚本，需要逐行提交
    注意 记得打开MySQL binlog

10. 观察任务状况
  
    ```bash
    bin/flink list 
    ```

  如果有任务请查看log日志，或者调整conf中的系统配置中内存和slot。

### 注意事项

1. 如果有多组规则，需要给每一组规则匹配database，table和 flink-connector的配置

    ```bash
    [table-rule.1]
    # pattern to match databases for setting properties
    database = ^console_19321.*$
    # pattern to match tables for setting properties
    table = ^.*$

    ############################################
    ### flink sink configurations
    ### DO NOT set `connector`, `table-name`, `database-name`, they are auto-generated
    ############################################
    flink.starrocks.jdbc-url=jdbc:mysql://192.168.1.1:9030
    flink.starrocks.load-url= 192.168.1.1:8030
    flink.starrocks.username=root
    flink.starrocks.password=
    flink.starrocks.sink.properties.column_separator=\x01
    flink.starrocks.sink.properties.row_delimiter=\x02
    flink.starrocks.sink.buffer-flush.interval-ms=15000

    [table-rule.2]
    # pattern to match databases for setting properties
    database = ^database2.*$
    # pattern to match tables for setting properties
    table = ^.*$

    ############################################
    ### flink sink configurations
    ### DO NOT set `connector`, `table-name`, `database-name`, they are auto-generated
    ############################################
    flink.starrocks.jdbc-url=jdbc:mysql://192.168.1.1:9030
    flink.starrocks.load-url= 192.168.1.1:8030
    flink.starrocks.username=root
    flink.starrocks.password=
    # 如果导入数据不方便选出合适的分隔符可以考虑使用Json格式，但是会有一定的性能损失,使用方法：用以下参数替换flink.starrocks.sink.properties.column_separator和flink.starrocks.sink.properties.row_delimiter参数
    flink.starrocks.sink.properties.strip_outer_array=true
    flink.starrocks.sink.properties.format=json
    ~~~

2. Flink.starrocks.sink 的参数可以参考[上文](##使用方式)，比如可以给不同的规则配置不同的导入频率等参数。

3. 针对分库分表的大表可以单独配置一个规则，比如：有两个数据库 edu_db_1，edu_db_2，每个数据库下面分别有course_1，course_2 两张表，并且所有表的数据结构都是相同的，通过如下配置把他们导入StarRocks的一张表中进行分析。

    ```bash
    [table-rule.3]
    # pattern to match databases for setting properties
    database = ^edu_db_[0-9]*$
    # pattern to match tables for setting properties
    table = ^course_[0-9]*$

    ############################################
    ### flink sink configurations
    ### DO NOT set `connector`, `table-name`, `database-name`, they are auto-generated
    ############################################
    flink.starrocks.jdbc-url=jdbc:mysql://192.168.1.1:9030
    flink.starrocks.load-url= 192.168.1.1:8030
    flink.starrocks.username=root
    flink.starrocks.password=
    flink.starrocks.sink.properties.column_separator=\x01
    flink.starrocks.sink.properties.row_delimiter=\x02
    flink.starrocks.sink.buffer-flush.interval-ms=5000
    ```

    这样会自动生成一个多对一的导入关系，在StarRocks默认生成的表名是 course__auto_shard，也可以自行在生成的配置文件中修改。

4. 如果在sql-client中命令行执行建表和同步任务，需要做对'\'字符进行转义

    ```bash
    'sink.properties.column_separator' = '\\x01'
    'sink.properties.row_delimiter' = '\\x02'  
    ```

5. 如何开启MySQL binlog  
  修改/etc/my.cnf
  
    ```bash
    #开启binlog日志
    log-bin=/var/lib/mysql/mysql-bin

    #log_bin=ON
    ##binlog日志的基本文件名
    #log_bin_basename=/var/lib/mysql/mysql-bin
    ##binlog文件的索引文件，管理所有binlog文件
    #log_bin_index=/var/lib/mysql/mysql-bin.index
    #配置serverid
    server-id=1
    binlog_format = row
    ```
  
  重启mysqld，然后可以通过 SHOW VARIABLES LIKE 'log_bin'; 确认是否已经打开。
