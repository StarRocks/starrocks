# Flink Connector

本文介绍 Flink 如何通过 flink-connector-starrocks 的 source 功能读取 StarRocks 数据。

> 如果 Flink 需要通过 flink-connector-starrocks 的 sink 功能，将数据写入至 StarRocks，请参见数据导入章节的 [Flink connector](../loading/Flink-connector-starrocks.md)。

## 功能简介

Flink 可以通过 flink-connector-starrocks 的 source 功能读取 StarRocks 的数据。相较于 Flink 官方提供的 Flink JDBC connector，flink-connector-starrocks 的 source 功能具备并行读取 StarRocks 的 BE 节点数据的能力，大大提高了数据读取效率。以下是两种连接器的实现方案对比。

- flink-connector-starrocks 的实现方案：Flink 先从 FE 节点获取查询计划（Query Plan），Flink 再将获取到的查询计划作为参数，下发至 BE 节点，然后获取 BE 节点返回的数据。

   ![asset](../assets/5.2.1.png)

- Flink JDBC connector 的实现方案：Flink JDBC connector 仅能从 FE 单点上串行读取数据，数据读取效率较低。

   ![asset](../assets/5.2.2.png)

## 操作步骤

### 步骤一：准备flink-connector-starrocks

1. 根据 Flink 的版本，选择对应版本。下载 JAR 包 [flink-connector-starrocks](https://github.com/StarRocks/flink-connector-starrocks/releases)。
2. 如需调试代码，可选择对应分支代码自行编译
3. 将下载或者编译的 JAR 包放在 Flink 的 lib 目录中。
4. 重启 Flink。

### 步骤二：调用 flink-connector-starrocks ，读取 StarRocks 数据

> flink-connector-starrocks 的 source 功能暂时无法保证 exactly-once 语义。如果读取任务失败，您需要重复本步骤，再次创建读取任务。

- 如您使用 Flink SQL 客户端（推荐），则需要参考如下命令，调用 flink-connector-starrocks，读取 StarRocks 的数据。相关参数说明，请参见[参数说明](#参数说明)。

   ```SQL
   -- 根据 StarRocks 的表，创建表和配置属性（包括 flink-connector-starrocks 和库表的信息）。
   CREATE TABLE flink_test (
       date_1 DATE,
       datetime_1 TIMESTAMP(6),
       char_1 CHAR(20),
       varchar_1 VARCHAR,
       boolean_1 BOOLEAN,
       tinyint_1 TINYINT,
       smallint_1 SMALLINT,
       int_1 INT,
       bigint_1 BIGINT,
       largeint_1 STRING,
       float_1 FLOAT,
       double_1 DOUBLE,FLI
       decimal_1 DECIMAL(27,9)
   ) WITH (
      'connector'='starrocks',
      'scan-url'='192.168.xxx.xxx:8030,192.168.xxx.xxx:8030',
      'jdbc-url'='jdbc:mysql://192.168.xxx.xxx:9030',
      'username'='root',
      'password'='xxxxxx',
      'database-name'='flink_test',
      'table-name'='flink_test'
   );
   -- 使用 SQL 语句读取 StarRocks 数据。
   select date_1, smallint_1 from flink_test where char_1 <> 'A' and int_1 = -126;
   ```

   > - 仅支持使用部分 SQL 语句读取 StarRocks 数据，如`select ... from table_name where ...`。暂不支持除 COUNT 外的聚合函数。
   > - 支持谓词下推。使用 SQL 语句时，支持自动进行谓词下推，比如上述例子中的过滤条件 `char_1 <> 'A' and int_1 = -126`，会下推到 connector 中转换成适用于 StarRocks 的语句进行查询，不需要额外配置。

- 如您使用 Flink DataStream ，则需要先添加依赖，然后调用 flink-connector-starrocks，读取 StarRocks 的数据。

1. 在 pom.xml 文件中添加如下依赖。

   > x.x.x需要替换为 flink-connector-starrocks 的最新版本号，您可以单击[版本信息](https://search.maven.org/search?q=g:com.starrocks)获取。

   ```SQL
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

2. 参考如下示例代码，调用 flink-connector-starrocks，读取 StarRocks 的数据。相关参数说明，请参见[参数说明](#参数说明)。

   ```Java
   StarRocksSourceOptions options = StarRocksSourceOptions.builder()
           .withProperty("scan-url", "192.168.xxx.xxx:8030,192.168.xxx.xxx:8030")
           .withProperty("jdbc-url", "jdbc:mysql://192.168.xxx.xxx:9030")
           .withProperty("username", "root")
           .withProperty("password", "xxxxxx")
           .withProperty("table-name", "flink_test")
           .withProperty("database-name", "test")
           .withProperty("cloumns", "char_1, date_1")        
           .withProperty("filters", "int_1 = 10")
           .build();
   TableSchema tableSchema = TableSchema.builder()
           .field("date_1", DataTypes.DATE())
           .field("datetime_1", DataTypes.TIMESTAMP(6))
           .field("char_1", DataTypes.CHAR(20))
           .field("varchar_1", DataTypes.STRING())
           .field("boolean_1", DataTypes.BOOLEAN())
           .field("tinyint_1", DataTypes.TINYINT())
           .field("smallint_1", DataTypes.SMALLINT())
           .field("int_1", DataTypes.INT())
           .field("bigint_1", DataTypes.BIGINT())
           .field("largeint_1", DataTypes.STRING())
           .field("float_1", DataTypes.FLOAT())
           .field("double_1", DataTypes.DOUBLE())
           .field("decimal_1", DataTypes.DECIMAL(27, 9))
           .build();
   StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
   env.addSource(StarRocksSource.source(options, tableSchema)).setParallelism(5).print();
   env.execute("StarRocks flink source");
   ```

## 参数说明

| 参数                        | 是否必填 | 数据类型 | 描述                                                         |
| --------------------------- | -------- | -------- | ------------------------------------------------------------ |
| connector                   | 是       | String   | 固定为 starrocks。                                          |
| scan-url                    | 是       | String   | FE 节点的连接地址，用于通过 Web 服务器访问 FE 节点。 具体格式为< FE 节点的 IP 地址>:< FE 的 http_port>，端口号默认为8030。多个地址之间用英文半角逗号分隔。例如192.168.xxx.xxx:8030,192.168.xxx.xxx:8030。 |
| jdbc-url                    | 是       | String   | FE 节点的连接地址，用于访问 FE 节点上的 MySQL 客户端。具体格式为 jdbc:mysql://< FE 节点的 IP 地址>:< FE 的 query_port>，端口号默认为9030。 |
| username                    | 是       | String   | StarRocks 中的用户名称。需具备目标数据库表的读权限。用户权限说明，请参见[用户权限](../administration/User_privilege.md)。 |
| password                    | 是       | String   | StarRocks 的用户密码。                                       |
| database-name               | 是       | String   | StarRocks 数据库的名称。                                     |
| table-name                  | 是       | String   | StarRocks 数据表的名称。                                     |
| scan.connect.timeout-ms     | 否       | String   | flink-connector-starrocks 连接 StarRocks 的时间上限，单位为毫秒，默认值为1000。超过该时间上限，则将报错。 |
| scan.params.keep-alive-min  | 否       | String   | 查询任务的保活时间，单位为分钟。默认值为10，建议取值大于等于5。      |
| scan.params.query-timeout-s | 否       | String   | 查询任务的超时时间，单位为秒，默认值为600。如果超过该时间，仍未返回查询结果，则停止查询任务。  |
| scan.params.mem-limit-byte  | 否       | String   | BE 节点中单个查询的内存上限，单位为字节，默认值为1073741824（1G）。 |
| scan.max-retries            | 否       | String   | 查询失败时的最大重试次数，默认值为1。超过该数量上限，则将报错。 |

## Flink 与 StarRocks 的数据类型映射关系

> 该数据类型映射关系仅适用于 Flink 读取 StarRocks 数据。如需要查看 Flink 将数据写入至 StarRocks 的数据类型映射关系，请参见数据导入章节的 [Flink connector](../loading/Flink-connector-starrocks.md)。

| StarRocks  | Flink     |
| ---------- | --------- |
| NULL       | NULL      |
| BOOLEAN    | BOOLEAN   |
| TINYINT    | TINYINT   |
| SMALLINT   | SMALLINT  |
| INT        | INT       |
| BIGINT     | BIGINT    |
| LARGEINT   | STRING    |
| FLOAT      | FLOAT     |
| DOUBLE     | DOUBLE    |
| DATE       | DATE      |
| DATETIME   | TIMESTAMP |
| DECIMAL    | DECIMAL   |
| DECIMALV2  | DECIMAL   |
| DECIMAL32  | DECIMAL   |
| DECIMAL64  | DECIMAL   |
| DECIMAL128 | DECIMAL   |
| CHAR       | CHAR      |
| VARCHAR    | STRING    |

## 后续步骤

Flink 成功读取 StarRocks 数据后，您可以使用 Flink 官方的 [Flink WEBUI](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/try-flink/flink-operations-playground/#flink-webui-界面) 界面观察读取任务。比如 Flink WEBUI 的 Metrics 页面会显示成功读取的数据行数（totalScannedRows）。
