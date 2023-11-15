# Use Flink connector to read data from StarRocks

This topic describes how to use the source function of flink-connector-starrocks to read data from StarRocks.

> If you need to use the sink function of flink-connector-starrocks to write data into StarRocks, see [Flink connector](../loading/Flink-connector-starrocks.md) in the Data Loading chapter.

## Introduction

You can use the source function of flink-connector-starrocks to read data from StarRocks. Different from the Flink JDBC connector, flink-connector-starrocks can read data from multiple StarRocks backends (BEs) in parallel, which significantly improves data reading efficiency.
Difference between the two connectors:

- flink-connector-starrocks: Flink first obtains the query plan from the frontend (FE), delivers the query plan as a parameter to BE nodes, and then obtains data results from BE nodes.

![flink-connector](../assets/5.3.2-1.png)

- Flink JDBC connector: Flink JDBC connector can only read data from the FE in a serial fashion. The data reading efficiency is low.

![JDBC connector](../assets/5.3.2-2.png)

## Procedure

### Step 1: Install flink-connector-starrocks

1. Select a flink-connector-starrocks version based on your Flink version and download the JAR package of [flink-connector-starrocks](https://github.com/StarRocks/flink-connector-starrocks/releases).
If you need to debug code, you can select the corresponding branch code and compile the code.
2. Place the downloaded or compiled JAR package in the `lib` directory of Flink.
3. Restart Flink.

### Step 2: Use flink-connector-starrocks to read StarRocks data

> The source function of flink-connector-starrocks cannot guarantee exactly-once semantics. If the reading task fails, you must repeat this step to create another reading task.

- If you use a Flink SQL client (recommended), you can read data from StarRocks by referring to the following command. For more information about the parameters in this command, see [Parameter description](https://docs.starrocks.com/en-us/main/unloading/Flink_connector#Parameter-description).

~~~SQL
-- Create a table in Flink based on the target StarRocks table and configure table properties (including information about flink-connector-starrocks, database, and table).

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

-- Execute an SQL statement to read data from StarRocks.

select date_1, smallint_1 from flink_test where char_1 `<>` 'A' and int_1 = -126;
~~~

- > Only some of the SQL statements can be used to read StarRocks data, such as

`select ... from table_name where ...`. Aggregate functions except for COUNT are not supported.

- > Predicate pushdown is supported. Predicates can be automatically pushed down when you execute SQL statements, such as the filter conditions in the preceding example.

> `char_1 < > 'A' and int_1 = -126` will be pushed down to the connector and converted into a statement suitable for querying StarRocks data. Additional configuration is not required.

- If you use Flink DataStream, you must add a dependency before you use flash-connector-starrocks to read StarRocks data.

Add the following dependency to the `pom.xml` file.

> Replace x.x.x with the latest version number of flink-connector-starrocks. You can click [version information](https://search.maven.org/search?q=g:com.starrocks)to obtain the latest version number.

~~~SQL
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
~~~

Use flink-connector-starrocks to read data from StarRocks by referring to the following sample code. The following table describes the parameters in these commands.

~~~Java
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
~~~

## Parameter description

| Parameter                   | Required | Data type | Description                                                  |
| --------------------------- | -------- | --------- | ------------------------------------------------------------ |
| connector                   | Yes      | String    | The connector. Set the value to starrocks.                   |
| scan-url                    | Yes      | String    | The scan URL of the FE node. The URL is used to access the FE node through the web server. The format is < FE IP address >: < FE HTTP port >. The port number defaults to 8030. Separate multiple addresses with commas, for example, 192.168.xxx.xxx:8030, 192.168.xxx.xxx:8030. |
| jdbc-url                    | Yes      | String    | The JDBC URL of the FE node. This URL is used to access the MySQL client on the FE node. The format is jdbc:mysql://< FE IP address >:< FE query port >. The port number defaults to 9030. |
| username                    | Yes      | String    | The username in StarRocks. The username must have read permissions to the target database and table. For more information, see [User permissions](../administration/User_privilege.md). |
| password                    | Yes      | String    | The password of the username.                                |
| database-name               | Yes      | String    | The name of the StarRocks database.                          |
| table-name                  | Yes      | String    | The name of the StarRocks table.                             |
| scan.connect.timeout-ms     | No       | String    | The maximum duration for flink-connector-starrocks to connect to StarRocks, in milliseconds. The default value is 1000. If this duration is exceeded, the connection times out and an error occurs. |
| scan.params.keep-alive-min  | No       | String    | The keep-alive duration of the query task, in minutes. The default value is 10. we recommend that you set this parameter to a value greater than or equal to 5. |
| scan.params.query-timeout-s | No       | String    | The timeout duration of the query task, in seconds. The default value is 600. If the query result is not returned within this duration, the query task is stopped. |
| scan.params.mem-limit-byte  | No       | String    | The maximum memory space allowed for a single query in the BE node, in bytes. The default value is 1073741824 (1 GB). |
| scan.max-retries            | No       | String    | The maximum number of retries when a query fails. The default value is 1. An error occurs if this value is exceeded. |

## Data type mapping between Flink and StarRocks

> The data type mapping in the following table applies only to reading StarRocks data from Flink. For the data type mapping for writing Flink data to StarRocks, see [Flink connector](../loading/Flink-connector-starrocks.md) in the Data Loading chapter.

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

## What to do next

After data is read from StarRocks, you can use [Flink WEBUI](https://nightlies.apache.org/flink/flink-docs-master/docs/try-flink/flink-operations-playground/#flink-webui) to observe the details of the reading task. For example, the **Metrics** page of Flink WEBUI displays the number of data rows that are read (`totalScannedRows`).
