# External tables

StarRocks supports access to other data sources by using external tables. External tables are created based on data tables that are stored in other data sources. StarRocks only stores the metadata of the data tables. You can use external tables to directly query data in other data sources. StarRocks supports the following data sources: MySQL, Elasticsearch, Hive, StarRocks, Apache Iceberg, and Apache Hudi. **Currently, you can only insert data from another StarRocks cluster into the current StarRocks cluster. You cannot read data from it. For data sources other than StarRocks, you can only read data from these data sources.**

## MySQL external table

In the star schema, data is generally divided into dimension tables and fact tables. Dimension tables have less data but involve UPDATE operations. Currently, StarRocks does not support direct UPDATE operations (update can be implemented by using the unique key model). In some scenarios, you can store dimension tables in MySQL for direct data read.

To query MySQL data, you must create an external table in StarRocks and map it to the table in your MySQL database. You need to specify the MySQL connection information when creating the table.

~~~sql
CREATE EXTERNAL TABLE mysql_external_table
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=mysql
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "3306",
    "user" = "mysql_user",
    "password" = "mysql_passwd",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test"
);
~~~

Parameters:

* **host**: the connection address of the MySQL database
* **port**: the port number of the MySQL database
* **user**: the username to log in to MySQL
* **password**: the password to log in to MySQL
* **database**: the name of the MySQL database
* **table**: the name of the table in the MySQL database

## StarRocks external table

From StarRocks 1.19 onwards, StarRocks allows you to use a StarRocks external table to write data from one StarRocks cluster to another. This achieves read-write separation and provides better resource isolation. You can first create a destination table in the destination StarRocks cluster. Then, in the source StarRocks cluster, you can create a StarRocks external table that has the same schema as the destination table and specify the information of the destination cluster and table in the `PROPERTIES` field.

Data can be written from  a source cluster to a  destination cluster by using INSERT INTO statement to write into a StarRocks external table. It can help realize the following goals:

* Data synchronization between StarRocks clusters.
* Read-write separation. Data is written to the source cluster, and data changes from the source cluster are synchronized to the destination cluster, which provides query services.

The following code shows how to create a destination table and an external table.

~~~SQL
# Create a destination table in the destination StarRocks cluster.
CREATE TABLE t
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=olap
DISTRIBUTED BY HASH(k1) BUCKETS 10;

# Create an external table in the source StarRocks cluster.
CREATE EXTERNAL TABLE external_t
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=olap
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "9020",
    "user" = "user",
    "password" = "passwd",
    "database" = "db_test",
    "table" = "t"
);

# Write data from a source cluster to a destination cluster by writing data into the StarRocks external table. The second statement is recommended for the production environment.
insert into external_t values ('2020-10-11', 1, 1, 'hello', '2020-10-11 10:00:00');
insert into external_t select * from other_table;
~~~

Parameters：

* **EXTERNAL:** This keyword indicates that the table to be created is an external table.
* **host:** This parameter specifies the IP address of the leader FE node of the destination StarRocks cluster.
* **port:**  This parameter specifies the RPC port of the leader FE node of the destination StarRocks cluster. You can set this parameter based on the rpc_port configuration in the **fe/fe.conf** file.
* **user:** This parameter specifies the username used to access the destination StarRocks cluster.
* **password:** This parameter specifies the password used to access the destination StarRocks cluster.
* **database:** This parameter specifies the database to which the destination table belongs.
* **table:** This parameter specifies the name of the destination table.

The following limits apply when you use a StarRocks external table:

* You can only run the INSERT INTO and SHOW CREATE TABLE commands on a StarRocks external table. Other data writing methods are not supported. In addition, you cannot query data from a StarRocks external table or perform DDL operations on the external table.
* The syntax of creating an external table is the same as creating a normal table, but the column names and other information in the external table must be the same as the destination table.
* The external table synchronizes table metadata from the destination table every 10 seconds. If a DDL operation is performed on the destination table, there may be a delay for data synchronization between the two tables.

## Elasticsearch external table

StarRocks and Elasticsearch are two popular analytics systems. StarRocks is performant in large-scale distributed computing. Elasticsearch is ideal for full-text search. StarRocks combined with Elasticsearch can deliver a more complete OLAP solution.

### Example of creating an Elasticsearch external table

~~~sql
CREATE EXTERNAL TABLE elastic_search_external_table
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=ELASTICSEARCH
PARTITION BY RANGE(k1)
()
PROPERTIES (
    "hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "doc"
);
~~~

Parameters

* **host**: The connection address of the Elasticsearch cluster. You can specify one or more addresses. StarRocks can parse the Elasticsearch version and index shard allocation from this address.
* **user**: The username of the Elasticsearch cluster with **basic authentication** enabled. Make sure you have the access to `/*cluster/state/*nodes/http` and the index.
* **password**: The password of the Elasticsearch cluster.
* **index**: The name of the Elasticsearch index that corresponds to the table in StarRocks. It can be an alias.
* **type**: the type of the index. Default value: doc.
* **transport**: This parameter is reserved. Default value: http.
* **es.nodes.wan.only**: indicates whether StarRocks only uses the addresses specified by `hosts` to access the Elasticsearch cluster and fetch data.

  * true: StarRocks only uses the addresses specified by `hosts` to access the Elasticsearch cluster and fetch data and does not sniff data nodes which shards of the Elasticsearch index reside in. If StarRocks cannot access the addresses of the data nodes inside the Elasticsearch cluster, you need to set this parameter to `true`.
  * false: default value. StarRocks uses the addresses specified by `host` to sniff data nodes on which the shards of the Elasticsearch cluster indexes are located. After StarRocks generates a query execution plan, the relevant BEs directly access the data nodes inside the Elasticsearch cluster to fetch data from the shards of indexes. If StarRocks can access the addresses of the data nodes inside the Elasticsearch cluster, we recommend that you retain the default value `false`.

### Predicate pushdown

StarRocks supports predicate pushdown. Filters can be pushed down to Elasticsearch for execution, which improves query performance. The following table lists the operators that support predicate pushdown.

|   SQL syntax  |   ES syntax  |
| :---: | :---: |
|  =   |  term query   |
|  in   |  terms query   |
|  \>=,  <=, >, <   |  range   |
|  and   |  bool.filter   |
|  or   |  bool.should   |
|  not   |  bool.must_not   |
|  not in   |  bool.must_not + terms   |
|  esquery   |  ES Query DSL  |

### Example

The **esquery function** is used to push down queries **that cannot be expressed in SQL** (such as match and geoshape) to Elasticsearch for filtering. The first parameter in the esquery function is used to associate an index. The second parameter is a JSON expression of basic Query DSL, which is enclosed in brackets {}. **The JSON expression must have but only one root key**, such as match, geo_shape, or bool.

* match query

~~~sql
select * from es_table where esquery(k4, '{
    "match": {
       "k4": "StarRocks on elasticsearch"
    }
}');
~~~

* geo-related query

~~~sql
select * from es_table where esquery(k4, '{
  "geo_shape": {
     "location": {
        "shape": {
           "type": "envelope",
           "coordinates": [
              [
                 13,
                 53
              ],
              [
                 14,
                 52
              ]
           ]
        },
        "relation": "within"
     }
  }
}');
~~~

* bool query

~~~sql
select * from es_table where esquery(k4, ' {
     "bool": {
        "must": [
           {
              "terms": {
                 "k1": [
                    11,
                    12
                 ]
              }
           },
           {
              "terms": {
                 "k2": [
                    100
                 ]
              }
           }
        ]
     }
  }');
~~~

### Note

* Elasticsearch earlier than 5.x scans data in a different way than that later than 5.x. Currently, **only versions later than 5.x** are supported.
* Elasticsearch clusters with HTTP basic authentication enabled are supported.
* Querying data from StarRocks may not be as fast as directly querying data from Elasticsearch, such as count-related queries. The reason is that Elasticsearch directly reads the metadata of target documents without the need to filter the real data, which accelerates the count query.

## External table for databases that support JDBC drivers

Since 2.3.0 version, StarRocks provides external tables to query databases that support JDBC drivers. This way, you can analyze the data of such databases in a blazing fast manner without the need to import the data into StarRocks.  This topic describes how to create an external table in StarRocks and query data in databases that support JDBC drivers.

### Prerequisites

When a JDBC resource is created, and a JDBC external table is queried for the first time, the FEs and BEs need to download the JDBC driver. Therefore, the machines on which the FEs and BEs are located must have access to the download URL of the JDBC driver during these two phases. The download URL is specified by the `driver_url` parameter when the JDBC resource is created.

### Create and manage JDBC resources

#### Create a JDBC resource

Before you create an external table to query data from a database, you need to create a JDBC resource in StarRocks to manage the connection information of the database. The database must support the JDBC driver and is referred as the "target database". After creating the resource, you can use it to create external tables.

Execute the following statement to create a JDBC resource named `jdbc0`:

~~~SQL
create external resource jdbc0
properties (
    "type"="jdbc",
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);
~~~

The required parameters in `properties` are as follows:

* `type`: the type of the resource. Set the value to `jdbc`.

* `user`: the username that is used to connect to the target database.

* `password`: the password that is used to connect to the target database.

* `jdbc_uri`: the URL that the JDBC driver uses to connect to the target database. The URL format must satisfy the database URL syntax. For the URL syntax of some common databases, visit the official websites of [MySQL](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html)、[Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/data-sources-and-URLs.html#GUID-6D8EFA50-AB0F-4A2B-88A0-45B4A67C361E)、[PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html)、[SQL Server](https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16).

> Note: The URL must include the name of the target database. For example, in the preceding code example, `jdbc_test` is the name of the target database that you want to connect.

* `driver_url`:  the download URL of the JDBC driver used by the target database.

* `driver_class`: the class name of the JDBC driver used by the target database. The class names of common xxx are as follows:
  * MySQL: com.mysql.jdbc.Driver (MySQL 5.x and earlier), com.mysql.cj.jdbc.Driver (MySQL 6.x and later)
  * SQL Server: com.microsoft.sqlserver.jdbc.SQLServerDriver
  * Oracle: oracle.jdbc.driver.OracleDriver
  * PostgreSQL: org.postgresql.Driver

After the resource is created, the FEs download the JDBC driver JAR package by using the URL that is specified in the `driver_url` parameter, generates a checksum, and saves the checksum to verify the correctness of the JDBC driver JAR package downloaded by BEs.

> Note: If the download of  the JDBC driver JAR package fails, the creation of the resource also fails.

When BEs query the JDBC external table for the first time and find that the corresponding JDBC driver JAR package does not exist on their machines, BEs download the JDBC driver JAR package by using the URL that is specified in the `driver_url` parameter, and all JDBC driver JAR packages are saved in the `${STARROCKS_HOME}/lib/jdbc_drivers` directory.

#### View JDBC resources

Execute the following statement to view all JDBC resources in StarRocks:

~~~SQL
SHOW RESOURCES;
~~~

> Note: The `ResourceType` column is `jdbc`.

#### Delete a JDBC resource

Execute the following statement to delete the JDBC resource named `jdbc0`:

~~~SQL
DROP RESOURCE "jdbc0";
~~~

> Note: After a JDBC resource is deleted, all JDBC external tables that are created by using that JDBC resource are unavailable. However, the data in the target database is not lost. If you still need to use StarRocks to query data in the target database, you can create the JDBC resource and the JDBC external tables again.

### Create a database

Execute the following statement to create and access a database named `jdbc_test` in StarRocks:

~~~SQL
CREATE DATABASE jdbc_test; 
USE jdbc_test; 
~~~

> Note: The database name that you specify in the preceding statement does not need to be same as the name of the target database.

### Create a JDBC external table

Execute the following statement to create a JDBC external table named `jdbc_tbl` in the database `jdbc_test`:

~~~SQL
create external table jdbc_tbl (
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=jdbc 
properties (
    "resource"="jdbc0",
    "table"="dest_tbl"
);
~~~

The required parameters in `properties` are as follows:

* `resource`: the name of the JDBC resource used to create the external table.

* `table`：the target table name in the database.

For supported data types and data type mapping between StarRocks and target databases, see [Data type mapping](./External_table.md#Data type mapping).

> Note:
>
> * Indexes are not supported.
> * You cannot use PARTITION BY or DISTRIBUTED BY to specify data distribution rules.

### Query a JDBC external table

Before you query JDBC external tables, you must execute the following statement to enable the Pipeline engine:

~~~SQL
set enable_pipeline_engine=true;
~~~

> Note: If the Pipeline engine is already enabled, you can skip this step.

Execute the following statement to query the data in the target database by using JDBC external tables.

~~~SQL
select * from JDBC_tbl;
~~~

StarRocks supports predicate pushdown by pushing down filter conditions to the target table. Executing filter conditions as close as possible to the data source can improve query performance. Currently, StarRocks can push down operators, including the binary comparison operators (`>`, `>=`, `=`, `<`, and `<=`), `IN`, `IS NULL`, and `BETWEEN ... AND ...` . However, StarRocks can not push down functions.

### Data type mapping

Currently, StarRocks can only query data of basic types in the target database, such as NUMBER, STRING, TIME, and DATE. If the ranges of data values in the target database are not supported by StarRocks, the query reports an error.

The mapping between the target database and StarRocks varies based on the type of the target database.

#### **MySQL and StarRocks**

| MySQL        | StarRocks |
| ------------ | --------- |
| BOOLEAN      | BOOLEAN   |
| TINYINT      | TINYINT   |
| SMALLINT     | SMALLINT  |
| MEDIUMINTINT | INT       |
| BIGINT       | BIGINT    |
| FLOAT        | FLOAT     |
| DOUBLE       | DOUBLE    |
| DECIMAL      | DECIMAL   |
| CHAR         | CHAR      |
| VARCHAR      | VARCHAR   |
| DATE         | DATE      |
| DATETIME     | DATETIME  |

#### **Oracle and StarRocks**

| Oracle          | StarRocks |
| --------------- | --------- |
| CHAR            | CHAR      |
| VARCHARVARCHAR2 | VARCHAR   |
| DATE            | DATE      |
| SMALLINT        | SMALLINT  |
| INT             | INT       |
| BINARY_FLOAT    | FLOAT     |
| BINARY_DOUBLE   | DOUBLE    |
| DATE            | DATE      |
| DATETIME        | DATETIME  |
| NUMBER          | DECIMAL   |

#### **PostgreSQL and StarRocks**

| PostgreSQL          | StarRocks |
| ------------------- | --------- |
| SMALLINTSMALLSERIAL | SMALLINT  |
| INTEGERSERIAL       | INT       |
| BIGINTBIGSERIAL     | BIGINT    |
| BOOLEAN             | BOOLEAN   |
| REAL                | FLOAT     |
| DOUBLE PRECISION    | DOUBLE    |
| DECIMAL             | DECIMAL   |
| TIMESTAMP           | DATETIME  |
| DATE                | DATE      |
| CHAR                | CHAR      |
| VARCHAR             | VARCHAR   |
| TEXT                | STRING    |

#### **SQL Server and StarRocks**

| SQL Server        | StarRocks |
| ----------------- | --------- |
| BOOLEAN           | BOOLEAN   |
| TINYINT           | TINYINT   |
| SMALLINT          | SMALLINT  |
| INT               | INT       |
| BIGINT            | BIGINT    |
| FLOAT             | FLOAT     |
| REAL              | DOUBLE    |
| DECIMALNUMERIC    | DECIMAL   |
| CHAR              | CHAR      |
| VARCHAR           | VARCHAR   |
| DATE              | DATE      |
| DATETIMEDATETIME2 | DATETIME  |

### Limits

* When you create JDBC external tables, you cannot create indexes on the tables or use PARTITION BY and DISTRIBUTED BY to specify data distribution rules for the tables.

* When you query JDBC external tables, StarRocks cannot push down functions to the tables.

* You cannot use the INSERT INTO ... SELECT ... statement to load the data of the target database into StarRocks.

## Hive external table

### Create a Hive resource

A Hive resource corresponds to a Hive cluster. You must configure the Hive cluster used by StarRocks, such as the Hive metastore address. You must specify the Hive resource that is used by the Hive external table.

* Create a Hive resource named hive0.

~~~sql
CREATE EXTERNAL RESOURCE "hive0"
PROPERTIES (
  "type" = "hive",
  "hive.metastore.uris" = "thrift://10.10.44.98:9083"
);
~~~

* View the resources created in StarRocks.

~~~sql
SHOW RESOURCES;
~~~

* Delete the resource named `hive0`.

~~~sql
DROP RESOURCE "hive0";
~~~

You can modify `hive.metastore.uris` of a Hive resource in StarRocks 2.3 and later versions. For more information, see [ALTER RESOURCE](../sql-reference/sql-statements/data-definition/ALTER%20RESOURCE.md).

### Create a database

~~~sql
CREATE DATABASE hive_test;
USE hive_test;
~~~

### Create a Hive external table

Syntax

~~~sql
CREATE EXTERNAL TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
PROPERTIES (
  "key" = "value"
);
~~~

Example: Create the external table `profile_parquet_p7` under the `rawdata` database in the Hive cluster corresponding to the `hive0` resource.

~~~sql
CREATE EXTERNAL TABLE `profile_wos_p7` (
  `id` bigint NULL,
  `first_id` varchar(200) NULL,
  `second_id` varchar(200) NULL,
  `p__device_id_list` varchar(200) NULL,
  `p__is_deleted` bigint NULL,
  `p_channel` varchar(200) NULL,
  `p_platform` varchar(200) NULL,
  `p_source` varchar(200) NULL,
  `p__city` varchar(200) NULL,
  `p__province` varchar(200) NULL,
  `p__update_time` bigint NULL,
  `p__first_visit_time` bigint NULL,
  `p__last_seen_time` bigint NULL
) ENGINE=HIVE
PROPERTIES (
  "resource" = "hive0",
  "database" = "rawdata",
  "table" = "profile_parquet_p7"
);
~~~

Description:

* Columns in the external table
  * The column names must be the same as column names in the Hive table.
  * The column order **does not need to be** the same as column order in the Hive table.
  * You can select only **some of the columns in the Hive table**, but you must select all the **partition key columns**.
  * Partition key columns of an external table do not need to be specified by using `partition by`. They must be defined in the same description list as other columns. You do not need to specify partition information. StarRocks will automatically synchronize this information from the Hive table.
  * Set `ENGINE` to HIVE.
* PROPERTIES:
  * **hive.resource**: the Hive resource that is used.
  * **database**: the Hive database.
  * **table**: the table in Hive. **view** is not supported.
* The following table describes the column data type mapping between Hive and StarRocks.

    |  Column type of Hive   |  Column type of StarRocks   | Description |
    | --- | --- | ---|
    |   INT/INTEGER  | INT    |
    |   BIGINT  | BIGINT    |
    |   TIMESTAMP  | DATETIME    | Precision and time zone information will be lost when you convert TIMESTAMP data into DATETIME data. You need to convert TIMESTAMP data into DATETIME data that does not have the time zone offset based on the time zone in sessionVariable. |
    |  STRING  | VARCHAR   |
    |  VARCHAR  | VARCHAR   |
    |  CHAR  | CHAR   |
    |  DOUBLE | DOUBLE |
    | FLOATE | FLOAT|

Note:

* Hive table schema changes **will not be automatically synchronized to the external table**. You must create another Hive external table in StarRocks.
* Currently, the supported Hive storage formats are Parquet, ORC, and CSV.

 > If the storage format is CSV, quotation marks cannot be used as escape characters.

* The SNAPPY and LZ4 compression formats are supported.

### Use a Hive external table

Query the total number of rows of `profile_wos_p7`.

~~~sql
select count(*) from profile_wos_p7;
~~~

### Configuration

* The path of the FE configuration file is `fe/conf`, to which the configuration file can be added if you need to customize the Hadoop cluster. For example: HDFS cluster uses a highly available nameservice, you need to put `hdfs-site.xml` under `fe/conf`.  If HDFS is configured with viewfs, you need to put the `core-site.xml` under `fe/conf`.
* The path of the BE configuration file is `be/conf`, to which configuration file can be added if you need to customize the Hadoop cluster. For example, HDFS cluster using a highly available nameservice, you need to put `hdfs-site.xml` under `be/conf`. If HDFS is configured with viewfs, you need to put `core-site.xml` under `be/conf`.
* The machine where BE is located need to configure JAVA_HOME as a jdk environment rather than a jre environment
* kerbero supports:
  1. To log in with `kinit -kt keytab_path principal` to all FE/BE machines, you need to have access to Hive and HDFS. The kinit command login is only good for a period of time and needs to be put into crontab to be executed regularly.
  2. Put `hive-site.xml/core-site.xml/hdfs-site.xml` under `fe/conf`, and put `core-site.xml/hdfs-site.xml` under `be/conf`.
  3. Add **Djava.security.krb5.conf:/etc/krb5.conf** to the **JAVA_OPTS/JAVA_OPTS_FOR_JDK_9** option of the **fe/conf/fe.conf** file.  **/etc/krb5.conf** is the path of the **krb5.conf** file. You can adjust the path based on your operating system.
  4. When you add a Hive resource, you must pass in a domain name to `hive.metastore.uris`. In addition, you must add the mapping between Hive/HDFS domain names and IP addresses in the **/etc/hosts** file*.*

* Configure support for AWS S3: Add the following configuration to `fe/conf/core-site.xml` and `be/conf/core-site.xml`.

   ~~~XML
   <configuration>
      <property>
         <name>fs.s3a.access.key</name>
         <value>******</value>
      </property>
      <property>
         <name>fs.s3a.secret.key</name>
         <value>******</value>
      </property>
      <property>
         <name>fs.s3a.endpoint</name>
         <value>s3.us-west-2.amazonaws.com</value>
      </property>
      <property>
      <name>fs.s3a.connection.maximum</name>
      <value>500</value>
      </property>
   </configuration>
   ~~~

   1. `fs.s3a.access.key`: the AWS access key ID.
   2. `fs.s3a.secret.key`: the AWS secret key.
   3. `fs.s3a.endpoint`: the AWS S3 endpoint to connect to.
   4. `fs.s3a.connection.maximu``m`: the maximum number of concurrent connections from StarRocks to S3. If an error `Timeout waiting for connection from poll` occurs during a query, you can set this parameter to a larger value.

### Metadata caching strategy

* Hive partitions information and the related file information are cached in StarRocks. The cache is refreshed at intervals specified by `hive_meta_cache_refresh_interval_s`. The default value is 7200.  `hive_meta_cache_ttl_s` specifies the timeout duration of the cache and the default value is 86400.
  * The cached data can also be refreshed manually.
    1. If a partition is added or deleted from a table in Hive, you must run the `REFRESH EXTERNAL TABLE hive_t` command to refresh the table metadata cached in StarRocks. `hive_t` is the name of the Hive external table in StarRocks.
    2. If data in some Hive partitions is updated, you must refresh the cached data in StarRocks by running the `REFRESH EXTERNAL TABLE hive_t PARTITION ('k1=01/k2=02', 'k1=03/k2=04')` command. `hive_t` is the name of the Hive external table in StarRocks. `'k1=01/k2=02'` and `'k1=03/k2=04'` are the names of Hive partitions whose data is updated.
    3. When you run `REFRESH EXTERNAL TABLE hive_t`, StarRocks first checks if the column information of the Hive external table is the same as the column information of the Hive table returned by the Hive Metastore. If the schema of the Hive table changes, such as adding columns or remove columns, StarRocks synchronizes the changes to the Hive external table. After synchronization, the column order of the Hive external table remains the same as the column order of the Hive table, with the partition column being the last column.
* When Hive data is stored in the Parquet, ORC, and CSV format, you can synchronize schema changes (such as ADD COLUMN and REPLACE COLUMN) of a Hive table to a Hive external table in StarRocks 2.3 and later versions.

## Apache Iceberg external table

To query the data in Iceberg, you need to create an Iceberg external table in StarRocks. When you create the table, you need to map the external table to the Iceberg table you want to query.

### Before you begin

Make sure that StarRocks has permissions to access the metadata service (such as Hive metastore), file system (such as HDFS), and object storage system (such as Amazon S3 and Alibaba Cloud Object Storage Service) used by Apache Iceberg.

### Precautions

* The Iceberg external table can be used to query only the following types of data:
  * Versions 1 (Analytic Data Tables) tables. Versions 2 (Row-level Deletes) tables are not supported. For the differences between Versions 1 tables and Versions 2 tables, see [Iceberg Table Spec](https://iceberg.apache.org/spec/).
  * Tables that are compressed in gzip (default format), Zstd, LZ4, or Snappy format.
  * Files that are stored in Parquet or ORC format.

* Iceberg external tables in StarRocks 2.3 and later versions support synchronizing schema changes of an Iceberg table while Iceberg external tables in versions earlier than StarRocks 2.3 do not. If the schema of an Iceberg table changes, you must delete the corresponding external table and create a new one.

### Procedure

#### Step 1: Create an Iceberg resource

Before you create an Iceberg external table, you must create an Iceberg resource in StarRocks. The resource is used to manage the Iceberg access information. Additionally, you also need to specify this resource in the statement that is used to create the external table. You can create a resource based on your business requirements:

* If the metadata of an Iceberg table is obtained from a Hive metastore, you can create a resource and set the catalog type to `HIVE`.

* If the metadata of an Iceberg table is obtained from other services, you need to create a custom catalog. Then create a resource and set the catalog type to `CUSTOM`.

##### Create a resource whose catalog type is `HIVE`

For example, create a resource named `iceberg0` and set the catalog type to `HIVE`.

~~~SQL
CREATE EXTERNAL RESOURCE "iceberg0" 
PROPERTIES ( "type" = "iceberg", "starrocks.catalog-type"="HIVE", "iceberg.catalog.hive.metastore.uris"="thrift://192.168.0.81:9083" 
);
~~~

The following table describes the related parameters.

| **Parameter**                       | **Description**                                              |
| ----------------------------------- | ------------------------------------------------------------ |
| type                                | The resource type. Set the value to `iceberg`.               |
| starrocks.catalog-type              | The catalog type of the resource. Both Hive catalog and custom catalog are supported. If you specify a Hive catalog, set the value to `HIVE`.If you specify a custom catalog, set the value to `CUSTOM`. |
| iceberg.catalog.hive.metastore.uris | The URI of the Hive metastore. The parameter value is in the following format: `thrift://< IP address of Iceberg metadata >:< port number >`. The port number defaults to 9083. Apache Iceberg uses a Hive catalog to access the Hive metastore and then queries the metadata of Iceberg tables. |

##### Create a resource whose catalog type is `CUSTOM`

A custom catalog needs to inherit the abstract class BaseMetastoreCatalog, and you need to implement the IcebergCatalog interface. For more information about how to create a custom catalog, see [IcebergHiveCatalog](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/main/java/com/starrocks/external/iceberg/IcebergHiveCatalog.java). Additionally, the class name of a custom catalog cannot be duplicated with the name of the class that already exists in StarRock. After the catalog is created, package the catalog and its related files, and place them under the **fe/lib** path of each frontend (FE). Then restart each FE. After you complete the preceding operations, you can create a resource whose catalog is a custom catalog.

For example, create a resource named `iceberg1` and set the catalog type to `CUSTOM`.

~~~SQL
CREATE EXTERNAL RESOURCE "iceberg1" 
PROPERTIES ( "type" = "iceberg", "starrocks.catalog-type"="CUSTOM", "iceberg.catalog-impl"="com.starrocks.IcebergCustomCatalog" 
);
~~~

The following table describes the related parameters.

| **Parameter**          | **Description**                                              |
| ---------------------- | ------------------------------------------------------------ |
| type                   | The resource type. Set the value to `iceberg`.               |
| starrocks.catalog-type | The catalog type of the resource. Both Hive catalog and custom catalog are supported. If you specify a Hive catalog, set the value to `HIVE`.If you specify a custom catalog, set the value to `CUSTOM`. |
| iceberg.catalog-impl   | The fully qualified class name of the custom catalog. FEs search for the catalog based on this name. If the catalog contains custom configuration items, you must add them to the `PROPERTIES` parameter as key-value pairs when you create an Iceberg external table. |

You can modify `hive.metastore.uris` and `iceberg.catalog-impl`of a Iceberg resource in StarRocks 2.3 and later versions. For more information, see [ALTER RESOURCE](../sql-reference/sql-statements/data-definition/ALTER%20RESOURCE.md).

##### View Iceberg resources

~~~SQL
SHOW RESOURCES;
~~~

##### Drop an Iceberg resource

For example, drop a resource named `iceberg0`.

~~~SQL
DROP RESOURCE "iceberg0";
~~~

Dropping an Iceberg resource makes all external tables that reference this resource unavailable. However, the corresponding data in Apache Iceberg is not deleted. If you still need to query the data in Apache Iceberg, create a new resource and a new external table.

#### Step 2: (Optional) Create a database

For example, create a database named `iceberg_test` in StarRocks.

~~~SQL
CREATE DATABASE iceberg_test; 
USE iceberg_test; 
~~~

> Note: The name of the database in StarRocks can be different from the name of the database in Apache Iceberg.

#### Step 3: Create an Iceberg external table

For example, create an Iceberg external table named `iceberg_tbl` in the database `iceberg_test`.

~~~SQL
CREATE EXTERNAL TABLE `iceberg_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=ICEBERG 
PROPERTIES ( 
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table" 
); 
~~~

The following table describes the related parameters.

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| ENGINE        | The engine name. Set the value to `ICEBERG`.                 |
| resource      | The name of the Iceberg resource that the external table references. |
| database      | The name of the database to which the Iceberg table belongs. |
| table         | The name of the Iceberg table.                               |

> Note:
   >
   > * The name of the external table can be different from the name of the Iceberg table.
   >
   > * The column names of the external table must be the same as those in the Iceberg table. The column order of the two tables can be different.

If you define configuration items in the custom catalog and want configuration items to take effect when you query data, you can add the configuration items to the `PROPERTIES` parameter as key-value pairs when you create an external table. For example, if you define a configuration item `custom-catalog.properties` in the custom catalog, you can run the following command to create an external table.

~~~SQL
CREATE EXTERNAL TABLE `iceberg_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=ICEBERG 
PROPERTIES ( 
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table",
    "custom-catalog.properties" = "my_property"
); 
~~~

When you create an external table, you need to specify the data types of columns in the external table based on the data types of columns in the Iceberg table. The following table shows the mapping of column data types.

| **Iceberg table** | **Iceberg external table** |
| ----------------- | -------------------------- |
| BOOLEAN           | BOOLEAN                    |
| INT               | TINYINT / SMALLINT / INT   |
| LONG              | BIGINT                     |
| FLOAT             | FLOAT                      |
| DOUBLE            | DOUBLE                     |
| DECIMAL(P, S)     | DECIMAL                    |
| DATE              | DATE / DATETIME            |
| TIME              | BIGINT                     |
| TIMESTAMP         | DATETIME                   |
| STRING            | STRING / VARCHAR           |
| UUID              | STRING / VARCHAR           |
| FIXED(L)          | CHAR                       |
| BINARY            | VARCHAR                    |

StarRocks does not support querying Iceberg data whose data type is TIMESTAMPTZ, STRUCT, LIST, and MAP.

#### Step 4: Query the data in Apache Iceberg

After an external table is created, you can query the data in Apache Iceberg by using the external table.

~~~SQL
select count(*) from iceberg_tbl;
~~~
