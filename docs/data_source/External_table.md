# External tables

StarRocks supports access to other data sources by using external tables. External tables are created based on data tables that are stored in other data sources. StarRocks only stores the metadata of the data tables. You can use external tables to directly query data in other data sources. StarRocks supports the following data sources: MySQL, Elasticsearch, Hive, StarRocks, Apache Iceberg, and Apache Hudi. **Currently, you can only write data from another StarRocks cluster into the current StarRocks cluster. You cannot read data from it. For data sources other than StarRocks, you can only read data from these data sources.**

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
|  Parameter  | Description   |
| :---: |     :---:     |
|  host |  The connection address of the Elasticsearch cluster. You can specify one or more addresses. StarRocks can parse the Elasticsearch version and index shard allocation from this address.   |
|  user |  The username of the Elasticsearch cluster with **basic authentication** enabled. Make sure you have the access to `/*cluster/state/*nodes/http` and the index.   |
|  password |  The password of the Elasticsearch cluster.   |
|  index |  The name of the Elasticsearch index that corresponds to the table in StarRocks. It can be an alias.   |
|  type  |  he type of the index. Default value: doc.   |
|  transport |  This parameter is reserved. Default value: http.   |

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

## Apache Iceberg external table

StarRocks allows you to query data in Apache Iceberg by using an external table, which achieves blazing-fast analytics on data lakes. This topic describes how to use an external table in StarRocks to query Apache Iceberg data.

### Prerequisites

StarRocks has permissions to access the Hive metastore, HDFS cluster, and object storage bucket that corresponds to Apache Iceberg.

### Precautions

* The Iceberg external table is read-only and can only be used for data queries.

* Only Iceberg V1 (copy on write) tables are supported. Iceberg V2 (merge on read) tables are not supported. For the differences between V1 and V2 tables, visit the [official website of Apache Iceberg](https://iceberg.apache.org/#spec/#format-versioning).

* The compression formats of Iceberg data files must be GZIP (default value), ZSTD, LZ4, and SNAPPY.

* The Iceberg catalog type must be a Hive catalog. The data storage format must be Parquet or ORC.

* Currently, StarRocks cannot synchronize [schema evolution](https://iceberg.apache.org/#evolution#schema-evolution) from Apache Iceberg. If the schema of the source Iceberg table changes, you must delete the Iceberg external table that corresponds to the source table and create another one in StarRocks.

### Procedure

#### Step 1: Create and manage Iceberg resources

Before you create a database and Iceberg external table in StarRocks, you must create an Iceberg resource in StarRocks. The resource manages the connection information with the Iceberg data source. After the resource is created, you can create an Iceberg external table.

* If the metadata of an Iceberg table is obtained from a Hive metastore, you can create a resource and set the catalog type to `HIVE`.

* If the metadata of an Iceberg table is obtained from other services, you need to create a custom catalog. Then create a resource and set the catalog type to `CUSTOM`.

##### Create a resource whose catalog type is `HIVE`

For example, create a resource named `iceberg0` and set the catalog type to `HIVE`.

~~~SQL
CREATE EXTERNAL RESOURCE "iceberg0" 

PROPERTIES ( 

"type" = "iceberg", 

"starrocks.catalog-type"="HIVE", 

"iceberg.catalog.hive.metastore.uris"="thrift://192.168.0.81:9083" 

);
~~~

|  Parameter  |   Description   |
| --- |   ---  |
|   type  |   The resource type. Set the value to `iceberg`.    |
|   starrocks.catalog-type  |  The catalog type. Only the Hive catalog is supported. The value is `HIVE`.    |
|   iceberg.catalog.hive.metastore.uris  |  The thrift URI of the Hive metastore. Apache Iceberg uses the Hive catalog to access the Hive metastore and create and manage tables. You must pass in the thrift URI of the Hive metastore. The parameter value is in the following format: thrift://< Hive metadata IP address >:< Port number >. The port number defaults to 9083.   |

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

##### View Iceberg resources

~~~SQL
SHOW RESOURCES;
~~~

##### Drop an Iceberg resource

For example, drop a resource named `iceberg0`.

~~~SQL
DROP RESOURCE "iceberg0";
~~~

> Dropping an Iceberg resource makes all Iceberg external tables that correspond to this resource unavailable. However, data in Apache Iceberg will not be lost. If you still need to query Iceberg data in StarRocks, create another Iceberg resource, database, and external table.

#### Step 2: Create an Iceberg database

Run the following command to create an Iceberg database named `iceberg_test` in StarRocks.

~~~SQL
CREATE DATABASE iceberg_test; 



USE iceberg_test; 
~~~

> The database name in StarRocks can be different from the name of the source database in Iceberg.

#### Step 3: Create an Iceberg external table

Run the following command to create an Iceberg external table named `iceberg_tbl` in the Iceberg database `iceberg_test`.

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

The following table describes related parameters.
|  Parameter  |  Description |
| --- | ---|
|  ENGINE  |  The engine name. Set the value to `ICEBERG`. |
|  resource  |  The name of the Iceberg resource in StarRocks. |
|  database  |  The name of the database in Iceberg. |
|  table  |  The name of the data table in Iceberg. |

* The name of the external table can be different from the name of the Iceberg data table.

* The column names of the external table must be the same as those in the Iceberg data table. The column order can be different.

* You can select all or part of the columns in the Iceberg data table based on your business needs. The following table provides the mapping between data types supported by StarRocks and Iceberg.

|  Apache Iceberg  |  StarRocks |
| --- | ---|
|  BOOLEAN  |  BOOLEAN |
|  INT  |  TINYINT, SMALLINT, INT |
|  LONG  |  BIGINT |
|  FLOAT  |  FLOAT |
|  DOUBLE  |  DOUBLE |
|  DECIMAL(P,S)  |  DECIMAL |
|  DATE  |  DATE, DATETIME |
|  TIME  |  BIGINT |
|  TIMESTAMP  |  DATETIME |
|  STRING  |  STRING, VARCHAR |
|  UUID  |  STRING, VARCHAR |
|  FIXED(L)  |  CHAR |
|  BINARY  |  VARCHAR |

> Currently, StarRocks does not support using an Iceberg external table to query Iceberg data whose data type is TIMESTAMPTZ, STRUCT, LIST, or MAP.

#### Step 4: Query Iceberg data

After the Iceberg external table is created, you can run the following command to directly query data in Apache Iceberg without having to import Iceberg data into StarRocks.

~~~SQL
select count(*) from iceberg_tbl;
~~~

## Hudi external table

StarRocks allows you to query data from Hudi data lakes by using Hudi external tables, thus facilitating blazing-fast data lake analytics. This topic describes how to create a Hudi external table in your StarRocks cluster and use the Hudi external table to query data from a Hudi data lake.

### Before you begin

Make sure that your StarRocks cluster is granted access to the Hive metastore, HDFS cluster, or bucket with which you can register Hudi tables.

### Precautions

* Hudi external tables for Hudi are read-only and can be used only for queries.

* StarRocks supports Hudi Copy on Write (CoW) tables but not Hudi Merge on Read (MoR) tables. For information about the differences between CoW and MoR, see [Table & Query Types](https://hudi.apache.org/docs/table_types/).

* StarRocks supports the following compression formats for Hudi files: gzip, zstd, LZ4, and Snappy. The default compression format for Hudi files is gzip.

* StarRocks cannot synchronize schema changes from Hudi managed tables. For more information, see [Schema Evolution](https://hudi.apache.org/docs/schema_evolution/). If the schema of a Hudi managed table is changed, you must delete the associated Hudi external table from your StarRocks cluster and then re-create that external table.

### Procedure

#### Step 1: Create and manage Hudi resources

You must create Hudi resources in your StarRocks cluster. The Hudi resources are used to manage the Hudi databases and external tables that you create in your StarRocks cluster.

##### Create a Hudi resource

Execute the following statement to create a Hudi resource named `hudi0`:

~~~SQL
CREATE EXTERNAL RESOURCE "hudi0" 
PROPERTIES ( 
    "type" = "hudi", 
    "hive.metastore.uris" = "thrift://192.168.7.251:9083"
);
~~~

The following table describes the parameters.

| Parameter           | Description                                                  |
| ------------------- | ------------------------------------------------------------ |
| type                | The type of the Hudi resource. Set the vaue to hudi.         |
| hive.metastore.uris | The Thrift URI of the Hive metastore to which the Hudi resource connects. After connecting the Hudi resource to a Hive metastore, you can create and manage Hudi tables by using Hive. The Thrift URI is in the <IP address of the Hive metastore\>:<Port number of the Hive metastore\> format. The default port number is 9083. |

From v2.3 onwards, StarRocks allows changing the `hive.metastore.uris` value of a Hudi resource. For more information, see [ALTER RESOURCE](<https://docs.starrocks.com/en-us/2.3/sql-reference/sql-statements/data-definition/ALTER> RESOURCE).

##### View Hudi resources

Execute the following statement to view all Hudi resources that are created in your StarRocks cluster:

~~~SQL
SHOW RESOURCES;
~~~

##### Delete a Hudi resource

Execute the following statement to delete the Hudi resource named `hudi0`:

~~~SQL
DROP RESOURCE "hudi0";
~~~

> Note:
>
> Deleting a Hudi resource causes unavailability of all Hudi external tables that are created by using that Hudi resource. However, the deletion does not affect your data stored in Hudi. If you still want to query your data from Hudi by using StarRocks, you must re-create Hudi resources, Hudi databases, and Hudi external tables in your StarRocks cluster.

#### Step 2: Create Hudi databases

Execute the following statement to create and open a Hudi database named `hudi_test` in your StarRocks cluster:

~~~SQL
CREATE DATABASE hudi_test; 
USE hudi_test; 
~~~

> Note:
>
> The name that you specify for the Hudi database in your StarRocks cluster does not need to be the same as the associated database in Hudi.

#### Step 3: Create Hudi external tables

Execute the following statement to create a Hudi external table named `hudi_tbl` in the `hudi_test` Hudi database:

~~~SQL
CREATE EXTERNAL TABLE `hudi_tbl` ( 
    `id` bigint NULL, 
    `data` varchar(200) NULL 
) ENGINE=HUDI 
PROPERTIES ( 
    "resource" = "hudi0", 
    "database" = "hudi", 
    "table" = "hudi_table" 
); 
~~~

The following table describes the parameters.

| Parameter | Description                                                  |
| --------- | ------------------------------------------------------------ |
| ENGINE    | The query engine of the Hudi external table. Set the value to `HUDI`. |
| resource  | The name of the Hudi resource in your StarRocks cluster.     |
| database  | The name of the Hudi database to which the Hudi external table belongs in your StarRocks cluster. |
| table     | The Hudi managed table with which the Hudi external table is associated. |

> Note:
>
> * The name that you specify for the Hudi external table does not need to be the same as the associated Hudi managed table.
>
> * The columns in the Hudi external table must have the same names but can be in a different sequence compared to their counterpart columns in the associated Hudi managed table.
>
> * You can select some or all columns from the associated Hudi managed table and create only the selected columns in the Hudi external table. The following table lists the mapping between the data types supported by Hudi and the data types supported by StarRocks.

| Data types supported by Hudi | Data types supported by StarRocks |
| ---------------------------- | --------------------------------- |
| BOOLEAN                      | BOOLEAN                           |
| INT                          | TINYINT/SMALLINT/INT              |
| DATE                         | DATE                              |
| TimeMillis/TimeMicros        | TIME                              |
| LONG                         | BIGINT                            |
| FLOAT                        | FLOAT                             |
| DOUBLE                       | DOUBLE                            |
| STRING                       | CHAR/VARCHAR                      |
| ARRAY                        | ARRAY                             |
| DECIMAL                      | DECIMAL                           |

> If some columns of a Hudi managed table are any of the FIXED, ENUM, UNION, MAP, and BYTES data types, you cannot access these columns by creating a Hudi external table associated with that Hudi managed table.

#### Step 4: Query data from a Hudi external table

After you create a Hudi external table associated with a specific Hudi managed table, you do not need to load data into the Hudi external table. To query data from Hudi, execute the following statement:

~~~SQL
SELECT COUNT(*) FROM hudi_tbl;
~~~
