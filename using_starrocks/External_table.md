# External Tables

StarRocks supports access to other data sources in the form of external tables. External tables are data tables that are stored in other data sources. Data sources supported by StarRocks are MySQL, HDFS, ElasticSearch, and Hive. Currently, it only supports reads not writes..

## MySQL External Tables

In the star model, data is generally divided into dimension tables and fact tables. Dimension tables have less data but involve UPDATE operations. Currently, UPDATE operation is not directly supported in StarRocks (it can be implemented by  the Unique data model). In some scenarios, you can store dimension tables in MySQL and read them directly when querying.

Before querying MySQL data, you need to create an external table in StarRocks to map with it. You need to specify the MySQL connection information when creating the table, as shown below:

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

Parameter description:

* **host**: the connection address of MySQL
* **port**: the port number of the MySQL connection
* **user**: the username to log in to MySQL
* **password**: the password to log in to MySQL
* **database**: the name of MySQL database
* **table**: the name of MySQL related data table

## HDFS External Tables

Similar to accessing MySQL, StarRocks needs to create external tables corresponding to the HDFS files before accessing them, as shown below:

~~~sql
CREATE EXTERNAL TABLE hdfs_external_table (
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=broker
PROPERTIES (
    "broker_name" = "broker_name",
    "path" = "hdfs://hdfs_host:hdfs_port/data1",
    "column_separator" = "|",
    "line_delimiter" = "\n"
)
BROKER PROPERTIES (
    "username" = "hdfs_username",
    "password" = "hdfs_password"
)
~~~

Parameter Description:

* **broker_name**: Broker name
* **path**: HDFS file path
* **column_separator**: column separator
* **line_delimiter**: row separator

StarRocks cannot access HDFS files directly, it needs to be accessed through the Broker. Therefore, when creating a table, you need to specify the information about the HDFS files as well as the Broker. For details about Broker, please refer to [Broker Import](...). /loading/BrokerLoad.md).

## ElasticSearch External Tables

StarRocks and ElasticSearch are both popular analytics systems. StarRocks is strong in large-scale distributed computing and ElasticSearch is strong in full-text search. StarRocks supports Elasticsearch access, which combines these two capabilities to provide a more complete OLAP solution.

### Example of building table

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

Parameter description:

* **host**: ES cluster connection address, one or more can be specified. StarRocks gets  to know the ES versionand index-shard distribution through this address.
* **user**: the username of the ES cluster with **basic authentication** enabled, make sure the user has access to `/*cluster/state/*nodes/http` and the index.
* **password**: the password of the corresponding user
* **index**: the name of the ES index that corresponds to the table in StarRocks, can be alias
* **type**: the type of index, default is **doc**
* **transport**: internal reserved, default is **http**

### Predicate pushdown

StarRocks supports predicate pushdown to push filters to ElasticSearch for execution, which improves query performance. The following table shows the operators that currently support pushdown.

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

Table 1: List of supported predicate pushdown

### Query example

The **esquery function** is used to push down queries **that cannot be expressed in sq (e.g.,match, geoshape, etc.) to ES for filtering. The first parameter of esquery is used to associate an index. The second parameter is the json representation of the basic Query DSL of ES, which is included using the brackets {}.** The root key of json has and can have only one **, such as match, geo_shape, bool, etc.

* Match query

~~~sql
select * from es_table where esquery(k4, '{
    "match": {
       "k4": "StarRocks on elasticsearch"
    }
}');
~~~

* geo-related queries.

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

* bool query:

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

* ES scans data differently before and after version 5.x. Currently, **only versions after 5.x** are supported.
* ES clusters that use the HTTP Basic authentication method are supported.
* Queries via StarRocks will be much slower than directly queryingES. Take count-related queries for example, ES directly reads the metadata related to the target documents without filtering the real data.

## Hive External Table

### Create Hive resources

A Hive resource corresponds to a Hive cluster. You need to configure  the Hive cluster used by StarRocks, such as the Hive meta store address. You need to specify the corresponding Hive resource when creating a Hive external table.

~~~sql
-- Create a Hive resource named hive0
CREATE EXTERNAL RESOURCE "hive0"
PROPERTIES (
  "type" = "hive",
  "hive.metastore.uris" = "thrift://10.10.44.98:9083"
);

-- View the resources created in StarRocks
SHOW RESOURCES;

-- Delete the resource named hive0
DROP RESOURCE "hive0";
~~~

### Create database

~~~sql
CREATE DATABASE hive_test;
USE hive_test;
~~~

### Create a Hive External Table

~~~sql
-- Syntax
CREATE EXTERNAL TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
PROPERTIES (
  "key" = "value"
);

-- Example: Create the external table of`profile_parquet_p7` under the raw data database in the Hive cluster corresponding to the hive0 resource
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

* External table columns.
  * Column names need to be consistent with the ones of the Hive table.
  * The order of the columns **does not need to be** consistent with the one of the Hive table.
  * You can select only **some of the columns in the Hive table**, but **partitioned columns** must be all included.
  * Partitioned columns of an external table do not need to be specified by the `partition by` statement. They should be defined in the same description list as normal columns. There is no need to specify partition information, StarRocks will automatically sync with the Hive table.
  * `ENGINE` is specified as HIVE.
* PROPERTIES:
  * **hive.resource**: Specifies the Hive resource to use.
  * **database**: Specifies the database in Hive.
  * **table**: Specifies the table in Hive, **view** is not supported.
* The supported column types are as follows:

    |  Column type of Hive   |  Column type of StarRocks   | Description |
    | --- | --- | ---|
    |   INT/INTEGER  | INT    |
    |   BIGINT  | BIGINT    |
    |   TIMESTAMP  | DATETIME    | Precision and time zone will be lost when converting Timestamp to Datetime. Timestamp will be converted to timezone free data time based on the timezone in sessionVariable |
    |  STRING  | VARCHAR   |
    |  VARCHAR  | VARCHAR   |
    |  CHAR  | CHAR   |
    |  DOUBLE | DOUBLE |
    | FLOATE | FLOAT|

Description:

* Hive table Schema changes **will not be automatically synchronized**, you need to recreate d the Hive external table in StarRocks.
* Currently, the supported Hive storage formats are Parquet and ORCs
* Snappy, and lz4 are supported as compression formats.

### Query Hive external table

~~~sql
-- Query the total number of rows of profile_wos_p7
select count(*) from profile_wos_p7;
~~~

### Configuration

* FE configuration file path is `fe/conf`, to which the configuration file can be added if you need to customize the Hadoop cluster. For example: HDFS cluster uses a highly available nameservice, you need to put `hdfs-site.xml` under `fe/conf`.  If HDFS is configured with viewfs, you need to put the `core-site.xml` under `fe/conf`.
* BE configuration file path is `be/conf`, to which configuration file can be added if you need to customize the Hadoop cluster. For example: HDFS cluster using a highly available nameservice, you need to put `hdfs-site.xml` under `be/conf`. If HDFS is configured with viewfs, you need to put `core-site.xml` under `be/conf`.
* The machine where BE is located need to configure JAVA_HOME as a jdk environment rather than a jre environment
* kerbero supports:
  1. To log in with `kinit -kt keytab_path principal` to all FE/BE machines, you need to have access to Hive and HDFS. The kinit command login is only good for a period of time and needs to be put into crontab to be executed regularly.
  2. Put `hive-site.xml/core-site.xml/hdfs-site.xml` under `fe/conf`, and put `core-site.xml/hdfs-site.xml` under `be/conf`.
