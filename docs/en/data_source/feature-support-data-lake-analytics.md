---
displayed_sidebar: docs
sidebar_label: "Feature Support"
---

# Feature Support: Data Lake Analytics

From v2.3 onwards, StarRocks supports managing external data sources and analyzing data in data lakes via external catalogs.

This document outlines the feature support for external catalogs and the supported version of the features involved.

## Universal features

This section lists the universal features of the External Catalog feature, including storage systems, file readers, credentials, privileges, and Data Cache.

### External storage systems

| Storage System          | Supported Version |
| :---------------------- | :---------------- |
| HDFS                    | v2.3+             |
| AWS S3                  | v2.3+             |
| Microsoft Azure Storage | v3.0+             |
| Google GCS              | v3.0+             |
| Alibaba Cloud OSS       | v3.1+             |
| Huawei Cloud OBS        | v3.1+             |
| Tencent Cloud COS       | v3.1+             |
| Volcengine TOS          | v3.1+             |
| Kingsoft Cloud KS3      | v3.1+             |
| MinIO                   | v3.1+             |
| Ceph S3                 | v3.1+             |

In addition to the native support for the storage systems listed above, StarRocks also supports the following types of object storage services:

- **HDFS-compatible object storage services such as COS Cloud HDFS, OSS-HDFS, and OBS PFS**
  - **Description**: You need to specify the object storage URI prefix in the BE configuration item `fallback_to_hadoop_fs_list`, and upload the .jar package provided by the cloud vendor to the directory **/lib/hadoop/hdfs/**. Note that you must create the external catalog using the prefix you specified in `fallback_to_hadoop_fs_list`.
  - **Supported Version(s)**: v3.1.9+, v3.2.4+
- **S3-compatible object storage services other than those listed above**
  - **Description**: You need to specify the object storage URI prefix in the BE configuration item `s3_compatible_fs_list`. Note that you must create the external catalog using the prefix you specified in `s3_compatible_fs_list`.
  - **Supported Version(s)**: v3.1.9+, v3.2.4+

### Compression formats

This section only lists the compression formats supported by each file format. For the file formats supported by each external catalog, please refer to the section on the corresponding external catalog.

| File Format  | Compression Formats                                          |
| :----------- | :----------------------------------------------------------- |
| Parquet      | NO_COMPRESSION, SNAPPY, LZ4, ZSTD, GZIP, LZO (v3.1.5+)       |
| ORC          | NO_COMPRESSION, ZLIB, SNAPPY, LZO, LZ4, ZSTD                 |
| Text         | NO_COMPRESSION, LZO (v3.1.5+)                                |
| Avro         | NO_COMPRESSION (v3.2.1+), DEFLATE (v3.2.1+), SNAPPY (v3.2.1+), BZIP2 (v3.2.1+) |
| RCFile       | NO_COMPRESSION (v3.2.1+), DEFLATE (v3.2.1+), SNAPPY (v3.2.1+), GZIP (v3.2.1+) |
| SequenceFile | NO_COMPRESSION (v3.2.1+), DEFLATE (v3.2.1+), SNAPPY (v3.2.1+), BZIP2 (v3.2.1+), GZIP (v3.2.1+) |

:::note

The Avro, RCFile, and SequenceFile file formats are read by Java Native Interface (JNI) instead of the native readers within StarRocks. Therefore, the read performance for these file formats may not be as good as that of Parquet and ORC.

:::

### Management, credential, and access control

| Feature                                  | Description                                                  | Supported Version(s) |
| :--------------------------------------- | :----------------------------------------------------------- | :------------------- |
| Information Schema                       | Supports Information Schema for external catalogs.           | v3.2+                |
| Data lake access control                 | Supports StarRocks' native RBAC model for external catalogs. You can manage the privileges of databases, tables, and views (currently, Hive views and Iceberge views only) in external catalogs just like those in the default catalog of StarRocks. | v3.0+                |
| Reuse external services on Apache Ranger | Supports reusing the external service (such as the Hive Service) on Apache Ranger for access control. | v3.1.9+              |
| Kerberos authentication                  | Supports Kerberos authentication for HDFS or Hive Metastore. | v2.3+                |

### Data Cache

| Feature                                      | Description                                                  | Supported Version(s) |
| :------------------------------------------- | :----------------------------------------------------------- | :------------------- |
| Data Cache (Block Cache)                     | From v2.5 onwards, StarRocks supported the Data Cache feature (then called Block Cache) implemented using CacheLib, which led to limited optimization potential for its extensibility. Starting from v3.0, StarRocks refactored the cache implementation and added new features to Data Cache, resulting in better performance with each subsequent version. | v2.5+                |
| Data rebalancing among local disks           | Supports data rebalancing strategy to ensure that data skew is controlled under 10%. | v3.2+                |
| Replace Block Cache with Data Cache          | **Parameter changes**<br />BE Configurations:<ul><li>Replace `block_cache_enable` with `datacache_enable`.</li><li>Replace `block_cache_mem_size` with `datacache_mem_size`.</li><li>Replace `block_cache_disk_size` with `datacache_disk_size`.</li><li>Replace `block_cache_disk_path` with `datacache_disk_path`.</li><li>Replace `block_cache_meta_path` with `datacache_meta_path`.</li><li>Replace `block_cache_block_size` with `datacache_block_size`.</li></ul>Session Variables:<ul><li>Replace `enable_scan_block_cache` with `enable_scan_datacache`.</li><li>Replace `enable_populate_block_cache` with `enable_populate_datacache`.</li></ul>After the cluster is upgraded to a version where Data Cache is available, the Block Cache parameters still take effect. The new parameters will override the old ones once Data Cache is enabled. The mixed usage of both groups of parameters is not allowed. Otherwise, some parameters will not take effect. | v3.2+                |
| New metrics for API that monitors Data Cache | Supports an individual API that monitors Data Cache including the cache capacity and hits. You can view Data Cache metrics via the interface `http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/stat`. | v3.2.3+              |
| Memory Tracker for Data Cache                | Supports Memory Tracker for Data Cache. You can view the memory-related metrics via the interface `http://${BE_HOST}:${BE_HTTP_PORT}/mem_tracker`. | v3.1.8+              |
| Data Cache Warmup                            | By executing CACHE SELECT, you can proactively populate the cache with the desired data from remote storage in advance to prevent the first query from taking too much time fetching the data. CACHE SELECT will not print data or incur calculations. It only fetches data. | v3.3+                |

## Hive Catalog

### Metadata

Hive Catalog's support for Hive Metastore (HMS) and AWS Glue mostly overlaps except that the automatic incremental update feature for HMS is not recommended. The default configuration is recommended in most cases.

The performance of metadata retrieval largely depends on the performance of the user's HMS or HDFS NameNode. Please consider all factors and base your judgment on test results.

- **[Default and Recommended] Best performance with a tolerance of minute-level data inconsistency**
  - **Configuration**: You can use the default setting. Data updated within 10 minutes (by default) is not visible. Old data will be returned to queries within this duration.
  - **Advantage**: Best query performance.
  - **Disadvantage**: Data inconsistency caused by latency.
  - **Supported Version(s)**: v2.5.5+ (Disabled by default in v2.5 and enabled by default in v3.0+)
- **Instant visibility of newly loaded data (files) without manual refresh**
  - **Configuration**: Disable the cache for the metadata of the underlying data files by setting the catalog property `enable_remote_file_cache` to `false`.
  - **Advantage**: Visibility of file changes with no delay.
  - **Disadvantage**: Lower performance when the file metadata cache is disabled. Each query must access the file list.
  - **Supported Version(s)**: v2.5.5+
- **Instant visibility of partition changes without manual refresh**
  - **Configuration**: Disable the cache for the Hive partition names by setting the catalog property `enable_cache_list_names` to `false`.
  - **Advantage**: Visibility of partition changes with no delay
  - **Disadvantage**: Lower performance when the partition name cache is disabled. Each query must access the partition list.
  - **Supported Version(s)**: v2.5.5+

:::tip

If you demand real-time updates on the data changes whilst the performance of your HMS is not optimized, you can enable the cache, disable the automatic incremental update, and manually refresh the metadata (using REFRESH EXTERNAL TABLE) via a scheduling system whenever there is a data change upstream.

:::

### Storage system

| Feature                         | Description                                                  | Supported Version(s)  |
| :------------------------------ | :----------------------------------------------------------- | :-------------------- |
| Recursive sub-directory listing | Enable recursive sub-directory listing by setting the Catalog property `enable_recursive_listing` to `true`. When recursive listing is enabled, StarRocks will read data from a table and its partitions and from the subdirectories within the physical locations of the table and its partitions. This feature is designed to address the issue of multi-layer nested directories. | v2.5.9+<br />v3.0.4+ (Disabled by default in v2.5 and v3.0, and enabled by default in v3.1+) |

### File formats and data types

#### File formats

| Feature | Supported File Formats                         |
| :------ | :--------------------------------------------- |
| Read    | Parquet, ORC, TEXT, Avro, RCFile, SequenceFile |
| Sink    | Parquet (v3.2+), ORC (v3.3+), TEXT (v3.3+)     |

#### Data types

INTERVAL, BINARY, and UNION types are not supported.

TEXT-formatted Hive table does not support MAP and STRUCT types.

### Hive views

StarRocks supports querying Hive views from v3.1.0 onwards.

:::note While StarRocks executes queries against a Hive view, it will try to parse the definition of the view using the syntax of StarRocks and Trino. An error will be returned if StarRocks cannot parse the definition of the view. There is a possibility that StarRocks failed to parse the Hive views created with functions exclusive to Hive or Spark.

:::

### Query statistics interfaces

| Feature                                                       | Supported Version(s) |
| :------------------------------------------------------------ | :------------------- |
| Supports SHOW CREATE TABLE to view Hive table schema          | v3.0+                |
| Supports ANALYZE to collect statistics                        | v3.2+                |
| Supports collecting histograms and STRUCT subfield statistics | v3.3+                |

### Data sinking

| Feature                | Supported Version(s) | Note                                                         |
| :--------------------- | :------------------- | :----------------------------------------------------------- |
| CREATE DATABASE        | v3.2+                | You can choose to specify the location for a database created in Hive or not. If you do not specify the location for the database, you will need to specify the location for the tables created under the database. Otherwise, an error will be returned. If you have specified the location for the database, tables without the location specified will inherit the location of the database. And if you have specified locations for both the database and the table, the table's location will take effect eventually. |
| CREATE TABLE           | v3.2+                | For both partitioned and non-partitioned tables.             |
| CREATE TABLE AS SELECT | v3.2+                |                                                              |
| INSERT INTO/OVERWRITE  | v3.2+                | For both partitioned and non-partitioned tables.             |
| CREATE TABLE LIKE      | v3.2.4+              |                                                              |
| Sink file size         | v3.3+                | You can define the maximum size of each data file to be sunk using the session variable `connector_sink_target_max_file_size`. |

## Iceberg Catalog

### Metadata

Iceberg Catalog supports HMS, Glue, and Tabular as its metastore. The default configuration is recommended in most cases.

Please note that the default value of the session variable `enable_iceberg_metadata_cache` has been changed to accommodate different scenarios:

- From v3.2.1 to v3.2.3, this parameter is set to `true` by default, regardless of what metastore service is used.
- In v3.2.4 and later, if the Iceberg cluster uses AWS Glue as metastore, this parameter still defaults to `true`. However, if the Iceberg cluster uses other metastore services such as Hive metastore, this parameter defaults to `false`.
- From v3.3.0 onwards, the default value of this parameter is set to `true` again because StarRocks supports the new Iceberg metadata framework. Iceberg Catalog and Hive Catalog now use the same metadata polling mechanism and FE configuration item `background_refresh_metadata_interval_millis`.

| Feature                                                      | Supported Version(s) |
| :----------------------------------------------------------- | :------------------- |
| Distributed metadata plan (Recommended for scenarios with a large volume of metadata) | v3.3+                |
| Manifest Cache (Recommended for scenarios with a small volume of metadata but high demand on latency) | v3.3+                |

### File formats

| Feature | Supported File Formats |
| :------ | :--------------------- |
| Read    | Parquet, ORC           |
| Sink    | Parquet                |

- Both Parquet-formatted and ORC-formatted Iceberg V1 tables support position deletes and equality deletes.
- ORC-formatted Iceberg V2 tables support position deletes from v3.0.0, and Parquet-formatted ones support position deletes from v3.1.0.
- ORC-formatted Iceberg V2 tables support equality deletes from v3.1.8 and v3.2.3, and Parquet-formatted ones support equality deletes from v3.2.5.

### Iceberg views

StarRocks supports querying Iceberg views from v3.3.2 onwards. Currently, only Iceberg views created through StarRocks are supported.

:::note

While StarRocks executes queries against an Iceberg view, it will try to parse the definition of the view using the syntax of StarRocks and Trino. An error will be returned if StarRocks cannot parse the definition of the view. There is a possibility that StarRocks failed to parse the Iceberg views created with functions exclusive to Iceberg or Spark.

:::

### Query statistics interfaces

| Feature                                                       | Supported Version(s) |
| :------------------------------------------------------------ | :------------------- |
| Supports SHOW CREATE TABLE to view Iceberg table schema       | v3.0+                |
| Supports ANALYZE to collect statistics                        | v3.2+                |
| Supports collecting histograms and STRUCT subfield statistics | v3.3+                |

### Data sinking

| Feature                | Supported Version(s) | Note                                                         |
| :--------------------- | :------------------- | :----------------------------------------------------------- |
| CREATE DATABASE        | v3.1+                | You can choose to specify the location for a database created in Iceberg or not. If you do not specify the location for the database, you will need to specify the location for the tables created under the database. Otherwise, an error will be returned. If you have specified the location for the database, tables without the location specified will inherit the location of the database. And if you have specified locations for both the database and the table, the table's location will take effect eventually. |
| CREATE TABLE           | v3.1+                | For both partitioned and non-partitioned tables.             |
| CREATE TABLE AS SELECT | v3.1+                |                                                              |
| INSERT INTO/OVERWRITE  | v3.1+                | For both partitioned and non-partitioned tables.             |

### Miscellaneous supports

| Feature                                                      | Supported Version(s) |
| :----------------------------------------------------------- | :------------------- |
| Supports reading TIMESTAMP-type partition formats `yyyy-MM-ddTHH:mm` and `yyyy-MM-dd HH:mm`. | v2.5.19+<br />v3.1.9+<br />v3.2.3+ |

## Hudi Catalog

- StarRocks supports querying the Parquet-formatted data in Hudi, and supports SNAPPY, LZ4, ZSTD, GZIP, and NO_COMPRESSION compression formats for Parquet files.
- StarRocks fully supports Hudi's Copy On Write (COW) tables and Merge On Read (MOR) tables.
- StarRocks supports SHOW CREATE TABLE to view Hudi table schema from v3.0.0 onwards.

## Delta Lake Catalog

- StarRocks supports querying the Parquet-formatted data in Delta Lake, and supports SNAPPY, LZ4, ZSTD, GZIP, and NO_COMPRESSION compression formats for Parquet files.
- StarRocks does not support querying the MAP-type and STRUCT-type data in Delta Lake.
- StarRocks supports SHOW CREATE TABLE to view Delta Lake table schema from v3.0.0 onwards.

## JDBC Catalog

| Catalog type | Supported Version(s) |
| :----------- | :------------------- |
| MySQL        | v3.0+                |
| PostgreSQL   | v3.0+                |
| ClickHouse   | v3.3+                |
| Oracle       | v3.2.9+              |
| SQL Server   | v3.2.9+              |

### MySQL

| Feature        | Supported Version(s) |
| :------------- | :------------------- |
| Metadata cache | v3.3+                |

#### Data type correspondance

| MySQL             | StarRocks           | Supported Version(s) |
| :---------------- | :------------------ | :------------------- |
| BOOLEAN           | BOOLEAN             | v2.3+                |
| BIT               | BOOLEAN             | v2.3+                |
| SIGNED TINYINT    | TINYINT             | v2.3+                |
| UNSIGNED TINYINT  | SMALLINT            | v3.0.6+<br />v3.1.2+ |
| SIGNED SMALLINT   | SMALLINT            | v2.3+                |
| UNSIGNED SMALLINT | INT                 | v3.0.6+<br />v3.1.2+ |
| SIGNED INTEGER    | INT                 | v2.3+                |
| UNSIGNED INTEGER  | BIGINT              | v3.0.6+<br />v3.1.2+ |
| SIGNED BIGINT     | BIGINT              | v2.3+                |
| UNSIGNED BIGINT   | LARGEINT            | v3.0.6+<br />v3.1.2+ |
| FLOAT             | FLOAT               | v2.3+                |
| REAL              | FLOAT               | v3.0.1+              |
| DOUBLE            | DOUBLE              | v2.3+                |
| DECIMAL           | DECIMAL32           | v2.3+                |
| CHAR              | VARCHAR(columnsize) | v2.3+                |
| VARCHAR           | VARCHAR             | v2.3+                |
| TEXT              | VARCHAR(columnsize) | v3.0.1+              |
| DATE              | DATE                | v2.3+                |
| TIME              | TIME                | v3.1.9+<br />v3.2.4+ |
| TIMESTAMP         | DATETIME            | v2.3+                |

### PostgreSQL

#### Data type correspondance

| MySQL     | StarRocks           | Supported Version(s) |
| :-------- | :------------------ | :------------------- |
| BIT       | BOOLEAN             | v2.3+                |
| SMALLINT  | SMALLINT            | v2.3+                |
| INTEGER   | INT                 | v2.3+                |
| BIGINT    | BIGINT              | v2.3+                |
| REAL      | FLOAT               | v2.3+                |
| DOUBLE    | DOUBLE              | v2.3+                |
| NUMERIC   | DECIMAL32           | v2.3+                |
| CHAR      | VARCHAR(columnsize) | v2.3+                |
| VARCHAR   | VARCHAR             | v2.3+                |
| TEXT      | VARCHAR(columnsize) | v2.3+                |
| DATE      | DATE                | v2.3+                |
| TIMESTAMP | DATETIME            | v2.3+                |

### ClickHouse 

Supported from v3.3.0 onwards.

### Oracle

Supported from v3.2.9 onwards.

### SQL Server

Supported from v3.2.9 onwards.

## Elasticsearch Catalog

Elasticsearch Catalog is supported from v3.1.0 onwards.

## Paimon Catalog

Paimon Catalog is supported from v3.1.0 onwards.

## MaxCompute Catalog

MaxCompute Catalog is supported from v3.3.0 onwards.

## Kudu Catalog

Kudu Catalog is supported from v3.3.0 onwards.

