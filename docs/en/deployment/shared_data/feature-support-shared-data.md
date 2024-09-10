---
displayed_sidebar: docs
sidebar_label: "Feature Support"
---

# Feature Support: Shared-data Clusters

:::tip
Each of the features below lists the version number that they were added in. If you are deploying a new cluster please deploy the latest patch release of version 3.2 or higher.
:::

## Overview

Shared-data StarRocks clusters feature a disaggregated storage and compute architecture. This allows data to be stored in remote storage, leading to lower storage costs, optimized resource isolation, and better service elasticity compared to a shared-nothing cluster.

This document outlines the feature support for shared-data clusters, covering deployment methods, storage configurations, caching mechanisms, Compaction, Primary Key table functionalities, and performance test results.

## Deployment

Shared-data clusters support deployments on physical/virtual machines and on Kubernetes via Operator.

Both deployment solutions have the following limitations:

- Mixed deployment of shared-nothing and shared-data mode is not supported.
- The transformation from a shared-nothing cluster to a shared-data cluster or vice versa is not supported.
- Heterogeneous deployments are not supported, meaning the hardware specifications of all CN nodes within a cluster must be the same.

### StarRocks Kubernetes Operator

StarRocks offers the [StarRocks Kubernetes Operator](https://github.com/StarRocks/starrocks-kubernetes-operator/releases) for shared-data deployment on Kubernetes.

You can scale shared-data clusters by the following methods:

- Manual operations.
- Automatic scaling using Kubernetes HPA (Horizontal Pod Autoscaler) strategies.

## Storage

Shared-data clusters support building storage volumes on HDFS and object storage.

### HDFS

#### Location

StarRocks supports the following locations for HDFS storage volume:

- HDFS: `hdfs://<host>:<port>/`

  > **NOTE**
  >
  > From v3.2, storage volumes support HDFS clusters with the NameNode HA mode enabled.

- WebHDFS (Supported from v3.2): `webhdfs://<host>:<http_port>/`

- ViewFS (Supported from v3.2): `viewfs://<ViewFS_cluster>/`

#### Authentication

StarRocks supports the following authentication methods for HDFS storage volume:

- Basic

- Username (Supported from v3.2)

- Kerberos Ticket Cache (Supported from v3.2)

  > **NOTE**
  >
  > StarRocks does not support automatic ticket refresh. You need to set up crontab tasks to refresh the ticket.

Authentication using Kerberos Keytab and Principal ID is not yet supported.

#### Usage notes

StarRocks supports storage volumes on HDFS and object storage. However, only one HDFS storage volume is allowed in each StarRocks instance. Creating multiple HDFS storage volumes may cause unknown behaviors of StarRocks.

### Object storage

#### Location

StarRocks supports the following object storage services for storage volumes:

- S3-compatible object storage services: `s3://<s3_path>`
  - AWS S3
  - GCS, OSS, OBS, COS, TOS, KS3, MinIO, and Ceph S3
- Azure Blob Storage (Supported from v3.1.1): `azblob://<azblob_path>`

#### Authentication

StarRocks supports the following authentication methods for different object storage services:

- AWS S3
  - AWS SDK
  - IAM user-based Credential
  - Instance Profile
  - Assumed Role
- GCS, OSS, OBS, COS, TOS, KS3, MinIO, and Ceph S3
  - Access Key pair
- Azure Blob Storage
  - Shared Key
  - Shared Access Signatures (SAS)

#### Partitioned Prefix

From v3.2.4, StarRocks supports creating storage volumes with the Partitioned Prefix feature for S3-compatible object storage systems. When this feature is enabled, StarRocks distributes the data into multiple partitions (sub-paths) under the bucket. It can easily multiply StarRocks' read and write performance on data files stored in the bucket.

### Storage volumes

- From v3.1.0 onwards, storage volumes can be created using the CREATE STORAGE VOLUME statement, and this method is recommended in later versions.
- The internal catalog `default_catalog` in shared-data clusters uses the default storage volume for data persistence. You can assign different storage volumes for databases and tables in `default_catalog` by setting the property `storage_volume`. If not configured, the property `storage_volume` is inherited in the order of catalog, database, and table.
- Currently, storage volumes can be used only for storing data in cloud-native tables. Future support will include external storage management, data loading, and backup capabilities.

## Cache

### Cache types

#### File Cache

File Cache was the initial caching mechanism introduced along with the shared-data cluster. It loads the cache at the segment file level. File Cache is not recommended in v3.1.7, v3.2.3, and later versions.

#### Data Cache

Data Cache is supported from v3.1.7 and v3.2.3 onwards to replace File Cache in earlier versions. Data Cache loads data from remote storage in blocks (on the order of MBs) on demand, without needing to load the entire file. It is recommended in the later versions and enabled by default in v3.2.3 and later.

#### Data Cache Warmup

StarRocks v3.3.0 introduces the Data Cache Warmup feature to accelerate queries in data lakes and shared-data clusters. Data Cache Warmup is an active process of populating the cache. By executing CACHE SELECT, you can proactively fetch the desired data from remote storage in advance.

### Configurations

- Table properties:
  - `datacache.enable`: Whether to enable the local disk cache. Default: `true`.
  - `datacache.partition_duration`: The validity duration of the cached data.
- BE configurations:
  - `starlet_use_star_cache`: Whether to enable Data Cache.
  - `starlet_star_cache_disk_size_percent`: The percentage of disk capacity that Data Cache can use at most in a shared-data cluster.

### Capabilities

- Data loading generates a local cache, whose eviction is only managed by the cache capacity control mechanism instead of `partition_duration`.
- StarRocks supports setting up regular tasks for Data Cache Warmup.

### Limitations

- StarRocks does not support multiple replicas for cached data.

## Compaction

### Observability

#### Partition-level Compaction status

From v3.1.9 onwards, you can view the Compaction status of partitions by querying `information_schema.partitions_meta`.

We recommend monitoring the following key metrics:

- **AvgCS**: Average Compaction score of all tablets in the partition.
- **MaxCS**: Maximum Compaction score among all tablets in the partition.

#### Compaction task status

From v3.2.0 onwards, you can view the status and progress of Compaction tasks by querying `information_schema.be_cloud_native_compactions`.

We recommend monitoring the following key metrics:

- **PROGRESS**: Current Compaction progress (in percentage) of the tablet.
- **STATUS**: The status of the compaction task. If any error occurs, detailed error messages will be returned in this field.

### Cancelling Compaction tasks

You can cancel specific compaction tasks using the CANCEL COMPACTION statement.

Example:

```SQL
CANCEL COMPACTION WHERE TXN_ID = 123;
```

> **NOTE**
>
> The CANCEL COMPACTION statement must be executed on the Leader FE node.

### Manual Compaction

From v3.1, StarRocks offers a SQL statement for manual Compaction. You can specify the table or partitions for compaction. For more information, refer to [Manual Compaction](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md#manual-compaction).

## Primary Key tables

The following table lists the major features of Primary Key tables and their support status in shared-data clusters:

| **Feature**                   | **Supported Version(s)** | **Description**                                              |
| ----------------------------- | ------------------------ | ------------------------------------------------------------ |
| Primary Key tables            | v3.1.0                   |                                                              |
| Primary Key index persistence | v3.2.0<br />v3.1.3       | <ul><li>Currently, shared-data clusters support Primary Key index persistence on local disks.</li><li>Persistence in remote storage will be supported in future releases.</li></ul> |
| Partial Update                | v3.1.0                   | Shared-data clusters support Partial Update in Row mode from v3.1.0 onwards and in Column mode from v3.3.1 onwards. |
| Conditional Update            | v3.1.0                   | Currently, the condition only supports 'Greater'.            |
| Hybrid row-column storage     | ❌                        | To be supported in future releases.                          |

## Query performance

The following test compares the query performance of a shared-data cluster with Data Cache disabled, one with Data Cache enabled, one that queries the dataset in Hive, and a shared-nothing cluster.

### Hardware Specifications

The cluster used in the test includes one FE node and five CN/BE nodes. The hardware specifications are as follows:

| **VM provider**       | Alibaba Cloud ECS   |
| --------------------- | ------------------- |
| **FE node**           | 8 Core 32 GB Memory |
| **CN/BE node**        | 8 Core 64 GB Memory |
| **Network bandwidth** | 8 Gbits/s           |
| **Disk**              | ESSD                |

### Software version

StarRocks v3.3.0

### Dataset

SSB 1TB dataset

:::note

The dataset and queries used in this comparison are from the [Star Schema Benchmark](../../benchmarking/SSB_Benchmarking.md/#test-sql-and-table-creation-statements).

:::

### Test Results

The following table shows the performance test results on thirteen queries and the sum of each cluster. The unit of query latency is milliseconds (ms).

| **Query** | **Shared-data Without Data Cache** | **Shared-data With Data Cache** | **Hive Catalog Without Data Cache**     | **Shared-nothing** |
| --------- | ---------------------------------- | ------------------------------- | --------------------------------------- | ------------------ |
| **Q01**   | 2742                               | 858                             | 9652                                    | 3555               |
| **Q02**   | 2714                               | 704                             | 8638                                    | 3183               |
| **Q03**   | 1908                               | 658                             | 8163                                    | 2980               |
| **Q04**   | 31135                              | 8582                            | 34604                                   | 7997               |
| **Q05**   | 26597                              | 7806                            | 29183                                   | 6794               |
| **Q06**   | 21643                              | 7147                            | 24401                                   | 5602               |
| **Q07**   | 35271                              | 15490                           | 38904                                   | 19530              |
| **Q08**   | 24818                              | 7368                            | 27598                                   | 6984               |
| **Q09**   | 21056                              | 6667                            | 23587                                   | 5687               |
| **Q10**   | 2823                               | 912                             | 16663                                   | 3942               |
| **Q11**   | 50027                              | 18947                           | 52997                                   | 19636              |
| **Q12**   | 10300                              | 4919                            | 36146                                   | 8136               |
| **Q13**   | 7378                               | 3386                            | 23153                                   | 6380               |
| **SUM**   | 238412                             | 83444                           | 333689                                  | 100406             |

### Conclusion

- The query performance of the shared-data cluster with Data Cache disabled and Parallel Scan and I/O merge optimization enabled is **1.4 times** that of the cluster that queries Hive data.
- The query performance of the shared-data cluster with Data Cache enabled and Parallel Scan and I/O merge optimization enabled is **1.2 times** that of the shared-nothing cluster.

## Other features to be supported

- Full-text inverted index
- Hybrid row-column storage
- Global dictionary object
- Generated column
- Backup and restore

