---
displayed_sidebar: "Chinese"
sidebar_position: 3
description: "湖仓分析"
toc_max_heading_level: 2
---
import DataLakeIntro from '../assets/commonMarkdown/datalakeIntro.md'
import Clients from '../assets/quick-start/_clientsCompose.mdx'

# 基于 Apache Iceberg 的数据湖分析

## 简介

- 使用 Docker Compose 部署对象存储、Apache Spark、Iceberg Catalog 和 StarRocks。
- 将 2023 年 5 月份的纽约市绿色出租车数据导入 Iceberg 数据湖。
- 配置 StarRocks 以访问 Iceberg Catalog。
- 使用 StarRocks 查询数据湖中的数据。

<DataLakeIntro />

## 前提条件

### Docker

- [安装 Docker](https://docs.docker.com/engine/install/)。
- 为 Docker 分配 5 GB RAM。
- 为 Docker 分配 20 GB 的空闲磁盘空间。

### SQL 客户端

您可以使用 Docker 环境提供的 SQL 客户端，也可以使用系统上的客户端。多数与 MySQL 兼容的客户端都可以使用，包括本教程中涵盖的 DBeaver 和 MySQL Workbench。

### curl

`curl` 命令用于下载数据集。您可以通过在操作系统的终端运行 `curl` 或 `curl.exe` 来检查是否已安装 curl。如果未安装 curl，[点击此处获取 curl](https://curl.se/dlwiz/?type=bin)。

---

## 术语

### FE

FE 节点负责元数据管理、客户端连接管理、查询计划和查询调度。每个 FE 在其内存中存储和维护完整的元数据副本，确保每个 FE 都能提供无差别的服务。

### CN

CN 节点负责在**存算分离**或**存算一体**集群中执行查询。

### BE

BE 节点在**存算一体**集群中负责数据存储和执行查询。使用 External Catalog（例如本指南中使用的 Iceberg Catalog）时，BE 仅存储本地数据。

---

## 环境

本教程使用了六个容器（服务），并且全部使用 Docker Compose 部署。这些服务及其职责如下：

| 服务                | 职责                                                                |
|---------------------|---------------------------------------------------------------------|
| **`starrocks-fe`**  | 负责元数据管理、客户端连接、查询规划和调度                                |
| **`starrocks-be`**  | 负责执行查询计划                                                      |
| **`rest`**          | 提供 Iceberg Catalog（元数据服务）                                        |
| **`spark-iceberg`** | 用于运行 PySpark 的 Apache Spark 环境                               |
| **`mc`**            | MinIO 配置（MinIO 命令行客户端）                                     |
| **`minio`**         | MinIO 对象存储                                                     |

## 下载 Docker 配置和纽约市绿色出租车数据

StarRocks 提供了一个 Docker Compose 文件，用于搭建包含以上必要容器的环境。请使用 curl 下载 Compose 文件和数据集。

Docker Compose 文件：
```bash
mkdir iceberg
cd iceberg
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/iceberg/docker-compose.yml
```

数据集：
```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/iceberg/datasets/green_tripdata_2023-05.parquet
```

## 在 Docker 中启动环境

:::tip
请从包含 `docker-compose.yml` 文件的路径运行所有 `docker compose` 命令。
:::

```bash
docker compose up -d
```

```plaintext
[+] Building 0.0s (0/0)                     docker:desktop-linux
[+] Running 6/6
 ✔ Container iceberg-rest   Started                         0.0s
 ✔ Container minio          Started                         0.0s
 ✔ Container starrocks-fe   Started                         0.0s
 ✔ Container mc             Started                         0.0s
 ✔ Container spark-iceberg  Started                         0.0s
 ✔ Container starrocks-be   Started
```

## 检查环境状态

启动完成后，您还需要检查服务的进度。FE 和 CN 大约需要 30 秒才能变为 `healthy` 状态。

请运行 `docker compose ps` 命令，直到 FE 和 BE 的状态变为 `healthy`。其他服务没有健康检查配置，但您需要与它们进行交互，以了解它们的工作状态是否正常：

:::tip
如果您已安装了 `jq` 并希望通过 `docker compose ps` 命令得到更加简短的返回，可以尝试以下命令：

```bash
docker compose ps --format json | jq '{Service: .Service, State: .State, Status: .Status}'
```

:::

```bash
docker compose ps
```

```bash
SERVICE         CREATED         STATUS                   PORTS
rest            4 minutes ago   Up 4 minutes             0.0.0.0:8181->8181/tcp
mc              4 minutes ago   Up 4 minutes
minio           4 minutes ago   Up 4 minutes             0.0.0.0:9000-9001->9000-9001/tcp
spark-iceberg   4 minutes ago   Up 4 minutes             0.0.0.0:8080->8080/tcp, 0.0.0.0:8888->8888/tcp, 0.0.0.0:10000-10001->10000-10001/tcp
starrocks-be    4 minutes ago   Up 4 minutes (healthy)   0.0.0.0:8040->8040/tcp
starrocks-fe    4 minutes ago   Up 4 minutes (healthy)   0.0.0.0:8030->8030/tcp, 0.0.0.0:9020->9020/tcp, 0.0.0.0:9030->9030/tcp
```

---

## PySpark

Iceberg 有多种交互方式，本教程使用 PySpark。如果您对 PySpark 不熟悉，可以从“更多信息”部分找到相关文档。以下步骤提供了您需要运行的所有命令。

### 拷贝数据集

将数据复制到 `spark-iceberg` 容器中。以下命令将数据集文件复制到 `spark-iceberg` 服务中的 `/opt/spark/` 路径：

```bash
docker compose \
cp green_tripdata_2023-05.parquet spark-iceberg:/opt/spark/
```

### 启动 PySpark

此命令将连接到 `spark-iceberg` 服务并启动 PySpark：

```bash
docker compose exec -it spark-iceberg pyspark
```

```plain
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.9.18 (main, Nov  1 2023 11:04:44)
Spark context Web UI available at http://6ad5cb0e6335:4041
Spark context available as 'sc' (master = local[*], app id = local-1701967093057).
SparkSession available as 'spark'.
>>>
```

### 将数据集读取到 DataFrame 中

DataFrame 是 Spark SQL 的一部分，提供了类似于数据库表或电子表格的数据结构。

纽约市绿色出租车数据为 Parquet 格式，由纽约出租车和豪华轿车委员会提供。您需要从 `/opt/spark` 路径导入文件，并通过查询前三行数据的前几列来检查前几行数据。这些命令需要在 `pyspark` Session 中运行。这些命令会完成以下目标：

- 从磁盘读取数据集文件到名为 `df` 的 DataFrame 中
- 显示 Parquet 文件的 Schema

```py
df = spark.read.parquet("/opt/spark/green_tripdata_2023-05.parquet")
df.printSchema()
```

```plaintext
root
 |-- VendorID: integer (nullable = true)
 |-- lpep_pickup_datetime: timestamp_ntz (nullable = true)
 |-- lpep_dropoff_datetime: timestamp_ntz (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- RatecodeID: long (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- passenger_count: long (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- ehail_fee: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- trip_type: long (nullable = true)
 |-- congestion_surcharge: double (nullable = true)

>>>
```
检查前三行数据的前七列：

```python
df.select(df.columns[:7]).show(3)
```
```plaintext
+--------+--------------------+---------------------+------------------+----------+------------+------------+
|VendorID|lpep_pickup_datetime|lpep_dropoff_datetime|store_and_fwd_flag|RatecodeID|PULocationID|DOLocationID|
+--------+--------------------+---------------------+------------------+----------+------------+------------+
|       2| 2023-05-01 00:52:10|  2023-05-01 01:05:26|                 N|         1|         244|         213|
|       2| 2023-05-01 00:29:49|  2023-05-01 00:50:11|                 N|         1|          33|         100|
|       2| 2023-05-01 00:25:19|  2023-05-01 00:32:12|                 N|         1|         244|         244|
+--------+--------------------+---------------------+------------------+----------+------------+------------+
only showing top 3 rows
```
### 将数据写入 Iceberg 表

在此步骤中创建的 Iceberg 表将在下一步中的 StarRocks External Catalog 中使用。

- Catalog 名：`demo`
- 数据库名：`nyc`
- 表名：`greentaxis`

```python
df.writeTo("demo.nyc.greentaxis").create()
```

## 配置 StarRocks 访问 Iceberg Catalog

现在您可以退出 PySpark，或者您可以打开一个新的终端来运行 SQL 命令。如果您打开了一个新的终端，请重新进入包含 `docker-compose.yml` 文件的 `quickstart` 路径。

### 使用 SQL 客户端连接到 StarRocks

#### SQL 客户端

<Clients />

---

您现在可以退出 PySpark 会话并连接到 StarRocks。

:::tip

从包含 `docker-compose.yml` 文件的路径运行此命令。

如果您使用的是 mysql CLI 之外的客户端，请现在打开该客户端。
:::


```bash
docker compose exec starrocks-fe \
  mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "
```

```plaintext
StarRocks >
```

### 创建 External Catalog

您可以通过创建 External Catalog 将 StarRocks 连接至您的数据湖。以下示例基于以上 Iceberg 数据源创建 External Catalog。具体配置内容将在示例后详细解释。

```sql
CREATE EXTERNAL CATALOG 'iceberg'
PROPERTIES
(
  "type"="iceberg",
  "iceberg.catalog.type"="rest",
  "iceberg.catalog.uri"="http://iceberg-rest:8181",
  "iceberg.catalog.warehouse"="warehouse",
  "aws.s3.access_key"="admin",
  "aws.s3.secret_key"="password",
  "aws.s3.endpoint"="http://minio:9000",
  "aws.s3.enable_path_style_access"="true",
  "client.factory"="com.starrocks.connector.iceberg.IcebergAwsClientFactory"
);
```

#### PROPERTIES

| 属性                              | 描述                                                                                   |
|:---------------------------------|:----------------------------------------------------------------------------------------|
| `type`                           | 在此示例中，类型为 `iceberg`。其他选项包括 Hive、Hudi、Delta Lake 和 JDBC。                    |
| `iceberg.catalog.type`           | 在此示例中使用了 `rest`。Tabular 提供了所使用的 Docker 镜像，并使用 REST。                      |
| `iceberg.catalog.uri`            | REST 服务器端点。                                                                         |
| `iceberg.catalog.warehouse`      | Iceberg Catalog 的标识符。在此示例中，Compose 文件中指定的 Warehouse 名称为 `warehouse`。      |
| `aws.s3.access_key`              | MinIO Access Key。在此示例中，Compose 文件中设置 Access Key 为 `admin`。                     |
| `aws.s3.secret_key`              | MinIO Secret Key。在此示例中，Compose 文件中设置 Secret Key 为 `password`。                  |
| `aws.s3.endpoint`                | MinIO 端点。                                                                             |
| `aws.s3.enable_path_style_access`| 使用 MinIO 作为对象存储时，该项为必需。格式：`http://host:port/<bucket_name>/<key_name>`。     |
| `client.factory`                 | 此示例中使用 `iceberg.IcebergAwsClientFactory`。`aws.s3.access_key` 和 `aws.s3.secret_key` 参数用于身份验证。|

```sql
SHOW CATALOGS;
```

```plaintext
+-----------------+----------+------------------------------------------------------------------+
| Catalog         | Type     | Comment                                                          |
+-----------------+----------+------------------------------------------------------------------+
| default_catalog | Internal | An internal catalog contains this cluster's self-managed tables. |
| iceberg         | Iceberg  | NULL                                                             |
+-----------------+----------+------------------------------------------------------------------+
2 rows in set (0.03 sec)
```

```sql
SET CATALOG iceberg;
```

```sql
SHOW DATABASES;
```
:::tip
此时返回的数据库是您在 PySpark 会话中创建的。当您添加了 `iceberg` Catalog 后，便可以在 StarRocks 中看到 `nyc` 数据库。
:::

```plaintext
+----------+
| Database |
+----------+
| nyc      |
+----------+
1 row in set (0.07 sec)
```

```sql
USE nyc;
```

```plaintext
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
```

```sql
SHOW TABLES;
```

```plaintext
+---------------+
| Tables_in_nyc |
+---------------+
| greentaxis    |
+---------------+
1 rows in set (0.05 sec)
```

```sql
DESCRIBE greentaxis;
```

:::tip
通过比较 StarRocks 使用的 Schema 与之前 PySpark 会话中的 `df.printSchema()` 的输出，你可以发现 Spark 的 `timestamp_ntz` 数据类型在 StarRocks 中表示为 `DATETIME`，以及其他 Schema 转换。
:::

```plaintext
+-----------------------+------------------+------+-------+---------+-------+
| Field                 | Type             | Null | Key   | Default | Extra |
+-----------------------+------------------+------+-------+---------+-------+
| VendorID              | INT              | Yes  | false | NULL    |       |
| lpep_pickup_datetime  | DATETIME         | Yes  | false | NULL    |       |
| lpep_dropoff_datetime | DATETIME         | Yes  | false | NULL    |       |
| store_and_fwd_flag    | VARCHAR(1048576) | Yes  | false | NULL    |       |
| RatecodeID            | BIGINT           | Yes  | false | NULL    |       |
| PULocationID          | INT              | Yes  | false | NULL    |       |
| DOLocationID          | INT              | Yes  | false | NULL    |       |
| passenger_count       | BIGINT           | Yes  | false | NULL    |       |
| trip_distance         | DOUBLE           | Yes  | false | NULL    |       |
| fare_amount           | DOUBLE           | Yes  | false | NULL    |       |
| extra                 | DOUBLE           | Yes  | false | NULL    |       |
| mta_tax               | DOUBLE           | Yes  | false | NULL    |       |
| tip_amount            | DOUBLE           | Yes  | false | NULL    |       |
| tolls_amount          | DOUBLE           | Yes  | false | NULL    |       |
| ehail_fee             | DOUBLE           | Yes  | false | NULL    |       |
| improvement_surcharge | DOUBLE           | Yes  | false | NULL    |       |
| total_amount          | DOUBLE           | Yes  | false | NULL    |       |
| payment_type          | BIGINT           | Yes  | false | NULL    |       |
| trip_type             | BIGINT           | Yes  | false | NULL    |       |
| congestion_surcharge  | DOUBLE           | Yes  | false | NULL    |       |
+-----------------------+------------------+------+-------+---------+-------+
20 rows in set (0.04 sec)
```

:::tip
StarRocks 的许多文档使用 `\G` 而不是分号 `;` 结束语句。`\G` 用于指示 mysql CLI 在垂直方向上呈现查询结果。

但由于许多 SQL 客户端不支持垂直格式输出，因此您需要将 `\G` 替换为 `;`。
:::

## 使用 StarRocks 查询

### 验证接单时间数据格式

```sql
SELECT lpep_pickup_datetime FROM greentaxis LIMIT 10;
```

```plaintext
+----------------------+
| lpep_pickup_datetime |
+----------------------+
| 2023-05-01 00:52:10  |
| 2023-05-01 00:29:49  |
| 2023-05-01 00:25:19  |
| 2023-05-01 00:07:06  |
| 2023-05-01 00:43:31  |
| 2023-05-01 00:51:54  |
| 2023-05-01 00:27:46  |
| 2023-05-01 00:27:14  |
| 2023-05-01 00:24:14  |
| 2023-05-01 00:46:55  |
+----------------------+
10 rows in set (0.07 sec)
```

#### 查询高峰时期

此查询按每小时聚合行程数据，并显示一天中最繁忙的时间段是 18:00。

```sql
SELECT COUNT(*) AS trips,
       hour(lpep_pickup_datetime) AS hour_of_day
FROM greentaxis
GROUP BY hour_of_day
ORDER BY trips DESC;
```

```plaintext
+-------+-------------+
| trips | hour_of_day |
+-------+-------------+
|  5381 |          18 |
|  5253 |          17 |
|  5091 |          16 |
|  4736 |          15 |
|  4393 |          14 |
|  4275 |          19 |
|  3893 |          12 |
|  3816 |          11 |
|  3685 |          13 |
|  3616 |           9 |
|  3530 |          10 |
|  3361 |          20 |
|  3315 |           8 |
|  2917 |          21 |
|  2680 |           7 |
|  2322 |          22 |
|  1735 |          23 |
|  1202 |           6 |
|  1189 |           0 |
|   806 |           1 |
|   606 |           2 |
|   513 |           3 |
|   451 |           5 |
|   408 |           4 |
+-------+-------------+
24 rows in set (0.08 sec)
```

---

## 总结

本教程向您展示了如何使用 StarRocks External Catalog，以查询 Iceberg REST Catalog 中的数据。除 Iceberg 外，您还可以集成 Hive、Hudi、Delta Lake 和 JDBC 等其他数据源。

在本教程中，您：

- 在 Docker 中部署了 StarRocks、Iceberg、PySpark 和 MinIO 环境
- 配置了 StarRocks External Catalog，以访问 Iceberg 中的数据
- 将纽约市出租车数据导入至 Iceberg 数据湖中
- 在 StarRocks 中直接查询数据湖中的数据，无需复制数据湖至本地

## 更多信息

[StarRocks Catalog](../data_source/catalog/catalog_overview.md)

[Apache Iceberg 文档](https://iceberg.apache.org/docs/latest/) 和 [快速入门（包括 PySpark）](https://iceberg.apache.org/spark-quickstart/)

[绿色出租车行程记录](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)数据集由纽约市提供，受到以下[使用条款](https://www.nyc.gov/home/terms-of-use.page)和[隐私政策](https://www.nyc.gov/home/privacy-policy.page)的约束。
