---
displayed_sidebar: "English"
sidebar_position: 3
description: Query Iceberg
---

# Query Iceberg
import Clients from '../assets/quick-start/_clientsCompose.mdx'

## Overview

- Deploy an Iceberg warehouse using Docker compose
- Load New York City Green Taxi data for the month of May 2023 into the Iceberg warehouse
- Configure StarRocks to access the Iceberg warehouse using an external catalog
- Query the data with StarRocks where it sits

## Prerequisites

### Docker

- [Docker](https://docs.docker.com/engine/install/)
- 5 GB RAM assigned to Docker
- 20 GB free disk space assigned to Docker

### SQL client

You can use the SQL client provided in the Docker environment, or use one on your system. Many MySQL compatible clients will work, and this guide covers the configuration of DBeaver and MySQL WorkBench.

### curl

`curl` is used to download the datasets. Check to see if you have it installed by running `curl` or `curl.exe` at your OS prompt. If curl is not installed, [get curl here](https://curl.se/dlwiz/?type=bin).

---

## Terminology

### FE
Frontend nodes are responsible for metadata management, client connection management, query planning, and query scheduling. Each FE stores and maintains a complete copy of metadata in its memory, which guarantees indiscriminate services among the FEs.

### BE
Backend nodes are responsible for both data storage and executing query plans in shared-nothing deployments. When using an external catalog (like the Iceberg catalog used in this guide) only local data is stored on the BE node(s).

---

## The environment

To run StarRocks with an Iceberg warehouse using Object Storage we need:

- A frontend node (FE)
- A backend node (BE)
- Object Storage
- rest
- mc: MinIO Client
- minio: MinIO Server
- spark-iceberg:

This guide uses MinIO, which is S3 compatible Object Storage provided under the GNU Affero General Public License.

## Download the Docker configuration and NYC Green Taxi data

In order to provide an environment with the three necessary containers StarRocks provides a Docker compose file.  Download the compose file and dataset with curl.

The Docker compose file:
```bash
mkdir iceberg
cd iceberg
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/iceberg/docker-compose.yml
```

And the dataset:
```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/iceberg/datasets/green_tripdata_2023-05.parquet
```

## Start the environment in Docker

:::tip
Run this command, and any other `docker compose` commands, from the directory containing the `docker-compose.yml` file.
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

## Check the status of the environment

Check the progress of the services. It should take around 30 seconds for the FE and BE to become healthy.

Run `docker compose ps` until the FE and BE show a status of `healthy`. The rest of the services do not have
healthcheck configurations, but you will be interacting with them and will know whether or not they are working:

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

There are several ways to interact with Iceberg, this guide uses PySpark. If you are not familiar
with PySpark there is documentation linked from the More Information section, but every command
that you need to run is provided below.

### Green Taxi dataset

Copy the data to the spark-iceberg container. This command will copy the dataset file to the `/opt/spark/` directory in the `spark-iceberg` service:

```bash
docker compose \
cp green_tripdata_2023-05.parquet spark-iceberg:/opt/spark/
```

### Launch PySpark

This command will connect to the `spark-iceberg` service and run the command `pyspark`:

```bash
docker compose exec -it spark-iceberg pyspark
```

```python
Using Python version 3.9.18 (main, Nov  1 2023 11:04:44)
Spark context Web UI available at http://3e198a12df0b:4041
Spark context available as 'sc' (master = local[*], app id = local-1701792401103).
SparkSession available as 'spark'.
```

### Read the dataset into a dataframe

The Green Taxi data is provided by NYC Taxi and Limousine Commission in Parquet format. Load the file from the `/opt/spark` directory and inspect the first few records by SELECTing from a temporary view. These commands should be run in the `pyspark` session. The commands:

- Read the dataset file from disk into a dataframe named `df`
- Display the schema of the Parquet file

```python
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

### Write to a table

The table created in this step will be in the catalog that will be made available in StarRocks in the next step.

- Catalog: `demo`
- Database: `nyc`
- Table: `greentaxis`

```python
df.writeTo("demo.nyc.greentaxis").create()
```

## Configure StarRocks to access the Iceberg Catalog

### Connect to StarRocks with a SQL client

#### SQL Clients

<Clients />

---

You can now exit the PySpark session and connect to StarRocks.

:::tip

Run this command from the directory containing the `docker-compose.yml` file.

If you are using a client other than the mysql CLI, open that now.
:::


```bash
docker compose exec starrocks-fe \
  mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "
```

```plaintext
StarRocks >
```

### Create an external catalog

The external catalog is the configuration that allows StarRocks to operate on
the Iceberg data as if it was in StarRocks databases and tables. The individual 
configuration properties will be detailed after the command.

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

- type: In this example the type is `iceberg`. Other options include Hive, Hudi, Delta Lake, and JDBC.
- iceberg.catalog.type: In this example `rest` is used. Tabular provides the Docker image used and Tabular uses rest.
- iceberg.catalog.uri: The rest server endpoint.
- iceberg.catalog.warehouse: 

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
The database that you see was created in your PySpark
session. When you added the CATALOG `iceberg`, the 
database `nyc` became visible in StarRocks.
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
Some of the SQL in this document, and many other documents in the StarRocks documentation, and with `\G` instead
of a semicolon. The `\G` causes the mysql CLI to render the query results vertically.

Many SQL clients do not interpret vertical formatting output, so you should replace `\G` with `;`.
:::

## Query with StarRocks

### Verify pickup datetime format

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

#### Find the busy hours

This query aggregates the trips on the hour of the day and shows that
the busiest hour of the day is 18:00.

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

```plaintext
*************************** 1. row ***************************
     Name: shared
     Type: S3
IsDefault: true
# highlight-start
 Location: s3://starrocks/shared/
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"http://minio:9000","aws.s3.region":"us-east-1","aws.s3.use_instance_profile":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
# highlight-end
  Enabled: true
  Comment:
1 row in set (0.03 sec)
```
---

## Summary

In this tutorial you:

- Deployed StarRocks and Iceberg in Docker
- Configured a StarRocks external catalog to provide access to the Iceberg warehouse
- Loaded taxi data provided by New York City into the Iceberg warehouse
- Queried the data with SQL in StarRocks without copying the data from the warehouse

## More information

[Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/) and [Quickstart](https://iceberg.apache.org/spark-quickstart/)

The [Green Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset is provided by New York City subject to these [terms of use](https://www.nyc.gov/home/terms-of-use.page) and [privacy policy](https://www.nyc.gov/home/privacy-policy.page).
