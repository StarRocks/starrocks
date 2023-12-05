---
displayed_sidebar: "English"
sidebar_position: 3
description: Query Iceberg
---

# Query Iceberg
import Clients from '../assets/quick-start/_clientsCompose.mdx'

## PySpark

### Green Taxi dataset

Copy the data to the spark-iceberg container:

```bash
docker compose cp datasets/green_tripdata_2023-05.parquet spark-iceberg:/opt/spark/
```

### Launch PySpark

```bash
docker compose exec -it spark-iceberg pyspark
```

```python
Using Python version 3.9.18 (main, Nov  1 2023 11:04:44)
Spark context Web UI available at http://3e198a12df0b:4041
Spark context available as 'sc' (master = local[*], app id = local-1701792401103).
SparkSession available as 'spark'.
```

### Read in the dataset

The Green Taxi data is provided by NYC Taxi and Limousine Commission in Parquet format. Load the file from the `/opt/spark` directory and inspect the first few records by SELECTing from a temporary view.

```python
parquetFile = spark.read.parquet("/opt/spark/green_tripdata_2023-05.parquet")
parquetFile.createOrReplaceTempView("parquetFile")
firsttenrows = spark.sql("SELECT * from parquetFile LIMIT 10")
firsttenrows.show()
```

```plaintext
+--------+--------------------+---------------------+------------------+----------+------------+------------+---------------+-------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+------------+---------+--------------------+
|VendorID|lpep_pickup_datetime|lpep_dropoff_datetime|store_and_fwd_flag|RatecodeID|PULocationID|DOLocationID|passenger_count|trip_distance|fare_amount|extra|mta_tax|tip_amount|tolls_amount|ehail_fee|improvement_surcharge|total_amount|payment_type|trip_type|congestion_surcharge|
+--------+--------------------+---------------------+------------------+----------+------------+------------+---------------+-------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+------------+---------+--------------------+
|       2| 2023-05-01 00:52:10|  2023-05-01 01:05:26|                 N|         1|         244|         213|              1|         6.99|       28.9|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|        31.4|           1|        1|                 0.0|
|       2| 2023-05-01 00:29:49|  2023-05-01 00:50:11|                 N|         1|          33|         100|              1|          6.6|       30.3|  1.0|    0.5|       5.0|         0.0|     NULL|                  1.0|       40.55|           1|        1|                2.75|
|       2| 2023-05-01 00:25:19|  2023-05-01 00:32:12|                 N|         1|         244|         244|              1|         1.34|        9.3|  1.0|    0.5|      2.36|         0.0|     NULL|                  1.0|       14.16|           1|        1|                 0.0|
|       2| 2023-05-01 00:07:06|  2023-05-01 00:27:33|                 N|         5|          82|          75|              1|         7.79|      22.73|  0.0|    0.0|      2.29|        6.55|     NULL|                  1.0|       32.57|           1|        1|                 0.0|
|       2| 2023-05-01 00:43:31|  2023-05-01 00:46:59|                 N|         1|          69|         169|              1|          0.7|        6.5|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|         9.0|           2|        1|                 0.0|
|       2| 2023-05-01 00:51:54|  2023-05-01 01:00:24|                 N|         1|         169|          69|              1|         1.54|       10.0|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|        12.5|           1|        1|                 0.0|
|       2| 2023-05-01 00:27:46|  2023-05-01 00:49:17|                 N|         1|          75|          80|              1|        10.75|       42.2|  1.0|    0.5|       0.0|        6.55|     NULL|                  1.0|       51.25|           1|        1|                 0.0|
|       1| 2023-05-01 00:27:14|  2023-05-01 00:41:23|                 N|         1|          41|         244|              1|          4.4|       15.0|  0.5|    1.5|       3.4|         0.0|     NULL|                  1.0|        20.4|           1|        1|                 0.0|
|       2| 2023-05-01 00:24:14|  2023-05-01 00:35:16|                 N|         1|          74|         151|              1|          2.6|       14.2|  1.0|    0.5|      4.18|         0.0|     NULL|                  1.0|       20.88|           1|        1|                 0.0|
|       2| 2023-05-01 00:46:55|  2023-05-01 00:59:19|                 N|         1|         166|         244|              1|         2.72|       15.6|  1.0|    0.5|      3.62|         0.0|     NULL|                  1.0|       21.72|           1|        1|                 0.0|
+--------+--------------------+---------------------+------------------+----------+------------+------------+---------------+-------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+------------+---------+--------------------+
```

### Create a data frame

```python
df = spark.table("parquetFile").show()
```

```plaintext
+--------+--------------------+---------------------+------------------+----------+------------+------------+---------------+-------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+------------+---------+--------------------+
|VendorID|lpep_pickup_datetime|lpep_dropoff_datetime|store_and_fwd_flag|RatecodeID|PULocationID|DOLocationID|passenger_count|trip_distance|fare_amount|extra|mta_tax|tip_amount|tolls_amount|ehail_fee|improvement_surcharge|total_amount|payment_type|trip_type|congestion_surcharge|
+--------+--------------------+---------------------+------------------+----------+------------+------------+---------------+-------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+------------+---------+--------------------+
|       2| 2023-05-01 00:52:10|  2023-05-01 01:05:26|                 N|         1|         244|         213|              1|         6.99|       28.9|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|        31.4|           1|        1|                 0.0|
|       2| 2023-05-01 00:29:49|  2023-05-01 00:50:11|                 N|         1|          33|         100|              1|          6.6|       30.3|  1.0|    0.5|       5.0|         0.0|     NULL|                  1.0|       40.55|           1|        1|                2.75|
|       2| 2023-05-01 00:25:19|  2023-05-01 00:32:12|                 N|         1|         244|         244|              1|         1.34|        9.3|  1.0|    0.5|      2.36|         0.0|     NULL|                  1.0|       14.16|           1|        1|                 0.0|
|       2| 2023-05-01 00:07:06|  2023-05-01 00:27:33|                 N|         5|          82|          75|              1|         7.79|      22.73|  0.0|    0.0|      2.29|        6.55|     NULL|                  1.0|       32.57|           1|        1|                 0.0|
|       2| 2023-05-01 00:43:31|  2023-05-01 00:46:59|                 N|         1|          69|         169|              1|          0.7|        6.5|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|         9.0|           2|        1|                 0.0|
|       2| 2023-05-01 00:51:54|  2023-05-01 01:00:24|                 N|         1|         169|          69|              1|         1.54|       10.0|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|        12.5|           1|        1|                 0.0|
|       2| 2023-05-01 00:27:46|  2023-05-01 00:49:17|                 N|         1|          75|          80|              1|        10.75|       42.2|  1.0|    0.5|       0.0|        6.55|     NULL|                  1.0|       51.25|           1|        1|                 0.0|
|       1| 2023-05-01 00:27:14|  2023-05-01 00:41:23|                 N|         1|          41|         244|              1|          4.4|       15.0|  0.5|    1.5|       3.4|         0.0|     NULL|                  1.0|        20.4|           1|        1|                 0.0|
|       2| 2023-05-01 00:24:14|  2023-05-01 00:35:16|                 N|         1|          74|         151|              1|          2.6|       14.2|  1.0|    0.5|      4.18|         0.0|     NULL|                  1.0|       20.88|           1|        1|                 0.0|
|       2| 2023-05-01 00:46:55|  2023-05-01 00:59:19|                 N|         1|         166|         244|              1|         2.72|       15.6|  1.0|    0.5|      3.62|         0.0|     NULL|                  1.0|       21.72|           1|        1|                 0.0|
|       2| 2023-05-01 00:47:56|  2023-05-01 01:03:36|                 N|         1|          95|          82|              1|         2.95|       18.4|  1.0|    0.5|       2.0|         0.0|     NULL|                  1.0|        22.9|           1|        1|                 0.0|
|       2| 2023-05-01 00:01:24|  2023-05-01 00:09:45|                 N|         1|          95|          95|              2|         1.69|       10.0|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|        12.5|           2|        1|                 0.0|
|       2| 2023-05-01 00:11:52|  2023-05-01 01:06:57|                 N|         1|         129|          82|              2|         2.47|       42.2|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|        44.7|           2|        1|                 0.0|
|       2| 2023-05-01 00:43:51|  2023-05-01 01:10:50|                 N|         1|         173|         129|              2|          3.6|       25.4|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|        27.9|           2|        1|                 0.0|
|       2| 2023-05-01 00:55:01|  2023-05-01 01:02:57|                 N|         1|          82|         129|              1|         1.13|        8.6|  1.0|    0.5|      2.22|         0.0|     NULL|                  1.0|       13.32|           1|        1|                 0.0|
|       2| 2023-05-01 00:55:36|  2023-05-01 01:08:30|                 N|         1|          42|          41|              1|         1.78|       13.5|  1.0|    0.5|       3.2|         0.0|     NULL|                  1.0|        19.2|           1|        1|                 0.0|
|       2| 2023-05-01 00:22:54|  2023-05-01 00:32:09|                 N|         1|          92|          53|              1|          2.2|       12.8|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|        15.3|           2|        1|                 0.0|
|       2| 2023-05-01 00:22:55|  2023-05-01 00:22:59|                 N|         5|         182|         182|              1|          0.0|       25.0|  0.0|    0.0|       5.2|         0.0|     NULL|                  1.0|        31.2|           1|        2|                 0.0|
|       2| 2023-05-01 00:22:40|  2023-05-01 00:30:56|                 N|         1|          95|         135|              1|         2.29|       12.1|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|        14.6|           2|        1|                 0.0|
|       2| 2023-05-01 00:53:11|  2023-05-01 01:11:20|                 N|         1|         127|         235|              1|         6.23|       27.5|  1.0|    0.5|       0.0|         0.0|     NULL|                  1.0|        30.0|           2|        1|                 0.0|
+--------+--------------------+---------------------+------------------+----------+------------+------------+---------------+-------------+-----------+-----+-------+----------+------------+---------+---------------------+------------+------------+---------+--------------------+
only showing top 20 rows
```

### Inspect the schema

```python
spark.table("parquetFile").printSchema(50)
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
```

### Write to a table

- Catalog: `demo`
- Database: `nyc`
- Table:  `greentaxis`

```python
parquetFile.writeTo("demo.nyc.greentaxis").create()
```

## Query with StarRocks

```bash
docker compose exec starrocks \
  mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

```plaintext
StarRocks >
```

```sql
DROP CATALOG IF EXISTS iceberg;
```

```sql
CREATE EXTERNAL CATALOG 'iceberg'
PROPERTIES
(
  "type"="iceberg",
  "iceberg.catalog.type"="rest",
  "iceberg.catalog.uri"="http://iceberg-rest:8181",
  "iceberg.catalog.warehouse"="starrocks",
  "aws.s3.access_key"="admin",
  "aws.s3.secret_key"="password",
  "aws.s3.endpoint"="http://minio:9000",
  "aws.s3.enable_path_style_access"="true",
  "client.factory"="com.starrocks.connector.iceberg.IcebergAwsClientFactory"
);
```

```sql
StarRocks > SHOW CATALOGS;
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

```sql
SELECT COUNT(*) FROM greentaxis;
```

```plaintext
+----------+
| count(*) |
+----------+
|    69174 |
+----------+
1 row in set (0.27 sec)
```

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

```sql
SELECT DISTINCT hour(lpep_pickup_datetime) FROM greentaxis LIMIT 1000;
```

```plaintext
+----------------------------+
| hour(lpep_pickup_datetime) |
+----------------------------+
|                          0 |
|                          1 |
|                          3 |
|                         12 |
|                         13 |
|                         14 |
|                         15 |
|                         20 |
|                         21 |
|                         22 |
|                         23 |
|                          2 |
|                          4 |
|                          5 |
|                          6 |
|                          7 |
|                          8 |
|                          9 |
|                         10 |
|                         11 |
|                         16 |
|                         17 |
|                         18 |
|                         19 |
+----------------------------+
24 rows in set (0.09 sec)
```

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


In systems that separate storage from compute data is stored in low-cost reliable remote storage systems such as Amazon S3, Google Cloud Storage, Azure Blob Storage, and other S3-compatible storage like MinIO. Hot data is cached locally and When the cache is hit, the query performance is comparable to that of storage-compute coupled architecture. Compute nodes (CN) can be added or removed on demand within seconds. This architecture reduces storage cost, ensures better resource isolation, and provides elasticity and scalability.

This tutorial covers:

- Running StarRocks in Docker containers
- Using MinIO for Object Storage
- Configuring StarRocks for shared-data
- Loading two public datasets
- Analyzing the data with SELECT and JOIN
- Basic data transformation (the **T** in ETL)

The data used is provided by NYC OpenData and the National Centers for Environmental Information at NOAA.

Both of these datasets are very large, and because this tutorial is intended to help you get exposed to working with StarRocks we are not going to load data for the past 120 years. You can run the Docker image and load this data on a machine with 4 GB RAM assigned to Docker. For larger fault-tolerant and scalable deployments we have other documentation and will provide that later.

There is a lot of information in this document, and it is presented with the step by step content at the beginning, and the technical details at the end. This is done to serve these purposes in this order:

1. Allow the reader to load data in a shared-data deployment and analyze that data.
2. Provide the configuration details for shared-data deployments.
3. Explain the basics of data transformation during loading.

---

## Prerequisites

### Docker

- [Docker](https://docs.docker.com/engine/install/)
- 4 GB RAM assigned to Docker
- 10 GB free disk space assigned to Docker

### SQL client

You can use the SQL client provided in the Docker environment, or use one on your system. Many MySQL compatible clients will work, and this guide covers the configuration of DBeaver and MySQL WorkBench.

### curl

`curl` is used to issue the data load job to StarRocks, and to download the datasets. Check to see if you have it installed by running `curl` or `curl.exe` at your OS prompt. If curl is not installed, [get curl here](https://curl.se/dlwiz/?type=bin).

---

## Terminology

### FE
Frontend nodes are responsible for metadata management, client connection management, query planning, and query scheduling. Each FE stores and maintains a complete copy of metadata in its memory, which guarantees indiscriminate services among the FEs.

### CN
Compute Nodes are responsible for executing query plans in shared-data deployments.

### BE
Backend nodes are responsible for both data storage and executing query plans in shared-nothing deployments.

:::note
This guide does not use BEs, this information is included here so that you understand the difference between BEs and CNs.
:::

---

## Launch StarRocks

To run StarRocks with shared-data using Object Storage we need:

- A frontend engine (FE)
- A compute node (CN)
- Object Storage

This guide uses MinIO, which is S3 compatible Object Storage provided under the GNU Affero General Public License.

In order to provide an environment with the three necessary containers StarRocks provides a Docker compose file. 

```bash
mkdir quickstart
cd quickstart
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/docker-compose.yml
```

```bash
docker compose up -d
```

Check the progress of the services. It should take around 30 seconds for the FE and CN to become healthy. The MinIO container will not show a health indicator, but you will be using the MinIO web UI and that will verify its health.

Run `docker compose ps` until the FE and CN show a status of `healthy`:

```bash
docker compose ps
```

```plaintext
SERVICE        CREATED          STATUS                    PORTS
starrocks-cn   25 seconds ago   Up 24 seconds (healthy)   0.0.0.0:8040->8040/tcp
starrocks-fe   25 seconds ago   Up 24 seconds (healthy)   0.0.0.0:8030->8030/tcp, 0.0.0.0:9020->9020/tcp, 0.0.0.0:9030->9030/tcp
minio          25 seconds ago   Up 24 seconds             0.0.0.0:9000-9001->9000-9001/tcp
```

---

## Generate MinIO credentials

In order to use MinIO for Object Storage with StarRocks you need to generate an **access key**.

### Open the MinIO web UI

Browse to http://localhost:9001/access-keys The username and password are specified in the Docker compose file, and are `minioadmin` and `minioadmin`. You should see that there are no access keys yet. Click **Create access key +**.

MinIO will generate a key, click **Create** and download the key.

![Make sure to click create](../assets/quick-start/MinIO-create.png)

:::note
The access key is not saved until you click on **Create**, do not just copy the key and navigate away from the page
:::

---

## SQL Clients

<Clients />

---

## Download the data

Download these two datasets to your FE container.

### Open a shell on the FE container

Open a shell and create a directory for the downloaded files:

```bash
docker compose exec starrocks-fe bash
```

```bash
mkdir quickstart
cd quickstart
```

### New York City crash data

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

### Weather data

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---

## Configure StarRocks for shared-data

At this point you have StarRocks running, and you have MinIO running. The MinIO access key is used to connect StarRocks and Minio.

### Connect to StarRocks with a SQL client

:::tip

Run this command from the directory containing the `docker-compose.yml` file.

If you are using a client other than the mysql CLI, open that now.
:::

```sql
docker compose exec starrocks-fe \
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

### Create a storage volume

Details for the configuration shown below:

- The MinIO server is available at the URL `http://minio:9000`
- The bucket created above is named `starrocks`
- Data written to this volume will be stored in a folder named `shared` within the bucket `starrocks`
:::tip
The folder `shared` will be created the first time data is written to the volume
:::
- The MinIO server is not using SSL
- The MinIO key and secret are entered as `aws.s3.access_key` and `aws.s3.secret_key`. Use the access key that you created in the MinIO web UI earlier.
- The volume `shared` is the default volume

:::tip
Edit the command before you run it and replace the highlighted access key information with the access key and secret that you created in MinIO.
:::

```bash
CREATE STORAGE VOLUME shared
TYPE = S3
LOCATIONS = ("s3://starrocks/shared/")
PROPERTIES
(
    "enabled" = "true",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.use_instance_profile" = "false",
    # highlight-start
    "aws.s3.access_key" = "IA2UYcx3Wakpm6sHoFcl",
    "aws.s3.secret_key" = "E33cdRM9MfWpP2FiRpc056Zclg6CntXWa3WPBNMy"
    # highlight-end
);

SET shared AS DEFAULT STORAGE VOLUME;
```

```sql
DESC STORAGE VOLUME shared\G
```

:::tip
Some of the SQL in this document, and many other documents in the StarRocks documentation, and with `\G` instead
of a semicolon. The `\G` causes the mysql CLI to render the query results vertically.

Many SQL clients do not interpret vertical formatting output, so you should replace `\G` with `;`.
:::

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

:::note
The folder `shared` will not be visible in the MinIO object list until data is written to the bucket.
:::

---

## Verify that data is stored in MinIO

Open MinIO [http://localhost:9001/browser/starrocks/](http://localhost:9001/browser/starrocks/) and verify that you have `data`, `metadata`, and `schema` entries in each of the directories under `starrocks/shared/`

:::tip
The folder names below `starrocks/shared/` are generated when you load the data. You should see a single directory below `shared`, and then two more below that. Inside each of those directories you will find the data, metadata, and schema entries.

![MinIO object browser](../assets/quick-start/MinIO-data.png)
:::

---

## Answer some questions

---

## Configuring StarRocks for shared-data

Now that you have experienced using StarRocks with shared-data it is important to understand the configuration. 

### CN configuration

The CN configuration used here is the default, as the CN is designed for shared-data use. The default configuration is shown below. You do not need to make any changes.

```bash
sys_log_level = INFO

be_port = 9060
be_http_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
```

### FE configuration

The FE configuration is slightly different from the default as the FE must be configured to expect that data is stored in Object Storage rather than on local disks on BE nodes.

The `docker-compose.yml` file generates the FE configuration in the `command`.

```yml
    command: >
      bash -c "echo run_mode=shared_data >> /opt/starrocks/fe/conf/fe.conf &&
      echo cloud_native_meta_port=6090 >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_path=starrocks >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_endpoint=minio:9000 >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_use_instance_profile=false >> /opt/starrocks/fe/conf/fe.conf &&
      echo cloud_native_storage_type=S3 >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_use_aws_sdk_default_behavior=true >> /opt/starrocks/fe/conf/fe.conf &&
      sh /opt/starrocks/fe/bin/start_fe.sh"
```

This results in this config file:

```bash title='fe/fe.conf'
LOG_DIR = ${STARROCKS_HOME}/log

DATE = "$(date +%Y%m%d-%H%M%S)"
JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:${LOG_DIR}/fe.gc.log.$DATE -XX:+PrintConcurrentLocks"

JAVA_OPTS_FOR_JDK_11="-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC -Xlog:gc*:${LOG_DIR}/fe.gc.log.$DATE:time"

sys_log_level = INFO

http_port = 8030
rpc_port = 9020
query_port = 9030
edit_log_port = 9010
mysql_service_nio_enabled = true

# highlight-start
run_mode=shared_data
aws_s3_path=starrocks
aws_s3_endpoint=minio:9000
aws_s3_use_instance_profile=false
cloud_native_storage_type=S3
aws_s3_use_aws_sdk_default_behavior=true
# highlight-end
```

:::note
This config file contains the default entries and the additions for shared-data. The entries for shared-data are highlighted.
:::

The non-default FE configuration settings:

:::note
Many configuration parameters are prefixed with `s3_`. This prefix is used for all Amazon S3 compatible storage types (for example: S3, GCS, and MinIO). When using Azure Blob Storage the prefix is `azure_`.
:::

#### `run_mode=shared_data`

This enables shared-data use.

#### `aws_s3_path=starrocks`

The bucket name.

#### `aws_s3_endpoint=minio:9000`

The MinIO endpoint, including port number.

#### `aws_s3_use_instance_profile=false`

When using MinIO an access key is used, and so instance profiles are not used with MinIO.

#### `cloud_native_storage_type=S3`

This specifies whether S3 compatible storage or Azure Blob Storage is used. For MinIO this is always S3.

#### `aws_s3_use_aws_sdk_default_behavior=true`

When using MinIO this parameter is always set to true.

---

## Summary

In this tutorial you:

- Deployed StarRocks and Minio in Docker
- Created a MinIO access key
- Configured a StarRocks Storage Volume that uses MinIO
- Loaded crash data provided by New York City and weather data provided by NOAA
- Analyzed the data using SQL JOINs to find out that driving in low visibility or icy streets is a bad idea

## More information

[StarRocks table design](../table_design/StarRocks_table_design.md)

[Materialized views](../cover_pages/mv_use_cases.mdx)

[Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)

The [Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) dataset is provided by New York City subject to these [terms of use](https://www.nyc.gov/home/terms-of-use.page) and [privacy policy](https://www.nyc.gov/home/privacy-policy.page).
