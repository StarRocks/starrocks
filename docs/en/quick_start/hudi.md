---
displayed_sidebar: "English"
sidebar_position: 4
description: Data Lakehouse with Apache Hudi
toc_max_heading_level: 3
---
import DataLakeIntro from '../assets/commonMarkdown/datalakeIntro.md'
import Clients from '../assets/quick-start/_clientsCompose.mdx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Data Lakehouse with Apache Hudi

This guide is a copy of the [Apache Hudi Docker demo](https://hudi.apache.org/docs/docker_demo) with StarRocks added as the compute engine.

This guide is written based on Hudi version 0.14.1 running in Docker on a mac with an Apple Silicon (M2) CPU. Some of the instructions are mac specific. If you are running on Linux or on a mac with an Intel CPU there may be differences. Please send feedback in a GitHub issue or using the feedback widget at the bottom of this page if you use this guide on a different architecture.

## Prerequisites

### Hudi repository

Clone the [Hudi repository](https://github.com/apache/hudi) to your local machine.

All of the steps in this guide will be run from either the `hudi/` directory or the `hudi/docker` directory of the Hudi repo.

:::important
This guide is based on Hudi release tag `0.14.1`. Checkout the tag by using the `git checkout` command:

```bash
cd hudi
git checkout release-0.14.1
```

:::

### Docker

- Docker Setup: For Mac, Please follow the steps as defined in [Install Docker Desktop on Mac](https://docs.docker.com/desktop/install/mac-install/). For running Spark-SQL queries, please ensure at least 8 GB memory and 4 CPUs are allocated to Docker (See Docker -> Preferences -> Advanced). Otherwise, spark-SQL queries could be killed because of memory issues.
- 20 GB free disk space assigned to Docker

### Kafka utility `kcat`

kcat : A command-line utility to publish/consume from kafka topics. Use `brew install kcat` to install kcat.

### Hosts file 

The demo references many services running in container by the hostname. Add the following settings to `/etc/hosts`

  ```bash
  127.0.0.1 adhoc-1
  127.0.0.1 adhoc-2
  127.0.0.1 namenode
  127.0.0.1 datanode1
  127.0.0.1 hiveserver
  127.0.0.1 hivemetastore
  127.0.0.1 kafkabroker
  127.0.0.1 sparkmaster
  127.0.0.1 zookeeper
  ```

### Java and Apache Maven

To build Hudi Java SE 8 and Apache Maven are required.

#### Java SE 8

To list all installed Java versions on a Mac run the following:

```bash
/usr/libexec/java_home -V
```

If you have Java SE 8 installed you should see it in the output of the above command:

```bash
1.8.0_401 (arm64) "Oracle Corporation" - "Java SE 8" /Library/Java/JavaVirtualMachines/jdk-1.8.jdk/Contents/Home
```

If Java SE 8 is not installed, install it following the [Java SE 8](https://www.oracle.com/java/technologies/downloads/#java8-mac) instructions.

To choose the Java version used to build Hudi, set the `JAVA_HOME` environment variable using the version_patch identifier from the output of `/usr/libexec/java_home -V` (`1.8.0_41` in the example output above):

```bash
export JAVA_HOME=`/usr/libexec/java_home -v 1.8.0_401`
```

Check the Java version again:

```bash
java -version
```

:::tip
The JAVA_HOME variable is set for the current shell, make sure to check `java -version` before running the Maven command to build Hudi.
:::

#### Apache Maven

Maven is a build automation tool for Java projects. Install with `brew install mvn` or see the [Apache Maven project](https://maven.apache.org/install.html).

### jq

jq is a lightweight and flexible command-line JSON processor. Use `brew install jq` to install jq.
  
### SQL client

You can use the SQL client provided in the Docker environment, or use one on your system. Many MySQL compatible clients will work, and this guide covers the configuration of DBeaver and MySQL WorkBench.

## Build Hudi

Use Java and Maven to build Hudi:

1. `cd` into the Hudi repository that you cloned onto your machine.
2. Checkout the release 0.14.1 branch:
  ```bash
  git checkout release-0.14.1
  ```
3. Verify the Java version, if it is not set to `1.8` in your shell export the `JAVA_HOME` environment variable again:
  ```bash
  java -version
  ```
4. Build Hudi:
  ```bash
  mvn clean package -Pintegration-tests -DskipTests -Dscala-2.11 -Dspark2.4
  ```

## Add StarRocks

The demo is deployed using a Docker compose file. Add StarRocks to the compose file for your architecture (mac AArch64 or Intel) by editing the file matching your architecture in `./docker/compose/`:

- AArch64: `./docker/compose/docker-compose_hadoop284_hive233_spark244_mac_aarch64.yml`
- Intel: `./docker/compose/docker-compose_hadoop284_hive233_spark244.yml`

Add the `starrocks` service to the compose file below the existing line `services:`:

```yml
  starrocks:
    image: starrocks/allin1-ubuntu:latest
    hostname: starrocks-fe
    container_name: starrocks
    healthcheck:
      test: 'mysql -u root -h starrocks-fe -P 9030 -e "show backends\G" |grep "Alive: true"'
      interval: 10s
      timeout: 5s
      retries: 3
    ports:
      - 8030:8030
      - 8040:8040
      - 9030:9030
```



## Bringing up Demo Cluster

The next step is to run the Docker compose script and setup configs for bringing up the cluster. These files are in the [Hudi repository](https://github.com/apache/hudi) which you should already have locally on your machine from the previous steps. 

This should pull the Docker images from Docker hub and setup the Docker cluster.

<Tabs
defaultValue="m1"
values={[
{ label: 'Intel', value: 'intel', },
{ label: 'Mac AArch64', value: 'm1', },
]}
>
<TabItem value="intel">

```bash
cd docker
./setup_demo.sh
```
```plaintext
....
....
....
[+] Running 17/17
⠿ adhoc-1 Pulled                                          2.9s
⠿ graphite Pulled                                         2.8s
⠿ spark-worker-1 Pulled                                   3.0s
⠿ kafka Pulled                                            2.9s
⠿ datanode1 Pulled                                        2.9s
⠿ hivemetastore Pulled                                    2.9s
⠿ hiveserver Pulled                                       3.0s
⠿ hive-metastore-postgresql Pulled                        2.8s
⠿ presto-coordinator-1 Pulled                             2.9s
⠿ namenode Pulled                                         2.9s
⠿ trino-worker-1 Pulled                                   2.9s
⠿ sparkmaster Pulled                                      2.9s
⠿ presto-worker-1 Pulled                                  2.9s
⠿ zookeeper Pulled                                        2.8s
⠿ adhoc-2 Pulled                                          2.9s
⠿ historyserver Pulled                                    2.9s
⠿ trino-coordinator-1 Pulled                              2.9s
[+] Running 17/17
⠿ Container zookeeper                  Started           41.0s
⠿ Container kafkabroker                Started           41.7s
⠿ Container graphite                   Started           41.5s
⠿ Container hive-metastore-postgresql  Running            0.0s
⠿ Container namenode                   Running            0.0s
⠿ Container hivemetastore              Running            0.0s
⠿ Container trino-coordinator-1        Runni...           0.0s
⠿ Container presto-coordinator-1       Star...           42.1s
⠿ Container historyserver              Started           41.0s
⠿ Container datanode1                  Started           49.9s
⠿ Container hiveserver                 Running            0.0s
⠿ Container trino-worker-1             Started           42.1s
⠿ Container sparkmaster                Started           41.9s
⠿ Container spark-worker-1             Started           50.2s
⠿ Container adhoc-2                    Started           38.5s
⠿ Container adhoc-1                    Started           38.5s
⠿ Container presto-worker-1            Started           38.4s
Copying spark default config and setting up configs
Copying spark default config and setting up configs
```

</TabItem>
<TabItem value="m1">

:::note Please note the following for Mac AArch64 users
<ul>
  <li> The demo must be built and run using the release-0.14.1 tag. </li>
  <li> Presto and Trino are not currently supported in the demo. </li>
  <li> You will see warnings that there is no history server for your architecture. You can ignore these. </li>
  <li> You wil see the warning "Unable to load native-hadoop library for your platform... using builtin-java classes where applicable." You can ignore this. </li>
</ul>
:::

```bash
cd docker
./setup_demo.sh --mac-aarch64
```

```plaintext
.......
......
[+] Running 12/12
⠿ adhoc-1 Pulled                                          2.9s
⠿ spark-worker-1 Pulled                                   3.0s
⠿ kafka Pulled                                            2.9s
⠿ datanode1 Pulled                                        2.9s
⠿ hivemetastore Pulled                                    2.9s
⠿ hiveserver Pulled                                       3.0s
⠿ hive-metastore-postgresql Pulled                        2.8s
⠿ namenode Pulled                                         2.9s
⠿ sparkmaster Pulled                                      2.9s
⠿ zookeeper Pulled                                        2.8s
⠿ adhoc-2 Pulled                                          2.9s
⠿ historyserver Pulled                                    2.9s
[+] Running 12/12
⠿ Container zookeeper                  Started           41.0s
⠿ Container kafkabroker                Started           41.7s
⠿ Container hive-metastore-postgresql  Running            0.0s
⠿ Container namenode                   Running            0.0s
⠿ Container hivemetastore              Running            0.0s
⠿ Container historyserver              Started           41.0s
⠿ Container datanode1                  Started           49.9s
⠿ Container hiveserver                 Running            0.0s
⠿ Container sparkmaster                Started           41.9s
⠿ Container spark-worker-1             Started           50.2s
⠿ Container adhoc-2                    Started           38.5s
⠿ Container adhoc-1                    Started           38.5s
Copying spark default config and setting up configs
Copying spark default config and setting up configs
```
</TabItem>

</Tabs
> 

At this point, the Docker cluster will be up and running. The demo cluster brings up the following services

   * HDFS Services (NameNode, DataNode)
   * Spark Master and Worker
   * Hive Services (Metastore, HiveServer2 along with PostgresDB)
   * Kafka Broker and a Zookeeper Node (Kafka will be used as upstream source for the demo)
   * Containers for Presto setup (Presto coordinator and worker)
   * Containers for Trino setup (Trino coordinator and worker)
   * StarRocks
   * Adhoc containers to run Hudi/Hive CLI commands

:::tip

With many containers running, `docker ps` output is easier to read if you pipe it to `jq`:

```bash
docker ps --format json | jq '{Name: .Names, State: .State, Status: .Status}'
```

```json
{
  "Name": "adhoc-1",
  "State": "running",
  "Status": "Up 47 seconds"
}
{
  "Name": "spark-worker-1",
  "State": "running",
  "Status": "Up 47 seconds"
}
{
  "Name": "adhoc-2",
  "State": "running",
  "Status": "Up 47 seconds"
}
{
  "Name": "sparkmaster",
  "State": "running",
  "Status": "Up 47 seconds"
}
{
  "Name": "hiveserver",
  "State": "running",
  "Status": "Up 47 seconds"
}
{
  "Name": "datanode1",
  "State": "running",
  "Status": "Up 47 seconds (healthy)"
}
{
  "Name": "hivemetastore",
  "State": "running",
  "Status": "Up 48 seconds (healthy)"
}
{
  "Name": "historyserver",
  "State": "running",
  "Status": "Up 48 seconds (healthy)"
}
{
  "Name": "namenode",
  "State": "running",
  "Status": "Up 48 seconds (healthy)"
}
{
  "Name": "starrocks",
  "State": "running",
  "Status": "Up 48 seconds (healthy)"
}
{
  "Name": "hive-metastore-postgresql",
  "State": "running",
  "Status": "Up 48 seconds"
}
{
  "Name": "zookeeper",
  "State": "running",
  "Status": "Up 48 seconds"
}
{
  "Name": "kafkabroker",
  "State": "running",
  "Status": "Up 48 seconds"
}
```

:::

## Demo

Stock Tracker data will be used to showcase different Hudi query types and the effects of Compaction.

Take a look at the directory `docker/demo/data`. There are 2 batches of stock data - each at 1 minute granularity.
The first batch contains stocker tracker data for some stock symbols during the first hour of trading window
(9:30 a.m to 10:30 a.m). The second batch contains tracker data for next 30 mins (10:30 - 11 a.m). Hudi will
be used to ingest these batches to a table which will contain the latest stock tracker data at hour level granularity.
The batches are windowed intentionally so that the second batch contains updates to some of the rows in the first batch.

### Step 0 : Switch to the hudi directory

The commands for the demo need to be run from the `hudi/` directory, if you are in the `hudi/docker/` directory cd up one directory.

### Step 1 : Publish the first batch to Kafka

Upload the first batch to Kafka topic 'stock ticks' 

```bash
cat docker/demo/data/batch_1.json | kcat -b kafkabroker -t stock_ticks -P
```

To check if the new topic shows up, use
```bash
kcat -b kafkabroker -L -J | jq .
```

Expected output:
```json
{
  "originating_broker": {
    "id": 1001,
    "name": "kafkabroker:9092/1001"
  },
  "query": {
    "topic": "*"
  },
  "brokers": [
    {
      "id": 1001,
      "name": "kafkabroker:9092"
    }
  ],
  "topics": [
    {
      "topic": "stock_ticks",
      "partitions": [
        {
          "partition": 0,
          "leader": 1001,
          "replicas": [
            {
              "id": 1001
            }
          ],
          "isrs": [
            {
              "id": 1001
            }
          ]
        }
      ]
    }
  ]
}
```

### Step 2: Incrementally ingest data from Kafka topic

Hudi comes with a tool named Hudi Streamer. This tool can connect to variety of data sources (including Kafka) to
pull changes and apply to Hudi table using upsert/insert primitives. Here, we will use the tool to download
json data from kafka topic and ingest to both COW and MOR tables we initialized in the previous step. This tool
automatically initializes the tables in the file-system if they do not exist yet.

#### Populate tables in HDFS

1. Open a shell in the `adhoc-2` container:

  ```bash
  docker exec -it adhoc-2 /bin/bash
  ```

2. Run the following spark-submit command to execute the Hudi Streamer and ingest to the `stock_ticks_cow` table in HDFS:

```bash
spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts  \
  --target-base-path /user/hive/warehouse/stock_ticks_cow \
  --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider
```

3. Run the following spark-submit command to execute the Hudi Streamer and ingest to the `stock_ticks_mor` table in HDFS:

```bash
spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path /user/hive/warehouse/stock_ticks_mor \
  --target-table stock_ticks_mor \
  --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --disable-compaction
```

4. Exit the shell

```bash
exit
```

:::info
During setup (look at `setup_demo.sh`), the configuration needed for Hudi Streamer is uploaded to HDFS. The configs contain Kafa connectivity settings and the Avro schema to be used for ingestion, along with key and partitioning fields.
:::

5. You can use the HDFS web-browser to look at the tables, open
[http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_cow](http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_cow) and explore the new partition folder created in the table along with a "commit" / "deltacommit" file under `.hoodie` which signals a successful commit.<br/><br/>
There will be a similar setup when you browse the MOR table
[http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_mor](http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_mor).


### Step 3: Sync with Hive

At this step, the tables are available in HDFS. We need to sync with Hive to create new Hive tables and add partitions
to run Hive queries against those tables.

1. Open a shell in the `adhoc-2` container:

```bash
docker exec -it adhoc-2 /bin/bash
```

2. This command takes in HiveServer URL and COW Hudi table location in HDFS and syncs the HDFS state to Hive:
```bash
/var/hoodie/ws/hudi-sync/hudi-hive-sync/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --partitioned-by dt \
  --base-path /user/hive/warehouse/stock_ticks_cow \
  --database default \
  --table stock_ticks_cow \
  --partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
```

Expected output:

```plaintext
.....
2020-01-25 19:51:28,953 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_cow
.....
```

3. Now run hive-sync for the second data-set in HDFS using Merge-On-Read (MOR table type)

```bash
/var/hoodie/ws/hudi-sync/hudi-hive-sync/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --partitioned-by dt \
  --base-path /user/hive/warehouse/stock_ticks_mor \
  --database default \
  --table stock_ticks_mor \
  --partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
```

Expected output:

```plaintext
...
2020-01-25 19:51:51,066 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_mor_ro
...
2020-01-25 19:51:51,569 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_mor_rt
....
```

4. Exit the shell:

```bash
exit
```

There are now Hive tables available. These will be queried using Hive queries and StarRocks queries in the next steps.

### Step 4 (a): Run Hive Queries

Run a hive query to find the latest timestamp ingested for stock symbol 'GOOG'. You will notice that both snapshot 
(for both COW and MOR _rt table) and read-optimized queries (for MOR _ro table) give the same value "10:29 a.m" as Hudi create a
parquet file for the first batch of data.

1. Open a shell in the `adhoc-2` container:

```bash
docker exec -it adhoc-2 /bin/bash
```

2. Start the Beeline client

```bash
beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false
```

:::tip
The rest of the steps will be run in the Beeline client at the `0: jdbc:hive2://hiveserver:10000>` prompt.
:::

3. Show tables

```bash
show tables;
```

Expected output:
```plaintext
+---------------------+--+
|      tab_name       |
+---------------------+--+
| stock_ticks_cow     |
| stock_ticks_mor_ro  |
| stock_ticks_mor_rt  |
+---------------------+--+
3 rows selected (0.203 seconds)
```


4. Look at the partitions that were added:

```bash
show partitions stock_ticks_mor_rt;
```

Expected output:
```plaintext
+----------------+--+
|   partition    |
+----------------+--+
| dt=2018-08-31  |
+----------------+--+
1 row selected (0.2 seconds)
```


#### Copy on write queries:

1. Query:

```bash
select symbol, max(ts) from stock_ticks_cow
group by symbol HAVING symbol = 'GOOG';
```

Expected output:
```plaintext
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
```

2. projection

```bash
select `_hoodie_commit_time`, symbol, ts, volume, open, close
from stock_ticks_cow where  symbol = 'GOOG';
```

Expected output:
```plaintext
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20240124213322801    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20240124213322801    | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
```


#### Merge-on-read queries

Lets run similar queries against M-O-R table. Lets look at both 
ReadOptimized and Snapshot(realtime data) queries supported by M-O-R table

1. Run ReadOptimized query. Note the latest timestamp:

```bash
select symbol, max(ts) from stock_ticks_mor_ro
group by symbol HAVING symbol = 'GOOG';
```

Expected output:
```plaintext
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
```


2. Run a snapshot query. Note that the timestamp matches the ReadOptimized query:

```bash
select symbol, max(ts) from stock_ticks_mor_rt
group by symbol HAVING symbol = 'GOOG';
```

Expected output:
```plaintext
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (1.805 seconds)
```


3. Run ReadOptimized and Snapshot project queries

```bash
select `_hoodie_commit_time`, symbol, ts, volume, open, close
from stock_ticks_mor_ro where  symbol = 'GOOG';
```

Expected output:
```plaintext
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20240124213736817    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20240124213736817    | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
2 rows selected (0.321 seconds)
```

```bash
select `_hoodie_commit_time`, symbol, ts, volume, open, close
from stock_ticks_mor_rt where  symbol = 'GOOG';
```

Expected output:
```plaintext
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20240124213736817    | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20240124213736817    | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
2 rows selected (0.452 seconds)
```

4. Exit the shell:

```bash
exit
```

### Step 4 (b): Run StarRocks Queries

```sql
docker exec -it starrocks \
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "
```

```sql
CREATE EXTERNAL CATALOG hudi_catalog_hms PROPERTIES (
  "type" = "hudi",
  "hive.metastore.uris" = "thrift://hivemetastore:9083"
);
```

```sql
SET CATALOG hudi_catalog_hms;
```

```sql
USE default;
```

```sql
SHOW TABLES;
```

```plaintext
+--------------------+
| Tables_in_default  |
+--------------------+
| stock_ticks_cow    |
| stock_ticks_mor_ro |
| stock_ticks_mor_rt |
+--------------------+
3 rows in set (0.02 sec)
```

```sql
REFRESH EXTERNAL TABLE stock_ticks_cow;

REFRESH EXTERNAL TABLE stock_ticks_mor_ro;

REFRESH EXTERNAL TABLE stock_ticks_mor_rt;
```

```sql
SELECT count(*) FROM stock_ticks_cow;
```
```plaintext
+----------+
| count(*) |
+----------+
|      197 |
+----------+
1 row in set (1.46 sec)
```

```sql
SELECT symbol, max(ts)
FROM stock_ticks_cow
GROUP BY symbol HAVING symbol = 'GOOG';
```

```plaintext
+--------+---------------------+
| symbol | max(ts)             |
+--------+---------------------+
| GOOG   | 2018-08-31 10:29:00 |
+--------+---------------------+
1 row in set (0.15 sec)
```

```sql
SELECT "_hoodie_commit_time", symbol, ts, volume, open, close
FROM stock_ticks_cow
WHERE symbol = 'GOOG';
```

```plaintext
+-----------------------+--------+---------------------+--------+-----------+----------+
| '_hoodie_commit_time' | symbol | ts                  | volume | open      | close    |
+-----------------------+--------+---------------------+--------+-----------+----------+
| _hoodie_commit_time   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 |
| _hoodie_commit_time   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 |
+-----------------------+--------+---------------------+--------+-----------+----------+
2 rows in set (0.17 sec)
```

```sql
SELECT symbol, max(ts)
FROM stock_ticks_mor_ro
GROUP BY symbol HAVING symbol = 'GOOG';
```

```plaintext
+--------+---------------------+
| symbol | max(ts)             |
+--------+---------------------+
| GOOG   | 2018-08-31 10:29:00 |
+--------+---------------------+
1 row in set (0.11 sec)
```

```sql
SELECT "_hoodie_commit_time", symbol, ts, volume, open, close
FROM stock_ticks_mor_ro
WHERE symbol = 'GOOG';
```
```plaintext
+-----------------------+--------+---------------------+--------+-----------+----------+
| '_hoodie_commit_time' | symbol | ts                  | volume | open      | close    |
+-----------------------+--------+---------------------+--------+-----------+----------+
| _hoodie_commit_time   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 |
| _hoodie_commit_time   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 |
+-----------------------+--------+---------------------+--------+-----------+----------+
2 rows in set (0.10 sec)
```

```sql
SELECT symbol, max(ts)
FROM stock_ticks_mor_ro
GROUP BY symbol HAVING symbol = 'GOOG';
```

```plaintext
+--------+---------------------+
| symbol | max(ts)             |
+--------+---------------------+
| GOOG   | 2018-08-31 10:29:00 |
+--------+---------------------+
1 row in set (0.18 sec)
```

```sql
SELECT "_hoodie_commit_time", symbol, ts, volume, open, close
FROM stock_ticks_mor_ro
WHERE symbol = 'GOOG';
```

```plaintext
+-----------------------+--------+---------------------+--------+-----------+----------+
| '_hoodie_commit_time' | symbol | ts                  | volume | open      | close    |
+-----------------------+--------+---------------------+--------+-----------+----------+
| _hoodie_commit_time   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 |
| _hoodie_commit_time   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 |
+-----------------------+--------+---------------------+--------+-----------+----------+
2 rows in set (0.08 sec)
```

```sql
SELECT "_hoodie_commit_time", symbol, ts, volume, open, close
FROM stock_ticks_mor_ro WHERE  symbol = 'GOOG';
```

```plaintext
+-----------------------+--------+---------------------+--------+-----------+----------+
| '_hoodie_commit_time' | symbol | ts                  | volume | open      | close    |
+-----------------------+--------+---------------------+--------+-----------+----------+
| _hoodie_commit_time   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 |
| _hoodie_commit_time   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 |
+-----------------------+--------+---------------------+--------+-----------+----------+
2 rows in set (0.07 sec)
```
### Step 5: Upload second batch to Kafka and run Hudi Streamer to ingest

Upload the second batch of data and ingest this batch using Hudi Streamer. As this batch does not bring in any new
partitions, there is no need to run hive-sync

```bash
cat docker/demo/data/batch_2.json | kcat -b kafkabroker -t stock_ticks -P

# Within Docker container, run the ingestion command
docker exec -it adhoc-2 /bin/bash

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_cow table in HDFS
spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path /user/hive/warehouse/stock_ticks_cow \
  --target-table stock_ticks_cow \
  --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_mor table in HDFS
spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path /user/hive/warehouse/stock_ticks_mor \
  --target-table stock_ticks_mor \
  --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --disable-compaction

exit
```

With Copy-On-Write table, the second ingestion by Hudi Streamer resulted in a new version of Parquet file getting created.
See `http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_cow/2018/08/31`

With Merge-On-Read table, the second ingestion merely appended the batch to an unmerged delta (log) file.
Take a look at the HDFS filesystem to get an idea: `http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_mor/2018/08/31`

### Step 6 (a): Run Hive Queries

With Copy-On-Write table, the Snapshot query immediately sees the changes as part of second batch once the batch
got committed as each ingestion creates newer versions of parquet files.

With Merge-On-Read table, the second ingestion merely appended the batch to an unmerged delta (log) file.
This is the time, when ReadOptimized and Snapshot queries will provide different results. ReadOptimized query will still
return "10:29 am" as it will only read from the Parquet file. Snapshot query will do on-the-fly merge and return
latest committed data which is "10:59 a.m".

```bash
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false

# Copy On Write Table:

0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+
1 row selected (1.932 seconds)

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924221953       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924224524       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.


# Merge On Read Table:

# Read Optimized Query
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (1.6 seconds)

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Snapshot Query
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924224537       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

exit
```

### Step 7 (a): Incremental Query for COPY-ON-WRITE Table

With 2 batches of data ingested, lets showcase the support for incremental queries in Hudi Copy-On-Write tables

Lets take the same projection query example

```bash
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924064621       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924065039       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
```

As you notice from the above queries, there are 2 commits - 20180924064621 and 20180924065039 in timeline order.
When you follow the steps, you will be getting different timestamps for commits. Substitute them
in place of the above timestamps.

To show the effects of incremental-query, let us assume that a reader has already seen the changes as part of
ingesting first batch. Now, for the reader to see effect of the second batch, he/she has to keep the start timestamp to
the commit time of the first batch (20180924064621) and run incremental query

Hudi incremental mode provides efficient scanning for incremental queries by filtering out files that do not have any
candidate rows using hudi-managed metadata.

```bash
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false

0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.mode=INCREMENTAL;
No rows affected (0.009 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.max.commits=3;
No rows affected (0.009 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_cow.consume.start.timestamp=20180924064621;
```

With the above setting, file-ids that do not have any updates from the commit 20180924065039 is filtered out without scanning.
Here is the incremental query :

```java
0: jdbc:hive2://hiveserver:10000>
0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG' and `_hoodie_commit_time` > '20180924064621';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924065039       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+
1 row selected (0.83 seconds)
0: jdbc:hive2://hiveserver:10000>
```


### Step 8: Schedule and Run Compaction for Merge-On-Read table

Lets schedule and run a compaction to create a new version of columnar  file so that read-optimized readers will see fresher data.
Again, You can use Hudi CLI to manually schedule and run compaction

```bash
docker exec -it adhoc-1 /bin/bash
root@adhoc-1:/opt# /var/hoodie/ws/hudi-cli/hudi-cli.sh
...
Table command getting loaded
HoodieSplashScreen loaded
===================================================================
*         ___                          ___                        *
*        /\__\          ___           /\  \           ___         *
*       / /  /         /\__\         /  \  \         /\  \        *
*      / /__/         / /  /        / /\ \  \        \ \  \       *
*     /  \  \ ___    / /  /        / /  \ \__\       /  \__\      *
*    / /\ \  /\__\  / /__/  ___   / /__/ \ |__|     / /\/__/      *
*    \/  \ \/ /  /  \ \  \ /\__\  \ \  \ / /  /  /\/ /  /         *
*         \  /  /    \ \  / /  /   \ \  / /  /   \  /__/          *
*         / /  /      \ \/ /  /     \ \/ /  /     \ \__\          *
*        / /  /        \  /  /       \  /  /       \/__/          *
*        \/__/          \/__/         \/__/    Apache Hudi CLI    *
*                                                                 *
===================================================================

Welcome to Apache Hudi CLI. Please type help if you are looking for help.
hudi->connect --path /user/hive/warehouse/stock_ticks_mor
18/09/24 06:59:34 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/09/24 06:59:35 INFO table.HoodieTableMetaClient: Loading HoodieTableMetaClient from /user/hive/warehouse/stock_ticks_mor
18/09/24 06:59:35 INFO util.FSUtils: Hadoop Configuration: fs.defaultFS: [hdfs://namenode:8020], Config:[Configuration: core-default.xml, core-site.xml, mapred-default.xml, mapred-site.xml, yarn-default.xml, yarn-site.xml, hdfs-default.xml, hdfs-site.xml], FileSystem: [DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_-1261652683_11, ugi=root (auth:SIMPLE)]]]
18/09/24 06:59:35 INFO table.HoodieTableConfig: Loading table properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
18/09/24 06:59:36 INFO table.HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ(version=1) from /user/hive/warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor loaded
hoodie:stock_ticks_mor->compactions show all
20/02/10 03:41:32 INFO timeline.HoodieActiveTimeline: Loaded instants [[20200210015059__clean__COMPLETED], [20200210015059__deltacommit__COMPLETED], [20200210022758__clean__COMPLETED], [20200210022758__deltacommit__COMPLETED], [==>20200210023843__compaction__REQUESTED]]
___________________________________________________________________
| Compaction Instant Time| State    | Total FileIds to be Compacted|
|==================================================================|

# Schedule a compaction. This will use Spark Launcher to schedule compaction
hoodie:stock_ticks_mor->compaction schedule --hoodieConfigs hoodie.compact.inline.max.delta.commits=1
....
Compaction successfully completed for 20180924070031

# Now refresh and check again. You will see that there is a new compaction requested

hoodie:stock_ticks_mor->refresh
18/09/24 07:01:16 INFO table.HoodieTableMetaClient: Loading HoodieTableMetaClient from /user/hive/warehouse/stock_ticks_mor
18/09/24 07:01:16 INFO table.HoodieTableConfig: Loading table properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
18/09/24 07:01:16 INFO table.HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ(version=1) from /user/hive/warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor loaded

hoodie:stock_ticks_mor->compactions show all
18/09/24 06:34:12 INFO timeline.HoodieActiveTimeline: Loaded instants [[20180924041125__clean__COMPLETED], [20180924041125__deltacommit__COMPLETED], [20180924042735__clean__COMPLETED], [20180924042735__deltacommit__COMPLETED], [==>20180924063245__compaction__REQUESTED]]
___________________________________________________________________
| Compaction Instant Time| State    | Total FileIds to be Compacted|
|==================================================================|
| 20180924070031         | REQUESTED| 1                            |

# Execute the compaction. The compaction instant value passed below must be the one displayed in the above "compactions show all" query
hoodie:stock_ticks_mor->compaction run --compactionInstant  20180924070031 --parallelism 2 --sparkMemory 1G  --schemaFilePath /var/demo/config/schema.avsc --retry 1  
....
Compaction successfully completed for 20180924070031

## Now check if compaction is completed

hoodie:stock_ticks_mor->refresh
18/09/24 07:03:00 INFO table.HoodieTableMetaClient: Loading HoodieTableMetaClient from /user/hive/warehouse/stock_ticks_mor
18/09/24 07:03:00 INFO table.HoodieTableConfig: Loading table properties from /user/hive/warehouse/stock_ticks_mor/.hoodie/hoodie.properties
18/09/24 07:03:00 INFO table.HoodieTableMetaClient: Finished Loading Table of type MERGE_ON_READ(version=1) from /user/hive/warehouse/stock_ticks_mor
Metadata for table stock_ticks_mor loaded

hoodie:stock_ticks_mor->compactions show all
18/09/24 07:03:15 INFO timeline.HoodieActiveTimeline: Loaded instants [[20180924064636__clean__COMPLETED], [20180924064636__deltacommit__COMPLETED], [20180924065057__clean__COMPLETED], [20180924065057__deltacommit__COMPLETED], [20180924070031__commit__COMPLETED]]
___________________________________________________________________
| Compaction Instant Time| State    | Total FileIds to be Compacted|
|==================================================================|
| 20180924070031         | COMPLETED| 1                            |

```

### Step 9: Run Hive Queries including incremental queries

You will see that both ReadOptimized and Snapshot queries will show the latest committed data.
Lets also run the incremental query for MOR table.
From looking at the below query output, it will be clear that the fist commit time for the MOR table is 20180924064636
and the second commit time is 20180924070031

```bash
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false

# Read Optimized Query
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+
1 row selected (1.6 seconds)

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924064636       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Snapshot Query
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+--+

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924064636       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

# Incremental Query:

0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.mode=INCREMENTAL;
No rows affected (0.008 seconds)
# Max-Commits covers both second batch and compaction commit
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.max.commits=3;
No rows affected (0.007 seconds)
0: jdbc:hive2://hiveserver:10000> set hoodie.stock_ticks_mor.consume.start.timestamp=20180924064636;
No rows affected (0.013 seconds)
# Query:
0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG' and `_hoodie_commit_time` > '20180924064636';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

exit
```

