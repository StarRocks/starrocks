---
displayed_sidebar: "English"
sidebar_position: 4
description: Data Lakehouse with Apache Hudi
toc_max_heading_level: 2
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


Now that you have a working Hudi environment add in StarRocks. Edit the Docker compose file and add in StarRocks. The compose files are in the `compose` dir of the `hudi` repo that you cloned:
- Intel: docker-compose_hadoop284_hive233_spark244.yml
- AArm64: docker-compose_hadoop284_hive233_spark244_mac_aarch64.yml

Add this:

```yml
  starrocks:
    image: starrocks/allin1-ubuntu:latest
    hostname: starrocks-fe
    container_name: allin1-ubuntu
    ports:
      - 8030:8030
      - 8040:8040
      - 9030:9030
```

Restart the demo environment with StarRocks added:

./setup_demo.sh --mac-aarch64


cd up one dir into the `hudi` dir to run the demo scenario 

```bash
cat docker/demo/data/batch_1.json | kcat -b kafkabroker -t stock_ticks -P
```

```bash
kcat -b kafkabroker -L -J | jq .
```

```bash
docker exec -it adhoc-2 /bin/bash
```
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

```bash
exit
```

[http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_cow](http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_cow)

```bash
docker exec -it adhoc-2 /bin/bash
```

# This command takes in HiveServer URL and COW Hudi table location in HDFS and sync the HDFS state to Hive
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
.....
2020-01-25 19:51:28,953 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_cow
.....

# Now run hive-sync for the second data-set in HDFS using Merge-On-Read (MOR table type)
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
...
2020-01-25 19:51:51,066 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_mor_ro
...
2020-01-25 19:51:51,569 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_mor_rt
....

```bash
exit
```
run the demo commands to load data with Kafka and confirm that directories and files are created (use a browser to check)

Queries in StarRocks:
```sql
docker exec -it allin1-ubuntu \
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "
```

```sql
CREATE EXTERNAL CATALOG hudi_catalog_hms PROPERTIES (     "type" = "hudi",       "hive.metastore.uris" = "thrift://hivemetastore:9083" );
```

```sql
SET CATALOG hudi_catalog_hms;
```

```sql
USE default;
```

```sql
SELECT count(*) FROM stock_ticks_cow;
```

ERROR 1064 (HY000): com.google.common.util.concurrent.UncheckedExecutionException: java.lang.NullPointerException: null value in entry: date=2018-08-31=null
StarRocks > select count(*) from stock_ticks_cow;
ERROR 1064 (HY000): com.google.common.util.concurrent.UncheckedExecutionException: java.lang.NullPointerException: null value in entry: date=2018-08-31=null
StarRocks > show tables;
+--------------------+
| Tables_in_default  |
+--------------------+
| stock_ticks_cow    |
| stock_ticks_mor_ro |
| stock_ticks_mor_rt |
+--------------------+
3 rows in set (0.02 sec)

StarRocks > select count(*) from stock_ticks_cow;
ERROR 1064 (HY000): com.google.common.util.concurrent.UncheckedExecutionException: java.lang.NullPointerException: null value in entry: date=2018-08-31=null

```sql
refresh external table stock_ticks_cow;
```

    -> ;
Query OK, 0 rows affected (0.14 sec)

StarRocks > select count(*) from stock_ticks_cow;
+----------+
| count(*) |
+----------+
|      197 |
+----------+
1 row in set (1.46 sec)

StarRocks > select count(*) from stock_ticks_cow;
+----------+
| count(*) |
+----------+
|      197 |
+----------+
1 row in set (0.18 sec)

StarRocks > elect symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
ERROR 1064 (HY000): Getting syntax error at line 1, column 0. Detail message: Unexpected input 'elect', the most similar input is {'SELECT', 'DELETE', 'EXECUTE', 'DEALLOCATE', 'SET', '(', ';'}.
StarRocks > select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
+--------+---------------------+
| symbol | max(ts)             |
+--------+---------------------+
| GOOG   | 2018-08-31 10:29:00 |
+--------+---------------------+
1 row in set (0.15 sec)

StarRocks > select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
+-----------------------+--------+---------------------+--------+-----------+----------+
| '_hoodie_commit_time' | symbol | ts                  | volume | open      | close    |
+-----------------------+--------+---------------------+--------+-----------+----------+
| _hoodie_commit_time   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 |
| _hoodie_commit_time   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 |
+-----------------------+--------+---------------------+--------+-----------+----------+
2 rows in set (0.17 sec)

StarRocks > select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
ERROR 1064 (HY000): com.google.common.util.concurrent.UncheckedExecutionException: java.lang.NullPointerException: null value in entry: date=2018-08-31=null
StarRocks > refresh external table stock_ticks_cow
    -> ;
Query OK, 0 rows affected (0.14 sec)

StarRocks > select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
ERROR 1064 (HY000): com.google.common.util.concurrent.UncheckedExecutionException: java.lang.NullPointerException: null value in entry: date=2018-08-31=null
StarRocks > refresh external table stock_ticks_mor_ro;
Query OK, 0 rows affected (0.08 sec)

StarRocks > select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
+--------+---------------------+
| symbol | max(ts)             |
+--------+---------------------+
| GOOG   | 2018-08-31 10:29:00 |
+--------+---------------------+
1 row in set (0.11 sec)

StarRocks > select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
+-----------------------+--------+---------------------+--------+-----------+----------+
| '_hoodie_commit_time' | symbol | ts                  | volume | open      | close    |
+-----------------------+--------+---------------------+--------+-----------+----------+
| _hoodie_commit_time   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 |
| _hoodie_commit_time   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 |
+-----------------------+--------+---------------------+--------+-----------+----------+
2 rows in set (0.10 sec)

StarRocks > select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
+--------+---------------------+
| symbol | max(ts)             |
+--------+---------------------+
| GOOG   | 2018-08-31 10:29:00 |
+--------+---------------------+
1 row in set (0.18 sec)

StarRocks > select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
+-----------------------+--------+---------------------+--------+-----------+----------+
| '_hoodie_commit_time' | symbol | ts                  | volume | open      | close    |
+-----------------------+--------+---------------------+--------+-----------+----------+
| _hoodie_commit_time   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 |
| _hoodie_commit_time   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 |
+-----------------------+--------+---------------------+--------+-----------+----------+
2 rows in set (0.08 sec)

StarRocks > select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
+-----------------------+--------+---------------------+--------+-----------+----------+
| '_hoodie_commit_time' | symbol | ts                  | volume | open      | close    |
+-----------------------+--------+---------------------+--------+-----------+----------+
| _hoodie_commit_time   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 |
| _hoodie_commit_time   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 |
+-----------------------+--------+---------------------+--------+-----------+----------+
2 rows in set (0.07 sec)

StarRocks >
```

Stop everything

From the docker dir:

- Intel: ./stop_demo.sh
- AArm64: ./stop_demo.sh --mac-aarch64


CREATE EXTERNAL CATALOG hudi_catalog_hms PROPERTIES (
  "type" = "hudi",
   "aws.s3.enable_ssl" = "false",
   "aws.s3.enable_path_style_access" = "true",
   "aws.s3.endpoint" = "starrocks-minio:9000",
   "aws.s3.access_key" = "pNqPG26oExf3Bz95qZcX",
   "aws.s3.secret_key" = "bvIuffcNjudNeEOCnSMdxkuy2WAdJ0wPirq7QEHk"
  "hive.metastore.uris" = "thrift://hivemetastore:9083"
);
Let's use a real world example to see how Hudi works end to end. For this purpose, a self contained
data infrastructure is brought up in a local Docker cluster within your computer. It requires the
Hudi repo to have been cloned locally. 

The steps have been tested on a Mac laptop

### Prerequisites



## Setting up Docker Cluster


### Build Hudi

The first step is to build Hudi. **Note** This step builds Hudi on default supported scala version - 2.11.

NOTE: Make sure you've cloned the [Hudi repository](https://github.com/apache/hudi) first. 

```bash
cd <HUDI_WORKSPACE>
git checkout release-0.14.1
mvn clean package -Pintegration-tests -DskipTests -Dscala-2.11 -Dspark2.4
```

### Bringing up Demo Cluster

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
   * Adhoc containers to run Hudi/Hive CLI commands

:::tip

With many containers running, `docker ps` output is easier to read if you pipe it to `jq`:

```bash
docker ps --format json | jq '{Image: .Image, State: .State, Status: .Status}'
```

```
{
  "Image": "apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkadhoc_2.4.4:linux-arm64-0.10.1",
  "State": "running",
  "Status": "Up 5 minutes"
}
{
  "Image": "apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkworker_2.4.4:linux-arm64-0.10.1",
  "State": "running",
  "Status": "Up 5 minutes"
}
{
  "Image": "apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkadhoc_2.4.4:linux-arm64-0.10.1",
  "State": "running",
  "Status": "Up 5 minutes"
}
{
  "Image": "apachehudi/hudi-hadoop_2.8.4-hive_2.3.3-sparkmaster_2.4.4:linux-arm64-0.10.1",
  "State": "running",
  "Status": "Up 5 minutes"
}
{
  "Image": "apachehudi/hudi-hadoop_2.8.4-hive_2.3.3:linux-arm64-0.10.1",
  "State": "running",
  "Status": "Up 5 minutes"
}
{
  "Image": "apachehudi/hudi-hadoop_2.8.4-datanode:linux-arm64-0.10.1",
  "State": "running",
  "Status": "Up 5 minutes (healthy)"
}
{
  "Image": "apachehudi/hudi-hadoop_2.8.4-history:latest",
  "State": "running",
  "Status": "Up 5 minutes (healthy)"
}
{
  "Image": "apachehudi/hudi-hadoop_2.8.4-hive_2.3.3:linux-arm64-0.10.1",
  "State": "running",
  "Status": "Up 5 minutes (healthy)"
}
{
  "Image": "apachehudi/hudi-hadoop_2.8.4-namenode:linux-arm64-0.10.1",
  "State": "running",
  "Status": "Up 5 minutes (healthy)"
}
{
  "Image": "menorah84/hive-metastore-postgresql:2.3.0",
  "State": "running",
  "Status": "Up 5 minutes"
}
{
  "Image": "arm64v8/zookeeper:3.4.12",
  "State": "running",
  "Status": "Up 5 minutes"
}
{
  "Image": "wurstmeister/kafka:2.12-2.0.1",
  "State": "running",
  "Status": "Up 5 minutes"
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

### Step 1 : Publish the first batch to Kafka

Upload the first batch to Kafka topic 'stock ticks' 

`cat docker/demo/data/batch_1.json | kcat -b kafkabroker -t stock_ticks -P`

To check if the new topic shows up, use
```java
kcat -b kafkabroker -L -J | jq .
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

```java
docker exec -it adhoc-2 /bin/bash

# Run the following spark-submit command to execute the Hudi Streamer and ingest to stock_ticks_cow table in HDFS
spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts  \
  --target-base-path /user/hive/warehouse/stock_ticks_cow \
  --target-table stock_ticks_cow --props /var/demo/config/kafka-source.properties \
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

# As part of the setup (Look at setup_demo.sh), the configs needed for Hudi Streamer is uploaded to HDFS. The configs
# contain mostly Kafa connectivity settings, the avro-schema to be used for ingesting along with key and partitioning fields.

exit
```

You can use HDFS web-browser to look at the tables
`http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_cow`.

You can explore the new partition folder created in the table along with a "commit" / "deltacommit"
file under .hoodie which signals a successful commit.

There will be a similar setup when you browse the MOR table
`http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_mor`


### Step 3: Sync with Hive

At this step, the tables are available in HDFS. We need to sync with Hive to create new Hive tables and add partitions
inorder to run Hive queries against those tables.

```java
docker exec -it adhoc-2 /bin/bash

# This command takes in HiveServer URL and COW Hudi table location in HDFS and sync the HDFS state to Hive
/var/hoodie/ws/hudi-sync/hudi-hive-sync/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --partitioned-by dt \
  --base-path /user/hive/warehouse/stock_ticks_cow \
  --database default \
  --table stock_ticks_cow \
  --partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
.....
2020-01-25 19:51:28,953 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_cow
.....

# Now run hive-sync for the second data-set in HDFS using Merge-On-Read (MOR table type)
/var/hoodie/ws/hudi-sync/hudi-hive-sync/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --partitioned-by dt \
  --base-path /user/hive/warehouse/stock_ticks_mor \
  --database default \
  --table stock_ticks_mor \
  --partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
...
2020-01-25 19:51:51,066 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_mor_ro
...
2020-01-25 19:51:51,569 INFO  [main] hive.HiveSyncTool (HiveSyncTool.java:syncHoodieTable(129)) - Sync complete for stock_ticks_mor_rt
....

exit
```
After executing the above command, you will notice

1. A hive table named `stock_ticks_cow` created which supports Snapshot and Incremental queries on Copy On Write table.
2. Two new tables `stock_ticks_mor_rt` and `stock_ticks_mor_ro` created for the Merge On Read table. The former
supports Snapshot and Incremental queries (providing near-real time data) while the later supports ReadOptimized queries. `http://namenode:50070/explorer.html#/user/hive/warehouse/stock_ticks_mor`


### Step 4 (a): Run Hive Queries

Run a hive query to find the latest timestamp ingested for stock symbol 'GOOG'. You will notice that both snapshot 
(for both COW and MOR _rt table) and read-optimized queries (for MOR _ro table) give the same value "10:29 a.m" as Hudi create a
parquet file for the first batch of data.

```java
docker exec -it adhoc-2 /bin/bash
beeline -u jdbc:hive2://hiveserver:10000 \
  --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
  --hiveconf hive.stats.autogather=false

# List Tables
0: jdbc:hive2://hiveserver:10000> show tables;
+---------------------+--+
|      tab_name       |
+---------------------+--+
| stock_ticks_cow     |
| stock_ticks_mor_ro  |
| stock_ticks_mor_rt  |
+---------------------+--+
3 rows selected (1.199 seconds)
0: jdbc:hive2://hiveserver:10000>


# Look at partitions that were added
0: jdbc:hive2://hiveserver:10000> show partitions stock_ticks_mor_rt;
+----------------+--+
|   partition    |
+----------------+--+
| dt=2018-08-31  |
+----------------+--+
1 row selected (0.24 seconds)


# COPY-ON-WRITE Queries:
=========================


0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+

Now, run a projection query:

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924221953       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924221953       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+


# Merge-On-Read Queries:
==========================

Lets run similar queries against M-O-R table. Lets look at both 
ReadOptimized and Snapshot(realtime data) queries supported by M-O-R table

# Run ReadOptimized Query. Notice that the latest timestamp is 10:29
0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (6.326 seconds)


# Run Snapshot Query. Notice that the latest timestamp is again 10:29

0: jdbc:hive2://hiveserver:10000> select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
+---------+----------------------+--+
| symbol  |         _c1          |
+---------+----------------------+--+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+--+
1 row selected (1.606 seconds)


# Run Read Optimized and Snapshot project queries

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

0: jdbc:hive2://hiveserver:10000> select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';
+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

exit
```

### Step 4 (b): Run Spark-SQL Queries
Hudi support Spark as query processor just like Hive. Here are the same hive queries
running in spark-sql

```java
docker exec -it adhoc-1 /bin/bash
$SPARK_INSTALL/bin/spark-shell \
  --jars $HUDI_SPARK_BUNDLE \
  --master local[2] \
  --driver-class-path $HADOOP_CONF_DIR \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  --deploy-mode client \
  --driver-memory 1G \
  --executor-memory 3G \
  --num-executors 1
...

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.4
      /_/

Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_212)
Type in expressions to have them evaluated.
Type :help for more information.

scala> spark.sql("show tables").show(100, false)
+--------+------------------+-----------+
|database|tableName         |isTemporary|
+--------+------------------+-----------+
|default |stock_ticks_cow   |false      |
|default |stock_ticks_mor_ro|false      |
|default |stock_ticks_mor_rt|false      |
+--------+------------------+-----------+

# Copy-On-Write Table

## Run max timestamp query against COW table

scala> spark.sql("select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'").show(100, false)
[Stage 0:>                                                          (0 + 1) / 1]SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes#StaticLoggerBinder for further details.
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+

## Projection Query

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20180924221953     |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20180924221953     |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+

# Merge-On-Read Queries:
==========================

Lets run similar queries against M-O-R table. Lets look at both
ReadOptimized and Snapshot queries supported by M-O-R table

# Run ReadOptimized Query. Notice that the latest timestamp is 10:29
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+


# Run Snapshot Query. Notice that the latest timestamp is again 10:29

scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:29:00|
+------+-------------------+

# Run Read Optimized and Snapshot project queries

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20180924222155     |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20180924222155     |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+-------------------+------+-------------------+------+---------+--------+
|_hoodie_commit_time|symbol|ts                 |volume|open     |close   |
+-------------------+------+-------------------+------+---------+--------+
|20180924222155     |GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |
|20180924222155     |GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|
+-------------------+------+-------------------+------+---------+--------+
```

### Step 4 (c): Run Presto Queries

Here are the Presto queries for similar Hive and Spark queries. 

:::note 
<ul>
  <li> Currently, Presto does not support snapshot or incremental queries on Hudi tables. </li>
  <li> This section of the demo is not supported for Mac AArch64 users at this time. </li>
</ul>
:::

```java
docker exec -it presto-worker-1 presto --server presto-coordinator-1:8090
presto> show catalogs;
  Catalog
-----------
 hive
 jmx
 localfile
 system
(4 rows)

Query 20190817_134851_00000_j8rcz, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:04 [0 rows, 0B] [0 rows/s, 0B/s]

presto> use hive.default;
USE
presto:default> show tables;
       Table
--------------------
 stock_ticks_cow
 stock_ticks_mor_ro
 stock_ticks_mor_rt
(3 rows)

Query 20190822_181000_00001_segyw, FINISHED, 2 nodes
Splits: 19 total, 19 done (100.00%)
0:05 [3 rows, 99B] [0 rows/s, 18B/s]


# COPY-ON-WRITE Queries:
=========================


presto:default> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20190822_181011_00002_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:12 [197 rows, 613B] [16 rows/s, 50B/s]

presto:default> select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_cow where symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20190822180221      | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20190822180221      | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085
(2 rows)

Query 20190822_181141_00003_segyw, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:02 [197 rows, 613B] [109 rows/s, 341B/s]


# Merge-On-Read Queries:
==========================

Lets run similar queries against M-O-R table. 

# Run ReadOptimized Query. Notice that the latest timestamp is 10:29
    presto:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20190822_181158_00004_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:02 [197 rows, 613B] [110 rows/s, 343B/s]


presto:default>  select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20190822180250      | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20190822180250      | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085
(2 rows)

Query 20190822_181256_00006_segyw, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:02 [197 rows, 613B] [92 rows/s, 286B/s]

presto:default> exit
```

### Step 4 (d): Run Trino Queries

Here are the similar queries with Trino.
:::note
<ul>
  <li> Currently, Trino does not support snapshot or incremental queries on Hudi tables. </li>
  <li> This section of the demo is not supported for Mac AArch64 users at this time. </li>
</ul>
:::

```java
docker exec -it adhoc-2 trino --server trino-coordinator-1:8091
trino> show catalogs;
 Catalog 
---------
 hive    
 system  
(2 rows)

Query 20220112_055038_00000_sac73, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
3.74 [0 rows, 0B] [0 rows/s, 0B/s]

trino> use hive.default;
USE
trino:default> show tables;
       Table        
--------------------
 stock_ticks_cow    
 stock_ticks_mor_ro 
 stock_ticks_mor_rt 
(3 rows)

Query 20220112_055050_00003_sac73, FINISHED, 2 nodes
Splits: 19 total, 19 done (100.00%)
1.84 [3 rows, 102B] [1 rows/s, 55B/s]

# COPY-ON-WRITE Queries:
=========================
    
trino:default> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1        
--------+---------------------
 GOOG   | 2018-08-31 10:29:00 
(1 row)

Query 20220112_055101_00005_sac73, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
4.08 [197 rows, 442KB] [48 rows/s, 108KB/s]

trino:default> select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_cow where symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close   
---------------------+--------+---------------------+--------+-----------+----------
 20220112054822108   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 
 20220112054822108   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 
(2 rows)

Query 20220112_055113_00006_sac73, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0.40 [197 rows, 450KB] [487 rows/s, 1.09MB/s]

# Merge-On-Read Queries:
==========================

Lets run similar queries against MOR table.

# Run ReadOptimized Query. Notice that the latest timestamp is 10:29
    
trino:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1        
--------+---------------------
 GOOG   | 2018-08-31 10:29:00 
(1 row)

Query 20220112_055125_00007_sac73, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0.50 [197 rows, 442KB] [395 rows/s, 888KB/s]

trino:default> select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close   
---------------------+--------+---------------------+--------+-----------+----------
 20220112054844841   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 
 20220112054844841   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 
(2 rows)

Query 20220112_055136_00008_sac73, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0.49 [197 rows, 450KB] [404 rows/s, 924KB/s]

trino:default> exit
```

### Step 5: Upload second batch to Kafka and run Hudi Streamer to ingest

Upload the second batch of data and ingest this batch using Hudi Streamer. As this batch does not bring in any new
partitions, there is no need to run hive-sync

```java
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

```java
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

### Step 6 (b): Run Spark SQL Queries

Running the same queries in Spark-SQL:

```java
docker exec -it adhoc-1 /bin/bash
$SPARK_INSTALL/bin/spark-shell \
  --jars $HUDI_SPARK_BUNDLE \
  --driver-class-path $HADOOP_CONF_DIR \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  --deploy-mode client \
  --driver-memory 1G \
  --master local[2] \
  --executor-memory 3G \
  --num-executors 1

# Copy On Write Table:

scala> spark.sql("select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'").show(100, false)
+------+-------------------+
|symbol|max(ts)            |
+------+-------------------+
|GOOG  |2018-08-31 10:59:00|
+------+-------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG'").show(100, false)

+----------------------+---------+----------------------+---------+------------+-----------+--+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+--+
| 20180924221953       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924224524       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+--+

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.


# Merge On Read Table:

# Read Optimized Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+
| symbol  |         _c1          |
+---------+----------------------+
| GOOG    | 2018-08-31 10:29:00  |
+---------+----------------------+
1 row selected (1.6 seconds)

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924222155       | GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |
+----------------------+---------+----------------------+---------+------------+-----------+

# Snapshot Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+
| symbol  |         _c1          |
+---------+----------------------+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+
| 20180924222155       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924224537       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+

exit
```

### Step 6 (c): Run Presto Queries

Running the same queries on Presto for ReadOptimized queries. 

:::note
This section of the demo is not supported for Mac AArch64 users at this time.
:::

```java
docker exec -it presto-worker-1 presto --server presto-coordinator-1:8090
presto> use hive.default;
USE

# Copy On Write Table:

presto:default>select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:59:00
(1 row)

Query 20190822_181530_00007_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:02 [197 rows, 613B] [125 rows/s, 389B/s]

presto:default>select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20190822180221      | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20190822181433      | GOOG   | 2018-08-31 10:59:00 |   9021 | 1227.1993 | 1227.215
(2 rows)

Query 20190822_181545_00008_segyw, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:02 [197 rows, 613B] [106 rows/s, 332B/s]

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.


# Merge On Read Table:

# Read Optimized Query
presto:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:29:00
(1 row)

Query 20190822_181602_00009_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:01 [197 rows, 613B] [139 rows/s, 435B/s]

presto:default>select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20190822180250      | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20190822180250      | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085
(2 rows)

Query 20190822_181615_00010_segyw, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:01 [197 rows, 613B] [154 rows/s, 480B/s]

presto:default> exit
```

### Step 6 (d): Run Trino Queries

Running the same queries on Trino for Read-Optimized queries.

:::note
This section of the demo is not supported for Mac AArch64 users at this time.
:::

```java
docker exec -it adhoc-2 trino --server trino-coordinator-1:8091
trino> use hive.default;
USE
    
# Copy On Write Table:

trino:default> select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1        
--------+---------------------
 GOOG   | 2018-08-31 10:59:00 
(1 row)

Query 20220112_055443_00012_sac73, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0.63 [197 rows, 442KB] [310 rows/s, 697KB/s]

trino:default> select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close   
---------------------+--------+---------------------+--------+-----------+----------
 20220112054822108   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 
 20220112055352654   | GOOG   | 2018-08-31 10:59:00 |   9021 | 1227.1993 | 1227.215 
(2 rows)

Query 20220112_055450_00013_sac73, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0.65 [197 rows, 450KB] [303 rows/s, 692KB/s]

As you can notice, the above queries now reflect the changes that came as part of ingesting second batch.

# Merge On Read Table:
# Read Optimized Query
    
trino:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
 symbol |        _col1        
--------+---------------------
 GOOG   | 2018-08-31 10:29:00 
(1 row)

Query 20220112_055500_00014_sac73, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0.59 [197 rows, 442KB] [336 rows/s, 756KB/s]

trino:default> select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close   
---------------------+--------+---------------------+--------+-----------+----------
 20220112054844841   | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02 
 20220112054844841   | GOOG   | 2018-08-31 10:29:00 |   3391 | 1230.1899 | 1230.085 
(2 rows)

Query 20220112_055506_00015_sac73, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0.35 [197 rows, 450KB] [556 rows/s, 1.24MB/s]

trino:default> exit
```

### Step 7 (a): Incremental Query for COPY-ON-WRITE Table

With 2 batches of data ingested, lets showcase the support for incremental queries in Hudi Copy-On-Write tables

Lets take the same projection query example

```java
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

```java
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

### Step 7 (b): Incremental Query with Spark SQL:

```java
docker exec -it adhoc-1 /bin/bash
$SPARK_INSTALL/bin/spark-shell \
  --jars $HUDI_SPARK_BUNDLE \
  --driver-class-path $HADOOP_CONF_DIR \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  --deploy-mode client \
  --driver-memory 1G \
  --master local[2] \
  --executor-memory 3G \
  --num-executors 1

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.4
      /_/

Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_212)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceReadOptions

# In the below query, 20180925045257 is the first commit's timestamp
scala> val hoodieIncViewDF =  spark.read.format("org.apache.hudi").option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL).option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20180924064621").load("/user/hive/warehouse/stock_ticks_cow")
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes#StaticLoggerBinder for further details.
hoodieIncViewDF: org.apache.spark.sql.DataFrame = [_hoodie_commit_time: string, _hoodie_commit_seqno: string ... 15 more fields]

scala> hoodieIncViewDF.registerTempTable("stock_ticks_cow_incr_tmp1")
warning: there was one deprecation warning; re-run with -deprecation for details

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow_incr_tmp1 where  symbol = 'GOOG'").show(100, false);
+----------------------+---------+----------------------+---------+------------+-----------+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+
| 20180924065039       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+
```

### Step 8: Schedule and Run Compaction for Merge-On-Read table

Lets schedule and run a compaction to create a new version of columnar  file so that read-optimized readers will see fresher data.
Again, You can use Hudi CLI to manually schedule and run compaction

```java
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

```java
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

### Step 10: Read Optimized and Snapshot queries for MOR with Spark-SQL after compaction

```java
docker exec -it adhoc-1 /bin/bash
$SPARK_INSTALL/bin/spark-shell \
  --jars $HUDI_SPARK_BUNDLE \
  --driver-class-path $HADOOP_CONF_DIR \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  --deploy-mode client \
  --driver-memory 1G \
  --master local[2] \
  --executor-memory 3G \
  --num-executors 1

# Read Optimized Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+
| symbol  |        max(ts)       |
+---------+----------------------+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+
1 row selected (1.6 seconds)

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+
| 20180924064636       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+

# Snapshot Query
scala> spark.sql("select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'").show(100, false)
+---------+----------------------+
| symbol  |     max(ts)          |
+---------+----------------------+
| GOOG    | 2018-08-31 10:59:00  |
+---------+----------------------+

scala> spark.sql("select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'").show(100, false)
+----------------------+---------+----------------------+---------+------------+-----------+
| _hoodie_commit_time  | symbol  |          ts          | volume  |    open    |   close   |
+----------------------+---------+----------------------+---------+------------+-----------+
| 20180924064636       | GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |
| 20180924070031       | GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |
+----------------------+---------+----------------------+---------+------------+-----------+
```

### Step 11:  Presto Read Optimized queries on MOR table after compaction
:::note
This section of the demo is not supported for Mac AArch64 users at this time.
:::

```java
docker exec -it presto-worker-1 presto --server presto-coordinator-1:8090
presto> use hive.default;
USE

# Read Optimized Query
resto:default> select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
  symbol |        _col1
--------+---------------------
 GOOG   | 2018-08-31 10:59:00
(1 row)

Query 20190822_182319_00011_segyw, FINISHED, 1 node
Splits: 49 total, 49 done (100.00%)
0:01 [197 rows, 613B] [133 rows/s, 414B/s]

presto:default> select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
 _hoodie_commit_time | symbol |         ts          | volume |   open    |  close
---------------------+--------+---------------------+--------+-----------+----------
 20190822180250      | GOOG   | 2018-08-31 09:59:00 |   6330 |    1230.5 |  1230.02
 20190822181944      | GOOG   | 2018-08-31 10:59:00 |   9021 | 1227.1993 | 1227.215
(2 rows)

Query 20190822_182333_00012_segyw, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:02 [197 rows, 613B] [98 rows/s, 307B/s]

presto:default>
```


This brings the demo to an end.

## Testing Hudi in Local Docker environment

You can bring up a Hadoop Docker environment containing Hadoop, Hive and Spark services with support for Hudi.
```java
$ mvn pre-integration-test -DskipTests
```
The above command builds Docker images for all the services with
current Hudi source installed at /var/hoodie/ws and also brings up the services using a compose file. We
currently use Hadoop (v2.8.4), Hive (v2.3.3) and Spark (v2.4.4) in Docker images.

To bring down the containers
```java
$ cd hudi-integ-test
$ mvn docker-compose:down
```

If you want to bring up the Docker containers, use
```java
$ cd hudi-integ-test
$ mvn docker-compose:up -DdetachedMode=true
```

Hudi is a library that is operated in a broader data analytics/ingestion environment
involving Hadoop, Hive and Spark. Interoperability with all these systems is a key objective for us. We are
actively adding integration-tests under __hudi-integ-test/src/test/java__ that makes use of this
docker environment (See __hudi-integ-test/src/test/java/org/apache/hudi/integ/ITTestHoodieSanity.java__ )


### Building Local Docker Containers:

The Docker images required for demo and running integration test are already in docker-hub. The Docker images
and compose scripts are carefully implemented so that they serve dual-purpose

1. The Docker images have inbuilt Hudi jar files with environment variable pointing to those jars (HUDI_HADOOP_BUNDLE, ...)
2. For running integration-tests, we need the jars generated locally to be used for running services within docker. The
   docker-compose scripts (see `docker/compose/docker-compose_hadoop284_hive233_spark244.yml`) ensures local jars override
   inbuilt jars by mounting local Hudi workspace over the Docker location
3. As these Docker containers have mounted local Hudi workspace, any changes that happen in the workspace would automatically 
   reflect in the containers. This is a convenient way for developing and verifying Hudi for
   developers who do not own a distributed environment. Note that this is how integration tests are run.

This helps avoid maintaining separate Docker images and avoids the costly step of building Hudi Docker images locally.
But if users want to test Hudi from locations with lower network bandwidth, they can still build local images
run the script
`docker/build_local_docker_images.sh` to build local Docker images before running `docker/setup_demo.sh`

Here are the commands:

```java
cd docker
./build_local_docker_images.sh
.....

[INFO] Reactor Summary:
[INFO]
[INFO] Hudi ............................................... SUCCESS [  2.507 s]
[INFO] hudi-common ........................................ SUCCESS [ 15.181 s]
[INFO] hudi-aws ........................................... SUCCESS [  2.621 s]
[INFO] hudi-timeline-service .............................. SUCCESS [  1.811 s]
[INFO] hudi-client ........................................ SUCCESS [  0.065 s]
[INFO] hudi-client-common ................................. SUCCESS [  8.308 s]
[INFO] hudi-hadoop-mr ..................................... SUCCESS [  3.733 s]
[INFO] hudi-spark-client .................................. SUCCESS [ 18.567 s]
[INFO] hudi-sync-common ................................... SUCCESS [  0.794 s]
[INFO] hudi-hive-sync ..................................... SUCCESS [  3.691 s]
[INFO] hudi-spark-datasource .............................. SUCCESS [  0.121 s]
[INFO] hudi-spark-common_2.11 ............................. SUCCESS [ 12.979 s]
[INFO] hudi-spark2_2.11 ................................... SUCCESS [ 12.516 s]
[INFO] hudi-spark_2.11 .................................... SUCCESS [ 35.649 s]
[INFO] hudi-utilities_2.11 ................................ SUCCESS [  5.881 s]
[INFO] hudi-utilities-bundle_2.11 ......................... SUCCESS [ 12.661 s]
[INFO] hudi-cli ........................................... SUCCESS [ 19.858 s]
[INFO] hudi-java-client ................................... SUCCESS [  3.221 s]
[INFO] hudi-flink-client .................................. SUCCESS [  5.731 s]
[INFO] hudi-spark3_2.12 ................................... SUCCESS [  8.627 s]
[INFO] hudi-dla-sync ...................................... SUCCESS [  1.459 s]
[INFO] hudi-sync .......................................... SUCCESS [  0.053 s]
[INFO] hudi-hadoop-mr-bundle .............................. SUCCESS [  5.652 s]
[INFO] hudi-hive-sync-bundle .............................. SUCCESS [  1.623 s]
[INFO] hudi-spark-bundle_2.11 ............................. SUCCESS [ 10.930 s]
[INFO] hudi-presto-bundle ................................. SUCCESS [  3.652 s]
[INFO] hudi-timeline-server-bundle ........................ SUCCESS [  4.804 s]
[INFO] hudi-trino-bundle .................................. SUCCESS [  5.991 s]
[INFO] hudi-hadoop-docker ................................. SUCCESS [  2.061 s]
[INFO] hudi-hadoop-base-docker ............................ SUCCESS [ 53.372 s]
[INFO] hudi-hadoop-base-java11-docker ..................... SUCCESS [ 48.545 s]
[INFO] hudi-hadoop-namenode-docker ........................ SUCCESS [  6.098 s]
[INFO] hudi-hadoop-datanode-docker ........................ SUCCESS [  4.825 s]
[INFO] hudi-hadoop-history-docker ......................... SUCCESS [  3.829 s]
[INFO] hudi-hadoop-hive-docker ............................ SUCCESS [ 52.660 s]
[INFO] hudi-hadoop-sparkbase-docker ....................... SUCCESS [01:02 min]
[INFO] hudi-hadoop-sparkmaster-docker ..................... SUCCESS [ 12.661 s]
[INFO] hudi-hadoop-sparkworker-docker ..................... SUCCESS [  4.350 s]
[INFO] hudi-hadoop-sparkadhoc-docker ...................... SUCCESS [ 59.083 s]
[INFO] hudi-hadoop-presto-docker .......................... SUCCESS [01:31 min]
[INFO] hudi-hadoop-trinobase-docker ....................... SUCCESS [02:40 min]
[INFO] hudi-hadoop-trinocoordinator-docker ................ SUCCESS [ 14.003 s]
[INFO] hudi-hadoop-trinoworker-docker ..................... SUCCESS [ 12.100 s]
[INFO] hudi-integ-test .................................... SUCCESS [ 13.581 s]
[INFO] hudi-integ-test-bundle ............................. SUCCESS [ 27.212 s]
[INFO] hudi-examples ...................................... SUCCESS [  8.090 s]
[INFO] hudi-flink_2.11 .................................... SUCCESS [  4.217 s]
[INFO] hudi-kafka-connect ................................. SUCCESS [  2.966 s]
[INFO] hudi-flink-bundle_2.11 ............................. SUCCESS [ 11.155 s]
[INFO] hudi-kafka-connect-bundle .......................... SUCCESS [ 12.369 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  14:35 min
[INFO] Finished at: 2022-01-12T18:41:27-08:00
[INFO] ------------------------------------------------------------------------
```
