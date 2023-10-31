# Deployment

Manual deployment allows users to quickly leverage StarRocks to do operation and maintenance tasks.

## Get the Binary Package

Please download the latest stable version of the StarRocks binary package.

For example, below is what you get after decompressing the package “starrocks-1.0.0.tar.gz”:

```Plain Text
StarRocks-XX-1.0.0

├── be  # BE Catalog
│   ├── bin
│   │   ├── start_be.sh # BE start command
│   │   └── stop_be.sh  # BE shutdown command
│   ├── conf
│   │   └── be.conf     # BE configuration file
│   ├── lib
│   │   ├── starrocks_be  # BE executable file
│   │   └── meta_tool
│   └── www
├── fe  # FE Catalog
│   ├── bin
│   │   ├── start_fe.sh # FE start command
│   │   └── stop_fe.sh  # FE shutdown command
│   ├── conf
│   │   └── fe.conf     # FE configuration file
│   ├── lib
│   │   ├── starrocks-fe.jar  # FE jar package
│   │   └── *.jar           # FE dependent jar packages
│   └── webroot
└── udf
```

## Environment Setup

You need three physical machines that support:

* Linux (Centos 7+)
* Java 1.8+

The CPU needs to support AVX2 instruction sets. When running `cat /proc/cpuinfo |grep avx2`, you should get a result output indicating the support. If not, we recommend that you replace the machine. StarRocks uses vectorization technology that requires instruction set support to be effective.

You can distribute and decompress the binary package to the deployment path of your target host, and create a user account to manage it.

## FE Deployment

### Basic Configuration for FE

The FE configuration file is `StarRocks-XX-1.0.0/fe/conf/fe.conf`. The default configuration is sufficient to start the cluster.

### FE Single Instance Deployment

cd StarRocks-XX-1.0.0/fe

Step 1: Customize the configuration file `conf/fe.conf`.

```bash
JAVA_OPTS = "-Xmx4096m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:$STARROCKS_HOME/log/fe.gc.log"
```

You can adjust `-Xmx4096m` basd on the FE memory size. Recommend to set the memory size to 16G or above to avoid GC. All StarRocks metadata is stored in the memory.

Step 2: Create a metadata directory and add the meta_dir config to `conf/fe.conf`

```bash
mkdir -p meta
```

Add the meta_dir config to `conf/fe.conf` :

```bash
meta_dir = the absolute path of meta dir you created before
```

Step 3: Start the FE.

```bash
bin/start_fe.sh --daemon
```

Step 4: Look up the log file (`log/fe.log`) to confirm that the FE has been started successfully.

* check `log/fe.log` to confirm.

```Plain Text
2020-03-16 20:32:14,686 INFO 1 [FeServer.start():46] thrift server started.

2020-03-16 20:32:14,696 INFO 1 [NMysqlServer.start():71] Open mysql server success on 9030

2020-03-16 20:32:14,696 INFO 1 [QeService.start():60] QE service start.

2020-03-16 20:32:14,825 INFO 76 [HttpServer$HttpServerThread.run():210] HttpServer started with port 8030

...
```

* If the FE fails to start, check if the port number is occupied. If so, modify the port number (`http_port`) in the configuration file.
* You can also use the jps command to view the java process and see if StarRocks FE exists.

### Use MySQL Client to Access FE

Step 1: Install the MySQL client if you haven’t done so.

Ubuntu：sudo apt-get install mysql-client

Centos：sudo yum install mysql-client

Step 2: Connect using the MySQL client.

```sql
mysql -h 127.0.0.1 -P9030 -uroot
```

> Note: The default root user password is empty. The port is query_port in fe/conf/fe.conf, default to 9030.

Step 3: Check the FE status.

```Plain Text
mysql> SHOW PROC '/frontends'\G

***1\. row***

Name: 172.16.139.24_9010_1594200991015

IP: 172.16.139.24

HostName: starrocks-sandbox01

EditLogPort: 9010

HttpPort: 8030

QueryPort: 9030

RpcPort: 9020

Role: FOLLOWER

IsMaster: true

ClusterId: 861797858

Join: true

Alive: true

ReplayedJournalId: 64

LastHeartbeat: 2020-03-23 20:15:07

IsHelper: true

ErrMsg:

1 row in set (0.03 sec)
```

`Role` is `FOLLOWER`, indicating that the FE is eligible to be elected as the leader. `IsMaster` is true, indicating that the FE is currently the leader node.

If the MySQL client connection is not successful, use the log file (`log/fe.warn.log`) for troubleshooting. Since it is the initial setup, feel free to start over if you encounter any unexpected problems.

### FE High-Availability Cluster Deployment

FE's high-availability clusters use a primary-secondary replication architecture to avoid single points of failure. FE uses the raft-like BDBJE protocol to complete leader selection, log replication, and failover. In FE clusters, instances are divided into two roles -- follower and observer. The follower is a voting member of the replication protocol, participating in the selection of the leader and submitting logs. Its general number is odd (2n+1). It takes majority (n+1) for confirmation and tolerates minority (n) failure. The observer is a non-voting member and is used to subscribe to replication logs asynchronously. The status of the observer lags behind the follower, similar to the leaner role in other replication protocols.

The FE cluster automatically selects the leader node from the followers. The leader node executes all state changes. A change can be initiated from a non-leader node, and then forwarded to the leader node for execution. The non-leader node records the LSN of the most recent change in the replication log. The read operation can be performed directly on the non-leader node, but it needs to wait until the state of the non-leader node gets synchronized with the LSN of the last operation. Observer nodes can increase the read load capacity of the FE cluster. Users with little urgency can read the observer nodes.

The clock difference between the FE nodes should not exceed 5s. Use the NTP protocol to calibrate the time.

A single FE node can only be deployed on one machine. The HTTP ports of all FE nodes need to be the same.

For cluster deployment, follow the following steps to add FE instances one by one.

Step 1: Distribute binary and configuration files (same as a single instance).

Step 2: Connect the MySQL client to the existing FE, and add the information of the new instance, including role, IP, port:

```sql
mysql> ALTER SYSTEM ADD FOLLOWER "host:port";
```

Or

```sql
mysql> ALTER SYSTEM ADD OBSERVER "host:port";
```

The host is the IP of the machine. If the machine has multiple IPs, select the IP in priority_networks. For example, `priority_networks=192.168.1.0/24` can be set to use the subnet `192.168.1.x` for communication. The port is `edit_log_port`, default to `9010`.

> Note: Due to security considerations, StarRocks' FE and BE can only listen to one IP for communication. If a machine has multiple network cards, StarRocks may not be able to automatically find the correct IP. For example, run the `ifconfig` command to get that `eth0 IP` is `192.168.1.1`, `docker0 : 172.17.0.1`. We can set the word network `192.168.1.0/24` to designate eth0 as the communication IP. Here we use [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) notation to specify the subnet range where the IP is located, so that it can be used on all BE and FE. `priority_networks` is written in both `fe.conf` and `be.conf`. This attribute indicates which IP to use when the FE or BE is started. The example is as follows: `priority_networks=10.1.3.0/24`.

If an error occurs, delete the FE by using the following command:

```sql
alter system drop follower "fe_host:edit_log_port";
alter system drop observer "fe_host:edit_log_port";
```

Step 3: FE nodes need to be interconnected in pairs to complete leader selection, voting, log submission, and replication. When the FE node is first initiated, a node in the existing cluster needs to be designated as a helper. The helper node gets the configuration information of all the FE nodes in the cluster to establish a connection. Therefore, during initiation, specify the `--helper` parameter:

```shell
./bin/start_fe.sh --helper host:port --daemon
```

The host is the IP of the helper node. If there are multiple IPs, select the IP in `priority_networks`. The port is `edit_log_port`, default to `9010`.

There is no need to specify the `--helper` parameter for future starts. The FE stores other FEs’ configuration information in the local directory. To start directly:

```shell
./bin/start_fe.sh --daemon
```

Step 4: Check the cluster status and confirm that the deployment is successful:

```Plain Text
mysql> SHOW PROC '/frontends'\G

***1\. row***

Name: 172.26.108.172_9010_1584965098874

IP: 172.26.108.172

HostName: starrocks-sandbox01

......

Role: FOLLOWER

IsMaster: true

......

Alive: true

......

***2\. row***

Name: 172.26.108.174_9010_1584965098874

IP: 172.26.108.174

HostName: starrocks-sandbox02

......

Role: FOLLOWER

IsMaster: false

......

Alive: true

......

***3\. row***

Name: 172.26.108.175_9010_1584965098874

IP: 172.26.108.175

HostName: starrocks-sandbox03

......

Role: FOLLOWER

IsMaster: false

......

Alive: true

......

3 rows in set (0.05 sec)
```

Alive is true, indicating the node is successfully added. In the above example, 172.26.108.172_9010_1584965098874 is the main FE node.

## BE Deployment

### Basic Configuration for BE

The BE configuration file is `StarRocks-XX-1.0.0/be/conf/be.conf`. The default configuration is sufficient to start the cluster.

### BE Instance Deployment

Users can use the following steps to add BE to the StarRocks cluster. In most cases, at least three BE instances are deployed. The steps for adding each instance are the same.

```shell
cd StarRocks-XX-1.0.0/be
```

Step 1: Create a data storage directory.

```shell
mkdir -p storage
```

Step 2: Add a BE node via the MySQL client.

```sql
mysql> ALTER SYSTEM ADD BACKEND "host:port";
```

The IP address should match the `priority_networks` setting; `portheartbeat_service_port` defaults to `9050`.

If an error occurs, delete the BE node using the following commands:

* `alter system decommission backend "be_host:be_heartbeat_service_port";`
* `alter system dropp backend "be_host:be_heartbeat_service_port";`

Step 3: Start the BE.

```shell
bin/start_be.sh --daemon
```

Step 4: Check the BE status to confirm it is ready.

```Plain Text
mysql> SHOW PROC '/backends'\G

***1\. row***

BackendId: 10002

Cluster: default\_cluster

IP: 172.16.139.24

HostName: starrocks-sandbox01

HeartbeatPort: 9050

BePort: 9060

HttpPort: 8040

BrpcPort: 8060

LastStartTime: 2020-03-23 20:19:07

LastHeartbeat: 2020-03-23 20:34:49

Alive: true

SystemDecommissioned: false

ClusterDecommissioned: false

TabletNum: 0

DataUsedCapacity: .000

AvailCapacity: 327.292 GB

TotalCapacity: 450.905 GB

UsedPct: 27.41 %

MaxDiskUsedPct: 27.41 %

ErrMsg:

Version:

1 row in set (0.01 sec)
```

`isAlive` is true, indicating that the BE is successfully connected to the cluster. If not, check the log file (`be.WARNING`) to determine the root cause.

The following message indicates that there is a problem with the `priority_networks` settings.

```Plain Text
W0708 17:16:27.308156 11473 heartbeat\_server.cpp:82\] backend ip saved in master does not equal to backend local ip127.0.0.1 vs. 172.16.179.26
```

At this time, you need to use the following command to drop the originally added BE, and then add it back with the correct IP.

```sql
mysql> ALTER SYSTEM DROP BACKEND "172.16.139.24:9050";
```

Since it is the initial setup, feel free to start over if you encounter any unexpected problems.

## Broker Deployment

The configuration file is `apache_hdfs_broker/conf/apache_hdfs_broker.conf`.

> Note: If the machine has multiple IPs, priority_networks needs to be configured in the same way as the FE.

If there is a special HDFS configuration, copy `hdfs-site.xml` to the conf directory.

Step 1:To start the broker:

```shell
./apache_hdfs_broker/bin/start_broker.sh --daemon
```

Step 2: Add the broker node to the cluster:

```sql
MySQL> ALTER SYSTEM ADD BROKER broker1 "172.16.139.24:8000";
```

Step 3: Check the broker status:

```plain text
MySQL> SHOW PROC '/brokers'\G
*************************** 1. row ***************************
          Name: broker1
            IP: 172.16.139.24
          Port: 8000
         Alive: true
 LastStartTime: 2020-04-01 19:08:35
LastUpdateTime: 2020-04-01 19:08:45
        ErrMsg: 
1 row in set (0.00 sec)
```

`Alive` is true, indicating the state is normal.

## Parameter Settings

* **Swappiness**

Shut down the swap space to avoid impacting performance when transferring data from the real memory to the virtual memory.

```shell
echo 0 | sudo tee /proc/sys/vm/swappiness
```

* **Compaction**

When using aggregate models or updating models, data gets imported at a high speed. You can modify the following parameters to speed up compaction.

```shell
cumulative_compaction_num_threads_per_disk = 4
base_compaction_num_threads_per_disk = 2
cumulative_compaction_check_interval_seconds = 2
```

* **Parallelism**

You can modify the parallelism of StarRocks (similar to clickhouse set max_threads= 8) when executing commands via the client. The parallelism can be set to half the number of the current machine's CPU cores.

```sql
set global parallel_fragment_exec_instance_num =  8;
```

## Use MySQL Client to Access StarRocks

### Root User Login

Use the MySQL client to connect to `query_port (9030)` of a certain FE instance. StarRocks has a built-in root user, its password is empty by default.

```shell
mysql -h fe_host -P9030 -u root
```

Clean up the environment:

```sql
mysql > drop database if exists example_db;

mysql > drop user test;
```

### Create a New User

Create a user using the following command:

```sql
mysql > create user 'test' identified by '123456';
```

### Create a Database

Only root account has the right to create a database. Log in as root user to create a database:

```sql
mysql > create database example_db;
```

After the database is created, you can view the database information using the following command:

```Plain Text
mysql > show databases;

+--------------------+
| Database           |
+--------------------+
| example_db         |
| information_schema |
+--------------------+
2 rows in set (0.00 sec)
```

The `information_schema` exists to be compatible with the MySQL protocol. In reality, the information may not be accurate. Therefore, it is recommended to directly query the corresponding database to get information.

### Authorization an Account

Once the database is created, you can authorize test accounts to have root account permissions. Once done, log in to the test account and manage the database:

```sql
mysql > grant all on example_db to test;
```

Log out of the root account and log in as a test user:

```sql
mysql > exit

mysql -h 127.0.0.1 -P9030 -utest -p123456
```

### Create Tables

StarRocks supports two ways of creating tables -- bucketing and composite partition.

In a composite partition:

* The first level is partition. Users can specify a certain key as the partition key (currently only integer and time are supported), and set the value range of each partition.
* The second level is called tablet, which is bucket. Users can specify several keys (or none, that is, all KEY columns) and the number of buckets for distributing data by HASH.

Composite partitions are recommended for the following scenarios:

* There are time keys or similar keys with ordered values. Such keys can be used as partition keys. Partition granularity can be evaluated based on import frequency, partition data volume, and etc.
* If there is a requirement to delete historical data (for example, only keep the data of the last N days), use composite partitions to achieve that goal. You can also delete data by sending a `DELETE` statement in the specified partition.
* To solve the problem of data skew. Each partition can individually specify the number of buckets. For example, when partitioning by day and the amount of data varies greatly each day, you can specify the number of buckets in the partition to reasonably divide the data into different partitions. It is recommended to choose easily differentiable keys as the bucket keys.

Users can use buckets, in which case the data is only distributed by HASH.

Next, let’s see how to create a table with buckets.

1. Switch to the database: `mysql> use example_db`.
2. Create a logical table named table1. Use the full hash to divide buckets; list the buckets as siteid and the number of buckets as 10. The schema of the table is as follows:

* siteid: The type is INT (4 bytes), the default value is 10
* cidy_code: The type is SMALLINT (2 bytes)
* username: The type is VARCHAR, the maximum length is 32, and the default value is an empty string
* pv: The type is BIGINT (8 bytes), and the default value is 0. This is an indicator column. StarRocks internally aggregates indicator columns. The aggregation method is sum (SUM).

```sql
mysql >
CREATE TABLE table1
(
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, citycode, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

Then, let’s see how to create a composite partition table. The schema of the table is as follows:

* event_day: The type is DATE, no default value
* siteid: The type is INT (4 bytes), the default value is 10
* cidy_code: The type is SMALLINT (2 bytes)
* username: The type is VARCHAR, the maximum length is 32, and the default value is an empty string
* pv: The type is BIGINT (8 bytes), the default value is 0. This is an indicator column. StarRocks internally aggregates indicator columns. The aggregation method of this column is sum (SUM).

We use the `event_day` as the partition key to create three partitions (p1, p2, p3).

* p1: The range is [minimum value, 2017-06-30)
* p2: The range is [2017-06-30, 2017-07-31)
* p3: The range is [2017-07-31, 2017-08-31)

Each partition is hashed into buckets using `siteid`; the number of buckets is 10.

```sql
CREATE TABLE table2
(
event_day DATE,
siteid INT DEFAULT '10',
citycode SMALLINT,
username VARCHAR(32) DEFAULT '',
pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, siteid, citycode, username)
PARTITION BY RANGE(event_day)
(
PARTITION p1 VALUES LESS THAN ('2017-06-30'),
PARTITION p2 VALUES LESS THAN ('2017-07-31'),
PARTITION p3 VALUES LESS THAN ('2017-08-31')
)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

Once the table is created, you can view the table information using the following command:

```Plain Text
mysql> show tables;

+-------------------------+
| Tables_in_example_db    |
+-------------------------+
| table1                  |
| table2                  |
+-------------------------+
2 rows in set (0.01 sec)


mysql> desc table1;

+----------+-------------+------+-------+---------+-------+
| Field    | Type        | Null | Key   | Default | Extra |
+----------+-------------+------+-------+---------+-------+
| siteid   | int(11)     | Yes  | true  | 10      |       |
| citycode | smallint(6) | Yes  | true  | N/A     |       |
| username | varchar(32) | Yes  | true  |         |       |
| pv       | bigint(20)  | Yes  | false | 0       | SUM   |
+----------+-------------+------+-------+---------+-------+
4 rows in set (0.00 sec)



mysql> desc table2;

+-----------+-------------+------+-------+---------+-------+
| Field     | Type        | Null | Key   | Default | Extra |
+-----------+-------------+------+-------+---------+-------+
| event_day | date        | Yes  | true  | N/A     |       |
| siteid    | int(11)     | Yes  | true  | 10      |       |
| citycode  | smallint(6) | Yes  | true  | N/A     |       |
| username  | varchar(32) | Yes  | true  |         |       |
| pv        | bigint(20)  | Yes  | false | 0       | SUM   |
+-----------+-------------+------+-------+---------+-------+
5 rows in set (0.00 sec)
```

## Build in docker

Refer to[Build in docker](../development/development.md)
