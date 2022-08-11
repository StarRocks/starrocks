# Deploy StarRocks

This topic describes how to deploy StarRocks. StarRocks supports deployment via binary installer and Docker image.

If you need to compile StarRocks from [source code](https://github.com/StarRocks/starrocks), see [Build in Docker](../administration/Build_in_docker.md) for detailed instruction.

If you need to scale your StarRocks cluster in or out, see [Scale In and Out](../administration/Scale_up_down.md) for detailed instruction.

## Deploy via binary installer

You can deploy StarRocks via binary installer.

The following example deploys an FE and a BE in the cluster. Please note that at least three BE nodes are required in production environment.

### Prerequisites

Before deploying StarRocks, make sure the following requirements are satisfied.

|Requirement|Description|Note|
|-----------|------------|------|
|Hardware|<ul><li>At least two physical or virtual machines in the cluster.</li><li>CPU of BE machines must support AVX2 instruction sets.</li><li>All nodes must be connected via 10GB ethernet NIC and switch.</li></ul>|<ul><li>8-cores CPU and 16GB memory are recommended for each FE node.</li> <li>16-cores CPU and 64GB memory are recommended for each BE node.</li><li>By running <code>cat /proc/cpuinfo \|grep avx2</code> to check the instruction sets supported by the CPU.</li></ul>|
|Operating system|CentOS (7 or later) is required on all nodes.| |
|Software|Install the following on all nodes: <ul><li><a href="https://www.oracle.com/java/technologies/downloads/">Oracle Java</a> (1.8 or later)</li><li><a href="https://www.mysql.com/downloads/">MySQL client</a> (5.5 or later)</li></ul>|  |
|System environment|<ul><li>Clock within the cluster must be synced.</li> <li>You must have the privilege to set <code>ulimit -n</code>.</li> </ul> | |

> Note
> StarRocks can handle 10M-100M rows per second per CPU thread according to the complexity of your workload. You can estimate how many CPU threads do you need in your cluster. StarRocks leverage column-store and compress when storing data, which can got a 4-10x compression ration. You can estimate your storage volume with the compression ration.

Other system configurations:

* It is recommended to disable swappiness to reduce the impact on performance.

```shell
echo 0 | sudo tee /proc/sys/vm/swappiness
```

* It is recommended to enable Overcommit, and set `cat /proc/sys/vm/overcommit_memory` as `1`.

```shell
echo 1 | sudo tee /proc/sys/vm/overcommit_memory
```

### Deploy FE node

This section describes how to deploy Frontend (FE) nodes. FE is the front layer of StarRocks. It manages system metadata, client connections, query plan, and query schedule.

#### Download and decompress the installer

[Download](https://www.starrocks.com/zh-CN/download) and decompress StarRocks installer.

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> Caution
> Replace the file name in the command as the real file name you downloaded.

#### Configure FE node

Enter **StarRocks-x.x.x/fe**.

```bash
cd StarRocks-x.x.x/fe
```

> Caution
> Replace the path in the command as the real path after the decompression.

Specify the FE configuration file **conf/fe.conf**. The following example only adds the metadata directory and Java directory to ensure the success of deployment. If you need to change more configuration items, see [Configuration](../administration/Configuration.md) for more instruction.

> Caution
> When there is multiple IP addresses in the machine, you need to configure a unique IP address for the FE node under `priority_networks` in **conf/fe.conf**.

Specify the metadata directory.

```Plain Text
meta_dir = ${STARROCKS_HOME}/meta
```

Specify the Java directory.

```Plain Text
JAVA_HOME = /path/to/your/java
```

> Caution
> Replace the path in the command as the real path to Java.

#### Create metadata directory

Create the metadata directory **meta** in FE.

```bash
mkdir -p meta
```

> Caution
> Make sure the path you created is identical to the path you specified in **conf/fe.conf**.

#### Start FE node

Run the following command to start the FE node.

```bash
bin/start_fe.sh --daemon
```

#### Verify if FE starts

You can verify if the FE node is started properly via the following methods:

* Check the FE log **log/fe.log**.

```Plain Text
2020-03-16 20:32:14,686 INFO 1 [FeServer.start():46] thrift server started.  // FE node is started successfully.
2020-03-16 20:32:14,696 INFO 1 [NMysqlServer.start():71] Open mysql server success on 9030  // You can connect the FE node with MySQL client via port `9030`.
2020-03-16 20:32:14,696 INFO 1 [QeService.start():60] QE service start.
2020-03-16 20:32:14,825 INFO 76 [HttpServer$HttpServerThread.run():210] HttpServer started with port 8030
...
```

* Run `jps` in your terminal to check the Java process, and make sure process **StarRocksFe** exists.
* Visit `FE ip:http_port` (default `http_port` is `8030`) in your browser, and enter the StarRocks WebUI. Login with username `root`, and the password id empty。

> Note
> If the FE node is not start properly because the port is occupied, you can change the `http_port` item in **conf/fe.conf**.

#### Add FE node to cluster

You need to add the FE node to the StarRocks cluster.

When the FE node is started, connect the FE node via MySQL client.

```bash
mysql -h 127.0.0.1 -P9030 -uroot
```

> Note
> `root` is the default user of a StarRocks cluster, and its password is empty. You need to connect via the `query_port` (default value `9030`) you have specified in **fe/conf/fe.conf**.

Check the status of FE node.

```sql
SHOW PROC '/frontends'\G
```

Example:

```Plain Text
MySQL [(none)]> SHOW PROC '/frontends'\G

*************************** 1. row ***************************
             Name: 172.26.xxx.xx_9010_1652926508967
               IP: 172.26.xxx.xx
         HostName: iZ8vb61k11tstgnvrmrdfdZ
      EditLogPort: 9010
         HttpPort: 8030
        QueryPort: 9030
          RpcPort: 9020
             Role: FOLLOWER
         IsMaster: true
        ClusterId: 1160043595
             Join: true
            Alive: true
ReplayedJournalId: 1303
    LastHeartbeat: 2022-05-19 11:27:16
         IsHelper: true
           ErrMsg:
        StartTime: 2022-05-19 10:15:21
          Version: 2.2.0-RC02-2ab1482
1 row in set (0.02 sec)
```

* If the field **Role** is **FOLLOWER**, this FE node is eligible to be elected as the Leader node.
* If the field **IsMaster** is **true**, this FE node is the Leader node.

If you failed to connect via MySQL client, you can check **log/fe.warn.log** to identify the problem.

During the **first deployment** of the cluster, you can re-deploy the FE node after deleting and re-creating its metadata directory.

#### Deploy FE node with high availability

StarRocks FE nodes support High Availability deployment. For detailed information, see [FE High Availability Deployment](../administration/Deployment.md)。

#### Stop FE node

Run the following command to stop the FE node.

```bash
./bin/stop_fe.sh --daemon
```

<br/>

### Deploy BE node

This section describes how to deploy Backend (BE) nodes. BE is the executing layer of StarRocks. It stores data and executes queries.

#### Download and decompress the installer

[Download](https://www.starrocks.com/zh-CN/download) and decompress StarRocks installer.

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> Caution
> Replace the file name in the command as the real file name you downloaded.

#### Configure BE node

Enter **StarRocks-x.x.x/be**.

```bash
cd StarRocks-x.x.x/be
```

> Caution
> Replace the path in the command as the real path after the decompression.

Specify the BE configuration file **conf/be.conf**. Because the default configuration can be used directly, no configuration item is changed in the following example. If you need to change more configuration items, see [Configuration](../administration/Configuration.md) for more instruction.

#### Create storage directory

Create the data storage directory **storage** in BE.

```bash
mkdir -p storage
```

> Caution
> Make sure the path you created is identical to the path you specified in **conf/be.conf**.

#### Add BE node to cluster

Run the following command to add the BE node to cluster.

```sql
ALTER SYSTEM ADD BACKEND "host:port";
```

> Caution
> Parameter `host` must match the pre-specified `priority_networks`, and parameter `port` must match the `heartbeat_service_port` specified in **be.conf** (default is`9050`).

If any problem occur while adding the BE node, you can drop it with the following command.

```sql
ALTER SYSTEM decommission BACKEND "host:port";
```

> Caution
> Parameter `host` and `port` must be identical with those when adding the node.

#### Start BE node

Run the following command to start the BE node.

```shell
bin/start_be.sh --daemon
```

#### Verify if BE starts

You can verify if the BE node is started properly via MySQL client.

```sql
SHOW PROC '/backends'\G
```

Example:

```Plain Text
MySQL [(none)]> SHOW PROC '/backends'\G

*************************** 1. row ***************************
            BackendId: 10003
              Cluster: default_cluster
                   IP: 172.26.xxx.xx
             HostName: sandbox-pdtw02
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2022-05-19 11:15:00
        LastHeartbeat: 2022-05-19 11:27:36
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 10
     DataUsedCapacity: .000
        AvailCapacity: 1.865 TB
        TotalCapacity: 1.968 TB
              UsedPct: 5.23 %
       MaxDiskUsedPct: 5.23 %
               ErrMsg:
              Version: 2.2.0-RC02-2ab1482
               Status: {"lastSuccessReportTabletsTime":"2022-05-19 11:27:01"}
    DataTotalCapacity: 1.865 TB
          DataUsedPct: 0.00 %
1 row in set (0.01 sec)
```

When the field `isAlive` is `true`, the BE node is properly started and added to the cluster.

If the BE node is not properly added to the cluster, you can check the **log/be.WARNING** file to identify the problem.

The following logs in the file indicates the parameter `priority_networks` is not properly set.

```Plain Text
W0708 17:16:27.308156 11473 heartbeat_server.cpp:82\] backend ip saved in leader does not equal to backend local ip127.0.0.1 vs. 172.16.xxx.xx
```

You can drop the wrong BE node and re-add it to the cluster with correct IP address.

```sql
ALTER SYSTEM DROP BACKEND "172.16.xxx.xx:9050";
```

During the **first deployment** of the cluster, you can re-deploy the BE node after deleting and re-creating its storage directory.

#### Stop BE node

Run the following command to stop the BE node.

```bash
./bin/stop_be.sh --daemon
```

<br/>

### Deploy Broker

This section describes how to deploy Broker. With broker, StarRocks can read data from source such as HDFS and S3, and pre-process, load, and backup the data with its own computing resources.

#### Download and decompress the installer

[Download](https://www.starrocks.com/zh-CN/download) and decompress StarRocks installer.

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> Caution
> Replace the file name in the command as the real file name you downloaded.

#### Configure Broker

Enter **StarRocks-x.x.x/apache_hdfs_broker**.

```bash
cd StarRocks-x.x.x/apache_hdfs_broker
```

> Caution
> Replace the file name in the command as the real file name you downloaded.

Specify the broker configuration file **conf/apache_hdfs_broker.conf**. Because the default configuration can be used directly, no configuration item is changed in the following example. You can copy your own HDFS cluster configuration file and paste it under **conf** directory.

#### Start Broker

Run the following command to start Broker.

```bash
./apache_hdfs_broker/bin/start_broker.sh --daemon
```

#### Add Broker to cluster

Run the following command to add the broker to cluster.

```sql
ALTER SYSTEM ADD BROKER broker1 "172.16.xxx.xx:8000";
```

> Note
> By default, the port for broker is `8000`。

#### verify if Broker starts

You can verify if the BE node is started properly via MySQL client.

```sql
SHOW PROC "/brokers"\G
```

Example:

```plain text
MySQL [(none)]> SHOW PROC "/brokers"\G

*************************** 1. row ***************************
          Name: broker1
            IP: 172.26.xxx.xx
          Port: 8000
         Alive: true
 LastStartTime: 2022-05-19 11:21:36
LastUpdateTime: 2022-05-19 11:28:31
        ErrMsg:
1 row in set (0.00 sec)
```

When the field `isAlive` is `true`, the broker is properly started and added to the cluster.

#### Stop Broker

Run the following command to stop Broker.

```bash
./bin/stop_broker.sh --daemon
```

<br/>

## Deploy with Docker

### Prerequisites

|Requirement|Description|
|---------|--------|
|Hardware|<ul><li>CPU supports AVX2 instruction sets.</li><li>8-cores CPU and 16GB memory are recommended.</li></ul>|
|Operating system|CentOS (7 or later)|
|Software|<ul><li>Docker</li><li>MySQL client (5.5 or later)</li></ul>|

### Create Dockerfile

Create a Dockerfile with the following template.

```shell
FROM centos:centos7

# Prepare StarRocks Installer.
RUN yum -y install wget
RUN mkdir -p /data/deploy/ 
RUN wget -SO /data/deploy/StarRocks-x.x.x.tar.gz <url_to_download_specific_ver_of_starrocks>
RUN cd /data/deploy/ && tar zxf StarRocks-x.x.x.tar.gz

# Install Java JDK.
RUN yum -y install java-1.8.0-openjdk-devel.x86_64
RUN rpm -ql java-1.8.0-openjdk-devel.x86_64 | grep bin$
RUN /usr/lib/jvm/java-1.8.0/bin/java -version

# Create directory for FE meta and BE storage in StarRocks.
RUN mkdir -p /data/deploy/StarRocks-x.x.x/fe/meta
RUN jps
RUN mkdir -p /data/deploy/StarRocks-x.x.x/be/storage

# Install relevant tools.
RUN yum -y install mysql net-tools telnet

# Run Setup script.
COPY run_script.sh /data/deploy/run_script.sh
RUN chmod +x /data/deploy/run_script.sh
CMD /data/deploy/run_script.sh
```

> Caution
> Replace `<url_to_download_specific_ver_of_starrocks>` with the real [download path](https://www.starrocks.com/zh-CN/download), and replace `StarRocks-x.x.x` with the real StarRocks version.

### Create script

build the script file `run_script.sh` to configure and start StarRocks.

```shell

#!/bin/bash

# Set JAVA_HOME.
export JAVA_HOME=/usr/lib/jvm/java-1.8.0

# Start FE.
cd /data/deploy/StarRocks-x.x.x/fe/bin/
./start_fe.sh --daemon

# Start BE.
cd /data/deploy/StarRocks-x.x.x/be/bin/
./start_be.sh --daemon

# Sleep until the cluster starts.
sleep 30;

# Set BE server IP.
IP=$(ifconfig eth0 | grep 'inet' | cut -d: -f2 | awk '{print $2}')
mysql -uroot -h${IP} -P 9030 -e "alter system add backend '${IP}:9050';"

# Loop to detect the process.
while sleep 60; do
  ps aux | grep starrocks | grep -q -v grep
  PROCESS_STATUS=$?

  if [ PROCESS_STATUS -ne 0 ]; then
    echo "one of the starrocks process already exit."
    exit 1;
  fi
done
```

> Caution
> Replace `StarRocks-x.x.x` with the real StarRocks version.

### Build Docker image

Run the following command to build Docker image.

```shell
docker build --no-cache --progress=plain -t starrocks:1.0 .
```

### Start Docker container

Run the following command to start the Docker container.

```shell
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 --privileged=true -itd --name starrocks-test starrocks:1.0
```

### Connect to StarRocks

Run the following command to connect to StarRocks after the container is started successfully.

```shell
mysql -uroot -h127.0.0.1 -P 9030
```

### Verify if cluster

Run the following SQL to verify if the cluster is started properly.

```sql
CREATE DATABASE TEST;

USE TEST;

CREATE TABLE `sr_on_mac` (
 `c0` int(11) NULL COMMENT "",
 `c1` date NULL COMMENT "",
 `c2` datetime NULL COMMENT "",
 `c3` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`c0`)
PARTITION BY RANGE (c1) (
  START ("2022-02-01") END ("2022-02-10") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(`c0`) BUCKETS 1 
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);


insert into sr_on_mac values (1, '2022-02-01', '2022-02-01 10:47:57', '111');
insert into sr_on_mac values (2, '2022-02-02', '2022-02-02 10:47:57', '222');
insert into sr_on_mac values (3, '2022-02-03', '2022-02-03 10:47:57', '333');


select * from sr_on_mac where c1 >= '2022-02-02';
```

If no error is returned, you have successfully deployed StarRocks in Docker.

## What to do next

Having Deployed StarRocks, you can:

* [Create a table](../sql-reference/sql-statements/data-definition/CREATE%20TABLE.md)
* [Upgrade StarRocks](../administration/Cluster_administration.md#Upgrade-a-cluster)
