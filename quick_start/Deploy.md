# 手动部署 StarRocks

本文介绍如何手动部署 StarRocks。StarRocks 支持以 Docker 镜像和二进制安装包形式手动部署于集群中。

如需从 [源码](https://github.com/StarRocks/starrocks) 编译安装 StarRocks，参考 [Docker 编译](../administration/Build_in_docker.md)。

以下示例仅部署一台 FE 节点以及一台 BE 节点。在正常应用环境中，一个 StarRocks 集群需要部署三个 BE 节点。

如需在生产环境中扩容，参考 [扩容缩容](../administration/Scale_up_down.md)。

## 通过二进制安装包部署

您可以通过二进制安装包跨节点部署 StarRocks。

### 前提条件

在部署 StarRocks 之前，请确保如下环境要求已满足。
|分类|描述|说明|
|-----------|------------|------|
|硬件要求|<ul><li>集群至少拥有两台物理或虚拟节点。</li><li>BE 节点 CPU 需支持 AVX2 指令集。</li><li>各节点间需要通过万兆网卡及万兆交换机连接。</li></ul>|<ul><li>FE 节点建议配置 8 核 或以上 CPU，16GB 或以上内存。</li> <li>BE 节点建议配置 16 核 或以上 CPU，64GB 或以上内存。</li><li>通过运行 <code>cat /proc/cpuinfo \|grep avx2</code> 命令查看节点 CPU 支持的指令集，若有结果返回则表明 CPU 支持 AVX2 指令集。</li></ul>|
|操作系统|所有节点操作系统需为 CentOS（7 或以上）。| |
|软件要求|所有节点需安装：<ul><li><a   href="https://www.oracle.com/java/technologies/downloads/">Oracle Java</a>（1.8 或以上）</li><li><a href="https://www.mysql.com/downloads/">MySQL 客户端</a>（5.5 或以上） </li></ul>|  |
|系统环境|<ul><li>集群时钟需保持同步。 </li> <li> 用户需要有设置 <code>ulimit -n</code> 权限。 </li> </ul> | |

> 说明
> 依据不同的工作负载复杂性，StarRocks 每个 CPU 线程每秒可以处理 10M 至 100M 行数据。您可以据此估计集群中需要多少 CPU 线程能够满足您的要求。而 StarRocks 在存储数据时利用列存储和压缩，可以达到 4-10 倍的压缩比，您可以使用该数据来估计集群所需的存储量。

其他系统参数配置：

* 建议关闭交换区，消除交换内存到虚拟内存时对性能的扰动。

```shell
echo 0 | sudo tee /proc/sys/vm/swappiness
```

* 建议使用 Overcommit，将 `cat /proc/sys/vm/overcommit_memory` 设置为 `1`。

```shell
echo 1 | sudo tee /proc/sys/vm/overcommit_memory
```

### 部署 FE 节点

本小节介绍如何配置部署 Frontend (FE) 节点。FE 是StarRocks的前端节点，负责管理元数据，管理客户端连接，进行查询规划，查询调度等工作。

#### 下载并解压安装包

[下载](https://www.starrocks.com/zh-CN/download) StarRocks 并解压二进制安装包。

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> 注意：将以上文件名修改为下载的二进制安装包名。

#### 配置 FE 节点

进入 **StarRocks-x.x.x/fe** 路径。

```bash
cd StarRocks-x.x.x/fe
```

> 注意：将以上路径名修改为解压后的路径名。

修改 FE 配置文件 **conf/fe.conf**。以下示例仅添加元数据目录和 Java 目录，以保证部署成功。如需在生产环境中对集群进行详细优化配置，参考 [FE 参数配置](../administration/Configuration.md#FE-配置项)。

> 注意：当一台机器拥有多个 IP 地址时，需要在 FE 配置文件 **conf/fe.conf** 中设置 `priority_networks`，为该节点设定唯一 IP。

添加元数据目录。

```Plain Text
meta_dir = ${STARROCKS_HOME}/meta
```

添加 Java 目录。

```Plain Text
JAVA_HOME = /path/to/your/java
```

> 注意：将以上路径修改为 Java 所在的本地路径。

#### 创建元数据路径

创建 FE 节点中的元数据路径 **meta**。

```bash
mkdir -p meta
```

> 注意：该路径需要与 **conf/fe.conf** 文件中配置路径保持一致。

#### 启动 FE 节点

运行以下命令启动 FE 节点。

```bash
bin/start_fe.sh --daemon
```

#### 确认 FE 启动成功

通过以下方式验证 FE 节点是否启动成功：

* 通过查看日志 **log/fe.log** 确认 FE 是否启动成功。

```Plain Text
2020-03-16 20:32:14,686 INFO 1 [FeServer.start():46] thrift server started.  // FE 节点启动成功。
2020-03-16 20:32:14,696 INFO 1 [NMysqlServer.start():71] Open mysql server success on 9030  // 可以使用 MySQL 客户端通过 `9030` 端口连接 FE。
2020-03-16 20:32:14,696 INFO 1 [QeService.start():60] QE service start.
2020-03-16 20:32:14,825 INFO 76 [HttpServer$HttpServerThread.run():210] HttpServer started with port 8030
...
```

* 通过运行 `jps` 命令查看 Java 进程，确认 **StarRocksFe** 进程是否存在。
* 通过在浏览器访问 `FE ip:http_port`（默认 `http_port` 为 `8030`），进入 StarRocks 的 WebUI，用户名为 `root`，密码为空。

> 说明：如果由于端口被占用导致 FE 启动失败，可修改配置文件 **conf/fe.conf** 中的端口号 `http_port`。

#### 添加 FE 节点

您可通过 MySQL 客户端连接 StarRocks 以添加 FE 节点。

在 FE 进程启动后，使用 MySQL 客户端连接 FE 实例。

```bash
mysql -h 127.0.0.1 -P9030 -uroot
```

> 说明：`root` 为 StarRocks 默认内置 user，密码为空，端口为 **fe/conf/fe.conf** 中的 `query\_port` 配置项，默认值为 `9030`。

查看 FE 状态。

```sql
SHOW PROC '/frontends'\G
```

示例：

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

* 当 **Role** 为 **FOLLOWER** 时，当前节点是一个能参与选主的 FE 节点。
* 当 **IsMaster** 为 **true** 时，当前 FE 节点为主节点。

如果 MySQL 客户端连接失败，可以通过查看 **log/fe.warn.log** 日志文件发现问题。

如果在**初次部署时**遇到任何意外问题，可以在删除并重新创建 FE 的元数据路径后，重新开始部署。

#### 部署 FE 节点的高可用集群

StarRocks 的 FE 节点支持 HA 模型部署，以保证集群的高可用。详细设置方式参考 [FE 高可用集群部署](/administration/Deployment.md#fe高可用部署)。

#### 停止 FE 节点

运行以下命令停止 FE 节点。

```bash
./bin/stop_fe.sh --daemon
```

<br/>

### 部署 BE 节点

本小节介绍如何配置部署 Backend (BE) 节点。BE 是StarRocks的后端节点，负责数据存储以及SQL执行等工作。以下例子仅部署一个 BE 节点。您可以通过重复以下步骤添加多个 BE 节点。

#### 下载并解压安装包

[下载](https://www.starrocks.com/zh-CN/download) StarRocks 并解压二进制安装包。

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> 注意：将以上文件名修改为下载的二进制安装包名。

#### 配置 BE 节点

进入 **StarRocks-x.x.x/be** 路径。

```bash
cd StarRocks-x.x.x/be
```

> 注意：将以上路径名修改为解压后的路径名。

修改 BE 节点配置文件 **conf/be.conf**。因默认配置即可启动集群，以下示例并未修改 BE 节点配置。如需在生产环境中对集群进行详细优化配置，参考 [BE 参数配置](../administration/Configuration.md#BE-参数配置)。

#### 创建数据路径

创建 BE 节点中的数据路径 **storage**。

```bash
mkdir -p storage
```

> 注意：该路径需要与 **be.conf** 文件中配置路径保持一致。

#### 添加 BE 节点

通过 MySQL 客户端将 BE 节点添加至 StarRocks 集群。

```sql
mysql> ALTER SYSTEM ADD BACKEND "host:port";
```

> 注意：`host` 需要与 `priority_networks` 相匹配，`port` 需要与 **be.conf** 文件中的设置的 `heartbeat_service_port` 相同，默认为 `9050`。

如添加过程出现错误，需要通过以下命令将该 BE 节点从集群移除。

```sql
mysql> ALTER SYSTEM decommission BACKEND "host:port";
```

> `host` 和 `port` 与添加的 BE 节点一致。

#### 启动 BE 节点

运行以下命令启动 BE 节点。

```shell
bin/start_be.sh --daemon
```

#### 确认 BE 启动成功

通过 MySQL 客户端确认 BE 节点是否启动成功。

```sql
SHOW PROC '/backends'\G
```

示例：

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

当 `isAlive` 为 `true` 时，当前 BE 节点正常接入集群。

如果 BE 节点没有正常接入集群，可以通过查看 **log/be.WARNING** 日志文件排查问题。

如果日志中出现类似以下的信息，说明 `priority_networks` 的配置存在问题。

```Plain Text
W0708 17:16:27.308156 11473 heartbeat_server.cpp:82\] backend ip saved in master does not equal to backend local ip127.0.0.1 vs. 172.16.xxx.xx
```

如遇到以上问题，可以通过 DROP 错误的 BE 节点，然后重新以正确的 IP 添加 BE 节点的方式来解决。

```sql
ALTER SYSTEM DROP BACKEND "172.16.xxx.xx:9050";
```

如果在**初次部署时**遇到任何意外问题，可以在删除并重新创建 BE 的数据路径后，重新开始部署。

#### 停止 BE 节点

运行以下命令停止 BE 节点。

```bash
./bin/stop_be.sh --daemon
```

<br/>

### 部署 Broker 节点

本小节介绍如何配置部署 Broker。通过部署的 Broker，StarRocks 可读取对应数据源（如HDFS、S3）上的数据，利用自身的计算资源对数据进行预处理和导入。除此之外，Broker 也被应用于数据导出，备份恢复等功能。

#### 下载并解压安装包

[下载](https://www.starrocks.com/zh-CN/download) StarRocks 并解压二进制安装包。

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> 注意：将以上文件名修改为下载的二进制安装包名。

#### 配置 Broker 节点

进入 **StarRocks-x.x.x/apache_hdfs_broker** 路径。

```bash
cd StarRocks-x.x.x/apache_hdfs_broker
```

> 注意：将以上路径名修改为解压后的路径名。

修改 Broker 节点配置文件 **conf/apache_hdfs_broker.conf**。因默认配置即可启动集群，以下示例并未修改 Broker 点配置。您可以直接复制自己的 HDFS 集群配置文件并粘贴至 **conf** 路径下。

#### 启动 Broker 节点

通过以下命令启动 Broker。

```bash
./apache_hdfs_broker/bin/start_broker.sh --daemon
```

#### 添加 Broker 节点

您可通过 MySQL 客户端连接 StarRocks 以添加或删除 Broker 节点。

```sql
ALTER SYSTEM ADD BROKER broker1 "172.16.xxx.xx:8000";
```

> 说明：默认配置中，Broker 节点的端口为 8000。

#### 确认 Broker 启动成功

通过 MySQL 客户端确认 Broker 节点是否启动成功。

```sql
SHOW PROC "/brokers"\G
```

示例：

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

当 `Alive` 为 `true` 时，当前 Broker 节点正常接入集群。

#### 停止 Broker 节点

运行以下命令停止 Broker 节点。

```bash
./bin/stop_broker.sh --daemon
```

<br/>

## 使用 Docker 部署

### 前提条件

|分类|描述|
|---------|--------|
|硬件要求|<ul><li>CPU 需支持 AVX2 指令集</li><li>建议配置 8 核 或以上 CPU，16GB 或以上内存。</li></ul>|
|操作系统|CentOS（7 或以上）|
|软件要求|<ul><li>Docker</li><li>MySQL 客户端（5.5 或以上）</li></ul>|

### 创建 Dockerfile

创建以下 Dockerfile。

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
RUN /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.322.b09-1.el7_9.x86_64/bin/java -version

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

> 注意：将以上 `<url_to_download_specific_ver_of_starrocks>` 替换为实际[下载地址](https://www.starrocks.com/zh-CN/download)，并将 `StarRocks-x.x.x` 替换为实际安装版本。

### 创建脚本文件

构建脚本文件 `run_script.sh` 以配置并启动 StarRocks。

```shell

#!/bin/bash

# Set JAVA_HOME.
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.322.b06-1.el7_9.x86_64

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

> 注意：将以上 `StarRocks-x.x.x` 替换为实际安装版本。

### 搭建 Docker 镜像

运行以下命令搭建 Docker 镜像。

```shell
docker build --no-cache --progress=plain -t starrocks:1.0 .
```

### 启动 Docker 容器

运行以下命令启动 Docker 容器。

```shell
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 --privileged=true -itd --name starrocks-test starrocks:1.0
```

### 连接 StarRocks

当 Docker 容器成功启动后，运行以下命令连接 StarRocks。

```shell
mysql -uroot -h127.0.0.1 -P 9030
```

### 确认部署成功

您可以运行以下 SQL 确认 StarRocks 是否部署成功。

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

如果无错误返回，则表明您已成功在 Docker 环境中部署 StarRocks。

## 下一步

成功部署 StarRocks 集群后，您可以：

* [创建表](Create_table.md)
* [导入和查询数据](Import_and_query.md)
