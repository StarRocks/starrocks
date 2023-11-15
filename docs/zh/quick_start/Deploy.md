# 手动部署

## 环境准备

集群节点需要以下环境支持：

* Linux (Centos 7+)
* 推荐 Oracle Java 1.8+
* CPU 需要支持 AVX2 指令集
* ulimit -n 配置 65535，启动脚本会自动设置，需要启动的用户有设置 ulimit -n 权限
* 集群时钟需同步
* 网络需要万兆网卡和万兆交换机

通过 `cat /proc/cpuinfo |grep avx2` 命令查看节点配置，有结果则 cpu 支持 AVX2 指令集。

测试集群建议节点配置：BE 推荐 16 核 64GB 以上，FE 推荐 8 核 16GB 以上。建议 FE，BE 独立部署。

系统参数配置建议：

关闭交换区，消除交换内存到虚拟内存时对性能的扰动。

```shell
echo 0 | sudo tee /proc/sys/vm/swappiness
```

建议使用 Overcommit，把 cat /proc/sys/vm/overcommit_memory 设成  1。

```shell
echo 1 | sudo tee /proc/sys/vm/overcommit_memory
```

## 下载 StarRocks

可以通过 [StarRocks 官网下载](https://www.mirrorship.cn/zh-CN/download/community) 获取二进制产品包。

下载的安装包可直接解压后进行安装部署，如有 [下载源码](https://github.com/StarRocks/starrocks) 并编译安装包的需求，可以使用 [Docker 进行编译](../administration/Build_in_docker.md)。

将 StarRocks 的二进制产品包分发到目标主机的部署路径并解压，可以考虑使用新建的 StarRocks 用户来管理。

以下载产品包为 starrocks-1.0.0.tar.gz 为例， 解压(tar -xzvf starrocks-1.0.0.tar.gz)后文件目录结构如下:

```Plain Text
StarRocks-XX-1.0.0
├── be  # BE目录
│   ├── bin
│   │   ├── start_be.sh     # BE启动脚本
│   │   └── stop_be.sh      # BE关闭脚本
│   ├── conf
│   │   └── be.conf         # BE配置文件
│   ├── lib
│   │   ├── starrocks_be    # BE可执行文件
│   │   └── meta_tool
│   └── www
├── fe  # FE目录
│   ├── bin
│   │   ├── start_fe.sh       # FE启动脚本
│   │   └── stop_fe.sh        # FE关闭脚本
│   ├── conf
│   │   └── fe.conf           # FE配置文件
│   ├── lib
│   │   ├── starrocks-fe.jar  # FE jar包
│   │   └── *.jar             # FE 依赖的jar包
│   └── webroot
└── udf
```

## 部署 FE

### FE 的基本配置

FE 的配置文件为 StarRocks-XX-1.0.0/fe/conf/fe.conf， 此处仅列出其中 JVM 配置和元数据目录配置，生产环境可参考 [FE 参数配置](../administration/Configuration.md#FE配置项) 对集群进行详细优化配置。

### FE 单实例部署

```bash
cd StarRocks-XX-1.0.0/fe
```

第一步: 配置文件 conf/fe.conf：

```bash
# 元数据目录
meta_dir = ${STARROCKS_HOME}/meta
# JVM配置
JAVA_OPTS = "-Xmx8192m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:$STARROCKS_HOME/log/fe.gc.log"
```

可以根据 FE 内存大小调整-Xmx8192m，为了避免 GC 建议 16G 以上，StarRocks 的元数据都在内存中保存。
<br/>

第二步: 创建元数据目录，需要与 fe.conf 中配置路径保持一致:

```bash
mkdir -p meta 
```

第三步: 启动 FE 进程:

```bash
bin/start_fe.sh --daemon
```

第四步: 确认启动 FE 启动成功。

* 查看日志 log/fe.log 确认。

```Plain Text
2020-03-16 20:32:14,686 INFO 1 [FeServer.start():46] thrift server started.
2020-03-16 20:32:14,696 INFO 1 [NMysqlServer.start():71] Open mysql server success on 9030
2020-03-16 20:32:14,696 INFO 1 [QeService.start():60] QE service start.
2020-03-16 20:32:14,825 INFO 76 [HttpServer$HttpServerThread.run():210] HttpServer started with port 8030
...
```

* 如果 FE 启动失败，可能是由于端口号被占用，可修改配置文件 conf/fe.conf 中的端口号 http_port。
* 使用 jps 命令查看 java 进程确认 "StarRocksFe" 存在。
* 使用浏览器访问 `FE ip:http_port`（默认 8030），打开 StarRocks 的 WebUI， 用户名为 root， 密码为空。

### 使用 MySQL 客户端访问 FE

StarRocks 可通过 Mysql 客户端进行连接，使用 Add/Drop 命令添加/删除 fe/be 节点，实现对集群的 [扩容/缩容](../administration/Scale_up_down.md) 操作。

第一步: 安装 mysql 客户端，版本建议 5.5+(如果已经安装，可忽略此步)：

```shell
Ubuntu：sudo apt-get install mysql-client
Centos：sudo yum install mysql-client
```

第二步: FE 进程启动后，使用 mysql 客户端连接 FE 实例：

```sql
mysql -h 127.0.0.1 -P9030 -uroot
```

注意：这里默认 root 用户密码为空，端口为 fe/conf/fe.conf 中的 query\_port 配置项，默认为 9030

第三步: 查看 FE 状态：

```Plain Text
mysql> SHOW PROC '/frontends'\G

************************* 1. row ************************
             Name: 172.16.139.11_9010_1594200991015
               IP: 172.16.139.11
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

**Role** 为 **FOLLOWER** 说明这是一个能参与选主的 FE；

**IsMaster** 为 **true**，说明该 FE 当前为主节点。

如果 MySQL 客户端连接不成功，请查看 log/fe.warn.log 日志文件，确认问题。由于是初次启动，如果在操作过程中遇到任何意外问题，都可以删除并重新创建 FE 的元数据目录，再从头开始操作。
<br/>

## 部署 BE

### BE 的基本配置

BE 的配置文件为 StarRocks-XX-1.0.0/be/conf/be.conf，默认配置即可启动集群，生产环境可参考 [BE 参数配置](../administration/Configuration.md#be-%E9%85%8D%E7%BD%AE%E9%A1%B9) 对集群进行详细优化配置。

### BE 部署

通过以下命令启动 be 并添加 be 到 StarRocks 集群， 一般至少在三个节点部署 3 个 BE 实例， 每个实例的添加步骤相同。

第一步: 创建数据目录（当前设置为 be.conf 中默认 storage_root_path 配置项路径）：

```shell
# 进入be的安装目录
cd StarRocks-XX-1.0.0/be
# 创建数据存储目录
mkdir -p storage
```

第二步: 通过 mysql 客户端添加 BE 节点：

* host 为与 priority_networks 设置相匹配的 IP，port 为 BE 配置文件中的 heartbeat_service_port，默认为 9050。

```sql
mysql> ALTER SYSTEM ADD BACKEND "host:port";
```

如出现错误，需要删除 BE 节点，可通过以下命令将 BE 节点从集群移除，host 和 port 与添加时一致：

```sql
mysql> ALTER SYSTEM decommission BACKEND "host:port";
```

具体参考 [扩容缩容](../administration/Scale_up_down.md#be%E6%89%A9%E7%BC%A9%E5%AE%B9)。

第三步: 启动 BE：

```shell
bin/start_be.sh --daemon
```

第四步: 查看 BE 状态, 确认 BE 就绪:

```Plain Text
mysql> SHOW PROC '/backends'\G

********************* 1. row **********************
            BackendId: 10002
              Cluster: default_cluster
                   IP: 172.16.139.11
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

<br/>

如果 isAlive 为 true，则说明 BE 正常接入集群。如果 BE 没有正常接入集群，请查看 log 目录下的 be.WARNING 日志文件确定原因。

如果日志中出现类似以下的信息，说明 priority_networks 的配置存在问题。

```Plain Text
W0708 17:16:27.308156 11473 heartbeat_server.cpp:82\] backend ip saved in master does not equal to backend local ip127.0.0.1 vs. 172.16.179.26
```

此时需要，先用以下命令 drop 掉原来加进去的 be，然后重新以正确的 IP 添加 BE。

```sql
MySQL> ALTER SYSTEM DROPP BACKEND "172.16.139.11:9050";
```

由于是初次启动，如果在操作过程中遇到任何意外问题，都可以删除并重新创建 storage 目录，再从头开始操作。

## 部署 Broker

配置文件为 apache_hdfs_broker/conf/apache_hdfs_broker.conf

> 注意：Broker 没有也不需要 priority_networks 参数，Broker 的服务默认绑定在 0.0.0.0 上，只需要在 ADD BROKER 时，填写正确可访问的 Broker IP 即可。

如果有特殊的 hdfs 配置，复制线上的 hdfs-site.xml 到 conf 目录下

启动 broker：

```shell
./apache_hdfs_broker/bin/start_broker.sh --daemon
```

添加 broker 节点到集群中：

```sql
MySQL> ALTER SYSTEM ADD BROKER broker1 "172.16.139.11:8000";
```

查看 broker 状态：

```plain text
MySQL> SHOW PROC "/brokers"\G
*************************** 1. row ***************************
          Name: broker1
            IP: 172.16.139.11
          Port: 8000
         Alive: true
 LastStartTime: 2020-04-01 19:08:35
LastUpdateTime: 2020-04-01 19:08:45
        ErrMsg: 
1 row in set (0.00 sec)
```

Alive 为 true 代表状态正常。

<br/>

## 扩展内容

### FE 的高可用集群部署

StarRocks FE 支持 HA 模型部署，保证集群的高可用，详细设置方式请参考 [FE 高可用集群部署](/administration/Deployment.md)。

### 集群升级

StarRocks 支持平滑升级，并支持回滚（1.18.2 及以后的版本无法回滚到其之前的版本），详细操作步骤请参考 [集群升级](../administration/Cluster_administration.md#集群升级)。
