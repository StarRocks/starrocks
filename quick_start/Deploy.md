# 手动部署

手动部署可以让用户快速体验StarRocks, 积累StarRocks的系统运维经验.  生产环境部署, 请使用管理平台和自动部署.

## 获取二进制产品包

请您联系StarRocks的技术支持或者销售人员获取最新稳定版的StarRocks二进制产品包.

比如您获得的产品包为starrocks-1.0.0.tar.gz, 解压(tar -xzvf starrocks-1.0.0.tar.gz)后内容如下:

```Plain Text
StarRocks-XX-1.0.0
├── be  # BE目录
│   ├── bin
│   │   ├── start_be.sh # BE启动命令
│   │   └── stop_be.sh  # BE关闭命令
│   ├── conf
│   │   └── be.conf     # BE配置文件
│   ├── lib
│   │   ├── starrocks_be  # BE可执行文件
│   │   └── meta_tool
│   └── www
├── fe  # FE目录
│   ├── bin
│   │   ├── start_fe.sh # FE启动命令
│   │   └── stop_fe.sh  # FE关闭命令
│   ├── conf
│   │   └── fe.conf     # FE配置文件
│   ├── lib
│   │   ├── starrocks-fe.jar  # FE jar包
│   │   └── *.jar           # FE 依赖的jar包
│   └── webroot
└── udf
```

## 环境准备

准备三台物理机, 需要以下环境支持：

* Linux (Centos 7+)
* Java 1.8+

CPU需要支持AVX2指令集，cat /proc/cpuinfo |grep avx2有结果输出表明CPU支持，如果没有支持，建议更换机器，StarRocks使用向量化技术需要一定的指令集支持才能发挥效果。

将StarRocks的二进制产品包分发到目标主机的部署路径并解压，可以考虑使用新建的StarRocks用户来管理。

## 部署FE

### FE的基本配置

FE的配置文件为StarRocks-XX-1.0.0/fe/conf/fe.conf, 默认配置已经足以启动集群, 有经验的用户可以查看手册的系统配置章节, 为生产环境定制配置，为了让用户更好的理解集群的工作原理, 此处只列出基础配置.

### FE单实例部署

```bash
cd StarRocks-XX-1.0.0/fe
```

第一步: 定制配置文件conf/fe.conf：

```bash
JAVA_OPTS = "-Xmx4096m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:$STARROCKS_HOME/log/fe.gc.log"
```

可以根据FE内存大小调整 -Xmx4096m，为了避免GC建议16G以上，StarRocks的元数据都在内存中保存。
<br/>

第二步: 创建元数据目录:

```bash
mkdir -p meta (1.19.x及以前的版本需要使用mkdir -p doris-meta)
```

<br/>

第三步: 启动FE进程:

```bash
bin/start_fe.sh --daemon
```

<br/>

第四步: 确认启动FE启动成功.

* 查看日志log/fe.log确认.

```Plain Text
2020-03-16 20:32:14,686 INFO 1 [FeServer.start():46] thrift server started.

2020-03-16 20:32:14,696 INFO 1 [NMysqlServer.start():71] Open mysql server success on 9030

2020-03-16 20:32:14,696 INFO 1 [QeService.start():60] QE service start.

2020-03-16 20:32:14,825 INFO 76 [HttpServer$HttpServerThread.run():210] HttpServer started with port 8030

...
```

* 如果FE启动失败，可能是由于端口号被占用，修改配置文件conf/fe.conf中的端口号http_port。
* 使用jps命令查看java进程确认"StarRocksFe"存在.
* 使用浏览器访问8030端口, 打开StarRocks的WebUI, 用户名为root, 密码为空.

### 使用MySQL客户端访问FE

第一步: 安装mysql客户端(如果已经安装，可忽略此步)：

Ubuntu：sudo apt-get install mysql-client

Centos：sudo yum install mysql-client
<br/>

第二步: 使用mysql客户端连接：

```sql
mysql -h 127.0.0.1 -P9030 -uroot
```

注意：这里默认root用户密码为空，端口为fe/conf/fe.conf中的query\_port配置项，默认为9030
<br/>

第三步: 查看FE状态：

```Plain Text
mysql> SHOW PROC '/frontends'\G

************************* 1. row ************************
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

<br/>

Role为FOLLOWER说明这是一个能参与选主的FE；IsMaster为true，说明该FE当前为主节点。
<br/>

如果MySQL客户端连接不成功，请查看log/fe.warn.log日志文件，确认问题。由于是初次启动，如果在操作过程中遇到任何意外问题，都可以删除并重新创建FE的元数据目录，再从头开始操作。
<br/>

### FE的高可用集群部署

FE的高可用集群采用主从复制架构, 可避免FE单点故障. FE采用了类raft的bdbje协议完成选主, 日志复制和故障切换. 在FE集群中, 多实例分为两种角色: follower和observer; 前者为复制协议的可投票成员, 参与选主和提交日志, 一般数量为奇数(2n+1), 使用多数派(n+1)确认, 可容忍少数派(n)故障; 而后者属于非投票成员, 用于异步订阅复制日志, observer的状态落后于follower, 类似其他复制协议中的learner角色.
<br/>

FE集群从follower中自动选出master节点, 所有更改状态操作都由master节点执行, 从FE的master节点可以读到最新的状态. 更改操作可以从非master节点发起, 继而转发给master节点执行,  非master节点记录最近一次更改操作在复制日志中的LSN, 读操作可以直接在非master节点上执行, 但需要等待非master节点的状态已经同步到最近一次更改操作的LSN, 因此读写非Master节点满足顺序一致性. Observer节点能够增加FE集群的读负载能力, 时效性要求放宽的非紧要用户可以读observer节点.
<br/>

FE节点之间的时钟相差不能超过5s, 使用NTP协议校准时间.

一台机器上只可以部署单个FE节点。所有FE节点的http\_port需要相同。
<br/>

集群部署按照下列步骤逐个增加FE实例.

第一步: 分发二进制和配置文件, 配置文件和单实例情形相同.
<br/>

第二步: 使用MySQL客户端连接已有的FE,  添加新实例的信息，信息包括角色、ip、port：

```sql
mysql> ALTER SYSTEM ADD FOLLOWER "host:port";
```

或

```sql
mysql> ALTER SYSTEM ADD OBSERVER "host:port";
```

host为机器的IP，如果机器存在多个IP，需要选取priority\_networks里的IP，例如priority\_networks=192.168.1.0/24 可以设置使用192.168.1.x 这个子网进行通信。port为edit\_log\_port，默认为9010。

> StarRocks的FE和BE因为安全考虑都只会监听一个IP来进行通信，如果一台机器有多块网卡，可能StarRocks无法自动找到正确的IP，例如 ifconfig 命令能看到  eth0 ip为 192.168.1.1, docker0:  172.17.0.1 ，我们可以设置 192.168.1.0/24 这一个子网来指定使用eth0作为通信的IP，这里采用是[CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing)的表示方法来指定IP所在子网范围，这样可以在所有的BE，FE上使用相同的配置。
> priority\_networks 是 FE 和 BE 相同的配置项，写在 fe.conf 和 be.conf 中。该配置项用于在 FE 或 BE 启动时，告诉进程应该绑定哪个IP。示例如下：
> `priority_networks=10.1.3.0/24`

如出现错误，需要删除FE，应用下列命令：

```sql
alter system drop follower "fe_host:edit_log_port";
alter system drop observer "fe_host:edit_log_port";
```

具体参考[扩容缩容](../administration/Scale_up_down.md)。
<br/>

第三步: FE节点之间需要两两互联才能完成复制协议选主, 投票，日志提交和复制等功能。 FE节点首次启动时，需要指定现有集群中的一个节点作为helper节点, 从该节点获得集群的所有FE节点的配置信息，才能建立通信连接，因此首次启动需要指定--helper参数：

```shell
./bin/start_fe.sh --helper host:port --daemon
```

host为helper节点的IP，如果有多个IP，需要选取priority\_networks里的IP。port为edit\_log\_port，默认为9010。

当FE再次启动时，无须指定--helper参数，因为FE已经将其他FE的配置信息存储于本地目录, 因此可直接启动：

```shell
./bin/start_fe.sh --daemon
```

<br/>

第四步: 查看集群状态, 确认部署成功：

```Plain Text
mysql> SHOW PROC '/frontends'\G

********************* 1. row **********************
    Name: 172.26.108.172_9010_1584965098874
      IP: 172.26.108.172
HostName: starrocks-sandbox01
......
    Role: FOLLOWER
IsMaster: true
......
   Alive: true
......
********************* 2. row **********************
    Name: 172.26.108.174_9010_1584965098874
      IP: 172.26.108.174
HostName: starrocks-sandbox02
......
    Role: FOLLOWER
IsMaster: false
......
   Alive: true
......
********************* 3. row **********************
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

节点的Alive显示为true则说明添加节点成功。以上例子中，

172.26.108.172\_9010\_1584965098874 为主FE节点。

## 部署BE

### BE的基本配置

BE的配置文件为StarRocks-XX-1.0.0/be/conf/be.conf, 默认配置已经足以启动集群, 不建议初尝用户修改配置, 有经验的用户可以查看手册的系统配置章节, 为生产环境定制配置. 为了让用户更好的理解集群的工作原理, 此处只列出基础配置.

### BE部署

用户可使用下面命令添加BE到StarRocks集群, 一般至少部署3个BE实例, 每个实例的添加步骤相同.

```shell
cd StarRocks-XX-1.0.0/be
```

第一步: 创建数据目录：

```shell
mkdir -p storage
```

<br/>

第二步: 通过mysql客户端添加BE节点：

```sql
mysql> ALTER SYSTEM ADD BACKEND "host:port";
```

这里IP地址为和priority\_networks设置匹配的IP，portheartbeat\_service\_port，默认为9050
<br/>

如出现错误，需要删除BE节点，应用下列命令：

* `alter system decommission backend "be_host:be_heartbeat_service_port";`
* `alter system dropp backend "be_host:be_heartbeat_service_port";`

具体参考[扩容缩容](../administration/Scale_up_down.md)。
<br/>

第三步: 启动BE：

```shell
bin/start_be.sh --daemon
```

<br/>

第四步: 查看BE状态, 确认BE就绪:

```Plain Text
mysql> SHOW PROC '/backends'\G

********************* 1. row **********************
            BackendId: 10002
              Cluster: default_cluster
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

<br/>

如果isAlive为true，则说明BE正常接入集群。如果BE没有正常接入集群，请查看log目录下的be.WARNING日志文件确定原因。
<br/>

如果日志中出现类似以下的信息，说明priority\_networks的配置存在问题。

```Plain Text
W0708 17:16:27.308156 11473 heartbeat\_server.cpp:82\] backend ip saved in master does not equal to backend local ip127.0.0.1 vs. 172.16.179.26
```

<br/>

此时需要，先用以下命令drop掉原来加进去的be，然后重新以正确的IP添加BE。

```sql
mysql> ALTER SYSTEM DROPP BACKEND "172.16.139.24:9050";
```

<br/>

由于是初次启动，如果在操作过程中遇到任何意外问题，都可以删除并重新创建storage目录，再从头开始操作。

## 部署Broker

配置文件为apache\_hdfs\_broker/conf/apache\_hdfs\_broker.conf

> 注意：Broker没有也不需要priority\_networks参数，Broker的服务默认绑定在0.0.0.0上，只需要在ADD BROKER时，填写正确可访问的Broker IP即可。

如果有特殊的hdfs配置，复制线上的hdfs-site.xml到conf目录下

启动：

```shell
./apache_hdfs_broker/bin/start_broker.sh --daemon
```

添加broker节点到集群中：

```sql
MySQL> ALTER SYSTEM ADD BROKER broker1 "172.16.139.24:8000";
```

查看broker状态：

```plain text
MySQL> SHOW PROC "/brokers"\G
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

Alive为true代表状态正常。

## 参数设置

* **Swappiness**

关闭交换区，消除交换内存到虚拟内存时对性能的扰动。

```shell
echo 0 | sudo tee /proc/sys/vm/swappiness
```

* **Compaction相关**

当使用聚合表或更新模型，导入数据比较快的时候，可在配置文件 `be.conf` 中修改下列参数以加速compaction。

```shell
cumulative_compaction_num_threads_per_disk = 4
base_compaction_num_threads_per_disk = 2
cumulative_compaction_check_interval_seconds = 2
```

* **并行度**

在客户端执行命令，修改StarRocks的并行度(类似clickhouse set max_threads= 8)。并行度可以设置为当前机器CPU核数的一半。

```sql
set global parallel_fragment_exec_instance_num =  8;
```

## 使用MySQL客户端访问StarRocks

### Root用户登录

使用MySQL客户端连接某一个FE实例的query_port(9030), StarRocks内置root用户，密码默认为空：

```shell
mysql -h fe_host -P9030 -u root
```

<br/>

清理环境：

```sql
mysql > drop database if exists example_db;

mysql > drop user test;
```

### 创建新用户

通过下面的命令创建一个普通用户：

```sql
mysql > create user 'test' identified by '123456';
```

### 创建数据库

StarRocks中root账户才有权建立数据库，使用root用户登录，建立example\_db数据库:

```sql
mysql > create database example_db;
```

  <br/>

数据库创建完成之后，可以通过show databases查看数据库信息：

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

information_schema是为了兼容mysql协议而存在，实际中信息可能不是很准确，所以关于具体数据库的信息建议通过直接查询相应数据库而获得。

### 账户授权

example_db创建完成之后，可以通过root账户example_db读写权限授权给test账户，授权之后采用test账户登录就可以操作example\_db数据库了：

```sql
mysql > grant all on example_db to test;
```

<br/>

退出root账户，使用test登录StarRocks集群：

```sql
mysql > exit

mysql -h 127.0.0.1 -P9030 -utest -p123456
```

### 建表

StarRocks支持支持单分区和复合分区两种建表方式。

<br/>

在复合分区中：

* 第一级称为Partition，即分区。用户可以指定某一维度列作为分区列（当前只支持整型和时间类型的列），并指定每个分区的取值范围。
* 第二级称为Distribution，即分桶。用户可以指定某几个维度列（或不指定，即所有KEY列）以及桶数对数据进行HASH分布。

以下场景推荐使用复合分区：

* 有时间维度或类似带有有序值的维度：可以以这类维度列作为分区列。分区粒度可以根据导入频次、分区数据量等进行评估。
* 历史数据删除需求：如有删除历史数据的需求（比如仅保留最近N 天的数据）。使用复合分区，可以通过删除历史分区来达到目的。也可以通过在指定分区内发送DELETE语句进行数据删除。
* 解决数据倾斜问题：每个分区可以单独指定分桶数量。如按天分区，当每天的数据量差异很大时，可以通过指定分区的分桶数，合理划分不同分区的数据,分桶列建议选择区分度大的列。

用户也可以不使用复合分区，即使用单分区。则数据只做HASH分布。

<br/>  

下面分别演示两种分区的建表语句：

1. 首先切换数据库：mysql > use example_db;
2. 建立单分区表建立一个名字为table1的逻辑表。使用全hash分桶，分桶列为siteid，桶数为10。这个表的schema如下：

* siteid：类型是INT（4字节）, 默认值为10
* city_code：类型是SMALLINT（2字节）
* username：类型是VARCHAR, 最大长度为32, 默认值为空字符串
* pv：类型是BIGINT（8字节）, 默认值是0; 这是一个指标列, StarRocks内部会对指标列做聚合操作, 这个列的聚合方法是求和（SUM）。这里采用了聚合模型，除此之外StarRocks还支持明细模型和更新模型，具体参考[数据模型介绍](../table_design/Data_model.md)。

建表语句如下:

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

1. 建立复合分区表

建立一个名字为table2的逻辑表。这个表的 schema 如下：

* event_day：类型是DATE，无默认值
* siteid：类型是INT（4字节）, 默认值为10
* city_code：类型是SMALLINT（2字节）
* username：类型是VARCHAR, 最大长度为32, 默认值为空字符串
* pv：类型是BIGINT（8字节）, 默认值是0; 这是一个指标列, StarRocks 内部会对指标列做聚合操作, 这个列的聚合方法是求和（SUM）

我们使用event_day列作为分区列，建立3个分区: p1, p2, p3

* p1：范围为 \[最小值, 2017-06-30)
* p2：范围为 \[2017-06-30, 2017-07-31)
* p3：范围为 \[2017-07-31, 2017-08-31)

每个分区使用siteid进行哈希分桶，桶数为10。

<br/>  

建表语句如下:

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

  <br/>

表建完之后，可以查看example\_db中表的信息:

```Plain Text
mysql> show tables;

+-------------------------+
| Tables_in_example_db    |
+-------------------------+
| table1                  |
| table2                  |
+-------------------------+
2 rows in set (0.01 sec)

  <br/>

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

  <br/>

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

## 使用Docker进行编译

具体参考[在容器内编译](../administration/Build_in_docker.md)。
