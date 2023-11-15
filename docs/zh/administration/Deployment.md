# 部署 FE 高可用集群

本文介绍如何部署 StarRocks 集群 FE 节点高可用集群。

FE 的高可用集群采用主从复制架构，可避免 FE 单点故障。FE 采用了类 Paxos 的 Berkeley DB Java Edition（BDBJE）协议完成选主，日志复制和故障切换。在 FE 集群中，多实例分为两种角色：Follower 和 Observer。前者为复制协议的可投票成员，参与选主和提交日志，一般数量为奇数（2n+1），使用多数派（n+1）确认，可容忍少数派（n）故障；后者属于非投票成员，用于异步订阅复制日志，Observer 的状态落后于 Follower，类似其他复制协议中的 Learner 角色。

FE 集群从 Follower 中自动选出 Leader 节点，所有更改状态操作都由 Leader 节点执行。最新状态可以从 Leader FE 节点读取。更改操作可以由非 Leader 节点发起，继而转发给 Leader 节点执行，非 Leader 节点在复制日志中的 LSN 记录最近一次更改操作。读操作可以直接在非 Leader 节点上执行，但需要等待非 Leader 节点的状态已经同步到最近一次更改操作的 LSN，因此非 Leader 节点的读写操作满足顺序一致性。Observer 节点能够增加 FE 集群的读负载能力，对时效性要求放宽的非紧要用户可以选择读 Observer 节点。

> 注意
>
> * FE 节点之间的时钟相差**不能超过 5s**。如果节点之间存在较大时钟差，请使用 NTP 协议校准时间。
> * 所有 FE 节点的 `http_port` 需保持相同，因此一台机器无法错开端口部署某个集群的多个 FE 节点。

## 下载并配置新 FE 节点

详细操作请参考 [手动部署 StarRocks FE 节点](/deployment/deploy_manually.md)。

## 添加新 FE 节点

使用 MySQL 客户端连接已有 FE 节点，添加新 FE 节点的信息，包括角色、IP 地址、以及 Port。

* 添加 Follower FE 节点。

```sql
ALTER SYSTEM ADD FOLLOWER "host:port";
```

* 添加 Observer FE  节点。

```sql
ALTER SYSTEM ADD OBSERVER "host:port";
```

参数：

* `host`：机器的 IP 地址。如果机器存在多个 IP 地址，则该项为 `priority_networks` 设置项下设定的唯一通信 IP 地址。
* `port`：`edit_log_port` 设置项下设定的端口，默认为 `9010`。

出于安全考虑，StarRocks 的 FE 节点和 BE 节点只会监听一个 IP 地址进行通信。如果一台机器有多块网卡，StarRocks 有可能无法自动找到正确的 IP 地址。例如，通过 `ifconfig` 命令查看到 `eth0` IP 地址为 `192.168.1.1`，`docker0` IP 地址为 `172.17.0.1`，您可以设置 `192.168.1.0/24` 子网以指定使用 `eth0` 作为通信 IP。此处采用 [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) 的表示方法来指定 IP 所在子网范围，以便在所有的 BE 及 FE 节点上使用相同的配置。

如出现错误，您可以通过命令删除相应 FE 节点。

* 删除 Follower FE 节点。

```sql
ALTER SYSTEM DROP FOLLOWER "host:port";
```

* 删除 Observer FE  节点。

```sql
ALTER SYSTEM drop OBSERVER "host:port";
```

具体操作参考[扩容缩容](../administration/Scale_up_down.md)。

## 连接 FE 节点

FE 节点需两两之间建立通信连接方可实现复制协议选主，投票，日志提交和复制等功能。当新的FE节点**首次**被添加到已有集群并启动时，您需要指定集群中现有的一个节点作为 helper 节点，并从该节点获得集群的所有 FE 节点的配置信息，才能建立通信连接。因此，在首次启动新 FE 节点时候，您需要通过命令行指定 `--helper` 参数。

```shell
./bin/start_fe.sh --helper host:port --daemon
```

参数：

* `host`：机器的IP 地址。如果机器存在多个 IP 地址，则该项为 `priority_networks` 设置项下设定的唯一通信 IP 地址。
* `port`：`edit_log_port` 设置项下设定的端口，默认为 `9010`。

## 确认 FE 集群部署成功

查看集群状态，确认部署成功。

```sql
SHOW PROC '/frontends'\G
```

以下示例中，`172.26.108.172_9010_1584965098874` 为主 FE 节点。

```Plain Text
mysql> SHOW PROC '/frontends'\G

********************* 1. row **********************
    Name: 172.26.108.172_9010_1584965098874
      IP: 172.26.108.172
HostName: starrocks-sandbox01
......
    Role: LEADER
......
   Alive: true
......
********************* 2. row **********************
    Name: 172.26.108.174_9010_1584965098874
      IP: 172.26.108.174
HostName: starrocks-sandbox02
......
    Role: FOLLOWER
......
   Alive: true
......
********************* 3. row **********************
    Name: 172.26.108.175_9010_1584965098874
      IP: 172.26.108.175
HostName: starrocks-sandbox03
......
    Role: FOLLOWER
......
   Alive: true
......
3 rows in set (0.05 sec)
```

节点的 `Alive` 项为 `true` 时，添加节点成功。
