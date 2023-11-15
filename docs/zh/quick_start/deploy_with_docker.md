# 使用 Docker 部署 StarRocks

本快速入门教程将指导您使用 Docker 在本地计算机上部署 StarRocks。在开始之前，您可以阅读 [系统架构](../introduction/Architecture.md) 了解更多概念细节。

您可以依照以下步骤部署一个简单的 StarRocks 集群，其中包含**一个 FE 节点**和**一个 BE 节点**。您可以基于该集群完成后续的 [创建表](../quick_start/Create_table.md) 和 [导入和查询数据](../quick_start/Import_and_query.md) 的快速入门教程，从而熟悉 StarRocks 的基本操作。

> **注意**
>
> 使用以下 Docker 镜像部署的 StarRocks 集群仅适用于小数据集验证 DEMO 的情况。不建议您将其用于大规模测试或生产环境。如需部署高可用 StarRocks 集群，请参见[部署概览](../deployment/deployment_overview.md)了解其他适合您场景的部署方式。

## 前提条件

在 Docker 容器中部署 StarRocks 之前，请确保如下环境要求已满足：

- **硬件要求**

  建议在具有 8 核 CPU 和 16 GB 内存以上的计算机上部署 StarRocks。

- **软件要求**

  您需要在计算机上安装以下软件：

  - [Docker Engine](https://docs.docker.com/engine/install/) (17.06.0 以上)
  - MySQL 客户端 (5.5 以上)

## 第一步：通过 Docker 镜像部署 StarRocks

从 [StarRocks Docker Hub](https://hub.docker.com/r/starrocks/allin1-ubuntu/tags) 下载 StarRocks Docker 镜像。您可以根据 Tag 选择特定版本的镜像。

```Bash
sudo docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 \
    -itd starrocks.docker.scarf.sh/starrocks/allin1-ubuntu
```

> **故障排除**
>
> 如果上述任何端口被占用，系统会打印错误日志 “docker: Error response from daemon: driver failed programming external connectivity on endpoint tender_torvalds (): Bind for 0.0.0.0:xxxx failed: port is already allocated”。您可以通过更改命令中冒号（:）之前的端口将其修改为主机上的其他可用端口。

您可以通过运行以下命令来检查容器是否已创建并正常运行：

```Bash
sudo docker ps
```

如下所示，如果 StarRocks 容器的 `STATUS` 为 `Up`，那么您已成功在 Docker 容器中部署 StarRocks。

```Plain
CONTAINER ID   IMAGE                                          COMMAND                  CREATED         STATUS                 PORTS                                                                                                                             NAMES
8962368f9208   starrocks/allin1-ubuntu:branch-3.0-0afb97bbf   "/bin/sh -c ./start_…"   4 minutes ago   Up 4 minutes           0.0.0.0:8037->8030/tcp, :::8037->8030/tcp, 0.0.0.0:8047->8040/tcp, :::8047->8040/tcp, 0.0.0.0:9037->9030/tcp, :::9037->9030/tcp   xxxxx
```

## 第二步：连接 StarRocks

成功部署后，您可以通过 MySQL 客户端连接该 StarRocks 集群。

```Bash
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

> **注意**
>
> 如果您在 `docker run` 命令中为 `9030` 端口分配了其他端口，则必须将上述命令中的 `9030` 替换为您所分配的端口。

您可以通过执行以下 SQL 查看 FE 节点的状态：

```SQL
SHOW PROC '/frontends'\G
```

示例：

```Plain
StarRocks > SHOW PROC '/frontends'\G
*************************** 1. row ***************************
             Name: 8962368f9208_9010_1681370634632
               IP: 8962368f9208
      EditLogPort: 9010
         HttpPort: 8030
        QueryPort: 9030
          RpcPort: 9020
             Role: LEADER
        ClusterId: 555505802
             Join: true
            Alive: true
ReplayedJournalId: 99
    LastHeartbeat: 2023-04-13 07:28:50
         IsHelper: true
           ErrMsg: 
        StartTime: 2023-04-13 07:24:11
          Version: BRANCH-3.0-0afb97bbf
1 row in set (0.02 sec)
```

- 如果 `Alive` 字段为 `true`，则该 FE 节点正常启动并加入集群。
- 如果字段 `Role` 为 `FOLLOWER`，则该 FE 节点有资格被选举为 Leader FE 节点。
- 如果字段 `Role` 为 `LEADER`，则该 FE 节点为 Leader FE 节点。

您可以通过执行以下 SQL 查看 BE 节点的状态：

```SQL
SHOW PROC '/backends'\G
```

示例：

```Plain
StarRocks > SHOW PROC '/backends'\G
*************************** 1. row ***************************
            BackendId: 10004
                   IP: 8962368f9208
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2023-04-13 07:24:25
        LastHeartbeat: 2023-04-13 07:29:05
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 30
     DataUsedCapacity: 0.000 
        AvailCapacity: 527.437 GB
        TotalCapacity: 1.968 TB
              UsedPct: 73.83 %
       MaxDiskUsedPct: 73.83 %
               ErrMsg: 
              Version: BRANCH-3.0-0afb97bbf
               Status: {"lastSuccessReportTabletsTime":"2023-04-13 07:28:26"}
    DataTotalCapacity: 527.437 GB
          DataUsedPct: 0.00 %
             CpuCores: 16
    NumRunningQueries: 0
           MemUsedPct: 0.02 %
           CpuUsedPct: 0.1 %
1 row in set (0.00 sec)
```

如果 `Alive` 字段为 `true`，则该 BE 节点正常启动并加入集群。

## 停止并移除 Docker 容器

完成整个快速入门教程后，您可以通过 StarRocks 容器的 ID 停止并删除该容器。

> **说明**
>
> 您可以通过运行 `sudo docker ps` 获取 Docker 容器的 `container_id`。

运行以下命令停止容器：

```Bash
# 将 <container_id> 替换为您 StarRocks 集群的容器 ID。
sudo docker stop <container_id>
```

如果您不再需要该容器，可以通过运行以下命令将其删除：

```Bash
# 将 <container_id> 替换为您 StarRocks 集群的容器 ID。
sudo docker rm <container_id>
```

> **注意**
>
> 删除容器的操作不可逆。删除前，请确保您已备份了容器中的重要数据。

## 下一步

成功部署 StarRocks 后，您可以继续完成有关 [创建表](../quick_start/Create_table.md) 和 [导入和查询数据](../quick_start/Import_and_query.md) 的快速入门教程。
