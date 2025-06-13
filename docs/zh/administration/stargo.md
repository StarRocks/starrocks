---
displayed_sidebar: docs
---

# 使用 StarGo 部署管理 StarRocks

本文介绍如何使用 StarGo 部署管理 StarRocks 集群。

> **说明**
>
> **目前 StarGo 正在由社区改造优化，建议使用社区的[最新版本](https://forum.mirrorship.cn/t/topic/4945)进行集群部署。因改造过程中迭代较快，功能调整较为频繁，故待社区改造工作基本完成后，本文再进行最终的更新。**

**下文为基于旧版本的部署操作，目前不再建议使用**。

StarGo 是一个用于管理多个 StarRocks 集群的命令行工具。通过 StarGo，您可以使用简单的命令行实现多集群的部署、查看、升级、启动与停止等操作。该工具从 2.3 版本开始支持。

## 部署 StarGo

在当前用户路径下下载并解压 StarGo 二进制安装包。

```shell
wget https://raw.githubusercontent.com/wangtianyi2004/starrocks-controller/main/stargo-pkg.tar.gz
tar -xzvf stargo-pkg.tar.gz
```

安装包包含以下文件。

- **stargo**：StarGo 二进制文件，无需安装。
- **deploy-template.yaml**：部署配置文件模板。
- **repo.yaml**：指定 StarRocks 安装包下载库的配置文件。

## 部署集群

您可以使用 StarGo 部署 StarRocks 集群。

### 前提条件

- 待部署集群至少需要一个中控机节点和三个部署机节点，所有节点可以混合部署于同一台机器。
- 中控机上需部署 StarGo。
- 中控机与部署机间需创建 SSH 互信。

以下示例创建了中控机 sr-dev@r0 与部署机 starrocks@r1、starrocks@r2 以及 starrocks@r3 间的 SSH 互信。

```plain text
## 创建 sr-dev@r0 到 starrocks@r1、r2、r3 的 ssh 互信。
[sr-dev@r0 ~]$ ssh-keygen
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r1
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r2
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r3

## 验证 sr-dev@r0 到 starrocks@r1、r2、r3 的 ssh 互信。
[sr-dev@r0 ~]$ ssh starrocks@r1 date
[sr-dev@r0 ~]$ ssh starrocks@r2 date
[sr-dev@r0 ~]$ ssh starrocks@r3 date
```

### 创建配置文件

根据以下 YAML 模板，创建部署 StarRocks 集群的拓扑文件。具体配置项参考[参数配置](../administration/management/FE_configuration.md)。

```yaml
global:
    user: "starrocks"   # 请修改为当前操作系统用户。
    ssh_port: 22

fe_servers:
  - host: 192.168.XX.XX
    ssh_port: 22
    http_port: 8030
    rpc_port: 9020
    query_port: 9030
    edit_log_port: 9010
    deploy_dir: StarRocks/fe
    meta_dir: StarRocks/fe/meta
    log_dir: StarRocks/fe/log
    priority_networks: 192.168.XX.XX/24 # 当机器有多个 IP 时，请在当前配置项中为当前节点指定唯一 IP。
    config:
      sys_log_level: "INFO"
  - host: 192.168.XX.XX
    ssh_port: 22
    http_port: 8030
    rpc_port: 9020
    query_port: 9030
    edit_log_port: 9010
    deploy_dir: StarRocks/fe
    meta_dir: StarRocks/fe/meta
    log_dir: StarRocks/fe/log
    priority_networks: 192.168.XX.XX/24 # 当机器有多个 IP 时，在当前配置项中为当前节点指定唯一 IP。
    config:
      sys_log_level: "INFO"
  - host: 192.168.XX.XX
    ssh_port: 22
    http_port: 8030
    rpc_port: 9020
    query_port: 9030
    edit_log_port: 9010
    deploy_dir: StarRocks/fe
    meta_dir: StarRocks/fe/meta
    log_dir: StarRocks/fe/log
    priority_networks: 192.168.XX.XX/24 # 当机器有多个 IP 时，在当前配置项中为当前节点指定唯一 IP。
    config:
      sys_log_level: "INFO"
be_servers:
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    webserver_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # 当机器有多个 IP 时，在当前配置项中为当前节点指定唯一 IP。
    config:
      create_tablet_worker_count: 3
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    webserver_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # 当机器有多个 IP 时，在当前配置项中为当前节点指定唯一 IP。
    config:
      create_tablet_worker_count: 3
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    webserver_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # 当机器有多个 IP 时，在当前配置项中为当前节点指定唯一 IP。
    config:
      create_tablet_worker_count: 3
```

### 创建部署目录（可选）

如果您在配置文件中设定的部署路径不存在，且您有创建该路径的权限，StarGo 将根据配置文件自动创建部署目录。如果路径已存在，请确保您有在该路径下拥有写入的权限。您也可以通过以下命令，在各部署节点分别创建部署路径。

- 在 FE 节点安装目录下上创建 **meta** 路径。

```shell
mkdir -p StarRocks/fe/meta
```

- 在 BE 节点安装目录下上创建 **storage** 路径。

```shell
mkdir -p StarRocks/be/storage
```

> 注意
>
> 请确保以上创建的路径与配置文件中的 `meta_dir` 和 `storage_dir` 相同。

### 部署 StarRocks

通过以下命令部署 StarRocks 集群。

```shell
./stargo cluster deploy <cluster_name> <version> <topology_file>
```

|参数|描述|
|----|----|
|cluster_name|创建的集群名|
|version|StarRocks 的版本|
|topology_file|配置文件名|

创建成功后，集群将会自动启动。当返回 beStatus 和feStatus 为 true 时，集群部署启动成功。

示例：

```plain text
[sr-dev@r0 ~]$ ./stargo cluster deploy sr-c1 v2.0.1 sr-c1.yaml
[20220301-234817  OUTPUT] Deploy cluster [clusterName = sr-c1, clusterVersion = v2.0.1, metaFile = sr-c1.yaml]
[20220301-234836  OUTPUT] PRE CHECK DEPLOY ENV:
PreCheck FE:
IP                    ssh auth         meta dir                   deploy dir                 http port        rpc port         query port       edit log port
--------------------  ---------------  -------------------------  -------------------------  ---------------  ---------------  ---------------  ---------------
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS

PreCheck BE:
IP                    ssh auth         storage dir                deploy dir                 webSer port      heartbeat port   brpc port        be port
--------------------  ---------------  -------------------------  -------------------------  ---------------  ---------------  ---------------  ---------------
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS


[20220301-234836  OUTPUT] PreCheck successfully. RESPECT
[20220301-234836  OUTPUT] Create the deploy folder ...
[20220301-234838  OUTPUT] Download StarRocks package & jdk ...
[20220302-000515    INFO] The file starrocks-2.0.1-quickstart.tar.gz [1227406189] download successfully
[20220302-000515  OUTPUT] Download done.
[20220302-000515  OUTPUT] Decompress StarRocks pakcage & jdk ...
[20220302-000520    INFO] The tar file /home/sr-dev/.starrocks-controller/download/starrocks-2.0.1-quickstart.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220302-000547    INFO] The tar file /home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220302-000556    INFO] The tar file /home/sr-dev/.starrocks-controller/download/jdk-8u301-linux-x64.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220302-000556  OUTPUT] Distribute FE Dir ...
[20220302-000603    INFO] Upload dir feSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/fe] to feTargetDir = [StarRocks/fe] on FeHost = [192.168.xx.xx]
[20220302-000615    INFO] Upload dir JDKSourceDir = [/home/sr-dev/.starrocks-controller/download/jdk1.8.0_301] to JDKTargetDir = [StarRocks/fe/jdk] on FeHost = [192.168.xx.xx]
[20220302-000615    INFO] Modify JAVA_HOME: host = [192.168.xx.xx], filePath = [StarRocks/fe/bin/start_fe.sh]
[20220302-000622    INFO] Upload dir feSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/fe] to feTargetDir = [StarRocks/fe] on FeHost = [192.168.xx.xx]
[20220302-000634    INFO] Upload dir JDKSourceDir = [/home/sr-dev/.starrocks-controller/download/jdk1.8.0_301] to JDKTargetDir = [StarRocks/fe/jdk] on FeHost = [192.168.xx.xx]
[20220302-000634    INFO] Modify JAVA_HOME: host = [192.168.xx.xx], filePath = [StarRocks/fe/bin/start_fe.sh]
[20220302-000640    INFO] Upload dir feSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/fe] to feTargetDir = [StarRocks/fe] on FeHost = [192.168.xx.xx]
[20220302-000652    INFO] Upload dir JDKSourceDir = [/home/sr-dev/.starrocks-controller/download/jdk1.8.0_301] to JDKTargetDir = [StarRocks/fe/jdk] on FeHost = [192.168.xx.xx]
[20220302-000652    INFO] Modify JAVA_HOME: host = [192.168.xx.xx], filePath = [StarRocks/fe/bin/start_fe.sh]
[20220302-000652  OUTPUT] Distribute BE Dir ...
[20220302-000728    INFO] Upload dir BeSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/be] to BeTargetDir = [StarRocks/be] on BeHost = [192.168.xx.xx]
[20220302-000752    INFO] Upload dir BeSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/be] to BeTargetDir = [StarRocks/be] on BeHost = [192.168.xx.xx]
[20220302-000815    INFO] Upload dir BeSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/be] to BeTargetDir = [StarRocks/be] on BeHost = [192.168.xx.xx]
[20220302-000815  OUTPUT] Modify configuration for FE nodes & BE nodes ...
############################################# START FE CLUSTER #############################################
############################################# START FE CLUSTER #############################################
[20220302-000816    INFO] Starting leader FE node [host = 192.168.xx.xx, editLogPort = 9010]
[20220302-000836    INFO] The FE node start succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-000836    INFO] Starting follower FE node [host = 192.168.xx.xx, editLogPort = 9010]
[20220302-000857    INFO] The FE node start succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-000857    INFO] Starting follower FE node [host = 192.168.xx.xx, editLogPort = 9010]
[20220302-000918    INFO] The FE node start succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-000918    INFO] List all FE status:
                                        feHost = 192.168.xx.xx       feQueryPort = 9030     feStatus = true
                                        feHost = 192.168.xx.xx       feQueryPort = 9030     feStatus = true
                                        feHost = 192.168.xx.xx       feQueryPort = 9030     feStatus = true

############################################# START BE CLUSTER #############################################
############################################# START BE CLUSTER #############################################
[20220302-000918    INFO] Starting BE node [BeHost = 192.168.xx.xx HeartbeatServicePort = 9050]
[20220302-000939    INFO] The BE node start succefully [host = 192.168.xx.xx, heartbeatServicePort = 9050]
[20220302-000939    INFO] Starting BE node [BeHost = 192.168.xx.xx HeartbeatServicePort = 9050]
[20220302-001000    INFO] The BE node start succefully [host = 192.168.xx.xx, heartbeatServicePort = 9050]
[20220302-001000    INFO] Starting BE node [BeHost = 192.168.xx.xx HeartbeatServicePort = 9050]
[20220302-001020    INFO] The BE node start succefully [host = 192.168.xx.xx, heartbeatServicePort = 9050]
[20220302-001020  OUTPUT] List all BE status:
                                        beHost = 192.168.xx.xx       beHeartbeatServicePort = 9050      beStatus = true
                                        beHost = 192.168.xx.xx       beHeartbeatServicePort = 9050      beStatus = true
                                        beHost = 192.168.xx.xx       beHeartbeatServicePort = 9050      beStatus = true
```

您可以通过[查看指定集群信息](#查看指定集群信息) 查看各节点是否部署成功。

您也可以通过连接 MySQL 客户端测试集群是否部署成功。

```shell
mysql -h 127.0.0.1 -P9030 -uroot
```

## 查看集群信息

您可以通过 StarGo 查看其管理的集群信息。

### 查看所有集群信息

通过以下命令查看其管理的所有集群信息。

```shell
./stargo cluster list
```

示例：

```shell
[sr-dev@r0 ~]$ ./stargo cluster list
[20220302-001640  OUTPUT] List all clusters
ClusterName      User        CreateDate                 MetaPath                                                      PrivateKey
---------------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-c1            starrocks   2022-03-02 00:08:15        /home/sr-dev/.starrocks-controller/cluster/sr-c1              /home/sr-dev/.ssh/id_rsa
```

### 查看指定集群信息

通过以下命令查看指定集群的信息。

```shell
./stargo cluster display <cluster_name>
```

示例：

```plain text
[sr-dev@r0 ~]$ ./stargo cluster display sr-c1
[20220302-002310  OUTPUT] Display cluster [clusterName = sr-c1]
clusterName = sr-c1
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage
```

## 启动集群

您可以通过 StarGo 启动其管理的集群。

### 启动集群所有节点

通过以下命令启动特定集群所有节点。

```shell
./stargo cluster start <cluster-name>
```

示例：

```plain text
[root@nd1 sr-controller]# ./stargo cluster start sr-c1
[20220303-190404  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-190404    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-190435    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-190446    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-190457    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-190458    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-190458    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
```

### 启动集群中特定角色节点

- 通过以下命令启动特定集群中 FE 节点。

```shell
./stargo cluster start <cluster_name> --role FE
```

- 通过以下命令启动特定集群中 BE 节点。

```shell
./stargo cluster start <cluster_name> --role BE
```

示例：

```plain text
[root@nd1 sr-controller]# ./stargo cluster start sr-c1 --role FE
[20220303-191529  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-191529    INFO] Starting FE cluster ....
[20220303-191529    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-191600    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-191610    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]

[root@nd1 sr-controller]# ./stargo cluster start sr-c1 --role BE
[20220303-194215  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-194215    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-194216    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-194217    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-194217    INFO] Starting BE cluster ...
```

### 启动集群中特定节点

通过以下命令启动集群的某一个节点。目前只支持启动 BE 的节点。

```shell
./stargo cluster start <cluster_name> --node <node_ID>
```

您可以通过[查看指定集群信息](#查看指定集群信息)查看集群中特定节点的 ID。

示例：

```plain text
[root@nd1 sr-controller]# ./stargo cluster start sr-c1 --node 192.168.xx.xx:9060
[20220303-194714  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-194714    INFO] Start BE node. [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
```

## 停止集群

您可以通过 StarGo 停止其管理的集群。

### 停止集群所有节点

通过以下命令停止特定集群所有节点。

```shell
./stargo cluster stop <cluster_name>
```

示例：

```plain text
[sr-dev@nd1 sr-controller]$ ./stargo cluster stop sr-c1
[20220302-180140  OUTPUT] Stop cluster [clusterName = sr-c1]
[20220302-180140  OUTPUT] Stop cluster sr-c1
[20220302-180140    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180143  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-180143    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180145  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-180145    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180148  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-180148  OUTPUT] Stop cluster sr-c1
[20220302-180148    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180148    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220302-180148    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180149    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220302-180149    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180149    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
```

### 停止集群中特定角色节点

- 通过以下命令停止特定集群中 FE 节点。

```shell
./stargo cluster stop <cluster_name> --role FE
```

- 通过以下命令停止特定集群中 BE 节点。

```shell
./stargo cluster stop <cluster_name> --role BE
```

示例：

```plain text
[sr-dev@nd1 sr-controller]$ ./stargo cluster stop sr-c1 --role BE
[20220302-180624  OUTPUT] Stop cluster [clusterName = sr-c1]
[20220302-180624  OUTPUT] Stop cluster sr-c1
[20220302-180624    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180624    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220302-180624    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180625    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220302-180625    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180625    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220302-180625    INFO] Stopping BE cluster ...

###########################################################################

[sr-dev@nd1 sr-controller]$ ./stargo cluster stop sr-c1 --role FE
[20220302-180849  OUTPUT] Stop cluster [clusterName = sr-c1]
[20220302-180849    INFO] Stopping FE cluster ....
[20220302-180849  OUTPUT] Stop cluster sr-c1
[20220302-180849    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180851  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-180851    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180854  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-180854    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180856  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
```

### 停止集群中特定节点

通过以下命令停止集群的某一个节点。

```shell
./stargo cluster stop <cluster_name> --node <node_ID>
```

您可以通过[查看指定集群信息](#查看指定集群信息)查看集群中特定节点的 ID。

示例：

```plain text
[root@nd1 sr-controller]# ./stargo cluster display sr-c1
[20220303-185400  OUTPUT] Display cluster [clusterName = sr-c1]
clusterName = sr-c1
[20220303-185400    WARN] All FE nodes are down, please start FE node and display the cluster status again.
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        DOWN        StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        DOWN        StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        DOWN        StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        DOWN        StarRocks/be                                   /dataStarRocks/be/storage
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        DOWN        StarRocks/be                                   /dataStarRocks/be/storage
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        DOWN        StarRocks/be                                   /dataStarRocks/be/storage

[root@nd1 sr-controller]# ./stargo cluster stop sr-c1 --node 192.168.xx.xx:9060
[20220303-185510  OUTPUT] Stop cluster [clusterName = sr-c1]
[20220303-185510    INFO] Stopping BE node. [BeHost = 192.168.xx.xx]
[20220303-185510    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
```

## 扩容集群

您可以通过 StarGo 扩容其管理的集群。

### 创建配置文件

根据以下 YAML 模板，创建扩容 StarRocks 集群的拓扑文件。您可以根据需求配置相应的 EF 和/或 BE 节点。具体配置项参考[参数配置](../administration/management/FE_configuration.md)。

```yaml
# 扩容 FE 节点。
fe_servers:
  - host: 192.168.xx.xx # 扩容 FE 节点的 IP 地址。
    ssh_port: 22
    http_port: 8030
    rpc_port: 9020
    query_port: 9030
    edit_log_port: 9010
    deploy_dir: StarRocks/fe
    meta_dir: StarRocks/fe/meta
    log_dir: StarRocks/fe/log
    priority_networks: 192.168.xx.xx/24 # 当机器有多个 IP 时，在当前配置项中为当前节点指定唯一 IP。
    config:
      sys_log_level: "INFO"
      sys_log_delete_age: "1d"

# 扩容 BE 节点。
be_servers:
  - host: 192.168.xx.xx # 扩容 BE 节点的 IP 地址。
    ssh_port: 22
    be_port: 9060
    webserver_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    config:
      create_tablet_worker_count: 3
```

### 创建 SSH 互信

如果扩容节点是新节点，您需要为其配置 SSH 节点互信。详细详细操作参考[前提条件](#前提条件)。

### 创建部署目录（可选）

如果您在配置文件中设定的部署路径不存在，且您有创建该路径的权限，StarGo 将根据配置文件自动创建部署目录。如果路径已存在，请确保您有在该路径下拥有写入的权限。您也可以通过以下命令，在各部署节点分别创建部署路径。

- 在新增 FE 节点安装目录下上创建 **meta** 路径。

```shell
mkdir -p StarRocks/fe/meta
```

- 在新增 BE 节点安装目录下上创建 **storage** 路径。

```shell
mkdir -p StarRocks/be/storage
```

> 注意
>
> 请确保以上创建的路径与配置文件中的 `meta_dir` 和 `storage_dir` 相同。

### 扩容 StarRocks 集群

通过以下命令扩容集群。

```shell
./stargo cluster scale-out <cluster_name> <topology_file>
```

示例：

```plain text
# 当前集群状态。
[root@nd1 sr-controller]# ./stargo cluster display sr-test       
[20220503-210047  OUTPUT] Display cluster [clusterName = sr-test]
clusterName = sr-test
clusterVerison = v2.0.1
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR                                         
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          /opt/starrocks-test/fe                              /opt/starrocks-test/fe/meta                       
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          /opt/starrocks-test/be                              /opt/starrocks-test/be/storage                    

# 扩容集群。
[sr-dev@nd1 sr-controller]$ ./stargo cluster scale-out sr-test sr-out.yaml
[20220503-213725  OUTPUT] Scale out cluster. [ClusterName = sr-test]
[20220503-213731  OUTPUT] PRE CHECK DEPLOY ENV:
PreCheck FE:
IP                    ssh auth         meta dir                        deploy dir                      http port        rpc port         query port       edit log port  
--------------------  ---------------  ------------------------------  ------------------------------  ---------------  ---------------  ---------------  ---------------
192.168.xx.xx         PASS             PASS                            PASS                            PASS             PASS             PASS             PASS           

PreCheck BE:
IP                    ssh auth         storage dir                     deploy dir                      webSer port      heartbeat port   brpc port        be port        
--------------------  ---------------  ------------------------------  ------------------------------  ---------------  ---------------  ---------------  ---------------
192.168.xx.xx         PASS             PASS                            PASS                            PASS             PASS             PASS             PASS           


[20220503-213731  OUTPUT] PreCheck successfully. RESPECT
[20220503-213731  OUTPUT] Create the deploy folder ...
[20220503-213732  OUTPUT] Download StarRocks package & jdk ...
[20220503-213732    INFO] The package has already exist [fileName = starrocks-2.0.1-quickstart.tar.gz, fileSize = 1227406189, fileModTime = 2022-05-03 17:32:03.478661923 +0800 CST]
[20220503-213732  OUTPUT] Download done.
[20220503-213732  OUTPUT] Decompress StarRocks pakcage & jdk ...
[20220503-213741    INFO] The tar file /home/sr-dev/.starrocks-controller/download/starrocks-2.0.1-quickstart.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220503-213837    INFO] The tar file /home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220503-213837    INFO] The tar file /home/sr-dev/.starrocks-controller/download/jdk-8u301-linux-x64.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220503-213837  OUTPUT] Distribute FE Dir ...
[20220503-213845    INFO] Upload dir feSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/fe] to feTargetDir = [StarRocks/fe] on FeHost = [192.168.xx.xx]
[20220503-213857    INFO] Upload dir JDKSourceDir = [/home/sr-dev/.starrocks-controller/download/jdk1.8.0_301] to JDKTargetDir = [StarRocks/fe/jdk] on FeHost = [192.168.xx.xx]
[20220503-213857    INFO] Modify JAVA_HOME: host = [192.168.xx.xx], filePath = [StarRocks/fe/bin/start_fe.sh]
[20220503-213857  OUTPUT] Distribute BE Dir ...
[20220503-213924    INFO] Upload dir BeSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/be] to BeTargetDir = [StarRocks/be] on BeHost = [192.168.xx.xx]
[20220503-213924  OUTPUT] Modify configuration for FE nodes & BE nodes ...
############################################# SCALE OUT FE CLUSTER #############################################
############################################# SCALE OUT FE CLUSTER #############################################
[20220503-213925    INFO] Starting follower FE node [host = 192.168.xx.xx, editLogPort = 9010]
[20220503-213945    INFO] The FE node start succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220503-213945    INFO] List all FE status:
                                        feHost = 192.168.xx.xx       feQueryPort = 9030     feStatus = true

############################################# START BE CLUSTER #############################################
############################################# START BE CLUSTER #############################################
[20220503-213945    INFO] Starting BE node [BeHost = 192.168.xx.xx HeartbeatServicePort = 9050]
[20220503-214016    INFO] The BE node start succefully [host = 192.168.xx.xx, heartbeatServicePort = 9050]
[20220503-214016  OUTPUT] List all BE status:
                                        beHost = 192.168.xx.xx       beHeartbeatServicePort = 9050      beStatus = true

# 扩容后集群状态。
[sr-dev@nd1 sr-controller]$ ./stargo cluster display sr-test 
[20220503-214302  OUTPUT] Display cluster [clusterName = sr-test]
clusterName = sr-test
clusterVerison = v2.0.1
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR                                         
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          /opt/starrocks-test/fe                              /opt/starrocks-test/fe/meta                       
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   StarRocks/fe/meta                            
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          /opt/starrocks-test/be                              /opt/starrocks-test/be/storage                    
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   StarRocks/be/storage                         
```

## 缩容集群

通过以下命令缩容集群中特定节点。

```shell
./stargo cluster scale-in <cluster_name> --node <node_id>
```

您可以通过[查看指定集群信息](#查看指定集群信息)查看集群中特定节点的 ID。

示例：

```plain text
[sr-dev@nd1 sr-controller]$ ./stargo cluster display sr-c1
[20220505-145649  OUTPUT] Display cluster [clusterName = sr-c1]
clusterName = sr-c1
clusterVerison = v2.0.1
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR                                         
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta                           
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta                           
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta                           
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage                        
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage                        
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage                        
[sr-dev@nd1 sr-controller]$ ./stargo cluster scale-in sr-c1 --node 192.168.88.83:9010
[20220621-010553  OUTPUT] Scale in cluster [clusterName = sr-c1, nodeId = 192.168.88.83:9010]
[20220621-010553    INFO] Waiting for stoping FE node [FeHost = 192.168.88.83]
[20220621-010606  OUTPUT] Scale in FE node successfully. [clusterName = sr-c1, nodeId = 192.168.88.83:9010]

[sr-dev@nd1 sr-controller]$ ./stargo cluster display sr-c1
[20220621-010623  OUTPUT] Display cluster [clusterName = sr-c1]
clusterName = sr-c1
clusterVerison = 
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR                                         
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.88.84:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta                           
192.168.88.85:9010          FE      192.168.xx.xx         9010/9030        UP/L        StarRocks/fe                                   /dataStarRocks/fe/meta                           
192.168.88.83:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage                        
192.168.88.84:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage                        
192.168.88.85:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage              
```

## 升降级集群

您可以通过 StarGo 升级或降级其管理的集群。

- 通过以下命令升级集群。

```shell
./stargo cluster upgrade <cluster_name>  <target_version>
```

- 通过以下命令降级集群。

```shell
./stargo cluster downgrade <cluster_name>  <target_version>
```

示例：

```plain text
[sr-dev@nd1 sr-controller]$ ./stargo cluster list
[20220515-195827  OUTPUT] List all clusters
ClusterName      Version     User        CreateDate                 MetaPath                                                      PrivateKey                                        
---------------  ----------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-test2         v2.0.1      test222     2022-05-15 19:35:36        /home/sr-dev/.starrocks-controller/cluster/sr-test2           /home/sr-dev/.ssh/id_rsa                          
[sr-dev@nd1 sr-controller]$ ./stargo cluster upgrade sr-test2 v2.1.3
[20220515-200358  OUTPUT] List all clusters
ClusterName      Version     User        CreateDate                 MetaPath                                                      PrivateKey                                        
---------------  ----------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-test2         v2.1.3      test222     2022-05-15 20:03:01        /home/sr-dev/.starrocks-controller/cluster/sr-test2           /home/sr-dev/.ssh/id_rsa                          

[sr-dev@nd1 sr-controller]$ ./stargo cluster downgrade sr-test2 v2.0.1 
[sr-dev@nd1 sr-controller]$ ./stargo cluster list
[20220515-200915  OUTPUT] List all clusters
ClusterName      Version     User        CreateDate                 MetaPath                                                      PrivateKey                                        
---------------  ----------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-test2         v2.0.1      test222     2022-05-15 20:08:40        /home/sr-dev/.starrocks-controller/cluster/sr-test2           /home/sr-dev/.ssh/id_rsa                
```

## 相关命令

|命令|描述|
|----|----|
|deploy|部署集群。|
|start|启动集群。|
|stop|停止集群。|
|scale-in|缩容集群。|
|scale-out|扩容集群。|
|upgrade|升级集群。|
|downgrade|降级集群。|
|display|查看特定集群。|
|list|查看所有集群。|
