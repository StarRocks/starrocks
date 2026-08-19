---
displayed_sidebar: docs
description: "StarGo 是一个用于管理多个 StarRocks 集群的命令行工具。"
---

# 使用 StarGo 部署和管理 StarRocks {#deploy-and-manage-starrocks-with-stargo}

使用 StarGo 部署和管理 StarRocks 集群。

StarGo 是一款用于管理多个 StarRocks 集群的命令行工具。您可以通过 StarGo 轻松地对多个集群进行部署、检查、升级、降级、启动和停止等操作。

## 安装 StarGo {#install-stargo}

下载以下文件到您的中控节点：

- **sr-ctl**：StarGo 的二进制文件。下载后无需安装。
- **sr-c1.yaml**：部署配置文件的模板。
- **repo.yaml**：StarRocks 安装程序下载路径的配置文件。

> 注意
您可以访问 `http://cdn-thirdparty.starrocks.com` 获取相应的安装索引文件和安装程序。

```shell
wget https://github.com/wangtianyi2004/starrocks-controller/raw/main/stargo-pkg.tar.gz
wget https://github.com/wangtianyi2004/starrocks-controller/blob/main/sr-c1.yaml
wget https://github.com/wangtianyi2004/starrocks-controller/blob/main/repo.yaml
```

赋予 **sr-ctl** 访问权限。

```shell
chmod 751 sr-ctl
```

## 部署 StarRocks 集群 {#deploy-starrocks-cluster}

您可以使用 StarGo 部署 StarRocks 集群。

### 前提条件 {#prerequisites}

- 待部署的集群必须至少包含一个中控节点和三个部署节点。所有节点均可部署在同一台机器上。
- 您需要在中控节点上部署 StarGo。
- 您需要在中控节点和三个部署节点之间建立相互 SSH 认证。

以下示例在中控节点 sr-dev@r0 和三个部署节点 starrocks@r1、starrocks@r2、starrocks@r3 之间建立相互认证。

```plain text
## Build the mutual authentication between sr-dev@r0 and starrocks@r1, 2, 3.
[sr-dev@r0 ~]$ ssh-keygen
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r1
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r2
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r3

## Verify the mutual authentication between sr-dev@r0 and starrocks@r1, 2, 3.
[sr-dev@r0 ~]$ ssh starrocks@r1 date
[sr-dev@r0 ~]$ ssh starrocks@r2 date
[sr-dev@r0 ~]$ ssh starrocks@r3 date
```

### 创建配置文件 {#create-configuration-file}

根据以下 YAML 模板创建 StarRocks 部署拓扑文件。详细信息请参见 [配置](./configuration/FE_parameters/FE_parameters.md)。

```yaml
global:
    user: "starrocks"   ## The current OS user.
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
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
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
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
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
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      sys_log_level: "INFO"
be_servers:
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    be_http_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      create_tablet_worker_count: 3
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    be_http_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      create_tablet_worker_count: 3
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    be_http_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      create_tablet_worker_count: 3
```

### 创建部署目录（可选） {#create-deployment-directory-optional}

如果待部署的 StarRocks 所在路径不存在，且您有权限创建这些路径，则无需手动创建这些路径，StarGo 会根据配置文件自动为您创建。如果这些路径已存在，请确保您具有对其的写权限。您也可以通过运行以下命令在每个节点上创建所需的部署目录。

- 在 FE 节点上创建 **meta** 目录。

```shell
mkdir -p StarRocks/fe/meta
```

- 在 BE 节点上创建 **storage** 目录。

```shell
mkdir -p StarRocks/be/storage
```

> 注意
请确保上述路径与配置文件中的配置项 `meta_dir` 和 `storage_dir` 保持一致。

### 部署 StarRocks {#deploy-starrocks}

通过运行以下命令部署 StarRocks 集群。

```shell
./sr-ctl cluster deploy <cluster_name> <version> <topology_file>
```

|参数|描述|
|----|----|
|cluster_name|待部署集群的名称。|
|version|StarRocks 版本。|
|topology_file|配置文件的名称。|

如果部署成功，集群将自动启动。当 beStatus 和 feStatus 均为 true 时，表示集群启动成功。

示例：

```plain text
[sr-dev@r0 ~]$ ./sr-ctl cluster deploy sr-c1 v2.0.1 sr-c1.yaml
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

您可以通过 [查看集群信息](#view-cluster-information)来测试集群。

您也可以通过使用 MySQL 客户端连接集群来测试它。

```shell
mysql -h 127.0.0.1 -P9030 -uroot
```

## 查看集群信息 {#view-cluster-information}

您可以查看 StarGo 所管理集群的信息。

### 查看所有集群的信息 {#view-the-information-of-all-clusters}

通过运行以下命令查看所有集群的信息。

```shell
./sr-ctl cluster list
```

示例：

```shell
[sr-dev@r0 ~]$ ./sr-ctl cluster list
[20220302-001640  OUTPUT] List all clusters
ClusterName      User        CreateDate                 MetaPath                                                      PrivateKey
---------------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-c1            starrocks   2022-03-02 00:08:15        /home/sr-dev/.starrocks-controller/cluster/sr-c1              /home/sr-dev/.ssh/id_rsa
```

### 查看特定集群的信息 {#view-the-information-of-a-specific-cluster}

通过运行以下命令查看特定集群的信息。

```shell
./sr-ctl cluster display <cluster_name>
```

示例：

```plain text
[sr-dev@r0 ~]$ ./sr-ctl cluster display sr-c1
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

## 启动集群 {#start-cluster}

您可以通过 StarGo 启动 StarRocks 集群。

### 启动集群中的所有节点 {#start-all-nodes-in-a-cluster}

通过运行以下命令启动集群中的所有节点。

```shell
./sr-ctl cluster start <cluster-name>
```

示例:

```plain text
[root@nd1 sr-controller]# ./sr-ctl cluster start sr-c1
[20220303-190404  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-190404    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-190435    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-190446    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-190457    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-190458    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-190458    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
```

### 启动特定角色的节点 {#start-nodes-of-a-specific-role}

- 启动集群中的所有 FE 节点。

```shell
./sr-ctl cluster start <cluster_name> --role FE
```

- 启动集群中的所有 BE 节点。

```shell
./sr-ctl cluster start <cluster_name> --role BE
```

示例:

```plain text
[root@nd1 sr-controller]# ./sr-ctl cluster start sr-c1 --role FE
[20220303-191529  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-191529    INFO] Starting FE cluster ....
[20220303-191529    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-191600    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-191610    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]

[root@nd1 sr-controller]# ./sr-ctl cluster start sr-c1 --role BE
[20220303-194215  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-194215    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-194216    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-194217    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-194217    INFO] Starting BE cluster ...
```

### 启动特定节点 {#start-a-specific-node}

启动集群中的特定节点。目前仅支持 BE 节点。

```shell
./sr-ctl cluster start <cluster_name> --node <node_ID>
```

您可以通过[查看特定集群的信息](#view-the-information-of-a-specific-cluster)来检查特定节点的 ID。

示例:

```plain text
[root@nd1 sr-controller]# ./sr-ctl cluster start sr-c1 --node 192.168.xx.xx:9060
[20220303-194714  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-194714    INFO] Start BE node. [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
```

## 停止集群 {#stop-cluster}

您可以通过 StarGo 停止 StarRocks 集群。

### 停止集群中的所有节点 {#stop-all-nodes-in-a-cluster}

通过运行以下命令停止集群中的所有节点。

```shell
./sr-ctl cluster stop <cluster_name>
```

示例:

```plain text
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster stop sr-c1
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

### 停止特定角色的节点 {#stop-nodes-of-a-specific-role}

- 停止集群中的所有 FE 节点。

```shell
./sr-ctl cluster stop <cluster_name> --role FE
```

- 停止集群中的所有 BE 节点。

```shell
./sr-ctl cluster stop <cluster_name> --role BE
```

示例:

```plain text
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster stop sr-c1 --role BE
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

[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster stop sr-c1 --role FE
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

### 停止特定节点 {#stop-a-specific-node}

停止集群中的特定节点。

```shell
./sr-ctl cluster stop <cluster_name> --node <node_ID>
```

您可以通过[查看特定集群的信息](#view-the-information-of-a-specific-cluster)来检查特定节点的 ID。

示例:

```plain text
[root@nd1 sr-controller]# ./sr-ctl cluster display sr-c1
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

[root@nd1 sr-controller]# ./sr-ctl cluster stop sr-c1 --node 192.168.xx.xx:9060
[20220303-185510  OUTPUT] Stop cluster [clusterName = sr-c1]
[20220303-185510    INFO] Stopping BE node. [BeHost = 192.168.xx.xx]
[20220303-185510    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
```

## 集群扩容 {#scale-cluster-out}

您可以通过 StarGo 对集群进行扩容。

### 创建配置文件 {#create-configuration-file-1}

根据以下模板创建扩容任务的拓扑文件。您可以根据需求指定文件以添加 FE 和/或 BE 节点。详细信息请参见[配置](./configuration/FE_parameters/FE_parameters.md)。

```yaml
# 添加一个 FE 节点。
fe_servers:
  - host: 192.168.xx.xx # The IP address of the new FE node.
    ssh_port: 22
    http_port: 8030
    rpc_port: 9020
    query_port: 9030
    edit_log_port: 9010
    deploy_dir: StarRocks/fe
    meta_dir: StarRocks/fe/meta
    log_dir: StarRocks/fe/log
    priority_networks: 192.168.xx.xx/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      sys_log_level: "INFO"
      sys_log_delete_age: "1d"

# 添加一个 BE 节点。
be_servers:
  - host: 192.168.xx.xx # The IP address of the new BE node.
    ssh_port: 22
    be_port: 9060
    be_http_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    config:
      create_tablet_worker_count: 3
```

### 建立 SSH 互信 {#build-ssh-mutual-authentication}

如果您要向集群添加新节点，则必须在新节点与中控节点之间建立互信。详细说明请参见[前提条件](#prerequisites)。

### 创建部署目录（可选） {#create-deployment-directory-optional-1}

如果要部署的新节点所在路径不存在，且您拥有创建该路径的权限，则您无需自行创建这些路径，StarGo 将根据配置文件为您创建。如果这些路径已存在，请确保您对其拥有写入权限。您也可以通过运行以下命令在每个节点上创建必要的部署目录。

- 在 FE 节点上创建 **meta** 目录。

```shell
mkdir -p StarRocks/fe/meta
```

- 在 BE 节点上创建 **storage** 目录。

```shell
mkdir -p StarRocks/be/storage
```

> 注意
请确保上述路径与配置文件中的配置项 `meta_dir` 和 `storage_dir` 保持一致。

### 对集群进行扩容 {#scale-the-cluster-out}

通过运行以下命令对集群进行扩容。

```shell
./sr-ctl cluster scale-out <cluster_name> <topology_file>
```

示例:

```plain text
# Status of the cluster before scale-out.
[root@nd1 sr-controller]# ./sr-ctl cluster display sr-test       
[20220503-210047  OUTPUT] Display cluster [clusterName = sr-test]
clusterName = sr-test
clusterVerison = v2.0.1
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR                                         
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          /opt/starrocks-test/fe                              /opt/starrocks-test/fe/meta                       
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          /opt/starrocks-test/be                              /opt/starrocks-test/be/storage                    

# Scale the cluster out.
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster scale-out sr-test sr-out.yaml
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
[20220503-213732  OUTPUT] Decompress StarRocks pakage & jdk ...
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

# Status of the cluster after scale-out.
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster display sr-test 
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

## 集群缩容 {#scale-cluster-in}

通过运行以下命令移除集群中的一个节点。

```shell
./sr-ctl cluster scale-in <cluster_name> --node <node_id>
```

您可以通过[查看特定集群的信息](#view-the-information-of-a-specific-cluster)来检查特定节点的 ID。

示例：

```plain text
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster display sr-c1
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
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster scale-in sr-c1 --node 192.168.88.83:9010
[20220621-010553  OUTPUT] Scale in cluster [clusterName = sr-c1, nodeId = 192.168.88.83:9010]
[20220621-010553    INFO] Waiting for stoping FE node [FeHost = 192.168.88.83]
[20220621-010606  OUTPUT] Scale in FE node successfully. [clusterName = sr-c1, nodeId = 192.168.88.83:9010]

[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster display sr-c1
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

## 升级或降级集群 {#upgrade-or-downgrade-the-cluster}

您可以通过 StarGo 升级或降级集群。

- 升级集群。

```shell
./sr-ctl cluster upgrade <cluster_name>  <target_version>
```

- 降级集群。

```shell
./sr-ctl cluster downgrade <cluster_name>  <target_version>
```

示例：

```plain text
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster list
[20220515-195827  OUTPUT] List all clusters
ClusterName      Version     User        CreateDate                 MetaPath                                                      PrivateKey                                        
---------------  ----------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-test2         v2.0.1      test222     2022-05-15 19:35:36        /home/sr-dev/.starrocks-controller/cluster/sr-test2           /home/sr-dev/.ssh/id_rsa                          
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster upgrade sr-test2 v2.1.3
[20220515-200358  OUTPUT] List all clusters
ClusterName      Version     User        CreateDate                 MetaPath                                                      PrivateKey                                        
---------------  ----------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-test2         v2.1.3      test222     2022-05-15 20:03:01        /home/sr-dev/.starrocks-controller/cluster/sr-test2           /home/sr-dev/.ssh/id_rsa                          

[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster downgrade sr-test2 v2.0.1 
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster list
[20220515-200915  OUTPUT] List all clusters
ClusterName      Version     User        CreateDate                 MetaPath                                                      PrivateKey                                        
---------------  ----------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-test2         v2.0.1      test222     2022-05-15 20:08:40        /home/sr-dev/.starrocks-controller/cluster/sr-test2           /home/sr-dev/.ssh/id_rsa                
```

## 相关命令 {#relevant-commands}

|命令|说明|
|----|----|
|deploy|部署集群。|
|start|启动集群。|
|stop|停止集群。|
|scale-in|缩容集群。|
|scale-out|扩容集群。|
|upgrade|升级集群。|
|downgrade|降级集群|
|display|查看特定集群的信息。|
|list|查看所有集群。|
