---
sidebar_position: 20
displayed_sidebar: docs
description: "如何手动部署存算一体 StarRocks 集群，其中 BE 节点同时处理存储和计算。"
products: [oss]
---

import ManualPrep from '../_assets/deployment/manual_prep.mdx'

# 手动部署存算一体 StarRocks

<ManualPrep />

本主题介绍如何手动部署存算一体 StarRocks 集群（其中 BE 同时负责存储和计算）。有关其他安装模式，参见 [部署概览](./deployment.md)。

要部署存算分离 StarRocks 集群（存储和计算分离），请参见 [手动部署存算分离 StarRocks](./deploy_shared_data_manually.md)。

## 步骤一：启动 Leader FE 节点 {/* #step-1-start-the-leader-fe-node */}

以下操作在 FE 实例上执行。

1. 创建一个专用目录用于存储元数据。我们建议将元数据存储在与 FE 部署文件不同的独立目录中。请确保该目录存在，并且您拥有对其的写权限。

   ```YAML
   # 将 <meta_dir> 替换为您要创建的元数据目录。
   mkdir -p <meta_dir>
   ```

2. 进入之前准备的 [StarRocks FE 部署文件](./preparation/prepare_deployment_files.md) 所在目录，并修改 FE 配置文件 **fe/conf/fe.conf**。

   a. 在配置项 `meta_dir` 中指定元数据目录。

   ```YAML
   # 将 <meta_dir> 替换为您已创建的元数据目录。
   meta_dir = <meta_dir>
   ```

   b. 如果 [环境配置检查清单](./preparation/environment_configurations.md#fe-ports) 中提到的任何 FE 端口被占用，您必须在 FE 配置文件中为其分配有效的替代端口。

   ```YAML
   http_port = aaaa        # Default: 8030
   rpc_port = bbbb         # Default: 9020
   query_port = cccc       # Default: 9030
   edit_log_port = dddd    # Default: 9010
   ```

   > **注意**
   >
   > 如果您想在集群中部署多个 FE 节点，必须为每个 FE 节点分配相同的 `http_port`。

   c. 如果您想为集群启用 IP 地址访问，必须在配置文件中添加配置项 `priority_networks`，并为该 FE 节点分配一个专用 IP 地址（CIDR 格式）。如果您想为集群启用 [FQDN 访问](../administration/management/enable_fqdn.md)，可以忽略此配置项。

   ```YAML
   priority_networks = x.x.x.x/x
   ```

   > **说明**
   >
   > - 您可以在终端中运行 `ifconfig` 来查看该实例所拥有的 IP 地址。
   > - 从 v3.3.0 起，StarRocks 支持基于 IPv6 的部署。

   d. 如果该实例上安装了多个 JDK，并且您想使用与环境变量 `JAVA_HOME` 中指定的不同的特定 JDK，您必须通过在配置文件中添加配置项 `JAVA_HOME` 来指定所选 JDK 的安装路径。

   ```YAML
   # 将 <path_to_JDK> 替换为所选 JDK 的安装路径。
   JAVA_HOME = <path_to_JDK>
   ```

   有关高级配置项的信息，请参见 [参数配置 - FE 配置项](../administration/configuration/FE_parameters/FE_parameters.md)。

3. 启动 FE 节点。

   - 要为集群启用 IP 地址访问，请运行以下命令启动 FE 节点：

     ```Bash
     ./fe/bin/start_fe.sh --daemon
     ```

   - 要为集群启用 FQDN 访问，请运行以下命令启动 FE 节点：

     ```Bash
     ./fe/bin/start_fe.sh --host_type FQDN --daemon
     ```

     请注意，首次启动节点时，您只需指定参数 `--host_type` 一次。

     > **注意**
     >
     > 在启动启用了 FQDN 访问的 FE 节点之前，请确保您已在 **/etc/hosts** 中为 all 实例分配了主机名。更多信息，请参见 [环境配置检查清单 - 主机名](./preparation/environment_configurations.md#hostnames)。

4. 检查 FE 日志以验证 FE 节点是否启动成功。

   ```Bash
   cat fe/log/fe.log | grep thrift
   ```

   类似于“2022-08-10 16:12:29,911 INFO (UNKNOWN x.x.x.x_9010_1660119137253(-1)|1) [FeServer.start():52] thrift server started with port 9020.” 这样的日志记录表明 FE 节点已正常启动。

## 步骤二：启动 BE 服务 {/* #step-2-start-the-be-service */}

:::note
您只能向存算一体集群添加 BE 节点，向存算分离集群添加 CN 节点。否则，可能会导致未知行为。
:::

以下操作在 BE 实例上执行。

1. 创建一个专用目录用于存储数据。我们建议将数据存储在与 BE 部署目录不同的独立目录中。请确保该目录存在，并且您拥有对其的写权限。

   ```YAML
   # 将 <storage_root_path> 替换为您要创建的数据存储目录。
   mkdir -p <storage_root_path>
   ```

2. 进入之前准备的 [StarRocks BE 部署文件](./preparation/prepare_deployment_files.md) 所在目录，并修改 BE 配置文件 **be/conf/be.conf**。

   a. 在配置项 `storage_root_path` 中指定数据目录。多个卷之间用分号（;）分隔。例如：`/data1;/data2`。

   ```YAML
   # 将 <storage_root_path> 替换为您已创建的数据目录。
   storage_root_path = <storage_root_path>
   ```

   b. 如果 [环境配置检查清单](./preparation/environment_configurations.md#be-ports) 中提到的任何 BE 端口被占用，您必须在 BE 配置文件中为其分配有效的替代端口。

   ```YAML
   be_port = vvvv                   # Default: 9060
   be_http_port = xxxx              # Default: 8040
   heartbeat_service_port = yyyy    # Default: 9050
   brpc_port = zzzz                 # Default: 8060
   starlet_port = uuuu              # Default: 9070
   ```

   c. 如果您想为集群启用 IP 地址访问，必须在配置文件中添加配置项 `priority_networks`，并为该 BE 节点分配一个专用 IP 地址（CIDR 格式）。如果您想为集群启用 FQDN 访问，可以忽略此配置项。

   ```YAML
   priority_networks = x.x.x.x/x
   ```

   > **说明**
   >
   > - 您可以在终端中运行 `ifconfig` 来查看该实例所拥有的 IP 地址。
   > - 从 v3.3.0 起，StarRocks 支持基于 IPv6 的部署。

   d. 如果该实例上安装了多个 JDK，并且您想使用与环境变量 `JAVA_HOME` 中指定的不同的特定 JDK，您必须通过在配置文件中添加配置项 `JAVA_HOME` 来指定所选 JDK 的安装路径。

   ```YAML
   # 将 <path_to_JDK> 替换为所选 JDK 的安装路径。
   JAVA_HOME = <path_to_JDK>
   ```

   有关高级配置项的信息，请参见 [参数配置 - BE 配置项](../administration/configuration/BE_parameters/BE_parameters.md)。

3. 启动 BE 节点。

   ```Bash
   ./be/bin/start_be.sh --daemon
   ```

   > **注意**
   >
   > - 在启用 FQDN 访问方式启动 BE 节点之前，请确保您已在 **/etc/hosts** 中为 all 实例分配了主机名。更多信息，请参见 [环境配置核对表 - 主机名](./preparation/environment_configurations.md#hostnames)。
   > - 启动 BE 节点时，您不需要指定参数 `--host_type`。

4. 检查 BE 日志以确认 BE 节点是否启动成功。

   ```Bash
   cat be/log/be.INFO | grep heartbeat
   ```

   类似 “I0810 16:18:44.487284 3310141 task_worker_pool.cpp:1387] Waiting to receive first heartbeat from frontend” 的日志记录表明 BE 节点已正常启动。

5. 您可以在其他 BE 实例上重复上述步骤来启动新的 BE 节点。

> **说明**
>
> - 当至少部署三个 BE 节点并将其添加到 StarRocks 集群后，将自动组成高可用的 BE 集群。
> - 如果您只想部署一个 BE 节点，则必须在 FE 配置文件 **fe/conf/fe.conf** 中将 `default_replication_num` 设置为 `1`。
>
>   ```YAML
>   default_replication_num = 1
>   ```

## 步骤 3：搭建集群 {/* #step-3-set-up-the-cluster */}

在 all FE 和 BE 节点都正常启动后，您就可以搭建 StarRocks 集群了。

以下步骤在 MySQL 客户端上执行。您必须安装 5.5.0 或更高版本的 MySQL 客户端。

1. 通过您的 MySQL 客户端连接到 StarRocks。您需要使用初始账户 `root` 登录，默认密码为空。

   ```Bash
   # 将 <fe_address> 替换为该 Leader FE 节点的 IP 地址（priority_networks）
   # 或 FQDN，并将 <query_port>（默认值：9030）替换为
   # 您在 fe.conf 中指定的 query_port。
   mysql -h <fe_address> -P<query_port> -uroot
   ```

2. 执行以下 SQL 来检查 Leader FE 节点的状态。

   ```SQL
   SHOW PROC '/frontends'\G
   ```

   示例：

   ```Plain
   MySQL [(none)]> SHOW PROC '/frontends'\G
   *************************** 1. row ***************************
                Name: x.x.x.x_9010_1686810741121
                  IP: x.x.x.x
         EditLogPort: 9010
            HttpPort: 8030
           QueryPort: 9030
             RpcPort: 9020
                Role: LEADER
           ClusterId: 919351034
                Join: true
               Alive: true
   ReplayedJournalId: 1220
       LastHeartbeat: 2023-06-15 15:39:04
            IsHelper: true
              ErrMsg: 
           StartTime: 2023-06-15 14:32:28
             Version: 3.0.0-48f4d81
   1 row in set (0.01 sec)
   ```

   - 如果字段 `Alive` 为 `true`，表示该 FE 节点已正常启动并添加到集群中。
   - 如果字段 `Role` 为 `FOLLOWER`，表示该 FE 节点有资格被选举为 Leader FE 节点。
   - 如果字段 `Role` 为 `LEADER`，表示该 FE 节点是 Leader FE 节点。

3. 将 BE 节点添加到集群中。

   ```SQL
   -- 将 <be_address> 替换为 BE 节点的 IP 地址（priority_networks）
   -- 或 FQDN，并将 <heartbeat_service_port>
   -- 替换为您在 be.conf 中指定的 heartbeat_service_port（默认值：9050）。
   ALTER SYSTEM ADD BACKEND "<be_address>:<heartbeat_service_port>";
   ```

   > **说明**
   >
   > 您可以使用上述命令一次添加多个 BE 节点。每个 `<be_address>:<heartbeat_service_port>` 对表示一个 BE 节点。

4. 执行以下 SQL 来检查 BE 节点的状态。

   ```SQL
   SHOW PROC '/backends'\G
   ```

   示例：

   ```Plain
   MySQL [(none)]> SHOW PROC '/backends'\G
   *************************** 1. row ***************************
               BackendId: 10007
                      IP: 172.26.195.67
           HeartbeatPort: 9050
                  BePort: 9060
                HttpPort: 8040
                BrpcPort: 8060
           LastStartTime: 2023-06-15 15:23:08
           LastHeartbeat: 2023-06-15 15:57:30
                   Alive: true
    SystemDecommissioned: false
   ClusterDecommissioned: false
               TabletNum: 30
        DataUsedCapacity: 0.000 
           AvailCapacity: 341.965 GB
           TotalCapacity: 1.968 TB
                 UsedPct: 83.04 %
          MaxDiskUsedPct: 83.04 %
                  ErrMsg: 
                 Version: 3.0.0-48f4d81
                  Status: {"lastSuccessReportTabletsTime":"2023-06-15 15:57:08"}
       DataTotalCapacity: 341.965 GB
             DataUsedPct: 0.00 %
                CpuCores: 16
       NumRunningQueries: 0
              MemUsedPct: 0.01 %
              CpuUsedPct: 0.0 %
   ```

   如果字段 `Alive` 为 `true`，表示该 BE 节点已正常启动并添加到集群中。

## 步骤 4：（可选）部署高可用 FE 集群 {/* #step-4-optional-deploy-a-high-availability-fe-cluster */}

高可用 FE 集群要求 StarRocks 集群中至少有三个 Follower FE 节点。在 Leader FE 节点启动成功后，您可以启动两个新的 FE 节点来部署高可用 FE 集群。

1. 通过您的 MySQL 客户端连接到 StarRocks。您需要使用初始账户 `root` 登录，默认密码为空。

   ```Bash
   # 将 <fe_address> 替换为该 Leader FE 节点的 IP 地址（priority_networks）
   # 或 FQDN，并将 <query_port>（默认值：9030）替换为
   # 您在 fe.conf 中指定的 query_port。
   mysql -h <fe_address> -P<query_port> -uroot
   ```

2. 执行以下 SQL 将新的 Follower FE 节点添加到集群中。

   ```SQL
   -- 将 <fe_address> 替换为 IP 地址（priority_networks）
   -- 或新 Follower FE 节点的 FQDN，并将 <edit_log_port> 替换为
   -- 您在 fe.conf 中指定的 edit_log_port（默认值：9010）。
   ALTER SYSTEM ADD FOLLOWER "<fe2_address>:<edit_log_port>";
   ```

   > **说明**
   >
   > - 您可以使用上述命令每次添加一个 Follower FE 节点。
   > - 如果您想添加 Observer FE 节点，请执行 `ALTER SYSTEM ADD OBSERVER "<fe_address>:<edit_log_port>"`。有关详细说明，请参见 [ALTER SYSTEM - FE](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md)。

3. 在新的 FE 实例上启动终端，创建一个专用于元数据存储的目录，导航到存储 StarRocks FE 部署文件的目录，并修改 FE 配置文件 **fe/conf/fe.conf**。更多说明，请参见 [步骤 1：启动 Leader FE 节点](#step-1-start-the-leader-fe-node)。基本上，您可以重复步骤 1 中的操作，**但用于启动 FE 节点的命令除外**。

   配置好 Follower FE 节点后，执行以下 SQL 为该 Follower FE 节点指定一个 helper 节点并启动该 Follower FE 节点。

   > **说明**
   >
   > 将新的 Follower FE 节点添加到集群时，必须为其指定一个 helper 节点（本质上是一个已存在的 Follower FE 节点），以同步元数据。

   - 要以 IP 地址访问方式启动新的 FE 节点，请运行以下命令启动 FE 节点：

     ```Bash
     # 将 <helper_fe_ip> 替换为 Leader FE 节点的 IP 地址（priority_networks），
     # 并将 <helper_edit_log_port>（默认值：9010）替换为
     # Leader FE 节点的 edit_log_port。
     ./fe/bin/start_fe.sh --helper <helper_fe_ip>:<helper_edit_log_port> --daemon
     ```

     请注意，首次启动节点时，您只需要指定一次参数 `--helper`。

   - 要使用 FQDN 访问启动新的 FE 节点，请运行以下命令启动该 FE 节点：

     ```Bash
     # 将 <helper_fqdn> 替换为 Leader FE 节点的 FQDN，
     # 并将 <helper_edit_log_port>（默认值：9010）替换为 Leader FE 节点的 edit_log_port。
     ./fe/bin/start_fe.sh --helper <helper_fqdn>:<helper_edit_log_port> \
           --host_type FQDN --daemon
     ```

     请注意，参数 `--helper` 和 `--host_type` 只需要在首次启动节点时指定一次。

4. 查看 FE 日志以验证 FE 节点是否启动成功。

   ```Bash
   cat fe/log/fe.log | grep thrift
   ```

   类似于“2022-08-10 16:12:29,911 INFO (UNKNOWN x.x.x.x_9010_1660119137253(-1)|1) [FeServer.start():52] thrift server started with port 9020.”这样的日志记录表明该 FE 节点已正确启动。

5. 重复上述步骤 2、3 和 4，直到您已正确启动 all 全部新的 Follower FE 节点，然后通过从您的 MySQL 客户端执行以下 SQL 来检查 FE 节点的状态：

   ```SQL
   SHOW PROC '/frontends'\G
   ```

   示例：

   ```Plain
   MySQL [(none)]> SHOW PROC '/frontends'\G
   *************************** 1. row ***************************
                Name: x.x.x.x_9010_1686810741121
                  IP: x.x.x.x
         EditLogPort: 9010
            HttpPort: 8030
           QueryPort: 9030
             RpcPort: 9020
                Role: LEADER
           ClusterId: 919351034
                Join: true
               Alive: true
   ReplayedJournalId: 1220
       LastHeartbeat: 2023-06-15 15:39:04
            IsHelper: true
              ErrMsg: 
           StartTime: 2023-06-15 14:32:28
             Version: 3.0.0-48f4d81
   *************************** 2. row ***************************
                Name: x.x.x.x_9010_1686814080597
                  IP: x.x.x.x
         EditLogPort: 9010
            HttpPort: 8030
           QueryPort: 9030
             RpcPort: 9020
                Role: FOLLOWER
           ClusterId: 919351034
                Join: true
               Alive: true
   ReplayedJournalId: 1219
       LastHeartbeat: 2023-06-15 15:39:04
            IsHelper: true
              ErrMsg: 
           StartTime: 2023-06-15 15:38:53
             Version: 3.0.0-48f4d81
   *************************** 3. row ***************************
                Name: x.x.x.x_9010_1686814090833
                  IP: x.x.x.x
         EditLogPort: 9010
            HttpPort: 8030
           QueryPort: 9030
             RpcPort: 9020
                Role: FOLLOWER
           ClusterId: 919351034
                Join: true
               Alive: true
   ReplayedJournalId: 1219
       LastHeartbeat: 2023-06-15 15:39:04
            IsHelper: true
              ErrMsg: 
           StartTime: 2023-06-15 15:37:52
             Version: 3.0.0-48f4d81
   3 rows in set (0.02 sec)
   ```

   - 如果字段 `Alive` 为 `true`，表示该 FE 节点已正确启动并已加入集群。
   - 如果字段 `Role` 为 `FOLLOWER`，表示该 FE 节点有资格被选举为 Leader FE 节点。
   - 如果字段 `Role` 为 `LEADER`，表示该 FE 节点是 Leader FE 节点。

## 停止 StarRocks 集群 {/* #stop-the-starrocks-cluster */}

您可以通过在相应实例上运行以下命令来停止 StarRocks 集群。

- 停止一个 FE 节点。

  ```Bash
  ./fe/bin/stop_fe.sh
  ```

- 停止一个 BE 节点。

  ```Bash
  ./be/bin/stop_be.sh
  ```

## 故障排查 {/* #troubleshooting */}

请尝试以下步骤来识别在启动 FE 或 BE 节点时发生的错误：

- 如果 FE 节点未正常启动，您可以通过查看 **fe/log/fe.warn.log** 中的日志来定位问题。

  ```Bash
  cat fe/log/fe.warn.log
  ```

  在定位并解决问题后，您必须先终止当前的 FE 进程，删除现有的 **meta** 目录，创建一个新的元数据存储目录，然后使用正确的配置重新启动该 FE 节点。

- 如果 BE 节点未正常启动，您可以通过查看 **be/log/be.WARNING** 中的日志来定位问题。

  ```Bash
  cat be/log/be.WARNING
  ```

  在定位并解决问题后，您必须先终止现有的 BE 进程，删除现有的 **storage** 目录，创建一个新的数据存储目录，然后使用正确的配置重新启动该 BE 节点。

## 后续操作 {/* #what-to-do-next */}

部署完您的 StarRocks 集群后，您可以继续阅读[部署后设置](./manage_deployment/post_deployment_setup.md)，了解初始管理措施的相关说明。
