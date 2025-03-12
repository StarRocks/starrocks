# CelerData Manager

## 简介

CelerData Manager 是一款可视化的数据库管理和开发工具。它提供以下功能，以提高您的 StarRocks 集群的运维效率并降低运维成本：

- 安装、部署、扩展、升级和回滚集群。
- 监控指标、发送警报、诊断和识别可能的问题。

CelerData Manager 还提供了一个易于使用的 SQL 编辑器来管理查询、跟踪 TopN 扫描和分析查询执行，帮助您加速查询并简化操作。

### 功能亮点

#### 集群生命周期管理

- 可视化集群部署
- 在线集群扩展/缩减，可视化节点添加和退役
- 一键升级和回滚

#### 动态集群监控和告警

- 提供 200 多个指标来监控集群性能、查询、数据摄取和 compaction（数据版本合并），实现实时、可视化监控。
- 识别 TopN 扫描和导入任务。
- 可视化数据摄取和 schema 变更。
- 用户可以使用电子邮件和 Webhook 自定义告警，并可以跟踪告警记录。

#### SQL 编辑器和简易查询分析

- 提供用户友好的 SQL 编辑器，供用户跟踪历史查询。
- 提供查询执行的可视化分析。
- 提供慢查询列表，帮助用户快速识别查询性能瓶颈。

### 基本概念

在安装 CelerData Manager 和 StarRocks 集群之前，先熟悉 StarRocks 集群的以下概念：

一个 StarRocks 集群由两类模块组成：核心模块和系统模块。

- 核心模块（下图中的黄色框内）
  - **Frontend (FE):** 负责元数据管理、客户端连接管理、查询计划和查询调度。
  - **Backend (BE):** 负责数据存储、查询执行、compaction 和副本管理。
  - **Broker**: 是 StarRocks 与外部 HDFS/对象存储服务之间的中间服务。Brokers 用于数据导入和导出。
- 系统模块（核心模块以外的模块）
  - **Web**: 为用户提供可视化的图形界面。
  - **Center service**: 拉取并汇总由 Agents 报告的信息，并提供查询服务。
  - **Agent**: 信息收集程序。它收集诸如指标等信息。

有关 StarRocks 的更多信息，请参见 [StarRocks 架构](https://docs.starrocks.io/en-us/latest/introduction/Architecture.)。

## 用户手册

### 集群生命周期管理

#### 安装准备

1. **获取您的 StarRocks 集群信息。**

   - 如果您已经有一个 StarRocks 集群，请获取集群架构（例如，集群中 FE 和 BE 节点的数量）、节点的 IP 地址和端口，以及访问节点的密码。
   - 如果您想部署一个新的 StarRocks 集群，您还需要规划集群架构、节点的 IP 地址和端口，以及密码。

2. **准备环境和依赖项。**

   > **注意**
   >
   > 目前，每个 CelerData Manager 只能管理一个 StarRocks 集群。如果您有多个集群并希望在同一台机器上安装多个 CelerData Manager，您必须为每个 CelerData Manager 配置一个外部网络端口。

   - 我们建议您的所有机器运行 Red Hat Enterprise Linux 7.9 或更高版本。
   - StarRocks 对硬件没有严格要求。它可以在低配置和高配置的机器上运行。测试环境的推荐配置是 8 个逻辑核心和 32 GB 内存或更高，在线环境的推荐配置是 16 个核心或更高。

     > CPU 必须支持 AVX2 指令集，因为 BE 节点需要 AVX2 以获得高性能。

     您可以运行以下命令来检查您的 CPU 是否支持 AVX2 指令集。

     ```Apache
     cat /proc/cpuinfo | grep avx2
     ```

   - 为外部服务配置外部网络端口以访问 CelerData Manager。如果您需要从数据中心或云中的机器访问 CelerData Manager，推荐的端口号范围是 **8000** 到 **9000**。

3. **为所有节点启用 SSH 免密码登录。**

   > **注意**
   >
   > 部署 CelerData Manager 时，您需要使用 SSH 和 Python 在 CelerData Manager 和 StarRocks 集群节点之间传输文件。因此，您必须为所有节点启用 **SSH 免密码登录**。如果遇到 `permission denied` 问题，请联系机器配置管理员。

   a. 生成 SSH 密钥对。

      ```Plain
      ssh-keygen -t rsa
      ```

   b. 将密钥对中的公钥复制到所有机器。注意，`fe1`、`be1`、`be2` 和 `be3` 是要部署的机器的 IP 地址或主机名。

      > 这里的 IP 地址是机器的内网 IP。机器内部通信的 SSH 也必须启用。

      ```SQL
      ssh-copy-id fe1
      ssh-copy-id be1
      ssh-copy-id be2
      ssh-copy-id be3
      ```

4. **为所有节点安装 python-setuptools。**

   部署 CelerData Manager 时，您必须安装 Python 和 **python-setuptools**。

   StarRocks 兼容 Python 2 和 Python 3。默认保存路径是 **/usr/bin/python.**

   ```SQL
   yum install -y python python-setuptools
   ```

5. **安装和部署 MySQL Server。**

   CelerData Manager 使用 MySQL 来管理与机器相关的信息，包括监控模块的元信息。

   如果您已经安装了 MySQL server，可以跳过此步骤。CelerData Manager 将创建一个数据库来存储相关信息。我们建议您使用由站点可靠性工程师（SRE）维护的 MySQL Server。

   多个 StarRocks 集群可以使用同一个 MySQL 数据库。我们建议您使用不同的名称来标识不同的集群。

6. **检查机器的磁盘配置。**

   检查是否可以在 **/etc/fstab** 中找到挂载到机器上的所有磁盘的信息。如果没有，磁盘在重启后无法自动挂载到机器。

#### 安装 CelerData Manager

1. 从您的销售经理处获取所需版本的 **CelerData-EE-x.x.x.tar.gz** 并解压此包。

   ```Apache
   tar -zxvf Celerdata-EE-x.x.x.tar.gz
   ```

2. 安装 Web UI 和 Center service。

   进入解压目录，运行 **install_path** 脚本生成 Web 界面，并安装 CelerData Manager。

   **install_path** 是您机器上 CelerData Manager 的安装目录。您可以使用默认路径。如果需要在同一台机器上安装多个 CelerData Manager 或需要自定义目录，可以根据需要修改 **安装目录** 或 **端口**。

   > "在同一台机器上安装多个 CelerData Manager" 意味着您可以在同一台机器上部署多个 CelerData Manager 来管理不同的集群。

   ```Bash
   cd Celerdata-EE-x.x.x

   sh bin/install.sh -h
   ```

   **install.sh** 中支持的配置：

   ```Apache
   -[d install_path] install_path(default: /home/disk1/celerdata/Celerdata-manager-20200101)
   -[y python_bin_path] python_bin_path(default: /usr/bin/python)
   -[p admin_console_port] admin_console_port(default: 19321)
   -[s supervisor_http_port] supervisor_http_port(default: 19320)
   ```

   如果在一台机器上部署多个 CelerData Manager，您必须为每个 CelerData Manager 指定安装路径。您可以使用集群名称或 IP 地址来区分 CelerData Manager。

   添加参数 `-d <directory name>` 和 `-p <port number>` 来修改 CelerData Manager 的文件目录和端口。

   ```bash

   sh bin/install.sh -d /home/disk1/Celerdata-manager \

      -p 19125  -s 19025

   ```

   成功执行此命令后，将生成指定的目录。

   生成的目录包含以下三个文件。此目录是 **Center service** 的安装目录。

   ```Apache

   drwxrwxr-x 9  4096 Feb 17 17:37 center

   -rwxrwxr-x 1   366 Feb 17 17:37 centerctl.sh

   drwxrwxr-x 3  4096 Feb 17 17:37 temp_supervisor

   ```

   安装完成后，您可以访问 Web 界面。

   - 对于本地访问，使用 `localhost:19321`。
   - 对于通过 IP 地址的外部访问，使用 `http://192.168.x.x:19321`。`19321` 是为 `-p` 指定的 `admin_console_port`。默认端口：19321。如果显示成功消息但仍无法访问 Web UI，请检查您的网络设置以确保端口未被防火墙阻止。
   - 如果需要禁用 **Web** 服务和管理 **Web** 服务的 **Supervisor**（例如，Supervisor 端口被占用或发生其他错误），或需要修改 **Web** 端口，可以运行 **centerctl.sh** 脚本：

   ```Bash
   cd Celerdata-manager-xxx
   ./centerctl.sh -i

   # 进入 Supervisor 界面。
   help # 查看命令。
   status  # 检查当前服务和命令，如停止和关闭。

   # 您还可以重新安装 CelerData Manager。
   ./centerctl.sh daemon
   ```

#### 通过 Web 部署 StarRocks 集群

1. 访问 Web 界面并为 CelerData Manager 配置一个 MySQL 数据库，用于存储管理、查询和告警信息。

   > 如果您有多个 CelerData 集群，我们强烈建议您为不同的集群配置不同的 MySQL 账户，以防止由于配置错误而导致的意外问题。

2. 配置完成后，点击 **测试连接**。如果测试成功（页面顶部显示 **OK**），点击 **下一步**。
3. 指定要部署的节点，以及 **Agent** 和 **Supervisor** 的安装目录。为 **Host IP** 输入内部网络 IP 地址，并使用其他参数的默认值。

   - **Host IP**: 您可以一次配置多个 IP 地址。用分号（;）分隔多个 IP 地址。
   - Supervisor 用于管理进程的启动和停止。
   - Agent 负责收集机器的统计信息。

所有安装均在用户环境中执行，不会影响系统环境。

   > **注意**
   >
   > - 系统有两种 **Supervisors**: 一种用于管理 Agent、BE、FE 和 Broker；另一种用于管理 **Web** 和 **Center service**。
   > - 如果要在部署 **Web** 和 **Center service** 的同一台机器上部署 Agent、FE、BE 和 Broker，请检查 Supervisor 端口是否与现有端口冲突。如果有冲突，请执行以下操作：修改之前配置的 `bin/install.sh -s ${new_port}` 以指定 CelerData Manager 所需的 Supervisor 端口，并确保管理 FE、BE 和 broker 的所有 Supervisors 和 Agents 使用默认端口。

4. 点击 **下一步**。在显示的对话框中，选择 **部署新集群** 或 **从现有集群迁移**。

  - 如果您已经部署了 FE 和 BE，但没有 CelerData Manager，请点击 **从现有集群迁移**。
  - 如果这是您第一次部署，即您的机器上没有运行 FE/BE 程序，请点击 **部署新集群**。

##### 迁移现有集群

> 如果您将集群从 StarRocks 开源版升级到企业版，则需要执行此步骤。如果要安装新集群，请执行 1.2.2 "安装新集群" 中的步骤。

1. **获取原始集群的信息。**

   如果您通过 MySQL 客户端连接到 StarRocks，请运行以下 SQL 命令查看并确认 FE、BE 和 broker 的信息。

   ```SQL
   show frontends;
   show backends;
   show broker;
   ```

   特别注意以下信息：

     - FE、BE 和 broker 的数量、IP 地址和版本
     - leader、follower 和 observer FEs 的信息

2. 禁用原始 StarRocks 集群的守护进程（如 Supervisor）并使用脚本启动它。

   > **重要**
   >
   > 如果不执行此步骤，新 CelerData Manager 的 Supervisor 将与旧 Supervisor 冲突，导致安装失败。

   假设安装目录都在 `~/Celerdata` 下。如果实际目录与此不同，请修改目录。

   ```Bash
   #### 使用脚本启动 BE。
   # 检查 BE 和 Supervisor 是否已启动。
   echo -e "\n==== BE ====" && ps aux | grep be && echo -e "\n==== supervisor ====" && ps aux | grep supervisor

   cd ~/Celerdata/be
   # 关闭 Supervisor 并使用脚本启动。
   ./control.sh stop && sleep 3 && bin/start_be.sh --daemon

   # 使用上述 echo 命令检查 BE 和 Supervisor 是否已启动。

   # 在您的 MySQL 客户端上检查 BE 启动。
   mysql> show backends;


   #### 使用脚本启动 FE。
   echo -e "\n==== FE ====" && ps aux | grep fe && echo -e "\n==== supervisor ====" && ps aux | grep supervisor

   cd ~/Celerdata/fe 
   # 关闭 Supervisor 并使用脚本启动。
   ./control.sh stop && sleep 2 && bin/start_fe.sh --daemon

   # 使用 echo 命令检查 FE 和 Supervisor 是否已启动。

   # 修改配置文件并重新运行。
   sed -i 's/DATE = `date +%Y%m%d-%H%M%S`/DATE = "$(date +%Y%m%d-%H%M%S)"/g' conf/fe.conf

   # 在您的 MySQL 客户端上检查 FE 启动。
   mysql> show frontends;


   #### 使用脚本启动 broker。
   echo -e "\n==== broker ====" && ps aux | grep broker &&
   echo -e "\n==== supervisor ====" && ps aux | grep supervisor

   cd ~/Celerdata/apache_hdfs_broker
   ./control.sh stop && sleep 2 && bin/start_broker.sh --daemon

   # 在 MySQL 中检查 broker 启动。
   mysql> show broker;


   #### 再次检查 Supervisor。
   ps aux | grep supervisor
   ```

3. 确保集群进入 "非守护进程" 状态，然后继续以下步骤。
4. 填写 FE、BE 和 broker 的安装目录。

   > **注意**
   >
   > - 此操作重新安装 FE 和 BE，配置文件从原始 FE 的元数据中获取。
   > - FE 元数据和 BE 存储保留原始数据路径。
   > - LOG 使用新目录中的路径。
   > - Udf、syslog、audit log、small_files 和 plugin_dir 使用新目录中的路径。

   在迁移 FE、BE 和 broker 时，分别配置升级路径。（需要原始实例的路径和安装目录）。

     - 升级前的路径：FE、BE 和 broker 的完整路径，例如 `/home/Celerdata/fe`。
     - 安装目录：整体安装目录，例如 `/home/Celerdata-manager-xxx`。

   您还可以批量执行配置。

5. 点击 **下一步**。在显示的页面上，点击 **全部迁移** 以执行自动迁移。您也可以点击 **迁移下一个** 逐个迁移节点。
6. 点击 **下一步** 配置 **Center service**。

   **Center service** 从 Agent 拉取信息，汇总并存储信息，并提供监控和告警服务。**Mail service** 是接收通知的邮箱，可以留空，稍后配置。

   配置 **Center service** 时可能会出现时区错误。如果出现此错误，请参阅 [UTC 错误](#utc-error) 进行故障排除。

7. 点击 **完成**。您将自动重定向到 Web 登录页面。默认账户是 **root**，密码为空。登录后将显示以下页面。
8. 复制图中的代码字符串并发送给 StarRocks 技术支持人员，他们将返回一个许可证字符串。输入许可证字符串后，点击 **OK** 使用 CelerData Manager。

完成此操作后，CelerData Manager 安装成功，您可以使用默认用户 `root`。`root` 的初始密码在安装过程中生成并显示。密码也在日志中：

```Bash
grep -r password manager/center/log/web/*
```

##### 安装新集群

1. 安装 FE。在 **配置 FE 实例** 对话框中，配置以下参数：
   - **FE Followers**: 我们建议您配置 1 或 3 个 follower FEs。
   - **FE Observers**: 您可以不指定 observer FEs。当查询压力增加时，您也可以添加 observer FEs。
   - **Meta Dir**: StarRocks 的元数据目录。类似于手动安装，我们建议您指定一个单独的元数据目录。
   - 对于安装目录、日志目录和端口号，使用默认值。
2. 安装 BE。在 **BE 设置** 对话框中，配置以下参数：
   - **Port**: 如果这些端口未被其他服务占用，我们建议您使用 **BE Port**、**Web Service Port**、**Heartbeat Port** 和 **BRPC Port** 的默认值。您可以不指定这些端口以使用默认值。
   - **Path:**
      - **Install Path**: 二进制文件安装目录。如果未填写，将在当前用户的主目录下创建一个类似于 `StarRocks-20250101` 的目录。目录中使用的日期是安装日期。如果您想在不同目录下安装 BE 节点，您需要指定绝对路径。
      - **Log Directory**: BE 节点的日志目录。建议使用默认值。如果您想在不同目录下存储日志文件，您需要指定绝对路径。
      - **Storage Path**: BE 节点上数据文件的存储路径。我们建议您指定一个单独的数据目录。您可以指定多个存储目录，用分号分隔。如果您想在不同目录下存储数据文件，您需要指定绝对路径。

   ![img]()

3. 安装 broker。我们建议您为所有节点安装一个 broker。

4. 安装 **Center service**。

**Center service** 从 Agent 拉取信息，汇总并存储信息，并提供监控和告警服务。**Mail service** 是接收通知的邮箱，可以留空，稍后配置。

在 **Center Service Setup** 对话框中，配置以下参数：

- **Service port**: 如果未被其他服务占用，我们建议您使用 **Center Service Port** 的默认值。您可以不指定端口以使用默认值。
- **Metrics storage path**: 存储集群指标的目录。建议使用默认值。如果您想在不同目录下存储指标，您需要指定绝对路径。
- **Metrics retention days**: 管理器保留指标的时间（以天为单位）。
- **Email server**: SMTP 服务器地址。
- **Email port**: SMTP 服务器端口。
- **Email user**: SMTP 用户名。
- **Email from**: 用于发送告警邮件的电子邮件地址。
- **Email password**: 启用 SMTP 后获得的授权密码。

![img]()

配置 **Center service** 时可能会出现时区错误，请参阅 [UTC 错误](#utc-error) 以获取解决方案。

完成此操作后，StarRocks 集群安装成功，您可以使用默认用户 **root** 和空密码登录 CelerData Manager（您可以通过参考 MySQL 中的相关操作更改密码和添加账户）。

从 v2.5 开始，CelerData Manager 将生成一个临时 root 用户和一个随机密码。在部署的最后一步，会提示显示密码为 ******，您需要记录密码。如果您未及时获取密码，您可以在日志中查询，日志记录了临时密码。命令如下：

```SQL
grep -r password manager/center/log/web/*
```

#### 启用 SSL

如果您不需要 SSL 配置，可以跳过此步骤。

1. 将 `ssl_cert` 和 `ssl_key` 行添加到 `<celerdata-manager installation dir>/center/conf/web.conf`：

   ```Shell
   [web]
   port =
   session_secret =
   session_name =
   ssl_cert =
   ssl_key =
   ```

   - `ssl_key` 是 `PEM 编码证书私钥` 的绝对路径。
   - `ssl_cert` 是 `PEM 编码证书主体` 的绝对路径。

2. 在 CelerData Manager 的安装目录中，运行 `./centerctl.sh restart web` 以重启 Web UI，并运行 `./centerctl.sh status web` 以检查 Web UI 的状态。如果状态显示为 RUNNING，则重启成功。
3. 在浏览器中访问 `https://mgr_host:port`。

#### 升级/回滚

##### 升级

从您的 CelerData 销售经理处获取目标版本的 CelerData Manager 安装包，然后解压。

```Shell
$ tar -zxvf  Celerdata-EE-x.x.x.tar.gz
$ cd Celerdata-EE-x.x.x/file
$ ls -l
```

解压后，您可以在 **file** 目录中看到两个安装包 **StarRocks-x.x.x.tar.gz** 和 **Manager-x.x.x.targ.z**。

**x.x.x** 是一个三位版本号。第一位是主版本号。第二位是次版本号，第三位是补丁版本号。补丁版本通常每两到四周发布一次。您可以在官方网站上阅读每个版本的 [发布说明](https://docs.starrocks.io/zh/releasenotes/release-3.4/)。

**file** 目录中的文件如下。在执行升级时，您必须选择此目录中的包。

```Shell
file
   ├── Manager-3.3.9.tar.gz
   ├── STREAM
   ├── StarRocks-3.3.9-ee.tar.gz
   ├── iperf-3.1.3.tar.gz
   ├── openjdk-11.0.20.1_1-linux-x64.tar.gz
   ├── openjdk-8u322-b06-linux-x64.tar.gz
   ├── rg
   └── supervisor-4.2.0.tar.gz
```

###### 升级 CelerData Manager

升级过程是先升级 CelerData Manager，然后再升级 StarRocks。升级 CelerData Manager 不会影响集群服务。

1. 在主页的右上角，点击 **root** 下拉列表并点击由版本指示的升级按钮。

   ![img]()

2. 在 **提示** 对话框中，输入解压 **file** 文件夹中的 **Manger-xxx.tar.gz** 的路径。
3. 确认所有文件上传成功后，点击 **确认** 以执行自动升级。

4. 升级成功后，您可以在 **root** 下拉列表中检查版本是否正确。

   确保 CelerData Manager 的版本是目标版本，然后您可以继续后续操作。

###### 升级 StarRocks 集群

在升级 StarRocks 集群之前，请确保 CelerData Manager 已经升级。

1. 选择 **Nodes** > **Version Management**。
2. 在出现的 **Version** 页面上，点击 **Add** 按钮以添加 StarRocks 版本。
3. 输入解压 **file** 文件夹中的 **StarRocks-x.x.x.tar.gz** 的路径。
4. 确认路径正确后，点击 **确认**。系统会自动安装版本（**操作** 列中的所有按钮变为不可点击）。
5. 新版本添加后，其状态显示为 OFFLINE，**操作** 列中的按钮变为可点击。
6. 点击 **切换** 按钮以执行升级。
7. 第一个 BE 节点升级（100%）后，系统暂停以供您检查。如果未发现问题，您可以点击 **全部升级** 以升级所有其他 FE 和 BE 节点。您也可以点击 **升级下一个** 逐个升级节点。
8. 如果第一个节点的升级失败，您可以点击 **全部回滚** 或 **回滚下一个**。如果升级失败并且您希望尽快恢复服务，您可以立即执行回滚。失败的原因会显示在编辑框中。如果问题仍然存在，您可以联系 StarRocks 技术支持。
9. 如果您点击 **全部升级**，页面底部会显示一个 **全部回滚** 按钮，以便您暂停升级。
10. 升级成功后，升级页面退出，您将被重定向到 **Node** 页面，其中显示目标版本，并且所有 FEs 和 BEs 的启动时间都是此次升级的时间。

##### 回滚

回滚操作类似于升级操作，涉及替换 **lib** 和 **bin** 文件夹并重启集群。

如果您想在升级期间或之后执行回滚，请选择历史版本并切换到该版本（当前版本变为不可点击）。

升级期间的回滚示例：

1. 点击 **Version Management**。
2. 在 **Version** 页面上，点击 **Add** 以添加新版本。
3. 指定升级文件的路径并点击 **确认**。新版本将添加到 **Version** 页面。
4. 点击目标版本旁边的 **切换** 按钮。当前版本变暗且不可点击。其他版本是可点击的。
5. 在出现的对话框中，点击 **确认**。
6. BE 升级后，您可以点击 **回滚上一个** 或 **全部回滚**。

回滚完成后，请在 **Nodes** 标签上检查版本。

#### 扩展/缩减

##### BE 扩展

1. 选择 **Nodes** > **Add Host**。点击 **Add Host** 以部署新的 Agent 来管理节点。（添加节点需要重新签署许可证。）

   ![img]()

   节点添加后，将显示在 **Add Host** 部分。

2. 在 **BE 节点** 部分，点击 **扩展** 并在 **BE 设置** 对话框中输入所需信息。

   ![img]()

3. 点击 **确认** 完成扩展。扩展完成后将自动触发负载均衡。

如果您需要增加许可证配额，请联系 StarRocks 技术支持。

##### BE 缩减

找到要移除的 BE 节点。在 **操作** 列中，点击 **退役** 以移除节点。退役过程需要几分钟。数据迁移完成后，您可以看到 BE 上的 tablets 数量逐渐降为 0，然后点击 **停止**。节点变为闲置节点，等待回收。

在退役过程中，您可以通过检查 tablets 的数量来查看进度。如果需要取消退役，请点击 **取消退役**。

##### FE 扩展

1. 在 **Nodes** 页面的 **Add Host** 部分，点击 **Add Host** 以部署新的 Agent 来管理节点。（添加节点需要重新签署许可证。）
2. 节点添加后，点击 **扩展** 并输入所需信息。点击 **确认** 完成扩展。

![img]()

##### FE 缩减

找到要移除的 FE 节点。在 **操作** 列中，点击 **退役** 以移除节点。节点退役后，在 **Editor** 页面上运行 `SHOW FRONTENDS;` 检查节点是否存在。

## 用户权限

### Root 密码

从 v2.5 开始，CelerData Manager 将生成一个临时 root 用户和一个随机密码。在部署的最后一步，会提示显示密码为 ******，您需要记录密码。如果您未及时获取密码，您可以在日志中查询，日志记录了临时密码。命令如下：

```Bash
cd  celerdata-manager-20201102/center
grep -r password /log/web/*
```

### SQL 编辑器

您可以通过 MySQL 客户端连接到 CelerData，或者使用 CelerData Manager 的 Web UI 创建集群用户并授予用户权限。下图显示了 Web UI 的 **Editor** 页面的操作。

![img]()

### 更改密码

我们建议您在安装后更改 root 密码并妥善保管密码。以下代码片段显示了如何更改密码。

```SQL
--- 示例
ALTER USER 'root' IDENTIFIED BY '123456';
-- 语法
ALTER USER user_identity [auth_option];

-- 参数
user_identity: 格式为 'user_name'@'host'

auth_option: {
IDENTIFIED BY 'auth_string'
IDENTIFIED WITH auth_plugin
IDENTIFIED  WITH auth_plugin BY 'auth_string'
IDENTIFIED WITH auth_plugin AS 'auth_string'
}

--- 示例
ALTER USER 'jack' IDENTIFIED BY '123456';
```

**参数说明：**

- **user_identity**

由两部分组成，`user_name` 和 `host`，格式为 `username@'userhost'`。对于 "host" 部分，您可以使用 `%` 进行模糊匹配。如果未指定 `host`，则默认使用 "%" ，表示用户可以从任何主机连接。

- **auth_option**

指定认证方法。目前支持以下认证方法：mysql_native_password 和 authentication_ldap_simple。

### 创建角色/用户

- 创建角色

  您可以将权限授予角色，并将角色分配给用户。具有相同角色的用户共享授予该角色的权限。

  使用以下命令创建角色：

  ```SQL
  --- 创建角色
  CREATE ROLE role1;
  --- 查询已创建的角色
  SHOW ROLES;
  ```

- 创建用户

  ```SQL
  -- 语法
  CREATE USER user_identity [auth_option] 
  [DEFAULT ROLE 'role_name'];

  -- 参数
  user_identity:'user_name'@'host'

  auth_option: {
      IDENTIFIED BY 'auth_string'
      IDENTIFIED WITH auth_plugin
      IDENTIFIED WITH auth_plugin BY 'auth_string'
      IDENTIFIED WITH auth_plugin AS 'auth_string'
  }

  -- 示例：创建用户并为用户分配默认角色。
  CREATE USER 'jack'@'%' IDENTIFIED BY '12345' DEFAULT ROLE 'my_role';
  ```

参数说明：

- **CREATE USER**

  创建一个 CelerData 用户。在 CelerData 中，user_identity 唯一标识一个用户。

- **user_identity**

  由两部分组成，`user_name` 和 `host`，格式为 `username@'userhost'`。对于 "host" 部分，您可以使用 `%` 进行模糊匹配。如果未指定 `host`，则默认使用 "%" ，表示用户可以从任何主机连接。

- **auth_option**

  指定认证方法。目前支持以下认证方法：mysql_native_password 和 authentication_ldap_simple。

- **DEFAULT ROLE**

  如果指定了角色，角色的权限将自动授予新创建的用户。如果未指定，用户默认没有任何权限。指定的角色必须已经存在。您可以参考 [CREATE ROLE](https://docs.starrocks.io/en-us/3.0/sql-reference/sql-statements/account-management/CREATE ROLE) 获取更多信息。

### 删除角色/用户

您可以使用以下命令删除角色/用户：

```SQL
  -- 删除角色。
  DROP ROLE role1;
  -- 删除用户。
  DROP USER 'jack'@'192.%'
```

### 授予权限

您可以使用以下命令将权限授予用户或角色：

```SQL
GRANT privilege_list ON db_name[.tbl_name] TO user_identity [ROLE role_name];

GRANT privilege_list ON RESOURCE resource_name TO user_identity [ROLE role_name];

-- 将 'spark_resource' 上的 USAGE 权限授予用户 'jack'@'%'。
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';

-- 将 'spark_resource' 上的 USAGE_PRIV 权限授予角色 'my_role'。
GRANT USAGE ON RESOURCE 'spark_resource' TO ROLE 'my_role';

-- 查询所有用户的权限。
SHOW ALL GRANTS; 

--- 查询指定用户的权限。
SHOW GRANTS FOR jack@'%';

-- 查询当前用户的权限。
SHOW GRANTS;
```

**参数说明：**

**privilege_list**

  可以授予用户或角色的权限。如果要一次授予多个权限，请用逗号（`,`）分隔权限。支持以下权限：

- `NODE_PRIV`: 管理集群节点的权限，例如添加节点和退役节点。只有 root 用户拥有此权限。不要将此权限授予其他用户。
- `ADMIN_PRIV`: 除 `NODE_PRIV` 之外的所有权限。
- `GRANT_PRIV`: 执行创建用户和角色、删除用户和角色、授予权限、撤销权限和设置密码等操作的权限。
- `SELECT_PRIV`: 从数据库和表中读取数据的权限。
- `LOAD_PRIV`: 将数据导入数据库和表的权限。
- `ALTER_PRIV`: 更改数据库和表的 schema 的权限。
- `CREATE_PRIV`: 创建数据库和表的权限。
- `DROP_PRIV`: 删除数据库或表的权限。
- `USAGE_PRIV`: 使用资源的权限。

在早期版本中，`ALL `和 `READ_WRITE` 将转换为 `SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV`; `READ_ONLY `将转换为 `SELECT_PRIV`。

上述权限可以分为以下三类：

- 节点权限：`NODE_PRIV`
- 数据库和表权限：`SELECT_PRIV`, `LOAD_PRIV`, `ALTER_PRIV`, `CREATE_PRIV`, 和 `DROP_PRIV`
- 资源权限：`USAGE_PRIV`

StarRocks 从 v3.0 开始实施新的基于角色的访问控制（RBAC）系统，其中权限被重命名并添加了新权限。您可以参考 [StarRocks 支持的权限](https://docs.starrocks.io/en-us/3.0/administration/privilege_item)。

**db_name [.tbl_name]**

数据库和表名。此参数支持以下三种格式：

- `.`: 表示所有数据库和表。如果指定此格式，则授予全局权限。
- `db.*`: 表示特定数据库及其所有表。
- `db.tbl`: 表示特定数据库中的特定表。

> 注意：使用 `db.*` 或 `db.tbl` 格式时，可以指定不存在的数据库或表。

**resource_name**

资源名。此参数支持以下两种格式：

- `*`: 表示所有资源。
- `resource`: 表示特定资源。

> 注意：使用 `resource` 格式时，可以指定不存在的资源。

**user_identity**

此参数包含两部分：`user_name` 和 `host`。`user_name` 表示用户名，`host` 表示用户的 IP 地址。您可以不指定 `host` 或为 `host` 指定一个域。如果不指定 `host`，则 `host` 默认为 `%`，表示可以从任何主机访问。如果为 `host` 指定一个域，权限生效可能需要一分钟。`user_identity` 参数必须由 CREATE USER 语句创建。

**role_name**

角色名称。

### **撤销权限**

您可以从指定用户或角色撤销权限。

```SQL
REVOKE privilege_list ON db_name[.tbl_name] FROM user_identity [ROLE role_name];

REVOKE privilege_list ON RESOURCE resource_name FROM user_identity [ROLE role_name];

-- 从用户撤销对数据库的权限。
REVOKE SELECT_PRIV ON db1.* FROM 'jack'@'192.%';

-- 撤销用户 jack 使用资源的权限。
REVOKE USAGE_PRIV ON RESOURCE 'spark_resource' FROM 'jack'@'192.%';
```

**参数说明：**

**user_identity**

用法与前述描述相同。`user_identity` 参数必须由 CREATE USER 语句创建。如果为 `host` 指定一个域，权限生效可能需要一分钟。您还可以从指定的 ROLE 撤销权限，角色必须存在。

### 设置用户属性

您可以设置用户属性，包括分配给用户的资源。

```SQL
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value'];
-- 将用户 jack 的最大连接数修改为 1000。
SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';
-- 将 jack 的 cpu_share 修改为 1000。
SET PROPERTY FOR 'jack' 'resource.cpu_share' = '1000';
```

这里的用户属性是指用户的属性，而不是 user_identity 的属性。例如，如果使用 CREATE USER 创建了两个用户，'jack'@'%' 和 'jack'@'192.%'，那么 SET PROPERTY 语句只能用于用户 jack，而不是 'jack'@'%' 或 'jack'@'192.%'。

**key:**

超级用户的属性：

- `max_user_connections`: 最大连接数
- `resource.cpu_share`: CPU 配额

普通用户的属性：

- `quota.normal`: 普通用户的配额
- `quota.high`: 高级用户的配额
- `quota.low`: 低级用户的配额

## 概览

登录 CelerData Manager 后，您可以在 **Dashboard** 页面查看集群状态，包括 **集群基本信息、集群概览、查询概览和告警**。

### 集群基本信息

显示 **数据信息** 和 **节点信息**。

- **数据信息** 包括总数据量、今天新增的数据量和昨天新增的数据量。
- **集群节点信息**

  Leader FE IP 和端口（端口是 BE 心跳端口）。

### 集群概览

**集群概览** 部分显示集群的一般信息。

显示 QPS、平均查询响应时间、99 百分位响应、数据库连接数和加载数据量。您还可以在右上角指定时间间隔。

#### **查询 QPS**

您可以查看 QPS 以监控集群负载。

**平均查询响应时间**

通过查询平均响应时间，您可以观察当前时间段内集群的平均查询延迟，以确定查询延迟是否符合预期，以及是否需要调整某些 SQL。

点击 **查看更多** 查看 50 百分位响应时间、75 百分位响应时间、90 百分位响应时间、95 百分位响应时间、99 百分位响应时间和 999 百分位响应时间。

#### 数据库连接数

您可以查看当前集群中的连接数。统计的不是连接用户的数量。来自同一账户到集群的连接也会被计数。

#### 数据摄取量

您可以查看集群的数据导入频率。y 坐标的默认单位是 KB/秒。

点击底部的 **查看更多** 查看导入任务的数量、导入的行数和导入的数据量。

### 查询概览

选择 **Dashboard** > **查询概览** 查看查询执行成功率、成功查询数和失败查询数。

点击底部的 **查看更多** 查看 **所有查询** 和 **慢查询**。

**所有查询** 显示集群中已执行的所有 SQL 语句，以及执行状态、耗时和执行 SQL 的用户。

您可以点击查询 ID 显示 SQL 详情、查询计划、执行时间和执行详情。

**查询计划** 显示 EXPLAIN 语句的结果。

**查询详情** 显示查询的详细配置文件。如果此选项卡不可点击，则未启用查询配置文件。您可以使用 `set is_report_success = true`（适用于 v2.5 之前的版本）或 `set enable_profile = true`（适用于 v2.5 及之后的版本）启用查询配置文件，然后再次运行 EXPLAIN。

### 告警

> **重要**
>
> 为了提前检测集群中的任何异常和风险，您必须为集群配置告警。有关更多信息，请参见 "告警和诊断" 部分。

在告警区域，您可以查看触发的告警数量。告警分为三种类型，按严重程度降序排列：Fatal、Warning 和 Info。

点击 **查看更多** 进入 **告警** 页面。

您可以查看告警记录、配置告警规则、阻止节点报告告警，并在触发告警时配置告警通知接收人。

## 管理集群和节点

### 管理集群

#### 集群性能

点击顶部导航栏中的 **Cluster** 显示客户性能信息，包括 CPU 使用率、内存使用率、磁盘 I/O 使用率、磁盘使用率、可用磁盘空间、数据包传输速率、传输数据包数量、数据包接收速率和接收的数据包数量。

您可以调整滑块，或在右上角调整时间间隔，以在特定时间观察此信息。

#### 查询监控

点击 **查询监控** 标签显示集群的查询信息，包括查询 QPS、查询响应时间（AVG）、50 百分位、75 百分位、90 百分位、95 百分位、99 百分位和 999 百分位。

#### 数据摄取量

点击 **数据加载** 标签显示集群的查询信息，包括发起的导入任务数量、加载的行数和加载的数据大小等信息。

#### Compaction

点击 **Compaction** 标签显示集群的查询信息，包括基线合并的数据版本、累积合并的数据版本、基线合并的数据量和累积合并的数据量。

#### TopN 扫描

点击 **扫描 TopN** 标签显示集群的查询信息，包括完成的扫描次数、扫描的数据大小和扫描的行数。

#### TopN 加载

点击 **加载 TopN** 标签显示集群的加载信息，包括完成的加载任务数量、加载的数据大小和加载的行数。

您可以点击一个表查询单个表的实时加载。以 starrocks_audit_tbl_ 为例。

### 管理节点

#### 机器信息

1. 点击顶部导航栏中的 **Nodes** 显示集群信息，包括 FE 节点信息、BE 节点信息和 Broker 节点信息。
2. 点击右上角的 **版本管理** 切换集群版本。
3. 点击 **添加** 并输入目标 StarRocks 版本的路径以添加版本。
4. 版本添加后，点击 **切换**。

您还可以启动和停止节点、编辑其配置或退役节点。

#### 指标

点击 **指标** 标签显示当前集群的监控信息。

您可以从指标列表中选择指标来监控集群。

## 管理数据库

### 查看数据库信息

#### Catalogs

1. 点击顶部导航栏中的 **Catalogs** 显示 catalog 信息，包括 catalog 名称、类型和注释。
2. 点击一个 catalog 查看其详细信息，包括数据库名称、大小和注释。
3. 点击一个数据库（例如，`jd`），显示 **数据库详情** 页面，其中包含三个标签：**表**、**任务** 和 **统计**。
4. 点击一个表查看表信息，例如，`site_access`。

   > **注意**
   >
   > 如果在此表上构建了物化视图，点击此表时也会显示物化视图。

   详细信息包括分区名称、可见数据版本、分区状态、分区键、分区范围、分桶列、桶数、副本数和分区的数据大小。

   - **可见版本**: 分区的数据版本数量。数量因导入任务的数量而异。更多的导入任务会导致此参数的值更大。
   - **状态**: 分区状态必须为 **NORMAL**。

5. 点击一个分区（`p20221206`）查看分区信息。

   **分区** 的下一级是 **Tablets**。此图显示 BE 节点上的 tablets 数量、数据行数、数据版本和数据大小。

   **数据组**: SHOW PROC 命令输出的总数据版本数。

6. 检查 BE 模式上的 tablets 详细信息。以 BE 节点 `10410` 为例。

   此图显示了 tablets ID、tablets 所在的 BE 节点、有效版本、数据行数、数据版本、数据大小和 tablets 的状态等信息。

   - **有效版本**: tablets 的累积版本数。大量的导入任务会导致此指标的值较大。
   - **数据组**: tablets 的版本数。加载频率越高，值越大。默认限制为 1000。值大于 1000 会影响加载性能。

#### 任务

以数据库 `jd` 为例。在 **任务** 标签上，您可以查看此数据库中导入任务的详细信息，包括 **Kafka 导入、其他导入**、**导出**、**Schema 变更**、**创建视图** 和 **克隆**。

-  **Kafka 导入、其他导入**

  您可以按标签名称、表名称和任务状态筛选任务。您可以对目标任务执行以下操作：恢复处于 PAUSED 状态的任务、停止正在运行的任务和删除任务。此外，您还可以查看其他任务信息。

- **导出**

  显示任务状态、进度、任务类型、开始时间、结束时间和详细信息。

- **Schema 变更**

  显示表名称、任务执行状态、进度、开始时间、结束时间、超时时间和详细信息。

-  **创建视图**

  显示物化视图名称、基表名称、创建状态、进度、开始时间、结束时间、超时时间和详细信息。

- **克隆**

  显示集群中副本克隆任务的信息，包括两种类型的任务（正在运行的任务和待处理的任务）。详细信息包括 tablets ID、数据库、表、分区和开始时间。

#### 统计

点击 **统计** 标签查看集群中 tablets 的信息，包括数据不一致的 tablets 和慢查询的 tablets。

### 使用数据库编辑器

点击顶部导航栏中的 **Editor** 编写 SQL。这样无需使用 MySQL 客户端连接到 CelerData。您可以直接在 Web UI 中运行 SQL。

- 编写 SQL

  ![img]()

- 选择数据库

  点击左侧窗格中的下拉列表选择要查询的数据库。

- 搜索表

  您可以在搜索框中输入关键字搜索表。此外，您可以点击右下部分的 **表详情** 标签查看表结构和表中的数据。

  ![img]()

- 标签

  您可以点击顶部区域的 **+** 添加自己的标签并打开一个新的编辑器页面。这样，您可以拥有一个单独的操作页面。

点击 **已保存的标签** 查看所有标签并管理标签，包括打开、删除、搜索和批量删除操作。

- 使用查询编辑器

  您可以在 **查询编辑器** 中编写 SQL，然后点击 **运行**。您可以选择执行单个查询或批量执行 SQL。执行后，您可以查看查询历史、查询结果和表详情。

  ![img]()

  - **查询历史** 标签显示查询 ID、查询开始时间、状态、持续时间和 SQL。
  - **查询结果** 标签显示查询 ID、执行时间、执行状态、返回行数和结果行总数。
  - **表详情** 标签显示列 ID、列类型、列值是否可为空、表是否为主键表、列的默认值和注释。您还可以查询表结构并查看表中的数据。

- 分析

  此功能相当于 `set is_report_success = true`，即启用配置文件报告以分析 SQL。

- 清除：清除 **查询编辑器** 窗格以编写新 SQL。

### 管理查询

点击顶部导航栏中的 **Queries** 查看所有查询记录（查询时间超过 **5s** 被视为慢查询）。

#### 所有查询

**所有查询** 标签显示查询的开始时间、ID、状态、持续时间、查询用户和 SQL 语句。如果您已启用查询配置文件功能（v2.5 及之后的版本使用 `set enable_profile = true`，v2.5 之前的版本使用 `set is_report_success = true`），点击相应的查询 ID 查看查询执行计划和查询配置文件。

- 您可以点击 SQL 语句进行复制。
- 您可以在搜索框中输入关键字筛选查询。
- 您可以点击 **所有查询** 下拉列表筛选查询。
- 您可以通过时间范围筛选查询记录。

查询记录按页面显示。在页面底部，您可以选择每页显示的条目数。您可以按页码顺序查看查询记录或选择目标页码。

#### 慢查询

**慢查询** 标签显示系统识别的所有慢查询。与 **所有查询** 标签类似，您可以使用搜索框、按执行状态或指定时间范围筛选查询。

## 告警和诊断

### 告警

#### 告警记录

选择 **告警** > **告警记录**。此标签显示历史告警记录。您可以根据时间、告警严重性和告警状态筛选告警。您还可以在搜索框中输入关键字搜索告警。

#### 告警规则

**告警规则** 标签显示所有已配置的告警规则。您可以在此页面上修改、删除和禁用告警指标。您还可以查看告警规则的详细信息和历史告警。

您可以点击右上角的 **创建** 添加告警规则。

- **触发周期**：您可以配置告警生效的时间范围，例如，08:00:00--18:00:00。
- **告警间隔**：告警报告间隔。在此参数指定的时间段内，告警不能重复报告。
- **节点**：触发告警的节点。例如，您可以配置一个 FE 节点。
- **指标：** 可以搜索告警指标。
- **规则名称：** 通常，名称与告警指标相同。您也可以自定义名称。
- **规则1**
  - 告警间隔：值 + 时间单位，例如，1 小时。
  - 触发条件：支持的条件是平均值和值。比较运算符是 `>=`、`>`、`=`、`<`、`<=`。您需要设置一个值阈值。示例：平均值 < 80%。
  - 告警严重性按升序排列
    - **信息：** 集群负载或其他功能超出正常范围，您需要注意。
    - **警告**：集群的某些功能不可用。您需要注意并修复。
    - **致命**：集群不可用。您需要尽早检查相关信息并与业务团队沟通以识别问题。
  -  您可以通过点击每个规则右侧的加号（+）添加告警规则。
- **备注**：您可以为告警规则添加备注以描述指标的含义和严重性。

#### 阻止节点

您可以阻止特定节点的告警。这样，与此节点相关的告警将不会被报告。此功能可避免在执行节点维护操作时触发不必要的告警。

#### 告警通知

目前，CelerData 支持通过电子邮件、钉钉机器人、飞书机器人和 Webhook 进行告警通知。您可以选择任何适合您需求的方法。以下部分描述如何配置电子邮件和 webhook。

##### 配置电子邮件

1. 点击 **root** > **设置** > **电子邮件 SMTP 设置**。
2. 点击 **编辑**。
3. 在 **SMTP 服务器** 字段中，输入要使用的 SMTP 服务器地址的主机名。例如，smtp.example.com。
4. 对于 **SMTP 端口**，输入要使用的 SMTP 端口号。
5. 在 **用户** 和 **密码** 字段中输入有效的 SMTP 服务器凭据。
6. 在 **FROM** 中，输入用于发送通知的电子邮件地址。
7. 选择 SMTP 服务器的 **认证类型**
8. （**可选**）输入 **To EMail** 并点击测试以验证电子邮件服务器配置的信息是否正确。
9. 点击 **保存**。

###### 配置 SMTP 服务器

您可以点击 **root** 下的 **设置** 配置邮箱和 SMTP 服务器。

![img]()

![img]()

###### 配置邮箱

在 **通知管理** 标签上，点击右上角的 **创建**。在 **创建** 对话框中配置电子邮件。

![img]()

##### 配置 Webhook

在 **Webhooks** 标签上，点击右上角的 **创建**。

您需要开发一个接口来接收来自服务器的 Webhook 告警。

CelerData 向配置的 URL 发送以下 HTTP 请求：

```Plain
method:
POST

header:
x-starrocks-db-signature = [signature: hex_str(sha1(secret+post_body))]
content-type = [req_header.content-type]

body:
{
 "level":        "告警严重性",
 "ruleName":     "告警规则名称",
 "alarmMessage": "告警消息",
 "startTime":    "告警开始时间",
}

注意
1. 接收方必须验证 x-starrocks-db-signature 的结果。计算方法如下：
使用 sha1 对字符串（Secret + 接收到的 post body）进行编码，并将其转换为十六进制字符串。

Golang 示例：
```go
hash := sha1.New()
hash.Write([]byte(secret))
hash.Write(body)
signBytes := hash.Sum(nil)
sign := fmt.Sprintf("%x", signBytes)
```

2. content-type 可以是 application/json 或 application/x-www-form-urlencoded。
3. Secret

### 诊断

#### 日志

选择 **诊断** > **日志**。在 **日志** 标签上，您可以查看 FE 和 BE 日志。您还可以在搜索框中或通过指定时间间隔搜索日志。

#### 系统诊断

您可以点击右上角的 **创建系统诊断** 收集以下信息以进行故障排除：

- **集群基本信息**：包括主机列表、硬件信息和 StarRocks 版本。
- **StarRocks 配置**：包括 `fe.conf` 和 `be.conf` 配置文件中的项目和会话变量。
- **StarRocks 日志**：包括 FE 和 BE 日志。
- **硬件测试**：CPU、环境变量、Iperf 网络接口测试结果、最大打开文件数（ulimit -n）、`/proc/sys/vm/overcommit_memory` 的配置和 `/proc/sys/vm/swappiness` 的配置。
- **慢查询**：特定时间段内的慢查询（默认 4 天）和配置文件。
- **系统指标**：与内存、CPU 和 io.util 相关的指标。
- **BE 内存信息**：参见 [内存管理](https://docs.starrocks.io/en-us/latest/administration/Memory_management)。

#### 硬件测试

硬件测试将检查所选节点的 CPU、内存、最大打开文件数（ulimit -n）、`/proc/sys/vm/overcommit_memory` 的配置、`/proc/sys/vm/swappiness` 的配置、Iperf 网络接口测试结果、环境变量和磁盘随机 I/O 测试结果。

## 卸载 CelerData Manager 和 StarRocks

1. 在所有节点所在的目录中依次运行以下命令。

   ```Bash
   cd  celerdata-manager-20201102/agent
   ./agentctl.sh stop all
   ./agentctl.sh shutdown
   ```

2. 在部署 CelerData Manager 的机器上运行以下命令。

   ```Bash
   cd  celerdata-manager-20201102/center
   ./centerctl.sh stop all
   ./centerctl.sh shutdown 
   ```

3. 删除（或备份）所有 **celerdata-xxx** 和 **celerdata-manager-xxx** 目录。如果您使用自定义目录，请停止 Supervisor 管理的所有进程并删除所有相关目录。

## 附录

### UTC 错误

如果在配置 **Center service** 时出现 UTC 时区错误，请在 **~/.bashrc** 文件中添加 **`export TZ = 'America/New_York';`** 并运行该文件。此操作将环境变量 **TZ** 设置为 `America/New_York` 并将此设置添加到系统变量中。

在安装 **Web** 服务之前，确认时区已更改：

```Shell
[ycg@StarRocks-sandbox04 ~]# export TZ=America/New_York
[ycg@StarRocks-sandbox04 ~]# date
Thu Jan 16 08:03:17 EST 2025
```