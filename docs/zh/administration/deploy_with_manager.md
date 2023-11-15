# 通过 StarRocks Manager 自动部署 StarRocks 集群

本文介绍如何使用 StarRocks Manager 自动部署 StarRocks 集群。

> **说明**
>
> StarRocks Manager 为企业版功能，如需试用，请至 [下载页面](https://www.mirrorship.cn/zh-CN/download/community) 下方点击「现在咨询」获取。

## 前提条件

您需要在所有拟部署 StarRocks 的节点上安装以下依赖:

|依赖|说明|
|----|----|
|JDK（1.8 或以上）|下载完成后，您需要在 FE 节点配置文件 **fe.conf** 中的 `JAVA_HOME` 配置项下指定 JDK 的安装路径。|
|Python（2.7 或以上）| |
|python-setuptools|您可以通过 `yum install setuptools` 或 `apt-get install setuptools` 安装。|
|MySQL（5.5 或以上）|您需要通过 MySQL 存储 StarRocksManager 平台的数据。|

## 安装 StarRocks Manager

下载并解压 StarRocks Manager 安装包。

解压完成后，安装 StarRocks Manager。

```shell
bin/install.sh -h \
-d /home/disk1/starrocks/starrocks-manager-20200101 \
-y  /usr/bin/python -p 19321 -s 19320
```

* `-d`：StarRocks Manager 的安装路径。
* `-y`：Python 路径。
* `-p`：`admin_console_port`，默认为 `19321`。
* `-s`：`supervisor_http_port`，默认为 `19320`。

## 安装部署 StarRocks

完成安装 StarRocks Manager 后，您可以在 Web 页面中安装部署 StarRocks 集群。

### 配置 MySQL 数据库

首先，您需要配置一个安装好的 MySQL 数据库，用于存储 StarRocks Manager 的管理、查询、报警等信息。

![配置 MySQL](../assets/8.1.1.3-1.png)

### 配置节点信息

添加需要部署的节点，并配置 Agent 和 Supervisor 安装目录和端口，Python 路径以及其他信息。

> 说明
> Agent 负责采集机器的统计信息，Supervisor 管理进程的启动与停止。两者均安装在用户环境，不会影响系统环境。

![配置节点](../assets/8.1.1.3-2.png)

### 安装 FE 节点

配置 FE 节点相关信息。端口的含义参考下方[端口列表](#端口列表)。

建议您配置 1 或者 3 个 Follower FE。如果请求压力较大，建议您酌情增加 Observer FE  数量。

![配置 FE 实例](../assets/8.1.1.3-3.png)

`Meta Dir`：StarRocks 的元数据目录。建议您配置独立的 **starrocks-meta** 和 FE 节点 log 目录。

### 安装 BE 节点

配置 FE 节点相关信息。端口的含义参考下方[端口列表](#端口列表)。

![配置 BE 实例](../assets/8.1.1.3-4.png)

### 安装 Broker

建议您在所有节点上安装 Broker。端口的含义参考下方[端口列表](#端口列表)。

![HDFS Broker](../assets/8.1.1.3-5.png)

### 安装中心服务

中心服务负责从 Agent 拉取并汇总信息后存储在 MySQL 中，并提供监控报警的服务。此处邮件服务是指通过邮箱接收报警通知。邮件服务可以稍后配置。

配置中心服务以及邮件服务的相关信息。

![配置中心服务](../assets/8.1.1.3-6.png)

## 端口列表

|实例名称|端口名称|默认端口|通讯方向|说明|
|---|---|---|---|---|
|BE|be_port|9060|FE&nbsp;&nbsp; --> BE|BE 上 thrift server 的端口，<br/>用于接收来自 FE 的请求。|
|BE|webserver_port|8040|BE ```<-->``` BE|BE 上的 http server 的端口。|
|BE|heartbeat_service_port|9050|FE --> BE|BE 上心跳服务端口（thrift），<br/>用于接收来自 FE 的心跳。|
|BE|brpc_port|8060|BE ```<-->``` BE|BE 上的 brpc 端口，<br/>用于 BE 之间通讯。|
|FE|**http_port**|**8030**|FE ```<-->``` 用户|FE 上的 http server 端口。|
|FE|rpc_port|9020|BE&nbsp;&nbsp; --> FE<br/> FE ```<-->``` FE|FE 上的 thrift server 端口。|
|FE|**query_port**|**9030**| FE ```<-->``` 用户|FE 上的 mysql server 端口。|
|FE|edit_log_port|9010|FE ```<-->``` FE|FE 上的 BDBJE 之间通信端口。|
|Broker|broker_ipc_port|8000|FE&nbsp;&nbsp; --> Broker <br/>BE&nbsp;&nbsp; --> Broker|Broker 上的 thrift server，<br/>用于接收请求。|

其中 `http_port`、`query_port` 是常用端口，前者用于网页访问 FE，后者用于 MySQL 客户端访问。

## FAQ

**Q**：如何设置 `ulimit`？

**A**：您可以通过在**所有机器**上运行 `ulimit -n 65536` 命令设置。如果系统提示您“没有权限”，请尝试以下方案：
  
首先，请在 **/etc/security/limits.conf** 添加如下配置：
  
```Plain Text
# 4个元素，具体可以参考 limits.conf 中的说明，*代表所有用户。
* soft nofile 65535
* hard nofile 65535
```
  
然后，请在 **/etc/pam.d/login** 和 **/etc/pam.d/sshd** 中添加如下配置：
  
```Plain Text
session  required  pam_limits.so
```
  
最后，请确认 **/etc/ssh/sshd_config** 中存在 **UsePAM yes**。如果没有，请添加该参数，并运行 `restart sshd`。

**Q**：安装 Python 时遇到问题 `__init__() takes 2 arguments (4 given)`，如何处理？
**A**：如果在安装 Python 时遇到问题 `__init__() takes 2 arguments (4 given)`，请执行如下步骤：

首先，请运行 `which python` 命令确认 Python 安装路径为 **/usr/bin/python**。
然后，请删除 python-setuptools 安装包：

```shell
yum remove python-setuptools
```

接下来，请删除 setuptool 相关文件。

```shell
rm /usr/lib/python2.7/site-packages/setuptool* -rf
```

最后，您需要获取 **ez_setup.py** 文件。

```shell
wget https://bootstrap.pypa.io/ez_setup.py -O - | python
```
