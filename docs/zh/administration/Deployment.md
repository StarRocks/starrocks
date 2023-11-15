# 集群部署

StarRocks的集群部署分为两种模式，第一种是使用命令部署，第二种是使用 StarRocksManager 自动化部署。自动部署的版本只需要在页面上简单进行配置、选择、输入后批量完成，并且包含Supervisor进程管理、滚动升级、备份、回滚等功能。命令部署的方式适用于希望和自有运维系统打通的用户，有助于管理员理解StarRocks的内部运行机制，直接定位处理一些更复杂的问题。

> 当前 StarRocksManager 为企业版功能，需要试用的用户可以到 [StarRocks官网](https://www.starrocks.com/zh-CN/download) 咨询（网页最下面，点击「现在咨询」StarRocks企业版）。

## StarRocksManager部署

### 安装依赖

在所有需要部署StarRocks的节点上安装以下依赖:

* JDK (1.8 以上)  并且配置好JAVA_HOME (比如 `~/.bashrc` 中增加 `export` ).
* python (2.7 以上)
* python-setuptools (`yum install setuptools or apt-get install setuptools`)

另外StarRocksManager本身需要连接一个MySQL来存储Manager管理平台的数据。

### 安装StarRocksManager部署工具

解压以后

~~~shell
$ bin/install.sh -h
-[d install_path] install_path(default: /home/disk1/starrocks/starrocks-manager-20200101)
-[y python_bin_path] python_bin_path(default: /usr/bin/python)
-[p admin_console_port] admin_console_port(default: 19321)
-[s supervisor_http_port] supervisor_http_port(default: 19320)
$ bin/install.sh
~~~

该步骤会安装一个简单的web页面来帮助安装StarRocks数据库

### 安装部署StarRocks

* 首先需要配置一个安装好的MySQL数据库，此MySQL用于存储StarRocksManager的管理、查询、报警等信息

![配置 MySQL](../assets/8.1.1.3-1.png)

* 选择需要部署的节点，以及agent和supervisor的安装目录，agent负责采集机器的统计信息，Supervisor管理进程的启动停止，所有安装都在用户环境，不会影响系统环境。

![配置节点](../assets/8.1.1.3-2.png)

* **安装FE**： `meta dir`是StarRocks的元数据目录，和命令安装类似，建议配置一个独立的starrocks-meta和fe的log 目录，FE follower建议配置1或者3个，在请求压力比较大的情况可以酌情增加observer

![配置 FE 实例](../assets/8.1.1.3-3.png)

* **安装BE**： 端口的含义参考下面[端口列表](#端口列表)

![配置 BE 实例](../assets/8.1.1.3-4.png)

* **安装Broker**，建议在所有节点上都安装Broker

![HDFS Broker](../assets/8.1.1.3-5.png)

* **安装Center service**： center service负责从agent拉取信息后汇总存储在MySQL中，并提供监控报警的服务。这里的邮件服务用来配置接收报警通知的邮箱，可以填空，以后再进行配置。

![配置中心服务](../assets/8.1.1.3-6.png)

### 端口列表

|实例名称|端口名称|默认端口|通讯方向|说明|
|---|---|---|---|---|
|BE|be_port|9060|FE&nbsp;&nbsp; --> BE|BE 上 thrift server 的端口，<br/>用于接收来自 FE 的请求|
|BE|webserver_port|8040|BE `<-->` BE|BE 上的 http server 的端口|
|BE|heartbeat_service_port|9050|FE&nbsp;&nbsp; --> BE|BE 上心跳服务端口（thrift），<br/>用于接收来自 FE 的心跳|
|BE|brpc_port|8060|FE `<-->` BE<br/>BE `<-->` BE|BE 上的 brpc 端口，<br/>用于 BE 之间通讯|
|FE|**http_port**|**8030**|FE `<-->` 用户|FE 上的 http server 端口|
|FE|rpc_port|9020|BE&nbsp;&nbsp; --> FE<br/> FE `<-->` FE|FE 上的 thrift server 端口|
|FE|**query_port**|**9030**| FE `<-->` 用户|FE 上的 mysql server 端口|
|FE|edit_log_port|9010|FE `<-->` FE|FE 上的 bdbje 之间通信端口|
|Broker|broker_ipc_port|8000|FE&nbsp;&nbsp; --> Broker <br/>BE&nbsp;&nbsp; --> Broker|Broker 上的 thrift server，<br/>用于接收请求|

其中 http_port(8030)、query_port(9030) 是常用端口，前者用于网页访问 FE，后者用于 MySQL 客户端访问。

### FAQ

Q：如何设置 `ulimit`？
A：您可以通过在**所有机器**上运行 `ulimit -n 65536` 命令设置。如果系统提示您“没有权限”，请尝试以下方案：

首先，请在 **/etc/security/limits.conf** 添加如下配置：

~~~Plain Text
# 4个元素，具体可以参考 limits.conf 中的说明，*代表所有用户。
* soft nofile 65535
* hard nofile 65535
~~~

然后，请在 **/etc/pam.d/login** 和 **/etc/pam.d/sshd** 中添加如下配置：

~~~Plain Text
session  required  pam_limits.so
~~~

最后，请确认 **/etc/ssh/sshd_config** 中存在 **UsePAM yes**。如果没有，请添加该参数，并运行 `restart sshd`。

Q：安装 Python 时遇到问题 `__init__() takes 2 arguments (4 given)`，如何处理？
A：如果在安装 Python 时遇到问题 `__init__() takes 2 arguments (4 given)`，请执行如下步骤：

首先，请运行 `which python` 命令确认 Python 安装路径为 **/usr/bin/python**。
然后，请删除 python-setuptools 安装包：

~~~shell
yum remove python-setuptools
~~~

接下来，请删除 setuptool 相关文件。

~~~shell
rm /usr/lib/python2.7/site-packages/setuptool* -rf
~~~

最后，您需要获取 **ez_setup.py** 文件。

~~~shell
wget https://bootstrap.pypa.io/ez_setup.py -O - | python
~~~

## 手动部署

手动部署参考 [StarRocks手动部署](../quick_start/Deploy.md)。
