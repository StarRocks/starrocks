---
displayed_sidebar: "Chinese"
---

# 部署 Broker 节点

本文介绍如何配置部署 Broker。

通过 Broker，StarRocks 可读取对应数据源（如 HDFS、S3）上的数据，利用自身的计算资源对数据进行预处理和导入。除此之外，Broker 也被应用于数据导出，备份恢复等功能。  
Broker 与 BE 之间使用网络传输数据，当 Broker 和 BE 部署在相同机器时会优先选择本地节点进行连接。  
部署 Broker 节点的数量建议与 BE 节点数量相等，并将所有 Broker 添加到相同的 Broker name 下（Broker 在处理任务时会自动调度数据传输压力）。

## 下载并解压安装包

[下载](https://www.mirrorship.cn/zh-CN/download/community) StarRocks 并解压二进制安装包。

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> **注意**
>
> 将以上文件名修改为下载的二进制安装包名。

## 配置 Broker 节点

进入 **StarRocks-x.x.x/apache_hdfs_broker** 路径。

```bash
cd StarRocks-x.x.x/apache_hdfs_broker
```

> **注意**
>
> 将以上路径名修改为解压后的路径名。

修改 Broker 节点配置文件 **conf/apache_hdfs_broker.conf**。因默认配置即可启动集群，以下示例并未修改 Broker 点配置。您可以直接复制自己的 HDFS 集群配置文件并粘贴至 **conf** 路径下。

| 配置项 | 默认值 | 单位 | 描述 |
| ------------------------- | ------------------ | ------ | ------------------------------------------------------------ |
| hdfs_read_buffer_size_kb | 8192 | KB | 用于从 HDFS 读取数据的内存的大小。 |
| hdfs_write_buffer_size_kb | 1024 | KB | 用于向 HDFS 写入数据的内存的大小。 |
| client_expire_seconds | 300 | Second | 客户端过期时间。如果在指定时间后未收到任何 ping，客户端会话将被删除。 |
| broker_ipc_port | 8000 | N/A | HDFS thrift RPC 端口。 |
| disable_broker_client_expiration_checking | false | N/A | 是否关闭过期 OSS 文件句柄的检查和清除。清除在某些情况下会导致 OSS 关闭时 Broker 卡住。为避免这种情况，您可以将此参数设置为 `true` 以禁用检查。 |
| sys_log_dir | `${BROKER_HOME}/log` | N/A | 用于存放系统日志（包括 INFO、WARNING、ERROR、FATAL）的目录。 |
| sys_log_level | INFO | N/A | 日志级别。有效值包括 INFO、WARNING、ERROR 和 FATAL。 |
| sys_log_roll_mode | SIZE-MB-1024 | N/A | 系统日志分卷方式。有效值包括 TIME-DAY、TIME-HOUR 和 SIZE-MB-nnn。默认值表示将日志拆分为每个 1 GB 的卷。 |
| sys_log_roll_num | 30 | N/A | 要保留的系统日志卷数。 |
| audit_log_dir | `${BROKER_HOME}/log` | N/A | 存储审计日志文件的目录。 |
| audit_log_modules | Empty string | N/A | StarRocks 为其生成审核日志条目的模块。默认情况下，StarRocks 会为 slow_query 模块和 query 模块生成审计日志。您可以指定多个模块，使用逗号（,）和一个空格分隔。|
| audit_log_roll_mode | TIME-DAY | N/A | 审计日志分卷方式。有效值包括 TIME-DAY、TIME-HOUR 和 SIZE-MB-nnn。 |
| audit_log_roll_num | 10 | N/A | 要保留的升级日志卷数。如果 `audit_log_roll_mode` 设置为 `SIZE-MB-nnn`，则此配置无效。 |
| sys_log_verbose_modules | com.starrocks | N/A | StarRocks 为其生成系统日志的模块。 有效值是 BE 中的 namespace，包括 `starrocks`、`starrocks::vectorized` 以及 `pipeline`。 |

## 启动 Broker 节点

通过以下命令启动 Broker。

```bash
./apache_hdfs_broker/bin/start_broker.sh --daemon
```

## 添加 Broker 节点

您可通过 MySQL 客户端连接 StarRocks 以添加或删除 Broker 节点。

```sql
ALTER SYSTEM ADD BROKER broker_name "host:port";
```

> **说明**
>
> - 默认配置中，Broker 节点的端口为 `8000`。
> - 同时添加多个 Broker 节点时，该组节点共同使用同一个 `broker_name`.

## 确认 Broker 启动成功

通过 MySQL 客户端确认 Broker 节点是否启动成功。

```sql
SHOW PROC "/brokers"\G
```

示例：

```plain text
MySQL [(none)]> SHOW PROC "/brokers"\G

*************************** 3. row ***************************
          Name: hdfs_broker
            IP: 172.26.xxx.x
          Port: 8000
         Alive: true
 LastStartTime: 2022-05-19 11:21:36
LastUpdateTime: 2022-05-19 11:28:31
        ErrMsg:

          Name: hdfs_broker
            IP: 172.26.198.2
          Port: 8000
         Alive: true
 LastStartTime: 2022-05-19 11:22:30
LastUpdateTime: 2022-05-19 11:28:31
        ErrMsg:

          Name: hdfs_broker
            IP: 172.26.198.3
          Port: 8000
         Alive: true
 LastStartTime: 2022-05-19 11:23:10
LastUpdateTime: 2022-05-19 11:28:31
        ErrMsg:
3 row in set (0.00 sec)
```

当 `Alive` 为 `true` 时，当前 Broker 节点正常接入集群。

## 停止 Broker 节点

运行以下命令停止 Broker 节点。

```bash
sh ./bin/stop_broker.sh --daemon
```

## 删除 Broker 节点

您可通过 MySQL 客户端连接 StarRocks 以添加或删除 Broker 节点。

```sql
ALTER SYSTEM DROP BROKER broker_name "host:port";
```
