# 部署 Broker 节点

本文介绍如何配置部署 Broker。

通过 Broker，StarRocks 可读取对应数据源（如 HDFS、S3）上的数据，利用自身的计算资源对数据进行预处理和导入。除此之外，Broker 也被应用于数据导出，备份恢复等功能。  
Broker 与 BE 之间使用网络传输数据，当 Broker 和 BE 部署在相同机器时会优先选择本地节点进行链接。  
部署 Broker 节点的数量建议与 BE 节点数量相等，并将所有 Broker 添加到相同的 Broker name 下（Broker 在处理任务时会自动调度数据传输压力）。

## 下载并解压安装包

[下载](https://www.starrocks.com/zh-CN/download) StarRocks 并解压二进制安装包。

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

<br/>
