# 部署 Broker 节点

本文介绍如何部署管理 Broker 节点。通过 Broker，StarRocks 可读取对应数据源（如 HDFS、S3）上的数据，利用自身的计算资源对数据进行预处理和导入。除此之外，Broker 也被应用于数据导出，备份恢复等功能。

建议您在每个部署 BE 节点的机器上部署一个 Broker 节点，并将所有 Broker 节点添加到同一 `broker_name` 下。Broker 节点在处理任务时会自动调度数据传输压力。

Broker 节点与 BE 节点之间使用网络传输数据。当 Broker 节点和 BE 节点部署在相同机器时，会优先选择本地 BE 节点进行数据传输。

## 准备工作

请参照[部署前提条件](../deployment/deployment_prerequisites.md)、[检查环境配置](../deployment/environment_configurations.md)、[准备部署文件](../deployment/prepare_deployment_files.md)完成准备工作。

## 启动 Broker 服务

以下操作在 BE 实例上执行。

1. 进入先前准备好的 [StarRocks Broker 部署文件](../deployment/prepare_deployment_files.md)所在路径，修改 Broker 配置文件 **apache_hdfs_broker/conf/apache_hdfs_broker.conf**。

   如果 Broker 节点的 HDFS Thrift RPC 端口（`broker_ipc_port`，默认值：`8000`）被占用，您必须在 Broker 配置文件中为其分配其他可用端口。

   ```YAML
   broker_ipc_port = aaaa        # 默认值：8000
   ```

   下表列出了 Broker 支持的配置项。

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
   | sys_log_verbose_modules | com.starrocks | N/A | StarRocks 为其生成系统日志的模块。 有效值是 BE 中的 namespace，包括 `starrocks`、`starrocks::debug`、`starrocks::fs`、`starrocks::io`、`starrocks::lake`、`starrocks::pipeline`、`starrocks::query_cache`、`starrocks::stream` 以及 `starrocks::workgroup`。 |

2. 启动 Broker 节点。

   ```bash
   ./apache_hdfs_broker/bin/start_broker.sh --daemon
   ```

3. 查看 Broker 日志，检查 Broker 节点是否启动成功。

   ```Bash
   cat apache_hdfs_broker/log/apache_hdfs_broker.log | grep thrift
   ```

4. 在其他 BE 实例上重复以上步骤，即可启动新的 Broker 节点。

## 添加 Broker 节点至集群

以下过程在 MySQL 客户端实例上执行。您必须安装 MySQL 客户端（5.5.0 或更高版本）。

1. 通过 MySQL 客户端连接到 StarRocks。您需要使用初始用户 `root` 登录，密码默认为空。

   ```Bash
   # 将 <fe_address> 替换为您连接的 FE 节点的 IP 地址（priority_networks）或 FQDN，
   # 并将 <query_port>（默认：9030）替换为您在 fe.conf 中指定的 query_port。
   mysql -h <fe_address> -P<query_port> -uroot
   ```

2. 执行以下 SQL 将 Broker 节点添加至集群。

   ```sql
   ALTER SYSTEM ADD BROKER <broker_name> "<broker_ip>:<broker_ipc_port>";
   ```

   > **NOTE**
   >
   > - 您可以通过一条 SQL 添加多个 Broker 节点。每对 `<broker_ip>:<broker_ipc_port>` 代表一个 Broker 节点。
   > - 您可以添加多个具有相同 `broker_name` 的 Brokers 节点。

3. 执行以下 SQL 查看 Broker 节点状态。

```sql
SHOW PROC "/brokers"\G
```

示例

```plain text
MySQL [(none)]> SHOW PROC "/brokers"\G
*************************** 1. row ***************************
          Name: broker1
            IP: x.x.x.x
          Port: 8000
         Alive: true
 LastStartTime: 2022-05-19 11:21:36
LastUpdateTime: 2022-05-19 11:28:31
        ErrMsg:
1 row in set (0.00 sec)
```

如果字段 `Alive` 为 `true`，说明该 Broker 节点正常启动并加入集群。

## 停止 Broker 节点

运行以下命令停止 Broker 节点。

```bash
./bin/stop_broker.sh --daemon
```

## 升级 Broker 节点

1. 进入 Broker 节点工作路径，并停止该节点。

   ```Bash
   # 将 <broker_dir> 替换为 Broker 节点的部署目录。
   cd <broker_dir>/apache_hdfs_broker
   sh bin/stop_broker.sh
   ```

2. 替换部署文件原有路径 **bin** 和 **lib** 为新版本的部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/lib  .   
   cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/bin  .
   ```

3. 启动该 Broker 节点。

   ```Bash
   sh bin/start_broker.sh --daemon
   ```

4. 查看节点是否启动成功。

   ```Bash
   ps aux | grep broker
   ```

5. 重复以上步骤升级其他 Broker 节点。

## 降级 Broker 节点

1. 进入 Broker 节点工作路径，并停止该节点。

   ```Bash
   # 将 <broker_dir> 替换为 Broker 节点的部署目录。
   cd <broker_dir>/apache_hdfs_broker
   sh bin/stop_broker.sh
   ```

2. 替换部署文件原有路径 **bin** 和 **lib** 为新版本的部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/lib  .   
   cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/bin  .
   ```

3. 启动该 Broker 节点。

   ```Bash
   sh bin/start_broker.sh --daemon
   ```

4. 查看节点是否启动成功。

   ```Bash
   ps aux | grep broker
   ```

5. 重复以上步骤降级其他 Broker 节点。
