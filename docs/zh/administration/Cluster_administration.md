# 管理集群

本文介绍如何管理维护 StarRocks 集群，包括启停集群，升级回滚等操作。

## 启动集群

在启动 StarRocks 集群之前，您需要确认相关节点的配置文件是否正确。

### 启动 FE 集群

#### 确认 FE 配置

启动 FE 节点前，您需要确认配置文件 **fe/conf/fe.conf** 中元数据路径 `meta_dir` 和相关通信端口的配置项是否正确。

|配置项|描述|默认值|
|---|---|---|
|meta_dir|FE 节点存储元数据的路径，您需要提前创建相应路径。|`${STARROCKS_HOME}`/meta|
|http_port|FE 节点的 HTTP Server 端口。|8030|
|rpc_port|FE 节点的 Thrift Server 端口。|9020|
|query_port|FE 节点的 MySQL Server 端口。|9030|
|edit_log_port|FE 集群（Leader，Follower，以及 Observer）间的通信端口。|9010|

> 注意
>
> 由于 FE 节点的元数据涉及整个 StarRocks 系统，十分关键，建议您将 `meta_dir` 部署于单独路径下。

#### 启动 FE 进程

进入 FE 进程的部署路径并运行命令启动服务。启动多 FE 节点时，您需要逐台启动 Follower FE 节点。

```shell
cd StarRocks-x.x.x/fe
sh bin/start_fe.sh --daemon
```

为了保证 FE 高可用，您需要部署多个 FE 节点。我们建议您部署 3 个 FE 节点，其中包含 1 个 Leader FE 节点和 2 个 Follower FE 节点。

在初次部署时，第一个启动的 FE 会默认成为 Leader FE，后启动的 Follower FE 在**首次启动**时需要指定已存活在集群中的 Follower FE 作为 helper 节点（仅首次启动时需要指定，helper 节点通常选用 Leader FE 节点）。

```shell
cd StarRocks-x.x.x/fe
sh bin/start_fe.sh --helper <Leader_FE_IP>:<edit_log_port> --daemon
```

> 注意
>
> 当拥有多个 Follower FE 节点时，集群内需要有半数以上的 Follower FE 节点存活才能够选举出 Leader FE 节点，从而提供查询服务。

每启动一台 FE 节点后，建议您验证该节点是否启动成功。您可以通过发送查询的方式进行验证。

### 启动 BE 集群

#### 确认 BE 配置

启动 BE 节点前，您需要确认配置文件 **be/conf/be.conf** 中数据存路径 `storage_root_path` 和相关通信端口的配置项是否正确。

|配置项|描述|默认值|
|---|---|---|
|storage_root_path|BE 节点存储数据路径，您需要提前创建相应路径。建议您为每个磁盘创建一个路径。|`${STARROCKS_HOME}`/storage|
|be_port|BE 上 Thrift Server 的端口，用于接收来自 FE 的请求。|9060|
|webserver_port|BE 上的 HTTP Server 的端口。|8040|
|heartbeat_service_port|BE 上 Thrift server 端口，用于接收来自 FE 的心跳。|9050|
|brpc_port|BE 节点间的通讯端口。|8060|

#### 启动 BE 进程

进入 BE 进程的部署路径并运行命令启动服务。

```shell
cd StarRocks-x.x.x/be
sh bin/start_be.sh --daemon
```

### 启动 CN 集群 (可选)

#### 确认 CN 配置

|配置项|描述|默认值|
|---|---|---|
|thrift_port|CN 上 Thrift Server 的端口，用于接收来自 FE 的请求。|9060|
|webserver_port|CN 上的 HTTP Server 的端口。|8040|
|heartbeat_service_port|CN 上 Thrift server 端口，用于接收来自 FE 的心跳。|9050|
|brpc_port|CN 与 BE 节点间的通讯端口。|8060|

#### 启动 CN 进程

进入 CN 进程的部署路径并运行命令启动服务。

```shell
cd StarRocks-x.x.x/be
sh bin/start_cn.sh --daemon
```

### 确认集群健康状态

在 FE、BE、CN 启动完成之后，您需要检查进程状态，以确定服务正常启动。

* 确认 FE 集群启动状态。

```shell
http://<fe_host>:<fe_http_port>/api/bootstrap
```

若返回 `{"status": "OK", "msg": "Success"}`，则集群正常启动。

* 确认 BE 集群启动状态。

```shell
http://<be_host>:<be_http_port>/api/health
```

若返回 `{"status": "OK", "msg": "To Be Added"}`，则集群正常启动。

* 确认 CN 集群启动状态。

```shell
http://<cn_host>:<cn_http_port>/api/health
```

若返回 `{"status": "OK", "msg": "To Be Added"}`，则集群正常启动。

确认正常启动后，如果执行查询时需要使用 CN 节点，扩展算力，则需要设置系统变量 [`prefer_compute_node`、`use_compute_nodes`](../reference/System_variable.md
)。

## 停止集群

### 停止 FE 集群

进入 FE 路径，运行命令停止 FE 集群。

```shell
cd StarRocks-x.x.x/fe
sh bin/stop_fe.sh
```

### 停止 BE 集群

进入 BE 路径，运行命令停止 BE 集群。

```shell
cd StarRocks-x.x.x/be
sh bin/stop_be.sh
```

### 停止 CN 集群

进入 CN 路径，运行命令停止 CN 集群。

```shell
cd StarRocks-x.x.x/be
sh bin/stop_cn.sh
```

## 升级集群

您可以通过滚动升级的方式平滑升级 StarRocks。StarRocks 的版本号遵循 Major.Minor.Patch 的命名方式，分别代表重大版本，大版本以及小版本。

> 注意
>
> * 由于 StarRocks 保证 BE 后向兼容 FE，因此您需要**先升级 BE 节点，再升级 FE 节点**。错误的升级顺序可能导致新旧 FE、BE 节点不兼容，进而导致 BE 节点停止服务。
> * StarRocks 2.0 之前的大版本升级时必须**逐个大版本**升级，2.0 之后的版本可以**跨大版本**升级。更为推荐的稳妥方式是逐个版本升级，比如 1.19->2.0->2.1->2.2->2.3->2.4。StarRocks 2.2 是当前的长期支持版本（Long Term Support，LTS），维护期为半年以上。
>
> |版本|可直接升级版本|注意事项|是否为 LTS 版本|
> |----|------------|--------|--------------|
> |1.18.x||更早版本需要按照 <a href="update_from_dorisdb.md">标准版 DorisDB 升级到社区版 StarRocks</a> 操作。|否|
> |1.19.x|必须从1.18.x升级||否|
> |2.0.x|必须从1.19.x升级|升级过程中需要暂时关闭 Clone。|否|
> |2.1.x|必须从2.0.x 升级|灰度升级前需要修改 <code>vector_chunk_size</code> 和 <code>batch_size</code>。|否|
> |2.2.x|可以从2.0.x 或 2.1.x 升级|回滚需要配置 <code>ignore_unknown_log_id</code> 为 <code>true</code>。|是|
> |2.3.x|可以从2.0.x、2.1.x 或 2.2.x 升级|不建议跨版本回滚。回滚需要配置 <code>ignore_unknown_log_id</code> 为 <code>true</code>。|否|
> |2.4.x|可以从2.0.x、2.1.x、2.2.x 或 2.3.x 升级|不建议跨版本回滚。如果您开启了 [FQDN 访问](../administration/enable_fqdn.md)，需先将集群修改为 IP 地址访问。|否|

### 下载安装文件

在完成数据正确性验证后，将新版本的 BE 和 FE 节点的安装包下载并分发至各自路径下。您也可以 [在 Docker 中编译](Build_in_docker.md) 对应 tag 的源码。建议您选择小版本号最高的版本。

### 升级 BE 前的准备

为了避免 BE 重启期间不必要的 Tablet 修复，进而影响升级后的集群性能，建议在升级前先在 Leader FE 上执行如下命令以禁用 Tablet 调度功能，

```sql
admin set frontend config ("max_scheduling_tablets"="0");
admin set frontend config ("disable_balance"="true");
admin set frontend config ("disable_colocate_balance"="true");
```

在**所有** BE 重启升级完成后，通过 `show backends` 命令确认所有 BE 的 `Alive` 状态为 `true` 后，启用 Tablet 调度功能，

```sql
admin set frontend config ("max_scheduling_tablets"="2000");
admin set frontend config ("disable_balance"="false");
admin set frontend config ("disable_colocate_balance"="false");
```

### 测试 BE 升级的正确性

1. 选择任意一个 BE 节点，停止该节点。
2. 替换改节点路径下的 **bin** 和 **lib** 文件夹。
3. 启动该 BE 节点，通过 BE 日志 **be.INFO** 查看是否启动成功。
4. 如果该 BE 节点启动失败，您可以先排查失败原因。如果错误不可恢复，您可以直接通过 `DROP BACKEND` 删除该 BE、清理数据后，使用上一个版本的 **starrocks_be** 重新启动该 BE 节点。然后通过 `ADD BACKEND` 重新添加 BE 节点。

> **警告**
>
> **该方法会导致系统丢失一个数据副本，请务必确保 3 个副本完整的情况下执行这个操作。**

### 升级 BE 节点

1. 进入 BE 路径，停止 BE 节点。

    ```shell
    cd StarRocks-x.x.x/be
    sh bin/stop_be.sh
    ```

2. 替换 BE 节点相关文件。

    > 注意
    >
    > BE 节点小版本升级中（例如从 2.0.x 升级到 2.0.y），您只需升级 **/lib/starrocks_be**。而大版本升级中（例如从 2.0.x 升级到 2.x.x），您需要替换 BE 节点路径下的 **bin** 和 **lib** 文件夹。

    以下示例以大版本升级为例。

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/be/lib  .
    cp -r /tmp/StarRocks-x.x.x/be/bin  .
    ```

3. 启动 BE 节点。

    ```shell
    sh bin/start_be.sh --daemon
    ```

4. 确认当前节点启动成功。

    ```shell
    ps aux | grep starrocks_be
    ```

5. 重复以上步骤，升级其他 BE 节点。

### 测试 FE 升级的正确性

> 注意
>
> 由于 FE 元数据极其重要，而升级出现异常可能导致所有数据丢失，请谨慎升级。

1. 在测试开发环境中单独使用新版本部署一个测试用的 FE 进程。
2. 修改测试用的 FE 的配置文件 **fe.conf**，将所有端口设置为与生产环境不同。
3. 在 **fe.conf** 添加配置 `cluster_id = 123456`。
4. 在 **fe.conf** 添加配置 `metadata_failure_recovery = true`。
5. 拷贝生产环境中 Leader FE 的元数据路径 **meta** 到测试环境。
6. 修改测试环境中 **meta/image/VERSION** 文件中的 `cluster_id` 为 `123456`（即与第 3 步中相同）。
7. 在测试环境中，运行 `sh bin/start_fe.sh` 启动 FE。
8. 通过 FE 日志 **fe.log** 验证测试 FE 节点是否成功启动。
9. 如果启动成功，运行 `sh bin/stop_fe.sh` 停止测试环境的 FE 节点进程。

> 说明
>
> 以上第 2 至 第 6 步的目的是防止测试环境的 FE 启动后，错误连接到线上环境中。

### 升级 FE 节点

升级 FE 集群时，您需要先升级各 Follower FE 节点，最后升级 Leader FE 节点，从而在 Follower FE 节点升级失败时可以提前发现问题，防止其影响集群查询功能。

1. 进入 FE 路径，停止 FE 节点。

    ```shell
    cd StarRocks-x.x.x/fe
    sh bin/stop_fe.sh
    ```

2. 替换 FE 相关文件。

    > **注意**
    >
    > FE 节点小版本升级中（例如从 2.0.x 升级到 2.0.y），您只需升级 **/lib/starrocks-fe.jar**。而大版本升级中（例如从 2.0.x 升级到 2.x.x），您需要替换 FE 节点路径下的 **bin**、**lib** 和 **spark-dpp** 文件夹。

    以下示例以大版本升级为例。

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
    cp -r /tmp/StarRocks-x.x.x/fe/bin  .
    cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
    ```

3. 启动 FE 节点。

    ```shell
    sh bin/start_fe.sh --daemon
    ```

4. 确认当前节点启动成功。

    ```shell
    ps aux | grep StarRocksFE
    ```

5. 重复以上步骤，升级其他 Follower FE 节点，并最后升级 Leader FE 节点。

### 升级 CN 节点

升级 CN 节点时，您需要根据升级的版本，选择对应的升级方式：

* 如果为小版本升级（例如从 2.0.x 升级到 2.0.y），则只需要替换二进制文件 **/lib/starrocks_be**，然后重启服务。
* 如果为大版本升级 （例如从 2.0.x 升级到 2.x.x），您需要替换 BE 节点路径下的 bin 和 lib 文件夹。

以下示例以大版本升级为例。

1. 进入 CN 路径，停止 CN 节点。推荐使用 graceful 的停止方式。使用该种方式停止，CN 会等待当前运行的任务运行结束后再退出进程。

    ```Shell
    cd StarRocks-x.x.x/be
    sh bin/stop_cn.sh --graceful
    ```

2. 替换 CN 节点相关文件。

    ```Shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/be/lib  .
    cp -r /tmp/StarRocks-x.x.x/be/bin  .
    ```

3. 启动 CN 节点。

    ```Shell
    sh bin/start_cn.sh --daemon
    ```

4. 确认当前节点启动成功。

    ```Shell
    ps aux | grep starrocks_be
    ```

5. 重复以上步骤，升级其他 CN 节点。

### 升级 Broker

1. 进入 Broker 路径，停止 Broker 节点。

    ```shell
    cd StarRocks-x.x.x/apache_hdfs_broker
    sh bin/stop_broker.sh
    ```

2. 替换 Broker 相关文件。

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/lib  .   
    cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/bin  .
    ```

3. 启动 Broker 节点。

    ```shell
    sh bin/start_broker.sh --daemon
    ```

4. 在升级下一台实例之前，您需要确认当前实例启动成功。

### 关于 StarRocks 1.19 升级至 2.0.x

如果您需要将 StarRocks 1.19 升级至 2.0.x，您必须在升级过程中关闭 Clone 以避免触发旧版本中的 Bug。该操作同样适用于从 StarRocks 2.1.5 或之前版本升级至 2.1.6 或之后版本。

升级开始前，您需要关闭 Clone。

```sql
ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "0");
```

升级结束后，开启 Clone。

```sql
ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets" = "2000");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "100");
```

### 关于 StarRocks 2.0.x 灰度升级至 2.1.x

如果您需要将 StarRocks 2.0 灰度升级至 2.1，则必须确保所有 BE 节点拥有一致的 `chunk size`，即 BE 节点在每个批次中处理数据的行数。

1. 在配置文件 **conf/be.conf** 中，将所有 BE 节点的配置项 `vector_chunk_size` 设为 `4096`（默认值为 4096，单位为行）。配置项修改将在重启后生效。
2. 为 FE 节点设定全局变量 `batch_size` 小于或等于 `4096`（默认值和建议值为 4096，单位为行）。

示例：

```plain text
-- 查询 batch_size
mysql> show variables like '%batch_size%';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| batch_size    | 1024  |
+---------------+-------+
1 row in set (0.00 sec)

-- 设置 batch_size
mysql> set global batch_size = 4096;
```

#### 故障排除

Q：升级 StarRocks 2.0 至 2.1 及后期版本后，通过 Stream Load 将 JSON 格式的 BOOLEAN 类型数据导入至整形列中返回 NULL。我该如何处理？

A：StarRocks 2.0 版本将所有字段作为字符串解析，然后进行类型转换。将执行格式为 JSON 且类型为 BOOLEAN 的数据（`true` 和 `false`）导入至整形列中时，StarRocks 2.0 会将数据转化为 `0` 和 `1` 后导入。StarRocks 2.1 及后期版本重制了 JSON Parser，直接按照目标列类型提取 JSON 字段，从而导致最终得到的数据为 NULL。

您可以通过在 [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md) 导入命令 `columns` 项中添加表达式 `tmp, target=if(tmp,1,0)` 解决该问题。完整导入命令如下：

```shell
curl --location-trusted -u <username>:<password> \
-H "columns: <col_name_1>, <col_name_2>, <tmp>, <col_name_3>=if(<tmp>,1,0)" \
-T demo.csv -XPUT \
http://<fe_ip>:<fe_http_port>/api/<db_name>/<table_name>/_stream_load
```

## 回滚集群

您可以通过命令回滚 StarRocks 版本，回滚范围包括所有安装包名为 **StarRocks-xx** 的版本。当升级后发现出现异常状况，不符合预期，想快速恢复服务的，可以按照下述操作回滚版本。

> **注意**
>
> 回滚操作与升级操作的顺序相反，应当**先回滚 FE 节点，再回滚 BE 节点** 。错误的回滚顺序可能导致新旧 FE、BE 节点不兼容，进而导致 BE 节点停止服务。

### 前置工作

回滚 StarRocks 2.2 及以后版本前，您需要在所有 FE 节点的配置文件 **fe.conf** 中增加配置项 `ignore_unknown_log_id=true`，否则回滚后系统可能无法启动。重启 StarRocks，CheckPoint 完成后，您需要将该设置项恢复为 `ignore_unknown_log_id=false`，然后重启 FE 以恢复正常配置。

### 下载安装文件

在完成数据正确性验证后，将需要回滚的旧版本 BE 和 FE 节点安装包下载并分发至各自路径下。

### 回滚 FE 节点

您需要先回滚各 Follower FE 节点，最后回滚 Leader FE 节点。以下示例以大版本回滚为例。

> 注意
>
> * FE 节点小版本回滚中（例如从 2.0.y 回滚到 2.0.x），您只需回滚 **/lib/starrocks-fe.jar**。
> * FE 节点大版本回滚中（例如从 2.x.x 升级到 2.0.x），您需要替换 FE 节点路径下的 **bin**、**lib** 和 **spark-dpp** 文件夹。
> * 从 2.1.x 回滚至 2.0.x及以前版本需要通过以下命令关闭 pipeline：`set global enable_pipeline_engine = false`。

1. 进入 FE 路径，停止 FE 节点。

    ```shell
    cd StarRocks-x.x.x/fe
    sh bin/stop_fe.sh
    ```

2. 替换 FE 相关文件。

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
    cp -r /tmp/StarRocks-x.x.x/fe/bin  .
    cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
    ```

3. 启动 FE 节点。

    ```shell
    sh bin/start_fe.sh --daemon
    ```

4. 确认当前节点启动成功。

    ```shell
    ps aux | grep StarRocksFE
    ```

5. 重复以上步骤，回滚其他 Follower FE 节点，并最后回滚 Leader FE 节点。

### 回滚 BE 节点前的准备

为了避免 BE 重启期间不必要的 Tablet 修复，进而影响回滚后的集群性能，建议在回滚前先在 Leader FE 上执行如下命令以禁用 Tablet 调度功能，

```sql
admin set frontend config ("max_scheduling_tablets"="0");
admin set frontend config ("disable_balance"="true");
admin set frontend config ("disable_colocate_balance"="true");
```

在**所有** BE 重启回滚完成后，通过 `show backends` 命令确认所有 BE 的 `Alive` 状态为 `true` 后，启用 Tablet 调度功能，

```sql
admin set frontend config ("max_scheduling_tablets"="2000");
admin set frontend config ("disable_balance"="false");
admin set frontend config ("disable_colocate_balance"="false");
```

### 回滚 BE 节点

以下示例以大版本回滚为例。

> **注意**
>
> BE 节点小版本回滚中（例如从 2.0.y 回滚到 2.0.x），您只需回滚 **/lib/starrocks_be**。而大版本回滚中（例如从 2.x.x 升级到 2.0.x），您需要替换 BE 节点路径下的 **bin** 和 **lib** 文件夹。

1. 进入 BE 路径，停止 BE 节点。

    ```shell
    cd StarRocks-x.x.x/be
    sh bin/stop_be.sh
    ```

2. 替换 BE 节点相关文件。

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/be/lib  .
    cp -r /tmp/StarRocks-x.x.x/be/bin  .
    ```

3. 启动 BE 节点。

    ```shell
    sh bin/start_be.sh --daemon
    ```

4. 确认当前节点启动成功。

    ```shell
    ps aux | grep starrocks_be
    ```

5. 重复以上步骤，回滚其他 BE 节点。

### 回滚 Broker

1. 进入 Broker 路径，停止 Broker 节点。

    ```shell
    cd StarRocks-x.x.x/apache_hdfs_broker
    sh bin/stop_broker.sh
    ```

2. 替换 Broker 相关文件。

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/lib  .   
    cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/bin  .
    ```

3. 启动 Broker 节点。

    ```shell
    sh bin/start_broker.sh --daemon
    ```

4. 在升级下一台实例之前，您需要确认当前实例启动成功。
