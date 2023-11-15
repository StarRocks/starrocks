# 集群管理

StarRocks 集群提供服务过程中，有升级等需要启停的操作，本文主要介绍集群日常管理的常用操作步骤。

## 集群启停

启动服务之前需要保证配置文件满足要求。

### FE 启动

FE 启动前需要重点关注的配置项有：元数据放置目录(meta_dir)、通信端口。

***meta_dir***：描述 FE 存储元数据的目录，需要提前创建好相应目录，系统默认设置为 `meta_dir = ${STARROCKS_HOME}/meta`, 若自定义 meta_dir 存储位置，则在 `fe/conf/fe.conf` 中声明自定义路径即可。由于 FE 的元数据是整个系统的元数据，十分关键，建议不要和其他进程混布。

FE 配置的通信端口有四个:

|端口名称|默认端口|作用|
|---|---|---|
|http_port|8030|FE 上的 http server 端口|
|rpc_port|9020|FE 上的 thrift server 端口|
|query_port|9030|FE 上的 mysql server 端口|
|edit_log_port|9010|FE Group(Master, Follower, Observer)之间通信用的端口|

FE 进程的启动方式十分简单：

1. 进入 FE 进程的部署目录
  
2. 运行 `sh bin/start_fe.sh --daemon` 启动服务

FE 为了保证高可用，会部署多个节点。线上典型的部署方式是 3 个 FE(1 Master + 2 Follower)。

多节点启动的时候，需要逐台启动 Follower。如果是集群升级，可以先升级 Follower 节点，最后升级 Master（此举目的是如果 Follower 升级出错可以提前发现问题，从而不影响集群查询功能）, 需要注意的是，当拥有多数 Follower 时，集群需要存活半数以上的 Follower 才可以选举出 Master，从而提供查询服务。

任何一台 FE 的启动，都建议进行验证，可以通过发送查询的方式予以验证。

### BE 启动

BE 启动前需要重点关注的配置项有：数据放置目录(storage_root_path)、通信端口。

***storage_root_path*** 描述 BE 放置存储文件的地方，需要事先创建好相应目录，建议每个磁盘创建一个目录。

BE 配置的通信端口有四个:

|端口名称|默认端口|作用|
|---|---|---|
|be_port|9060|BE 上 thrift server 的端口，用于接收来自 FE 的请求|
|webserver_port|8040|BE 上的 http server 的端口|
|heartbeat_service_port|9050|BE 上心跳服务端口（thrift），用于接收来自 FE 的心跳|
|brpc_port|8060|BE 上的 brpc 端口，用于 BE 之间通讯|

### 确认集群健康状态

BE 和 FE 启动完成之后，需要检查进程状态，以确定服务正常启动。

* 运行 `http://be_host:be_http_port/api/health`  确认 BE 启动状态。

```shell
http://<be_host>:<be_http_port>/api/health
```

* 运行 `http://fe_host:fe_http_port/api/bootstrap` 确认 FE 启动状态。

  * 返回 `{"status": "OK", "msg": "Success"}` 表示启动正常。

```shell
http://<fe_host>:<fe_http_port>/api/bootstrap
```

* 进入 FE 目录 运行 `sh bin/stop_fe.sh`

* 进入 BE 目录 运行 `sh bin/stop_be.sh`

## 集群升级

StarRocks 可以通过滚动升级的方式，平滑进行升级。**升级顺序是先升级 BE，再升级 FE**。StarRocks 保证 BE 后向兼容 FE。升级的过程可以分为：测试升级的正确性，滚动升级，观察服务。

### 升级 BE 前的准备

为了避免 BE 重启期间不必要的 Tablet 修复，进而影响升级后的集群性能，建议在升级前先在 FE Leader 上执行如下命令以禁用 Tablet 调度功能，

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

* 在完成数据正确性验证后，将 BE 和 FE 新版本的二进制文件分发到各自目录下。

* 小版本升级（例 2.0.x 升级到 2.0.x），BE 只需升级 starrocks_be；FE 只需升级 starrocks-fe.jar。

* 大版本升级（例 2.0.x 升级到 2.x.x），则需要替换 FE、BE 的 bin 和 lib 文件夹。

### 升级

* 确认新版本的文件替换完成。

* 逐台重启 BE 后，再逐台重启 FE。
  
* 确认前一个实例启动成功后，再重启下一个实例。

#### BE 升级

```shell
cd be_work_dir
sh bin/stop_be.sh
mv lib lib.bak 
mv bin bin.bak
cp -r /tmp/StarRocks-SE-x.x.x/be/lib  .   
cp -r /tmp/StarRocks-SE-x.x.x/be/bin  .  
sh bin/start_be.sh --daemon
ps aux | grep starrocks_be
```

#### FE 升级

```shell
cd fe_work_dir
sh bin/stop_fe.sh
mv lib lib.bak 
mv bin bin.bak
cp -r /tmp/StarRocks-SE-x.x.x/fe/lib  .   
cp -r /tmp/StarRocks-SE-x.x.x/fe/bin  .
sh bin/start_fe.sh --daemon
ps aux | grep StarRocksFE
```

#### Broker 升级

```shell
cd broker_work_dir 
mv lib lib.bak 
mv bin bin.bak
cp -r /tmp/StarRocks-SE-x.x.x/apache_hdfs_broker/lib  .   
cp -r /tmp/StarRocks-SE-x.x.x/apache_hdfs_broker/bin  .
sh bin/stop_broker.sh
sh bin/start_broker.sh --daemon
ps aux | grep broker
```

特别注意：

BE、FE 启动顺序不能颠倒。因为如果升级导致新旧 FE、BE 不兼容，从新 FE 发出的命令可能会导致旧的 BE 挂掉。

## 回滚方案

StarRocks 各版本（包名为 StarRocks-xx 的版本）之间都是支持回滚操作的，回滚操作与升级操作一致，顺序相反。**回滚顺序是先回滚 FE，再回滚 BE** 。当升级后发现出现异常状况，不符合预期，想快速恢复服务的，可以按照下述操作回滚版本。

### 回滚准备

### 回滚 BE 节点前的准备

为了避免 BE 重启期间不必要的 Tablet 修复，进而影响回滚后的集群性能，建议在回滚前先在 FE Leader 上执行如下命令以禁用 Tablet 调度功能，

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

* 小版本回滚（例 2.0.x 回滚至 2.0.x），BE 只需替换 starrocks_be；FE 只需替换 starrocks-fe.jar。

* 大版本回滚（例 2.x.x 回滚至 2.0.x），则需要替换 FE、BE 的 bin 和 lib 文件夹。

### 回滚

* 确认文件替换完成。

* 逐台重启 FE 后，再逐台重启 BE。
  
* 确认前一个实例启动成功后，再重启下一个实例。

#### FE 回滚

```shell
cd fe_work_dir 
mv lib libtmp.bak 
mv bin bintmp.bak 
mv lib.bak lib   
mv bin.bak bin 
sh bin/stop_fe.sh
sh bin/start_fe.sh --daemon
ps aux | grep StarRocksFE
```

#### BE 回滚

```shell
cd be_work_dir 
mv lib libtmp.bak 
mv bin bintmp.bak 
mv lib.bak lib
mv bin.bak bin
sh bin/stop_be.sh
sh bin/start_be.sh --daemon
ps aux | grep starrocks_be
```

#### Broker 回滚

```shell
cd broker_work_dir 
mv lib libtmp.bak 
mv bin bintmp.bak
mv lib.bak lib
mv bin.bak bin
sh bin/stop_broker.sh
sh bin/start_broker.sh --daemon
ps aux | grep broker
```

特别注意：

BE、FE 回滚顺序不能颠倒。因为如果回滚导致新旧 FE、BE 不兼容，从高版本 FE 发出的命令可能会导致低版本的 BE 挂掉。

### StarRocks 2.0 灰度升级至 2.1 的注意事项

如果需要将 StarRocks 2.0 灰度升级至 2.1，则需要提前进行如下设置，确保所有 BE 节点的 chunk size（即 BE 节点在每个批次中处理数据的行数）一致。

* 所有 BE 节点的配置项 `vector_chunk_size` 是 4096（默认值为 4096，单位为行）。
  > 您需要在 BE 节点的配置文件 be.conf 中设置配置项 `vector_chunk_size`。配置项设置后且需要重启才能生效。
* FE 节点的全局变量 `batch_size` 小于或等于 4096 （默认值和建议值为 4096，单位为行）。

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

## 正确性验证

### 测试 BE 升级的正确性

* 任意选择一个 BE 节点，部署最新的 starrocks_be 二进制文件。

* 重启该 BE 节点，通过 BE 日志 be.INFO 查看是否启动成功。
  
* 如果启动失败，可以先排查原因。如果错误不可恢复，可以直接通过 `DROP BACKEND` 删除该 BE、清理数据后，使用上一个版本的 starrocks_be 重新启动 BE。然后重新 `ADD BACKEND`。（**该方法会导致丢失一个数据副本，请务必确保 3 副本完整的情况下，执行这个操作！！！**）
  
### 测试 FE 升级的正确性

* 单独使用新版本部署一个测试用的 FE 进程（比如自己本地的开发机）

* 修改测试用的 FE 的配置文件 fe.conf，将 **所有端口设置为与线上不同**。
  
* 在 fe.conf 添加配置：cluster_id = 123456
  
* 在 fe.conf 添加配置：metadata_failure_recovery = true
  
* 拷贝线上环境 Master FE 的元数据目录 meta 到测试环境
  
* 将拷贝到测试环境中的 meta/image/VERSION 文件中的 cluster_id 修改为 123456（即与第 3 步中相同）
* 在测试环境中，运行 `sh bin/start_fe.sh` 启动 FE
  
* 通过 FE 日志 fe.log 观察是否启动成功。
  
* 如果启动成功，运行 `sh bin/stop_fe.sh` 停止测试环境的 FE 进程。

**以上 2-6 步的目的是防止测试环境的 FE 启动后，错误连接到线上环境中。**

**因为 FE 元数据十分关键，升级出现异常可能导致所有数据丢失，请特别谨慎，尤其是大版本的升级。**
