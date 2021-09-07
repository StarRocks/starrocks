# 集群管理

## 集群启停

StarRocks集群部署完成之后，紧接着就是启动集群，提供服务。启动服务之前需要保证配置文件满足要求。

### FE启动

FE启动前需要重点关注的配置项有：元数据放置目录(meta_dir)、通信端口。

***meta_dir***：描述FE存储元数据的目录，需要提前创建好相应目录，并在配置文件中写明。由于FE的元数据是整个系统的元数据，十分关键，建议不要和其他进程混布。

FE配置的通信端口有四个:

|端口名称|默认端口|作用|
|---|---|---|
|http_port|8030|FE 上的 http server 端口|
|rpc_port|9020|FE 上的 thrift server 端口|
|query_port|9030|FE 上的 mysql server 端口|
|edit_log_port|9010|FE Group(Master, Follower, Observer)之间通信用的端口|

FE进程的启动方式十分简单：

* 进入FE进程的部署目录
  
* 运行`sh bin/start_fe.sh --daemon`启动服务

 FE为了保证高可用，会部署多个节点。线上典型的部署方式是3个FE(1 Master + 2 Follower)。

多节点启动的时候，建议先逐台启动Follower，然后启动Master（如果Follower出错可以提前发现）。

任何一台FE的启动，都建议进行验证，可以通过发送查询的方式予以验证。

### BE启动

BE启动前需要重点关注的配置项有：数据放置目录(storage_root_path)、通信端口。

***storage_root_path***描述BE放置存储文件的地方，需要事先创建好相应目录，建议每个磁盘创建一个目录。

BE配置的通信端口有三个:

|端口名称|默认端口|作用|
|---|---|---|
|be_port|9060|BE 上 thrift server 的端口，用于接收来自 FE 的请求|
|webserver_port|8040|BE 上的 http server 的端口|
|heartbeat_service_port|9050|BE 上心跳服务端口（thrift），用户接收来自 FE 的心跳|

### 确认集群健康状态

BE和FE启动完成之后，需要检查进程状态，以确定服务正常启动。

* 运行 `http://be_host:be_http_port3/api/health`  确认BE启动状态

  * 返回 {"status": "OK","msg": "To Be Added"} 表示启动正常。

* 运行 `http://fe_host:fe_http_port/api/bootstrap` 确认FE启动状态。

  * 返回 {"status":"OK","msg":"Success"} 表示启动正常。

### 集群停止

* 进入FE目录 运行`sh bin/stop_fe.sh`

* 进入BE目录 运行`sh bin/stop_be.sh`

### 集群升级

StarRocks可以通过滚动升级的方式，平滑进行升级。**升级顺序是先升级BE，再升级FE**。StarRocks保证BE后向兼容FE。升级的过程可以分为：测试升级的正确性，滚动升级，观察服务。

### 升级准备

* 在完成数据正确性验证后，将 BE 和 FE 新版本的二进制文件分发到各自目录下。

* 小版本升级，BE 只需升级 starrocks_be；FE 只需升级 starrocks-fe.jar。
* 大版本升级，则可能需要升级其他文件（包括但不限于 bin/ lib/ 等）；如果不确定是否需要替换其他文件，全部替换即可。

### 升级

* 确认新版本的文件替换完成。

* 逐台重启 BE 后，再逐台重启 FE。
  
* 确认前一个实例启动成功后，再重启下一个实例。

#### BE 升级

```shell
cd be_work_dir 
mv lib lib.bak 
cp -r /tmp/StarRocks-SE-x.x.x/be/lib  .   
sh bin/stop_be.sh
sh bin/start_be.sh --daemon
ps aux | grep starrocks_be
```

#### FE 升级

```shell
cd fe_work_dir 
mv lib lib.bak 
cp -r /tmp/StarRocks-SE-x.x.x/fe/lib  .   
sh bin/stop_fe.sh
sh bin/start_fe.sh --daemon
ps aux | grep StarRocksFe
```

特别注意：

BE、FE启动顺序不能颠倒。因为如果升级导致新旧 FE、BE 不兼容，从新 FE 发出的命令可能会导致旧的 BE 挂掉。但是因为已经部署了新的 BE 文件，BE 通过守护进程自动重启后，即已经是新的 BE 了。

### 测试BE升级的正确性

* 任意选择一个BE节点，部署最新的starrocks_be二进制文件。

* 重启该BE节点，通过BE日志be.INFO查看是否启动成功。
  
* 如果启动失败，可以先排查原因。如果错误不可恢复，可以直接通过 `DROP BACKEND` 删除该 BE、清理数据后，使用上一个版本的 starrocks_be 重新启动 BE。然后重新 `ADD BACKEND`。（**该方法会导致丢失一个数据副本，请务必确保3副本完整的情况下，执行这个操作！！！**）
  
### 测试FE升级的正确性

* 单独使用新版本部署一个测试用的 FE 进程（比如自己本地的开发机）

* 修改测试用的 FE 的配置文件 fe.conf，将**所有端口设置为与线上不同**。
  
* 在fe.conf添加配置：cluster_id=123456
  
* 在fe.conf添加配置：metadata_failure_recovery=true
  
* 拷贝线上环境Master FE的元数据目录starrocks-meta到测试环境
  
* 将拷贝到测试环境中的 starrocks-meta/image/VERSION 文件中的 cluster_id 修改为 123456（即与第3步中相同）
* 在测试环境中，运行 `sh bin/start_fe.sh` 启动 FE
  
* 通过FE日志fe.log观察是否启动成功。
  
* 如果启动成功，运行 `sh bin/stop_fe.sh` 停止测试环境的 FE 进程。

**以上 2-6 步的目的是防止测试环境的FE启动后，错误连接到线上环境中。**

**因为FE元数据十分关键，升级出现异常可能导致所有数据丢失，请特别谨慎，尤其是大版本的升级。**
