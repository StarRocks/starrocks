---
displayed_sidebar: "Chinese"
---

# 部署常见问题

本页列举了部署 StarRocks 时可能会遇到的常见问题及潜在解决方案。

## 如何在配置文件 **fe.conf** 中 `priority_networks` 参数下配置固定 IP？

**问题描述**

假设当前节点有两个 IP 地址：`192.168.108.23` 和 `192.168.108.43`。

* 如果您将 `priority_networks` 设定为 `192.168.108.23/24`，StarRocks 会将该地址识别为 `192.168.108.43`。
* 如果您将 `priority_networks` 设定为 `192.168.108.23/32`，启动后 StarRocks 会出错，并将该地址识别为 `127.0.0.1`。

**解决方案**

以上问题有以下两种解决方案：

* 删去 CIDR 后缀 `32` 或者将其改为 `28`。
* 将 StarRocks 升级至 2.1 或更新版本。

## 安装 BE 节点后启动失败，并返回错误 "StarRocks BE http service did not start correctly, exiting"。我该如何解决？

如果在安装 BE 后启动报错 `StarRocks Be http service did not start correctly,exiting`，该问题是 BE 节点 `be_http_port` 端口被占用导致。您需要修改 BE 配置文件 **be.conf** 中的 `be_http_port` 配置项并重启 BE 服务使配置生效。如果多次修改为未被占用的端口，系统仍然重复报错，您需要检查节点是否装有 Yarn 等程序，确认监听端口选择修改监听规则，或者 BE 的端口选取范围绕过。

<!--
## 在部署企业版 StarRocks 的过程当中，配置节点时报错：“Failed to Distribute files to node”。我该如何解决？

以上错误是由于 FE 节点间 setuptools 版本不匹配导致。您需要使用 root 权限在集群的所有机器上执行以下命令：

```shell
yum remove python-setuptools
rm /usr/lib/python2.7/site-packages/setuptool* -rf
wget https://bootstrap.pypa.io/ez_setup.py -O - | python
```
-->

## StarRocks 是否支持动态修改 FE、BE 配置项？

部分 FE 和 BE 节点的配置项支持动态修改。具体操作参考 [配置参数](../administration/FE_configuration.md)。

* 动态修改 FE 节点配置项：
  * 使用 SQL 方式动态修改：

    ```sql
    ADMIN SET FRONTEND CONFIG ("key" = "value");
    ```

    示例：

    ```sql
    --示例：
    ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "false");
    ```

  * 使用命令行方式动态修改：

    ```shell
    curl --location-trusted -u username:password \
    'http://<ip>:<fe_http_port>/api/_set_config?key=value'
    ```

    示例：

    ```plain text
    curl --location-trusted -u <username>:<password> \
    'http://192.168.110.101:8030/api/_set_config?enable_statistic_collect=true'
    ```

* 动态修改 BE 节点配置项：
  * 使用命令行方式动态修改：

    ```plain text
    curl -XPOST -u username:password \
    'http://<ip>:<be_http_port>/api/update_config?key=value'

> 注意
>
> 使用以上方式修改参数时，请确保当前用户拥有远程登录权限。

以下示例创建了用户 test 并赋予其相应权限。

```sql
CREATE USER 'test'@'%' IDENTIFIED BY '123456';
GRANT SELECT_PRIV ON . TO 'test'@'%';
```

## 为 BE 节点增加磁盘空间后，数据存储无法均衡负载且报错 “Failed to get scan range, no queryable replica found in tablet: xxxxx”。我该如何解决？

**问题描述**

该错误可能发生在往主键表 (Primary Key) 导入数据时，BE 节点磁盘空间不足，导致 BE Crash。扩容磁盘后，由于 PK 表目前还不支持 BE 内部磁盘间的均衡，数据存储无法负载均衡。

**解决方案:**

该问题（PK 表不支持 BE 内磁盘间均衡）目前仍在修复当中，您可以通过以下两种方式解决：

* 手动均衡数据存储负载，比如通过把使用率高的磁盘上的数据目录 copy 到一个磁盘空间更大的目录。
* 如果当前磁盘中的数据非重要数据，建议您直接删除掉磁盘并修改相应磁盘路径。如果系统继续报错，您需要通过 `TRUNCATE TABLE` 命令清除当前表中的数据。

## 重启集群时，FE 启动失败并报错 “Fe type:unknown ,is ready :false”。我该如何解决？

请确认 Leader FE 节点是否已启动。如果未启动，请尝试逐台重启集群中的 FE 节点。

## 安装集群时报错 “failed to get service info err”。我该如何解决？

请检查当前机器是否开启了 OpenSSH Daemon（SSHD）。

您可以通过以下命令查看 SSHD 状态。

```shell
/etc/init.d/sshd status
```

## BE 启动失败并报错 “Fail to get master client from cache. host= port=0 code=THRIFT_RPC_ERROR“。我该如何解决？

请通过以下命令检查 **be.conf** 中指定的端口是否被占用。

```shell
netstat  -anp  |grep  port
```

如果被占用，请更换其他空闲端口后重启。

## 通过 StarRocks Manager 升级集群时报错 “Failed to transport upgrade files to agent host. src:…”。我该如何解决？

以上错误由部署路径的磁盘空间不足所导致。在升级集群时，StarRocks Manager 会将新版本的二进制文件分发至各个节点，若部署目录的磁盘空间不足，则文件无法被分发，出现上述报错。

请检查对应的磁盘的存储空间。如存储空间不足，请扩展相应磁盘空间。

## 新扩容节点的 FE 状态正常，但是在 StarRocks Manager 的 **诊断** 页面下，该 FE 节点日志展示报错 “Failed to search log“。我该如何解决？

默认设置下，StarRocks Manager 会在 30 秒内去获取新部署 FE 节点路径配置。如果当前 FE 节点启动较慢或由于其他原因导致 30 秒内未响应就会出现上述问题。请通过 **/starrocks-manager-xxx/center/log/webcenter/log/web/drms.INFO** 路径检查 StarRocks Manager Web 日志，检索日志中是否含有 “Failed to update fe configurations” 报错。若有，请重启对应的 FE 服务，StarRocks Manager 会重新获取路径配置。

## 启动 FE 失败并报错 “exceeds max permissable delta:5000ms”。我该如何解决？

以上错误由于集群内不同机器的时差超过 5 秒导致。您需要通过校准机器时间解决该问题。

## 如果 BE 节点有多块磁盘做存储，如何设置 `storage_root_path` 配置项？

该配置项位于 BE 配置文件 **be.conf** 中。您可以使用 `;` 分隔多个磁盘路径。

## 添加新的 FE 节点至集群后报错 “invalid cluster id: xxxxxxxx”。我该如何解决？

以上错误是由于第一次启动集群时未使用 `--helper` 选项添加该 FE 节点，从而导致不同节点的元数据不一致。您需要将相应目录下的所有元数据清空，并通过 `--helper` 选项重新添加该 FE 节点至集群。

## 当前 FE 节点已经启动，且状态为 `transfer：follower`，但是调用 `show frontends` 命令返回 `isAlive` 状态为 `false`。我该如何解决？

以上问题是由于超过半数的 Java Virtual Machine（JVM）内存被使用且未标记 checkpoint。通常情况下，每积累 50,000 条日志，系统便会标记一个 checkpoint。建议您在业务低峰期修改各 FE 节点的 JVM 参数并重启 FE 使修改生效。

## 查询报错 “could not initialize class com.starrocks.rpc.BackendServiceProxy”。我该如何解决？

* 请确认环境变量 `$JAVA_HOME` 是否为正确的 JDK 路径。
* 请确认所有节点的 JDK 是否是同一个版本，所有节点需要使用相同版本的 JDK。
