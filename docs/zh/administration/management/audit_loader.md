---
displayed_sidebar: docs
---

# 通过 AuditLoader 管理 StarRocks 中的审计日志

本文档介绍如何通过插件 AuditLoader 在 StarRocks 内部管理审计日志。

在 StarRocks 中，所有的审计信息仅存储在日志文件 **fe/log/fe.audit.log** 中，无法直接通过 StarRocks 进行访问。AuditLoader 插件可实现审计信息的入库，让您在 StarRocks 内方便的通过 SQL 进行集群审计信息的查看和管理。安装 AuditLoader 插件后，StarRocks 在执行 SQL 后会自动调用 AuditLoader 插件收集 SQL 的审计信息，然后将审计信息在内存中攒批，最后基于 Stream Load 的方式导入至 StarRocks 表中。

## 创建审计日志库表

在 StarRocks 集群中为审计日志创建数据库和表。详细操作说明参阅 [CREATE DATABASE](../../sql-reference/sql-statements/Database/CREATE_DATABASE.md) 和 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。

> **注意**
>
> - 请勿更改示例中的表属性，否则将导致日志导入失败。
> - StarRocks 各个大版本的审计日志字段个数存在差异，为保证版本通用性，新版本的审计插件选取了各大版本中通用的日志字段进行入库。若业务中需要更完整的字段，可替换工程中的 `fe-plugins-auditloader\lib\starrocks-fe.jar`，同时修改代码中与字段相关的内容后重新编译打包。

```SQL
CREATE DATABASE starrocks_audit_db__;

CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (
  `queryId`           VARCHAR(64)                COMMENT "查询的唯一ID",
  `timestamp`         DATETIME         NOT NULL  COMMENT "查询开始时间",
  `queryType`         VARCHAR(12)                COMMENT "查询类型（query, slow_query, connection）",
  `clientIp`          VARCHAR(32)                COMMENT "客户端IP",
  `user`              VARCHAR(64)                COMMENT "查询用户名",
  `authorizedUser`    VARCHAR(64)                COMMENT "用户唯一标识，既user_identity",
  `resourceGroup`     VARCHAR(64)                COMMENT "资源组名",
  `catalog`           VARCHAR(32)                COMMENT "Catalog名",
  `db`                VARCHAR(96)                COMMENT "查询所在数据库",
  `state`             VARCHAR(8)                 COMMENT "查询状态（EOF，ERR，OK）",
  `errorCode`         VARCHAR(512)               COMMENT "错误码",
  `queryTime`         BIGINT                     COMMENT "查询执行时间（毫秒）",
  `scanBytes`         BIGINT                     COMMENT "查询扫描的字节数",
  `scanRows`          BIGINT                     COMMENT "查询扫描的记录行数",
  `returnRows`        BIGINT                     COMMENT "查询返回的结果行数",
  `cpuCostNs`         BIGINT                     COMMENT "查询CPU耗时（纳秒）",
  `memCostBytes`      BIGINT                     COMMENT "查询消耗内存（字节）",
  `stmtId`            INT                        COMMENT "SQL语句增量ID",
  `isQuery`           TINYINT                    COMMENT "SQL是否为查询（1或0）",
  `feIp`              VARCHAR(128)               COMMENT "执行该语句的FE IP",
  `stmt`              VARCHAR(1048576)           COMMENT "原始SQL语句",
  `digest`            VARCHAR(32)                COMMENT "慢SQL指纹",
  `planCpuCosts`      DOUBLE                     COMMENT "查询规划阶段CPU占用（纳秒）",
  `planMemCosts`      DOUBLE                     COMMENT "查询规划阶段内存占用（字节）",
  `pendingTimeMs`     BIGINT                     COMMENT "查询在队列中等待的时间（毫秒）",
  `candidateMVs`      VARCHAR(65533)             COMMENT "候选MV列表",
  `hitMvs`            VARCHAR(65533)             COMMENT "命中MV列表"
  `warehouse`         VARCHAR(128)               COMMENT "仓库名称"
) ENGINE = OLAP
DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)
COMMENT "审计日志表"
PARTITION BY RANGE (`timestamp`) ()
DISTRIBUTED BY HASH (`queryId`) BUCKETS 3 
PROPERTIES (
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-30",  --表示只保留最近30天的审计信息，可视需求调整。
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.buckets" = "3",
  "dynamic_partition.enable" = "true",
  "replication_num" = "3"  --若集群中BE个数不大于3，可调整副本数为1，生产集群不推荐调整。
);
```

`starrocks_audit_tbl__` 表是动态分区表。 默认情况下，第一个动态分区将在建表后 10 分钟创建。分区创建后审计日志方可导入至表中。 您可以使用以下语句检查表中的分区是否创建完成：

```SQL
SHOW PARTITIONS FROM starrocks_audit_db__.starrocks_audit_tbl__;
```

待分区创建完成后，您可以继续下一步。

## 下载并配置 AuditLoader

1. [下载](https://releases.mirrorship.cn/resources/AuditLoader.zip) AuditLoader 安装包。该插件兼容目前在维护的所有 StarRocks 版本。

2. 解压安装包。

    ```shell
    unzip auditloader.zip
    ```

    解压生成以下文件：

    - **auditloader.jar**：审计插件代码编译后得到的程序 jar 包。
    - **plugin.properties**：插件属性文件，用于提供审计插件在 StarRocks 集群内的描述信息，无需修改。
    - **plugin.conf**：插件配置文件，用于提供插件底层进行 Stream Load 写入时的配置参数，需根据集群信息修改。通常只建议修改其中的 `user` 和 `password` 信息。

3. 修改 **plugin.conf** 文件以配置 AuditLoader。您必须配置以下项目以确保 AuditLoader 可以正常工作：

    - `frontend_host_port`：FE 节点 IP 地址和 HTTP 端口，格式为 `<fe_ip>:<fe_http_port>`。推荐使用默认值，即 `127.0.0.1:8030` 。StarRocks 中各个 FE 是独立管理各自的审计信息。在安装插件后，各个 FE 分别会启动各自的后台线程进行审计信息的获取和攒批，并通过 Stream Load 写入。 `frontend_host_port` 配置项用于为插件后台 Stream Load 任务提供 HTTP 协议的 IP 和端口，该参数不支持配置为多个值。其中，参数的 IP 部分可以使用集群内任意某个 FE 的 IP，但并不推荐这样配置，因为若对应的 FE 出现异常，其他 FE 后台的审计信息写入任务也会因无法通信导致写入失败。推荐配置为默认的 `127.0.0.1:8030`，让各个 FE 均使用自身的 HTTP 端口进行通信，以此规避其他 FE 异常时对通信的影响（所有的写入任务最终都会被 FE 自动转发到 FE Leader 节点执行）。
    - `database`：审计日志库名。
    - `table`：审计日志表名。
    - `user`：集群用户名。该用户必须具有对应表的 INSERT 权限。
    - `password`：集群用户密码。
    - `secret_key`：用于加密密码的 Key（字符串，长度不得超过 16 个字节）。如果该参数为空，则表示不对 **plugin.conf** 中的密码进行加解密，您只需在 `password` 处直接配置明文密码。如果该参数不为空，表示需要通过该 Key 对密码进行加解密，您需要在 `password` 处配置加密后的字符串。加密后的密码可在 StarRocks 中通过 `AES_ENCRYPT` 函数生成：`SELECT TO_BASE64(AES_ENCRYPT('password','secret_key'));`。
    - `enable_compute_all_query_digest`：是否对所有查询都生成 Hash SQL 指纹（StarRocks 默认只为慢查询开启 SQL 指纹）。需注意插件中的指纹计算方法与 FE 内部的方法不一致，FE 会对 SQL 语句[规范化处理](../Query_planning.md#%E6%9F%A5%E7%9C%8B-sql-%E6%8C%87%E7%BA%B9)，而插件不会，且如果开启该参数，指纹计算会额外占用集群内的计算资源。
    - `filter`：审计信息入库的过滤条件。该参数基于 Stream Load 中的 [WHERE 参数](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#opt_properties) 实现，即 `-H "where: <condition>"`，默认值为空字符串。示例：`filter=isQuery=1 and clientIp like '127.0.0.1%' and user='root'`。

4. 重新打包以上文件。

    ```shell
    zip -q -m -r auditloader.zip auditloader.jar plugin.conf plugin.properties
    ```

5. 将压缩包分发至所有 FE 节点运行的机器。请确保所有压缩包都存储在相同的路径下，否则插件将安装失败。分发完成后，请复制压缩包的绝对路径。

  > **注意**
  >
  > 您也可将 **auditloader.zip** 分发至所有 FE 都可访问到的 HTTP 服务中（例如 `httpd` 或 `nginx`），然后使用网络路径安装。注意这两种方式下 **auditloader.zip** 在执行安装后都需要在该路径下持续保留，不可在安装后删除源文件。

## 安装 AuditLoader

通过以下语句安装 AuditLoader 插件：

```SQL
INSTALL PLUGIN FROM "<absolute_path_to_package>";
```

以安装本地插件包为例，安装命令示例：

```SQL
mysql> INSTALL PLUGIN FROM "/opt/module/starrocks/auditloader.zip";
```

若通过网络路径安装，还需在安装命令的属性中提供插件压缩包的 md5 信息，命令示例：

```sql
INSTALL PLUGIN FROM "http://xx.xx.xxx.xxx/extra/auditloader.zip" PROPERTIES("md5sum" = "3975F7B880C9490FE95F42E2B2A28E2D");
```

详细操作说明参阅 [INSTALL PLUGIN](../../sql-reference/sql-statements/cluster-management/plugin/INSTALL_PLUGIN.md)。

## 验证安装并查询审计日志

1. 您可以通过 [SHOW PLUGINS](../../sql-reference/sql-statements/cluster-management/plugin/SHOW_PLUGINS.md) 语句检查插件是否安装成功。

    以下示例中，插件 `AuditLoader` 的 `Status` 为 `INSTALLED`，即代表安装成功。

    ```Plain
    mysql> SHOW PLUGINS\G
    *************************** 1. row ***************************
        Name: __builtin_AuditLogBuilder
        Type: AUDIT
    Description: builtin audit logger
        Version: 0.12.0
    JavaVersion: 1.8.31
    ClassName: com.starrocks.qe.AuditLogBuilder
        SoName: NULL
        Sources: Builtin
        Status: INSTALLED
    Properties: {}
    *************************** 2. row ***************************
        Name: AuditLoader
        Type: AUDIT
    Description: Available for versions 2.5+. Load audit log to starrocks, and user can view the statistic of queries
        Version: 4.2.1
    JavaVersion: 1.8.0
    ClassName: com.starrocks.plugin.audit.AuditLoaderPlugin
        SoName: NULL
        Sources: /x/xx/xxx/xxxxx/auditloader.zip
        Status: INSTALLED
    Properties: {}
    2 rows in set (0.01 sec)
    ```

2. 随机执行 SQL 语句以生成审计信息，并等待 60 秒（或您在配置 AuditLoader 时在 `max_batch_interval_sec` 项中指定的时间）以允许 AuditLoader 将审计日志攒批导入至StarRocks 中。

3. 查询审计日志表。

    ```SQL
    SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__;
    ```

    以下示例演示审计日志成功导入的情况：

    ```SQL
    mysql> SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__\G
    *************************** 1. row ***************************
         queryId: 84a69010-d47e-11ee-9647-024228044898
       timestamp: 2024-02-26 16:10:35
       queryType: query
        clientIp: xxx.xx.xxx.xx:65283
            user: root
    authorizedUser: 'root'@'%'
    resourceGroup: default_wg
         catalog: default_catalog
              db: 
           state: EOF
       errorCode:
       queryTime: 3
       scanBytes: 0
        scanRows: 0
      returnRows: 1
       cpuCostNs: 33711
    memCostBytes: 4200
          stmtId: 102
         isQuery: 1
            feIp: xxx.xx.xxx.xx
            stmt: SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__
          digest:
    planCpuCosts: 0
    planMemCosts: 0
       warehouse: default_warehouse
    1 row in set (0.01 sec)
    ```

## 故障排除

如果在动态分区创建成功且插件安装成功后仍然长时间没有审计日志导入至表中，您需要检查 **plugin.conf** 文件是否配置正确。 如需修改配置文件，您需要首先卸载插件：

```SQL
UNINSTALL PLUGIN AuditLoader;
```

AuditLoader 的日志会打印在各个 FE 的 **fe.log** 中。您可以在 **fe.log** 中检索关键字 `audit`，用排查 Stream Load 任务的思路来定位问题。所有配置设置正确后，您可以按照上述步骤重新安装。
