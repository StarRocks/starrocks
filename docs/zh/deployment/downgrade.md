---
displayed_sidebar: "Chinese"
---

# 降级 StarRocks

本文介绍如何降级您的 StarRocks 集群。

如果升级 StarRocks 集群后出现异常，您可以将其降级到之前的版本，以快速恢复集群。

## 概述

请在降级前查看本节中的信息。建议您按照文中推荐的操作降级集群。

### 降级路径

- **小版本降级**

  您可以跨小版本降级您的 StarRocks 集群，例如，从 v2.2.11 直接降级到 v2.2.6。

- **大版本降级**

  出于兼容性和安全原因，我们强烈建议您将 StarRocks 集群按**大版本逐级降级**。例如，要将 StarRocks v2.5 集群降级到 v2.2，需要按照以下顺序降级：v2.5.x --> v2.4.x --> v2.3.x --> v2.2.x。

- **重大版本降级**

  - 您无法跨版本降级至 v1.19，必须先降级至 v2.0。
  - 您只能将集群从 v3.0 降级到 v2.5.3 以上版本。
    - StarRocks 在 v3.0 版本中升级了 BDB 库。由于 BDB JE 无法回滚，所以降级后您必须继续使用 v3.0 的 BDB 库。
    - 升级至 v3.0 后，集群默认使用新的 RBAC 权限系统。降级后您只能使用 RBAC 权限系统。

> **注意**
>
> 如果您在升级之后进行了回滚，之后有计划再次执行升级，比如 2.5->3.0->2.5->3.0。为了避免第二次升级时，部分 FE 节点元数据升级失败，建议您在降级完成后执行如下操作：
>
> 1. 执行 [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/Administration/ALTER_SYSTEM.md) 创建新的元数据快照文件。
> 2. 等待元数据快照文件同步至其他 FE 节点。
>
> 您可以通过查看 Leader FE 节点的日志文件 **fe.log** 确认元数据快照文件是否推送完成。如果日志打印以下内容，则说明快照文件推送完成："push image.xxx from subdir [] to other nodes. totally xx nodes, push succeeded xx nodes"。

### 降级流程

StarRocks 的降级流程与 [升级流程](../deployment/upgrade.md#升级流程) 相反。所以**您需要先降级 FE，再降级 BE 和CN**。错误的降级顺序可能会导致 FE 与 BE/CN 不兼容，进而导致服务崩溃。对于 FE 节点，您必须先降级所有 Follower FE 节点，最后降级 Leader FE 节点。

## 准备工作

准备过程中，如果您需要进行大版本或重大版本降级，则必须进行兼容性配置。在全面降级集群所有节点之前，您还需要对其中一个 FE 和 BE 节点进行降级正确性测试。

### 兼容性配置

如需进行大版本或重大版本降级，则必须进行兼容性配置。除了通用的兼容性配置外，还需根据降级前版本进行具体配置。

- **通用兼容性配置**

降级前，请关闭 Tablet Clone。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("disable_balance"="true");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="true");
```

完成降级，并且所有 BE 节点状态变为 `Alive` 后，您可以重新开启 Tablet Clone。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "10000");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "500");
ADMIN SET FRONTEND CONFIG ("disable_balance"="false");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="false");
```

- **如果您自 v2.2 及以后版本降级**

设置 FE 配置项 `ignore_unknown_log_id` 为 `true`。由于该配置项为静态参数，所以必须在 FE 配置文件 **fe.conf** 中修改，并且在修改完成后重启节点使修改生效。降级结束且第一次 Checkpoint 完成后，您可以将其重置为 `false` 并重新启动节点。

- **如果您启用了 FQDN 访问**

如果您启用了 FQDN 访问（自 v2.4 起支持），需要降级至 v2.4 之前版本，则必须在降级之前切换到 IP 地址访问。有关详细说明，请参考 [回滚 FQDN](../administration/enable_fqdn.md#回滚)。

## 降级 FE

通过降级正确性测试后，您可以先降级 FE 节点。您必须先降级 Follower FE 节点，然后再降级 Leader FE 节点。

1. 进入 FE 节点工作路径，并停止该节点。

   ```Bash
   # 将 <fe_dir> 替换为 FE 节点的部署目录。
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

2. 替换部署文件原有路径 **bin**、**lib** 以及 **spark-dpp** 为旧版本的部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   mv spark-dpp spark-dpp.bak
   cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
   cp -r /tmp/StarRocks-x.x.x/fe/bin  .
   cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
   ```

   > **注意**
   >
   > 如需将 StarRocks v3.0 降级至 v2.5，则必须在替换部署文件后执行以下步骤：
   >
   > 1. 将 v3.0 部署文件中的**fe/lib/starrocks-bdb-je-18.3.13.jar** 复制到 v2.5 部署文件的 **fe/lib** 路径下。
   > 2. 删除文件 **fe/lib/je-7.\*.jar**。

3. 启动该 FE 节点。

   ```Bash
   sh bin/start_fe.sh --daemon
   ```

4. 查看节点是否启动成功。

   ```Bash
   ps aux | grep StarRocksFE
   ```

5. 重复以上步骤降级其他 Follower FE 节点，最后降级 Leader FE 节点。

   > **注意**
   >
   > 如需将 StarRocks v3.0 降级至 v2.5，则必须在降级完成后执行以下步骤：
   >
   > 1. 执行 [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/Administration/ALTER_SYSTEM.md) 创建新的元数据快照文件。
   > 2. 等待元数据快照文件同步至其他 FE 节点。
   >
   > 如果不运行该命令，部分降级操作可能会失败。ALTER SYSTEM CREATE IMAGE 命令仅在 v2.5.3 及更高版本支持。

## 降级 BE

降级所有 FE 节点后，您可以继续降级 BE 节点。

1. 进入 BE 节点工作路径，并停止该节点。

   ```Bash
   # 将 <be_dir> 替换为 BE 节点的部署目录。
   cd <be_dir>/be
   ./bin/stop_be.sh
   ```

2. 替换部署文件原有路径 **bin** 和 **lib** 为旧版本的部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. 启动该 BE 节点。

   ```Bash
   sh bin/start_be.sh --daemon
   ```

4. 查看节点是否启动成功。

   ```Bash
   ps aux | grep starrocks_be
   ```

5. 重复以上步骤降级其他 BE 节点。

## 降级 CN

1. 进入 CN 节点工作路径，并优雅停止该节点。

   ```Bash
   # 将 <cn_dir> 替换为 CN 节点的部署目录。
   cd <cn_dir>/be
   ./bin/stop_cn.sh --graceful
   ```

2. 替换部署文件原有路径 **bin** 和 **lib** 为新版本的部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. 启动该 CN 节点。

   ```Bash
   sh bin/start_cn.sh --daemon
   ```

4. 查看节点是否启动成功。

   ```Bash
   ps aux | grep starrocks_be
   ```

5. 重复以上步骤降级其他 CN 节点。
