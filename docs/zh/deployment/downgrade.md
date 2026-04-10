---
displayed_sidebar: docs
---

# 降级 StarRocks

本文介绍如何降级您的 StarRocks 集群。

如果升级 StarRocks 集群后出现异常，您可以将其降级到之前的版本，以快速恢复集群。

## 概述

请在降级前查看本节中的信息。建议您按照文中推荐的操作降级集群。

### 降级路径

- **小版本降级**

  您可以跨小版本降级您的 StarRocks 集群，例如，从 v3.5.11 直接降级到 v3.5.6。

- **大版本降级**

  出于兼容性和安全原因，我们强烈建议您将 StarRocks 集群按**大版本逐级降级**。例如，要将 StarRocks v3.5 集群降级到 v3.2，需要按照以下顺序降级：v3.5.x --> v3.4.x --> v3.3.x --> v3.2.x。

- **重大版本降级**

  您只能将 StarRocks v4.1 集群降级至 v4.0.6 及更高版本。

  :::warning

  **降级说明**

  - 将 StarRocks 升级至 v4.1 后，请勿降级至 v4.0.6 之前的任何 v4.0 版本。

    由于 v4.1 引入了数据布局的内部变更（与 Tablet 分割和数据分布机制相关），升级至 v4.1 的集群生成的元数据和存储结构可能与早期版本不完全兼容。因此，从 v4.1 降级仅支持降至 v4.0.6 或更高版本。不支持降级至 v4.0.6 之前的版本。此限制源于早期版本在解析 Tablet 布局和分布元数据时的向后兼容性约束。

  :::


### 降级流程

StarRocks 的降级流程与 [升级流程](../deployment/upgrade.md#升级流程) 相反。所以**您需要先降级 FE，再降级 BE 和CN**。错误的降级顺序可能会导致 FE 与 BE/CN 不兼容，进而导致服务崩溃。对于 FE 节点，您必须先降级所有 Follower FE 节点，最后降级 Leader FE 节点。

## 准备工作

准备过程中，如果您需要进行大版本或重大版本降级，则必须进行兼容性配置。在全面降级集群所有节点之前，您还需要对其中一个 FE 和 BE 节点进行降级正确性测试。

### 兼容性配置

如需进行大版本或重大版本降级，则必须进行兼容性配置。除了通用的兼容性配置外，还需根据降级前版本进行具体配置。

- **通用兼容性配置**

降级前，请关闭 Tablet Clone。如果您已经关闭 Balancer，可以跳过该步骤。

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

## 降级 FE

通过降级正确性测试后，您可以先降级 FE 节点。您必须先降级 Follower FE 节点，然后再降级 Leader FE 节点。

:::note

如需将 v3.3.0 及以上集群降级至 v3.2，需在降级前执行以下操作：

1. 确保降级前的 v3.3 集群中发起的所有 ALTER TABLE SCHEMA CHANGE 事物已完成或取消。
2. 通过以下命令清理所有事务历史记录：

   ```SQL
   ADMIN SET FRONTEND CONFIG ("history_job_keep_max_second" = "0");
   ```

3. 通过以下命令确认无历史记录遗留：

   ```SQL
   SHOW PROC '/jobs/<db>/schema_change';
   ```
:::

1. 生成新的元数据快照。

   a. 执行 [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md) 创建新的元数据快照文件。

   b. 通过查看 Leader FE 节点的日志文件 **fe.log** 确认元数据快照文件是否推送完成。如果日志打印以下内容，则说明快照文件推送完成：

   ```
   push image.xxx from subdir [] to other nodes. totally xx nodes, push succeeded xx nodes
   ```

2. 进入 FE 节点工作路径，并停止该节点。

   ```Bash
   # 将 <fe_dir> 替换为 FE 节点的部署目录。
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

3. 替换部署文件原有路径 **bin**、**lib** 以及 **spark-dpp** 为旧版本的部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   mv spark-dpp spark-dpp.bak
   cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
   cp -r /tmp/StarRocks-x.x.x/fe/bin  .
   cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
   ```

4. 启动该 FE 节点。

   ```Bash
   ./bin/start_fe.sh --daemon
   ```

5. 查看节点是否启动成功。

   ```Bash
   ps aux | grep StarRocksFE
   ```

6. 重复以上步骤 2~5 降级其他 Follower FE 节点，最后降级 Leader FE 节点。

   > **注意**
   >
   > 如果您在升级之后进行了回滚，之后有计划再次执行升级，比如 3.5->4.0->3.5->4.0。为了避免第二次升级时，部分 FE 节点元数据升级失败，建议您在降级完成后再次执行步骤 1，生成新的元数据快照。

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
   ./bin/start_be.sh --daemon
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
   ./bin/start_cn.sh --daemon
   ```

4. 查看节点是否启动成功。

   ```Bash
   ps aux | grep starrocks_be
   ```

5. 重复以上步骤降级其他 CN 节点。
