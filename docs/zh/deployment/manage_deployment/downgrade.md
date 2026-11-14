---
sidebar_position: 30
displayed_sidebar: docs
description: "升级 StarRocks 后出现异常时，可通过降级恢复到之前版本。"
products: [oss]
---

# 降级 StarRocks

本主题介绍如何降级您的 StarRocks 集群。

如果在升级 StarRocks 集群后出现异常，您可以将其降级到之前的版本，以快速恢复集群。

## 概述 {/* #overview */}

在降级之前，请查看本节中的信息，并执行任何推荐的操作。

### 降级路径 {/* #downgrade-paths */}

- **对于补丁版本降级**

  您可以跨补丁版本降级 StarRocks 集群，例如直接从 v3.5.11 降级到 v3.5.6。

- **对于次版本降级**

  出于兼容性和安全性考虑，我们强烈建议您**按次版本逐一连续降级** StarRocks 集群。例如，要将 StarRocks v3.5 集群降级到 v3.2，您需要按照以下顺序进行降级：v3.5.x --> v3.4.x --> v3.3.x --> v3.2.x。

- **对于主版本降级**

  您只能将 StarRocks v4.1 集群降级到 v4.0.6 及更高版本。

  :::warning
  **降级说明**

  - 将 StarRocks 升级到 v4.1 后，请NOT降级到低于 v4.0.6 的任何 v4.0 版本。

    由于 v4.1 中引入了数据布局方面的内部变更（与 tablet 拆分和分布机制相关），升级到 v4.1 的集群可能会生成与早期版本不完全兼容的元数据和存储结构。因此，从 v4.1 降级仅支持降级到 v4.0.6 及更高版本，不支持降级到 v4.0.6 之前的版本。此限制是由于早期版本在解析 tablet 布局和分布元数据方面存在向后兼容性约束。
  :::

### 降级步骤 {/* #downgrade-procedure */}

StarRocks 的降级步骤与[升级步骤](./upgrade.md#upgrade-procedure)顺序相反。因此，您需要**先降级 FE，再降级 BE 和 CN**。如果降级顺序错误，可能会导致 FE 与 BE/CN 之间不兼容，从而使服务崩溃。对于 FE 节点，您必须先降级所有all Follower FE 节点，然后再降级 Leader FE 节点。

## 开始之前 {/* #before-you-begin */}

在准备阶段，如果您要进行次版本或主版本降级，则必须执行兼容性配置。此外，在降级集群中所有all节点之前，您还需要在其中一个 FE 或 BE 上执行降级可用性测试。

### 执行兼容性配置 {/* #perform-compatibility-configuration */}

如果您想将 StarRocks 集群降级到更早的次版本或主版本，则必须执行兼容性配置。除通用兼容性配置外，具体配置会因您降级前的 StarRocks 集群版本而异。

- **通用兼容性配置**

在降级 StarRocks 集群之前，您必须禁用 tablet 克隆。如果您已禁用均衡器，则可以跳过此步骤。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("disable_balance"="true");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="true");
```

降级完成后，如果所有 all BE 节点的状态变为 `Alive`，您可以重新启用 tablet 克隆。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "10000");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "500");
ADMIN SET FRONTEND CONFIG ("disable_balance"="false");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="false");
```

## 降级 FE {/* #downgrade-fe */}

:::note
要将集群从 v3.3.0 或更高版本降级到 v3.2，请在降级前执行以下步骤：

1. 在降级之前，请确保在 v3.3 集群中发起的所有 all ALTER TABLE SCHEMA CHANGE 事务均已完成或取消。

2. 执行以下命令以清除所有 all 事务历史记录：

   ```SQL
   ADMIN SET FRONTEND CONFIG ("history_job_keep_max_second" = "0");
   ```

3. 运行以下命令以验证没有遗留的历史记录：

   ```SQL
   SHOW PROC '/jobs/<db>/schema_change';
   ```
:::

完成兼容性配置和可用性测试后，您就可以降级 FE 节点了。您必须先降级 Follower FE 节点，然后再降级 Leader FE 节点。

1. 创建元数据快照。

   a. 运行 [ALTER SYSTEM CREATE IMAGE](../../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md) 以创建元数据快照。

   b. 您可以通过查看 Leader FE 的日志文件 **fe.log** 来检查 image 文件是否已同步。如果日志中出现类似 "push image.* from subdir [] to other nodes. totally xx nodes, push succeeded xx nodes" 的记录，则说明 image 文件已成功同步。

2. 进入 FE 节点的工作目录，并停止该节点。

   ```Bash
   # 将 <fe_dir> 替换为 FE 节点的部署目录。
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

3. 将 **bin**、**lib** 和 **spark-dpp** 目录下的原部署文件替换为早期版本的部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   mv spark-dpp spark-dpp.bak
   cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
   cp -r /tmp/StarRocks-x.x.x/fe/bin  .
   cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
   ```

4. 启动 FE 节点。

   ```Bash
   ./bin/start_fe.sh --daemon
   ```

5. 检查 FE 节点是否启动成功。

   ```Bash
   ps aux | grep StarRocksFE
   ```

6. 重复上述步骤 2 到步骤 5，降级其他 Follower FE 节点，最后降级 Leader FE 节点。

   > **注意**
   >
   > 假设您在一次失败的升级后降级了集群，并想要再次升级集群，例如 3.5->4.0->3.5->4.0。为防止部分 Follower FE 出现元数据升级失败，请在升级前重复步骤 1 以触发新的快照。

## 降级 BE {/* #downgrade-be */}

降级 FE 节点后，您可以继续降级集群中的 BE 节点。

1. 进入 BE 节点的工作目录并停止该节点。

   ```Bash
   # 将 <be_dir> 替换为 BE 节点的部署目录。
   cd <be_dir>/be
   ./bin/stop_be.sh
   ```

2. 用早期版本的部署文件替换 **bin** 和 **lib** 下的原部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. 启动 BE 节点。

   ```Bash
   ./bin/start_be.sh --daemon
   ```

4. 检查 BE 节点是否启动成功。

   ```Bash
   ps aux | grep starrocks_be
   ```

5. 重复上述步骤以降级其他 BE 节点。

## 降级 CN {/* #downgrade-cn */}

1. 进入 CN 节点的工作目录并优雅地停止该节点。

   ```Bash
   # 将 <cn_dir> 替换为 CN 节点的部署目录。
   cd <cn_dir>/be
   ./bin/stop_cn.sh --graceful
   ```

2. 用早期版本的部署文件替换 **bin** 和 **lib** 下的原部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. 启动 CN 节点。

   ```Bash
   ./bin/start_cn.sh --daemon
   ```

4. 检查 CN 节点是否启动成功。

   ```Bash
   ps aux | grep  starrocks_be
   ```

5. 重复上述步骤以降级其他 CN 节点。
