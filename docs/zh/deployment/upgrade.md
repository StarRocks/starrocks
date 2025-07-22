---
displayed_sidebar: docs
---

# 升级 StarRocks

本文介绍如何升级您的 StarRocks 集群。

## 重要信息

:::important
在升级 StarRocks 之前，您应该：

- 阅读您要升级到的 StarRocks 版本的[发行说明](https://docs.starrocks.io/releasenotes/release-3.5/)，以及当前版本和目标版本之间的所有版本，并：
  - 记录 StarRocks 内的任何行为变化
  - 记录 StarRocks 与用于导入、导出、可视化等的外部系统之间的任何集成变化
- 验证目标版本的[部署先决条件](./deployment_prerequisites.md)。例如，StarRocks 3.5.x 需要 JDK 17，而 Ubuntu 上的 StarRocks 3.4.x 需要 JDK 11。
:::

## 概述

在升级之前，请查看本节中的信息，并执行任何推荐的操作。

### StarRocks 版本

StarRocks 的版本由三个数字表示，格式为 **Major.Minor.Patch**，例如 `2.5.4`。第一个数字表示 StarRocks 的主版本，第二个数字表示次版本，第三个数字表示补丁版本。

> **注意**
>
> 请注意，您不能将现有的存算一体集群升级为存算分离集群，反之亦然。您必须部署一个新的存算分离集群。

### 升级路径

- **对于补丁版本升级**

  您可以跨补丁版本升级您的 StarRocks 集群，例如，从 v2.2.6 直接升级到 v2.2.11。

- **对于次版本升级**

  从 StarRocks v2.0 开始，您可以跨次版本升级 StarRocks 集群，例如，从 v2.2.x 直接升级到 v2.5.x。然而，出于兼容性和安全性考虑，我们强烈建议您**连续从一个次版本升级到另一个次版本**。例如，要将 StarRocks v2.2 集群升级到 v2.5，您需要按以下顺序升级：v2.2.x --> v2.3.x --> v2.4.x --> v2.5.x。

- **对于主版本升级**

  要将您的 StarRocks 集群升级到 v3.0，您必须首先将其升级到 v2.5。

> **注意**
>
> 假设您需要执行连续的次版本升级，例如，2.4->2.5->3.0->3.1->3.2，或者在升级失败后降级了集群并希望再次升级集群，例如，2.5->3.0->2.5->3.0。为了防止某些 Follower FE 的元数据升级失败，请在两次连续升级之间或在第二次升级尝试之前的降级之后执行以下步骤：
>
> 1. 运行 [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md) 创建一个新镜像。
> 2. 等待新镜像同步到所有 Follower FE。
>
> 您可以通过查看 Leader FE 的日志文件 **fe.log** 来检查镜像文件是否已同步。日志中记录的 "push image.* from subdir [] to other nodes. totally xx nodes, push successful xx nodes" 表示镜像文件已成功同步。

### 升级步骤

StarRocks 支持**滚动升级**，允许您在不停止服务的情况下升级集群。设计上，BEs 和 CNs 与 FEs 向后兼容。因此，您需要**先升级 BEs 和 CNs，然后再升级 FEs**，以便在升级过程中集群能够正常运行。以相反的顺序升级可能导致 FEs 和 BEs/CNs 之间的不兼容，从而导致服务崩溃。对于 FE 节点，您必须先升级所有 Follower FE 节点，然后再升级 Leader FE 节点。

## 开始之前

在准备过程中，如果您要进行次版本或主版本升级，必须执行兼容性配置。您还需要在集群中的一个 FE 和 BE 上执行升级可用性测试，然后再升级所有节点。

### 执行兼容性配置

如果您想将 StarRocks 集群升级到更高的次版本或主版本，必须执行兼容性配置。除了通用的兼容性配置外，详细配置因您要升级的 StarRocks 集群版本而异。

- **通用兼容性配置**

在升级 StarRocks 集群之前，必须禁用 tablet 克隆。如果您已禁用负载均衡器，则可以跳过此步骤。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("disable_balance"="true");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="true");
```

升级后，且所有 BE 节点的状态为 `Alive`，您可以重新启用 tablet 克隆。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "10000");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "500");
ADMIN SET FRONTEND CONFIG ("disable_balance"="false");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="false");
```

- **如果从 v2.0 升级到更高版本**

在升级您的 StarRocks v2.0 集群之前，必须设置以下 BE 配置和系统变量。

1. 如果您修改过 BE 配置项 `vector_chunk_size`，必须在升级前将其设置为 `4096`。由于它是一个静态参数，您必须在 BE 配置文件 **be.conf** 中修改它，并重启节点以使修改生效。
2. 将系统变量 `batch_size` 全局设置为小于或等于 `4096`。

   ```SQL
   SET GLOBAL batch_size = 4096;
   ```

## 升级 BE

通过升级可用性测试后，您可以首先升级集群中的 BE 节点。

1. 进入 BE 节点的工作目录并停止节点。

   ```Bash
   # 将 <be_dir> 替换为 BE 节点的部署目录。
   cd <be_dir>/be
   ./bin/stop_be.sh
   ```

2. 用新版本的文件替换 **bin** 和 **lib** 下的原始部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. 启动 BE 节点。

   ```Bash
   sh bin/start_be.sh --daemon
   ```

4. 检查 BE 节点是否成功启动。

   ```Bash
   ps aux | grep starrocks_be
   ```

5. 重复上述步骤以升级其他 BE 节点。

## 升级 CN

1. 进入 CN 节点的工作目录并优雅地停止节点。

   ```Bash
   # 将 <cn_dir> 替换为 CN 节点的部署目录。
   cd <cn_dir>/be
   ./bin/stop_cn.sh --graceful
   ```

2. 用新版本的文件替换 **bin** 和 **lib** 下的原始部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. 启动 CN 节点。

   ```Bash
   sh bin/start_cn.sh --daemon
   ```

4. 检查 CN 节点是否成功启动。

   ```Bash
   ps aux | grep starrocks_be
   ```

5. 重复上述步骤以升级其他 CN 节点。

## 升级 FE

在升级所有 BE 和 CN 节点后，您可以升级 FE 节点。您必须先升级 Follower FE 节点，然后再升级 Leader FE 节点。

1. 进入 FE 节点的工作目录并停止节点。

   ```Bash
   # 将 <fe_dir> 替换为 FE 节点的部署目录。
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

2. 用新版本的文件替换 **bin**、**lib** 和 **spark-dpp** 下的原始部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   mv spark-dpp spark-dpp.bak
   cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
   cp -r /tmp/StarRocks-x.x.x/fe/bin  .
   cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
   ```

3. 启动 FE 节点。

   ```Bash
   sh bin/start_fe.sh --daemon
   ```

4. 检查 FE 节点是否成功启动。

   ```Bash
   ps aux | grep StarRocksFE
   ```

5. 重复上述步骤以升级其他 Follower FE 节点，最后升级 Leader FE 节点。