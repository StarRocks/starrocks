---
sidebar_position: 20
displayed_sidebar: docs
description: "升级 StarRocks 集群到新版本的方法和注意事项。"
keywords: ['shengji']
products: [oss]
---

# 升级 StarRocks

本主题介绍如何升级 StarRocks 集群。

## 重要信息 {/* #important-information */}

:::important
在升级 StarRocks 之前，您应该：

- 阅读您要升级到的目标版本 StarRocks 的[发行说明](https://docs.starrocks.io/releasenotes/release-3.5/)，以及当前版本与目标版本之间所有 all 版本的发行说明，并：
  - 记录 StarRocks 内部的任何行为变化
  - 记录 StarRocks 与用于导入、导出、可视化等用途的外部系统之间集成的任何变化
- 确认目标版本的[部署前提条件](../preparation/deployment_prerequisites.md)。例如，StarRocks 3.5.x 需要 JDK 17，而 StarRocks 3.4.x 在 Ubuntu 上需要 JDK 11。
:::

## 概述 {/* #overview */}

在升级之前，请先查看本节中的信息，并执行任何推荐的操作。

### StarRocks 版本 {/* #starrocks-versions */}

StarRocks 的版本号由三个数字组成，格式为 **Major.Minor.Patch**，例如 `2.5.4`。第一个数字表示 StarRocks 的主版本，第二个数字表示次版本，第三个数字表示补丁版本。

> **注意**
>
> 请注意，您无法将现有的存算一体集群升级为存算分离集群，反之亦然。您必须部署一个新的存算分离集群。

### 升级路径 {/* #upgrade-paths */}

- **补丁版本升级**

  您可以跨补丁版本升级 StarRocks 集群，例如直接从 v2.2.6 升级到 v2.2.11。

- **次版本升级**

  从 StarRocks v2.0 开始，您可以跨次版本升级 StarRocks 集群，例如直接从 v2.2.x 升级到 v2.5.x。但出于兼容性和安全性的考虑，我们强烈建议您**按次版本逐一连续升级** StarRocks 集群。例如，要将 StarRocks v2.2 集群升级到 v2.5，您需要按以下顺序升级：v2.2.x --> v2.3.x --> v2.4.x --> v2.5.x。

- **主版本升级**

  要将 StarRocks 集群升级到 v3.0，您必须先将其升级到 v2.5。

> **注意**
>
> 假设您需要执行连续的次版本升级，例如 2.4->2.5->3.0->3.1->3.2，或者您在升级失败后降级了集群，并希望再次升级集群，例如 2.5->3.0->2.5->3.0。为防止某些 Follower FE 的元数据升级失败，请在两次连续升级之间，或在降级之后、第二次尝试升级之前，执行以下步骤：
>
> 1. 运行 [ALTER SYSTEM CREATE IMAGE](../../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md) 以创建一个新镜像。
> 2. 等待新镜像同步到 all Follower FE。
>
> 您可以通过查看 Leader FE 的日志文件 **fe.log** 来检查镜像文件是否已同步。如果日志中出现类似 "push image.* from subdir [] to other nodes. totally xx nodes, push succeeded xx nodes" 的记录，则表示镜像文件已成功同步。

### 升级流程 {/* #upgrade-procedure */}

StarRocks 支持**滚动升级**，让您可以在不停止服务的情况下升级集群。按照设计，BE 和 CN 与 FE 保持向后兼容。因此，您需要**先升级 BE 和 CN，再升级 FE**，以便集群在升级过程中能够正常运行。如果按相反顺序升级，可能会导致 FE 与 BE/CN 之间不兼容，从而使服务崩溃。对于 FE 节点，您必须先升级 all Follower FE 节点，然后再升级 Leader FE 节点。

## 开始之前 {/* #before-you-begin */}

在准备阶段，如果您要进行次版本或主版本升级，必须执行兼容性配置。在升级集群中的 all 节点之前，您还需要在其中一个 FE 和 BE 上执行升级可用性测试。

### 执行兼容性配置 {/* #perform-compatibility-configuration */}

如果您想将 StarRocks 集群升级到更高的次版本或主版本，必须执行兼容性配置。除通用兼容性配置外，具体配置还会根据您升级前 StarRocks 集群的版本而有所不同。

- **通用兼容性配置**

在升级 StarRocks 集群之前，您必须禁用 tablet 克隆。如果您已经禁用了 balancer，则可以跳过此步骤。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("disable_balance"="true");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="true");
```

升级完成，且 all BE 节点的状态变为 `Alive` 后，您可以重新启用 tablet 克隆。

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "10000");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "500");
ADMIN SET FRONTEND CONFIG ("disable_balance"="false");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="false");
```

- **如果您从 v2.0 升级到更高版本**

在升级 StarRocks v2.0 集群之前，您必须设置以下 BE 配置项和系统变量。

1. 如果您已修改了 BE 配置项 `vector_chunk_size`，则在升级前必须将其设置为 `4096`。由于这是一个静态参数，您必须在 BE 配置文件 **be.conf** 中修改它，并重启节点才能使修改生效。
2. 将系统变量 `batch_size` 全局设置为小于或等于 `4096`。

   ```SQL
   SET GLOBAL batch_size = 4096;
   ```

## 升级 BE {/* #upgrade-be */}

通过升级可用性测试后，您可以先升级集群中的 BE 节点。

1. 进入 BE 节点的工作目录，然后停止该节点。

   ```Bash
   # 将 <be_dir> 替换为 BE 节点的部署目录。
   cd <be_dir>/be
   ./bin/stop_be.sh
   ```

2. 用新版本的部署文件替换 **bin** 和 **lib** 目录下的原始部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   # 如果在旧版本中使用了自定义函数（UDF），您需要将旧版本的 UDF 目录复制到新的 lib 目录中。
   cp -r lib.bak/udf lib/
   ```

3. 启动 BE 节点。

   ```Bash
   ./bin/start_be.sh --daemon
   ```

4. 检查 BE 节点是否启动成功。

   ```Bash
   ps aux | grep starrocks_be
   ```

5. 重复上述步骤以升级其他 BE 节点。

## 升级 CN {/* #upgrade-cn */}

1. 进入 CN 节点的工作目录，优雅地停止该节点。

   ```Bash
   # 将 <cn_dir> 替换为 CN 节点的部署目录。
   cd <cn_dir>/be
   ./bin/stop_cn.sh --graceful
   ```

2. 用新版本的部署文件替换 **bin** 和 **lib** 目录下的原始部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   # 如果在旧版本中使用了自定义函数（UDF），您需要将旧版本的 UDF 目录复制到新的 lib 目录中。
   cp -r lib.bak/udf lib/
   ```

3. 启动 CN 节点。

   ```Bash
   ./bin/start_cn.sh --daemon
   ```

4. 检查 CN 节点是否启动成功。

   ```Bash
   ps aux | grep starrocks_be
   ```

5. 重复上述步骤以升级其他 CN 节点。

## 升级 FE {/* #upgrade-fe */}

在升级完所有 all BE 和 CN 节点后，您可以升级 FE 节点。您必须先升级 Follower FE 节点，然后再升级 Leader FE 节点。

1. 进入 FE 节点的工作目录，然后停止该节点。

   ```Bash
   # 将 <fe_dir> 替换为 FE 节点的部署目录。
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

2. 用新版本的部署文件替换 **bin**、**lib** 和 **spark-dpp** 目录下的原始部署文件。

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
   ./bin/start_fe.sh --daemon
   ```

4. 检查 FE 节点是否启动成功。

   ```Bash
   ps aux | grep StarRocksFE
   ```

5. 重复上述步骤以升级其他 Follower FE 节点，最后升级 Leader FE 节点。
