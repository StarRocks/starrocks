# 升级 StarRocks

本文介绍如何升级您的 StarRocks 集群。

## 概述

请在升级前查看本节中的信息。建议您按照文中推荐的操作升级集群。

### StarRocks 版本

StarRocks 的版本号由三个数字表示，格式为 **Major.Minor.Patch**，例如 `2.5.4`。第一个数字代表 StarRocks 的重大版本，第二个数字代表大版本，第三个数字代表小版本。目前，StarRocks 为部分版本提供长期支持（Long-time Support，LTS），维护期为半年以上。

| **StarRocks 版本** | **是否为 LTS 版本** |
| ------------------ | ------------------- |
| v1.19.x            | 否                  |
| v2.0.x             | 否                  |
| v2.1.x             | 否                  |
| v2.2.x             | 否                  |
| v2.3.x             | 否                  |
| v2.4.x             | 否                  |
| v2.5.x             | 是                  |
| v3.0.x             | 否                  |

### 升级路径

- **小版本升级**

  您可以跨小版本升级您的 StarRocks 集群，例如，从 v2.2.6 直接升级到 v2.2.11。

- **大版本升级**

  从 StarRocks v2.0 开始，您可以跨大版本升级 StarRocks 集群，例如，从 v2.2.x 直接升级到 v2.5.x。但出于兼容性和安全原因，我们强烈建议您将 StarRocks 集群按**大版本逐级升级**。例如，要将 StarRocks v2.2 集群升级到 v2.5，需要按照以下顺序升级：v2.2.x --> v2.3.x --> v2.4.x --> v2.5.x。

- **重大版本升级**

  - 您必须从 v1.19 升级到 v2.0。
  - 您必须从 v2.5 升级到 v3.0。

### 升级流程

StarRocks 支持**滚动升级**，允许您在不停止服务的情况下升级您的集群。按照设计，BE 和 CN 向后兼容 FE。因此，**您需要先升级 BE 和 CN，然后升级 FE**，以便让您的集群在升级的同时也能正常运行。错误的升级顺序可能会导致 FE 与 BE/CN 不兼容，进而导致服务崩溃。对于 FE 节点，您必须先升级所有 Follower FE 节点，最后升级 Leader FE 节点。

## 准备工作

准备过程中，如果您需要进行大版本或重大版本升级，则必须进行兼容性配置。在全面升级集群所有节点之前，您还需要对其中一个 FE 和 BE 节点上进行升级正确性测试。

### 兼容性配置

如需进行大版本或重大版本升级，则必须进行兼容性配置。除了通用的兼容性配置外，还需根据升级前版本进行具体配置。

- **通用兼容性配置**

升级前，请关闭 Tablet Clone。

```SQL
ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("disable_balance"="true");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="true");
```

完成升级，并且所有 BE 节点状态变为 `Alive` 后，您可以重新开启 Tablet Clone。

```SQL
ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets" = "2000");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "100");
ADMIN SET FRONTEND CONFIG ("disable_balance"="false");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="false");
```

- **自 v2.0 升级**

自 v2.0 版本升级至其他大版本时，您需要设置以下 BE 设置项以及系统变量。

1. 如果您曾经修改过 BE 配置项 `vector_chunk_size`，升级前必须将其设置为`4096`。由于该配置项为静态参数，所以必须在 BE 配置文件 **be.conf** 中修改，并且在修改完成后重启节点使修改生效。
2. 全局设置系统变量 `batch_size` 为小于等于 `4096` 的值。

   ```SQL
   SET GLOBAL batch_size = 4096;
   ```

## 升级 BE

通过升级正确性测试后，您可以先升级集群中的 BE 节点。

1. 进入 BE 节点工作路径，并停止该节点。

   ```Bash
   # 将 <be_dir> 替换为 BE 节点的部署目录。
   cd <be_dir>/be
   ./bin/stop_be.sh
   ```

2. 替换部署文件原有路径 **bin** 和 **lib** 为新版本的部署文件。

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

5. 重复以上步骤升级其他 BE 节点。

## 升级 CN

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

5. 重复以上步骤升级其他 CN 节点。

## 升级 FE

升级所有 BE 和 CN 节点后，您可以继续升级 FE 节点。您必须先升级 Follower FE 节点，然后再升级 Leader FE 节点。

1. 进入 FE 节点工作路径，并停止该节点。

   ```Bash
   # 将 <fe_dir> 替换为 FE 节点的部署目录。
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

2. 替换部署文件原有路径 **bin**、**lib** 以及 **spark-dpp** 为新版本的部署文件。

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   mv spark-dpp spark-dpp.bak
   cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
   cp -r /tmp/StarRocks-x.x.x/fe/bin  .
   cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
   ```

3. 启动该 FE 节点。

   ```Bash
   sh bin/start_fe.sh --daemon
   ```

4. 查看节点是否启动成功。

   ```Bash
   ps aux | grep StarRocksFE
   ```

5. 重复以上步骤升级其他 Follower FE 节点，最后升级 Leader FE 节点。

  > **注意**
  >
  > 如果您从 v2.5 升级至 v3.0 之后，进行了回滚，然后再次升级至 v3.0，为了避免部分 Follower FE 节点元数据升级失败，则必须在升级完成后执行以下步骤：
  >
  > 1. 执行 [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/Administration/ALTER%20SYSTEM.md) 创建新的元数据快照文件。
  > 2. 等待元数据快照文件同步至其他 FE 节点。
  >
  > 您可以通过查看 Leader FE 节点的日志文件 **fe.log** 确认元数据快照文件是否推送完成。如果日志打印以下内容，则说明快照文件推送完成："push image.* from subdir [] to other nodes. totally xx nodes, push successed xx nodes"。
