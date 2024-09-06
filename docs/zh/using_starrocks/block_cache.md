---
displayed_sidebar: docs
toc_max_heading_level: 2
---

# Data Cache

自 StarRocks v3.1.7 和 v3.2.3 版本起，StarRocks 引入 Data Cache 功能用以加速存算分离集群中的查询。该功能将取代早期版本中的 File Cache 功能。Data Cache 以 Block（MB 级别）为单位，按需从远端存储中加载数据。相较之下，File Cache 无论查询需要读取多少数据，都需要在后台线程加载整个数据文件。

相比 File Cache，Data Cache 具有以下优势：

- 可以减少对对象存储的读取，从而降低访问对象存储的成本（假设对象存储服务按访问频率收费）。
- 无需后台加载线程，可以降低对本地磁盘的写入压力和 CPU 使用率，从而减少对其他写入或查询任务的影响。
- 可以优化有效缓存率（因为 File Cache 会加载文件中不常用的数据）。
- 提供更精确的缓存控制能力，避免了 File Cache 淘汰缓存不及时导致磁盘被写满的状况。

## 启用 Data Cache

自 v3.2.3 版本起，StarRocks 默认启用 Data Cache。如果您需要在 v3.1 版本集群中使用 Data Cache，或此前曾手动禁用了此功能，需执行以下流程启用 Data Cache。

执行以下语句在集群运行时动态启用 Data Cache：

```SQL
UPDATE information_schema.be_configs SET VALUE = 1 
WHERE name = "starlet_use_star_cache";
```

动态配置将在 CN 节点重新启动后失效。

如需永久启用 Data Cache，需要将以下配置添加到 CN 配置文件 **cn.conf** 中，并重新启动 CN 节点：

```Properties
starlet_use_star_cache = true
```

## 配置 Data Cache

您可以通过以下 CN（BE）配置项配置 Data Cache：

- [storage_root_path](../administration/management/BE_configuration.md#storage_root_path)（在存算分离集群中，此项用于指定用于缓存数据的根路径。）
- [starlet_use_star_cache](../administration/management/BE_configuration.md#starlet_use_star_cache)
- [starlet_star_cache_disk_size_percent](../administration/management/BE_configuration.md#starlet_star_cache_disk_size_percent)

:::note

如果您在升级至 v3.1.7、v3.2.3 或更高版本之前启用了 File Cache，则需要检查是否修改了配置项 `starlet_cache_evict_high_water`。该项的默认值为 `0.2`，表示 File Cache 将使用 80% 的存储空间来缓存数据文件。如果您曾修改过此项，您必须在升级时针对 `starlet_star_cache_disk_size_percent` 做相应配置。假设您之前将 `starlet_cache_evict_high_water` 设置为 `0.3`，在升级 StarRocks 集群时，您需要将 `starlet_star_cache_disk_size_percent` 设置为 `70`，以确保 Data Cache 使用的磁盘容量上限与 File Cache 相同。

:::

## 查看 Data Cache 状态

- 执行以下语句查看是否已启用 Data Cache：

  ```SQL
  SELECT * FROM information_schema.be_configs 
  WHERE NAME LIKE "%starlet_use_star_cache%";
  ```

- 执行以下语句查看存储缓存数据的根路径：

  ```SQL
  SELECT * FROM information_schema.be_configs 
  WHERE NAME LIKE "%storage_root_path%";
  ```

  缓存数据存储在 `storage_root_path` 的子路径 `starlet_cache/star_cache` 下。

- 执行以下语句以查看 Data Cache 的磁盘使用上限：

  ```SQL
  SELECT * FROM information_schema.be_configs 
  WHERE NAME LIKE "%starlet_star_cache_disk_size_percent%";
  ```

## 监控 Data Cache

StarRocks 提供了多种监控 Data Cache 的指标。

### Dashboard 模板

您可以根据您的 StarRocks 环境下载以下 Grafana Dashboard 模板：

- [虚拟机部署 StarRocks 存算分离集群 Dashboard 模板](http://starrocks-thirdparty.oss-cn-zhangjiakou.aliyuncs.com/StarRocks-Shared_data-for-vm.json)
- [Kubernetes 部署 StarRocks 存算分离集群 Dashboard 模板](http://starrocks-thirdparty.oss-cn-zhangjiakou.aliyuncs.com/StarRocks-Shared_data-for-k8s.json)

有关为 StarRocks 部署监控和警报服务的更多说明，请参考 [监控报警](../administration/management/monitoring/Monitor_and_Alert.md)。

### 重要指标

#### fslib read io_latency

Data Cache 的读延迟。

#### fslib write io_latency

Data Cache 的写延迟。

#### fslib star cache meta memory size

Data Cache 的预估内存使用量。

#### fslib star cache data disk size

Data Cache 的实际磁盘占用。

## 禁用 Data Cache

执行以下语句在集群运行时动态禁用 Data Cache：

```SQL
UPDATE information_schema.be_configs SET VALUE = 0 
WHERE name = "starlet_use_star_cache";
```

动态配置将在 CN 节点重新启动后失效。

如需永久禁用 Data Cache，需要将以下配置添加到 CN 配置文件 **cn.conf** 中，并重新启动 CN 节点：

```Properties
starlet_use_star_cache = false
```

## 清除缓存数据

在紧急情况下，您可以清除缓存数据。此举不会影响远程存储中的原始数据。

按照以下步骤清除 CN 节点上的缓存数据：

1. 删除存储缓存数据的子目录，即 `${storage_root_path}/starlet_cache`。

   示例：

   ```Bash
   # 假设 `storage_root_path = /data/disk1;/data/disk2`
   rm -rf /data/disk1/starlet_cache/
   rm -rf /data/disk2/starlet_cache/
   ```

2. 重启 CN 节点。

## 使用说明

- 如果云原生表的 `datacache.enable` 属性设置为 `false`，则不会为该表启用 Data Cache。
- 如果云原生表的 `datacache.partition_duration` 属性设置为一个特定的时间范围，则超出该时间范围的数据将不会被缓存。
- 从 v3.3 版本降级到 v3.2.8 及其之前版本时，如需复用先前缓存数据，需要手动修改 **starlet_cache** 目录下 Blockfile 文件名，将文件名格式从 `blockfile_{n}.{version}` 改为 `blockfile_{n}`，即去掉版本后缀。v3.2.9 及以后版本自动兼容 v3.3 文件名格式，无需手动执行该操作。您可使用以下脚本进行批量修改：

```Bash
#!/bin/bash

# 将 <starlet_cache_path> 替换为集群的 Data Cache 目录，例如 /usr/be/storage/starlet_cache
starlet_cache_path="<starlet_cache_path>"

for blockfile in ${starlet_cache_path}/blockfile_*; do
    if [ -f "$blockfile" ]; then
        new_blockfile=$(echo "$blockfile" | cut -d'.' -f1)
        mv "$blockfile" "$new_blockfile"
    fi
done
```

## 已知问题

### 内存占用过高

- 现象：当集群在低负载条件下运行时，CN 节点的总内存使用量远远超过每个模块内存使用量的总和。
- 识别：如果 `fslib star cache meta memory size` 和 `fslib star cache data memory size` 的总和占据了 CN 节点总内存使用量中的较大比例，可能表明存在此问题。
- 影响版本：v3.1.8 及以下小版本，v3.2.3 及以下小版本
- 修复版本：v3.1.9、v3.2.4
- 解决方案：
  - 将集群升级到已修复版本。
  - 如果您不想升级集群，可以清除 CN 节点的 `${storage_root_path}/starlet_cache/star_cache/meta` 目录，并重新启动节点。

## Q&A

### Q1：为什么通过 `du`、`ls` 命令看到的 Data Cache 目录占用的空间远大于实际的数据量？

Data Cache 的磁盘占用空间代表的是历史上的最高水位，与当前实际缓存的数据量没有关系。例如，假设导入了 100 GB 的数据，之后进行了 Compaction，数据量变成了 200 GB，然后又进行了数据 GC，数据量变成了 100 GB，那么 Data Cache 的磁盘占用空间仍然保持在最高水位 200 GB，但 Data Cache 内部的实际缓存数据量是 100 GB。

### Q2：Data Cache 是否会自动淘汰？

不会。Data Cache 只有在达到磁盘使用上限（默认设置下为磁盘容量的 80%）时才会开始淘汰数据。淘汰过程并不会实际删除数据，只是将旧缓存的磁盘位置标记为空，后续的新缓存写入时会覆盖旧缓存。因此，即使发生淘汰，磁盘占用空间也不会下降，不会影响实际使用。

### Q3：为什么磁盘水位一直维持在配置的最高点，不下降？

参考 Q2。Data Cache 的淘汰机制不会实际删除数据，只是将旧缓存标记为可覆盖，因此磁盘占用空间不会下降。

### Q4：为什么 DROP TABLE 后，远程存储显示表文件已删除，但 Data Cache 实际存储的数据量没有下降？

DROP TABLE 不会触发 Data Cache 删除缓存的数据。被删除表的缓存会随着时间的推移，通过 Data Cache 的内部 LRU 逻辑逐渐淘汰，不影响实际使用。

### Q5：为什么配置了 80% 的磁盘容量，实际使用却达到了 90% 甚至更多？

Data Cache 的磁盘容量使用是精确的，不会超过配置中设置的容量。磁盘实际使用超过 80% 可能是由以下原因导致的：

- 运行产生的日志文件。
- CN 崩溃产生的 Core 文件。
- 主键表的持久化索引（存储于 `${storage_root_path}/persist/` 目录下）。
- 混部 BE/CN/FE 实例共享一个磁盘。
- 外部表的缓存（存储于 `${STARROCKS_HOME}/block_cache/` 目录下）。
- 磁盘自身问题，如使用少见的 ext3 文件系统。

您可以在磁盘根目录或各个子目录执行 `du -h . -d 1` 查看具体占用空间的目录，然后删除意外的占用部分。或者，也可以通过配置 `starlet_star_cache_disk_size_percent` 调小 Data Cache 可使用的磁盘容量。

### Q6：为什么各个节点之间的缓存占用差异很大？

由于单机缓存的天然限制，各节点之间的缓存占用无法保证一致。在不影响查询延迟的情况下，这种缓存占用差异是可以接受的。该差异可能由以下因素导致：

- 不同节点加入集群的时间先后。
- 不同节点拥有的 Tablet 数量差异。
- 不同 Tablet 内数据量差异。
- 不同节点的 Compaction 和 GC 情况差异。
- 节点发生过 Crash 或 OOM 等问题。

综上所述，缓存占用的差异是由多个因素共同影响的。
