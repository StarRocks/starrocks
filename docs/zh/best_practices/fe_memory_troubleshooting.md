---
displayed_sidebar: docs
keywords: ['FE', 'Coordinator Node','memory', 'OOM', 'JVM', 'troubleshooting', 'heap']
---

# FE / 协调节点内存满问题故障排查

防止"FE内存已满"问题，快速恢复，并尽可能找出原因以防止再次发生。

:::note

- This information is applicable to FE nodes and Coordinator nodes (shared-data deployments). For readability, we will use "FE" to refer to both types of frontend nodes.
- This article mainly analyzes the situation and solutions of in-heap memory, without covering off-heap memory. If the OOM is caused by off-heap memory issues, consider reducing the JVM XMX configuration or expanding the machine's memory.

:::

## FE 内存结构与分配建议

### FE 内存组成

前端服务（FE）运行在 JVM 上，其内存消耗来自 Java 堆内存和堆外内存开销。

#### JVM 内存模块概述

在FE进程中，通过JVM分配的所有内存大致可以分为以下几个模块：

| 模块 | 类型 | 描述 |
|:--------|-------------|:------|
| **Java 堆** | 堆 | Java 对象实例的内存，例如 Plan Cache、元数据、临时对象等。|
| **类** | 堆外 | 加载 Class 时生成的元数据等结构 |
| **线程** | 堆外 | 线程占用的内存，主要是每个线程的栈空间（通常每个线程约 1MB）。|
| **代码** | 堆外 | JVM 将字节码编译为本机机器码时占用的内存 |
| **GC** | 堆外 | 垃圾收集器的内部工作数据结构 |
| **编译器** | 堆外 | HotSpot 编译器使用的内存可能与 Code 的内存类似。 |
| **其他** | 堆外 | 包括直接内存，如 Direct ByteBuffer、Netty buffer 等。 |
| **符号** | 堆外 | SymbolTable 结构，如字符串常量池、整数常量池等。 |
| **本机内存跟踪 (NMT)** | 堆外 | NMT 自身记录的内存使用所占用的内存 |
| **Arena 块** | 堆外 | JVM 内部使用 malloc 分配的临时内存块，用于临时存储和重用 |
| **日志记录** | 堆外 | 日志系统（如 log4j）占用的内存 |
| **参数** | 堆外 | JVM 启动参数传递所占用的内存 |
| **模块** | 堆外 | Java 模块系统中使用的结构 |

考虑到JVM和StarRocks的运行特性，FE的内存主要由以下几部分组成：

1. **JVM堆内存（Heap）**
   - 存储 Java 对象实例，例如 Plan Cache、元数据缓存等。
   - 最大大小通过参数 `-Xmx` 设置，并受垃圾回收机制的显著影响。

2. **非堆内存（Metaspace）**
   - 存储类定义、常量池等元数据。
   - Java 8 及更高版本替代了 PermGen。

3. **直接内存**
   - Netty、ByteBuffer、日志缓冲区、RPC 框架等使用的堆外内存。
   - 由 `sun.misc.Unsafe` 分配，通常不受 GC 控制。

4. **线程栈内存**
   - 每个 Java 线程在创建时会申请一个线程栈，默认大小约为 1MB（可通过 `-Xss` 配置）。
   - 当线程数量较多时（例如高并发场景），容易导致内存积压。

5. **JNI / 本地内存调用**
   - 数据库在通过日志组件（如 RocksDB、Netty）、Cache 等调用本地层时可能会消耗内存。

### JVM 参数配置建议

在 JVM 中，堆内存通过 `Xmx` 进行配置。通常，进程使用的内存小于 `Xmx` 的 130% 是合理的。也就是说，除了 JVM `Xmx` 配置的内存之外，还会使用一部分堆外内存。例如，如果 JVM 配置了 21g，则在 `top` 中看到的进程 RSS 内存使用量约为 27g。

根据经验，tablet 数量与 FE 内存的推荐关系如下：

| Tablet 数量 | 推荐 FE 内存配置 |
|:-------------|:------------------------------------|
| 少于 1M | 16 GB                               |
| 1M - 2M      | 32 GB                               |
| 2M - 5M      | 64 GB                               |
| 5M - 10M     | 128 GB                              |

对于独立部署环境，建议为系统预留足够的内存。除了为系统预留的内存外，还需要考虑堆外内存。

对于混合部署环境，需要根据其他服务的使用情况尽可能预留足够的内存，以防止 OOM。

在 FE 独立部署的情况下（不建议混合部署）：

1. 当机器内存小于 32G 时，`-Xmx`（最大堆大小）应配置为机器内存的最多 70%。
2. 当机器内存大于 32G 时，`-Xmx`（最大堆大小）应配置为机器内存的最多 80%。
3. 由于 FE leader 节点需要执行 checkpoint，通常情况下，leader 节点的内存约为 follower 节点的两倍。

在 3.4+ 版本中，leader 会选择一个 follower 节点执行 checkpoint，然后下载 follower 的结果并分发给其他 follower。leader 的内存使用量将接近 follower 的内存使用量。请参阅[PR #52103](https://github.com/StarRocks/starrocks/pull/52103)。

### FE JVM 监控与内存 Profile 部署

对于 FE 状态，堆监控和 GC 监控能够有效监控服务状态，也是事后排查问题的关键线索。建议及时配置监控，并在必要时配置相应的告警。

具体监控指标如下：

| 指标                  | 描述                             |
|:---------------------------|:----------------------------------------|
| `jvm_heap_size_bytes`      | 堆内存使用量。                      |
| `jvm_non_heap_size_bytes`  | 主要包括 metaspace、代码缓存等，不包含所有堆外内存。 |
| `jvm_old_gc{type="count"}` | Full GC 次数。                          |
| `jvm_old_gc{type="time"}`  | Full GC 时间。                           |

这些指标及其他指标的监控信息请参阅[监控与告警](../administration/management/monitoring/Monitor_and_Alert.md)

内存 profile 可以分析堆内存的突然增长。需要在服务安装完成后确认其是否正常工作。

在 3.3.6+ 版本中，数据库会定期打印内存 profile 并输出到 `log/proc_profile`，格式为 HTML，通过 tgz 压缩。

控制配置：

```properties
proc_profile_collect_interval_s = 600
proc_profile_collect_time_s = 300
proc_profile_cpu_enable = true
proc_profile_mem_enable = true
```

对于 3.3.6 之前的版本，可以通过脚本定期打印 profile。在每个 FE 的安装目录下新建一个名为 `mem_profiler.sh` 的脚本文件，并将以下内容复制到该文件中。

:::note

此脚本对 FE 进程无影响，不影响服务的正常使用，并会自动清理已过期 48 小时的文件。

:::

```bash
#!/bin/bash
mkdir -p mem_alloc_log

cleanup_old_files() {
    find mem_alloc_log -name "alloc-profile-*.html" -mmin +2880 -exec rm -f {} \;
}

while true
do
    cleanup_old_files

    current_time=$(date +'%Y-%m-%d-%H-%M-%S')
    file_name="mem_alloc_log/alloc-profile-${current_time}.html"
    ./bin/profiler.sh -e alloc --alloc 2m -d 300 -f "$file_name" `cat bin/fe.pid`
done
```

```bash
# 后台启动
chmod +x mem_profiler.sh
nohup ./mem_profiler.sh > mem_profiler.out 2>&1 &

# 检查进程是否存在
ps aux | grep mem_profiler.sh

# 停止进程
pkill -f mem_profiler.sh
```

## FE 崩溃

FE 崩溃的原因大致分为 4 种类型。

### 1. FE 进程内存占用过高，被操作系统杀死

您可以通过查询系统日志来检查是否为此原因：

```bash
# 查看 dmesg
dmesg | grep -iE 'out of memory|oom|kill|killed process'

# 查看消息日志
sudo grep -Ei 'killed process|oom.kill|sending SIG' -C5 /var/log/messages /var/log/syslog 2>/dev/null | tail -n 100
```

被 OOM 杀死后，日志可能会打印系统进程的内存使用情况。OOM 显示的 `total_vm` 和 `rss` 等字段以页为单位，默认 1 页 = 4KB：

- `total_vm = 8793525` → 表示已申请约 8793525 × 4KB ≈ 33.5 GB 的虚拟内存
- `rss = 7218120` → 表示实际已使用约 27.5 GB 的物理内存，即 7218120 × 4KB

OOM 日志示例：

```
Jun 10 11:20:24 tp-prod-bigdata-sr-ss-fe-2-b kernel: [ pid ]  uid  tgid  total_vm     rss  nr_ptes  swapents  oom_score_adj  name
Jun 10 11:21:58 tp-prod-bigdata-sr-ss-fe-2-b kernel: [22654]  1005 22654   8793525 7218120    14943         0              0  java
```

检查实际 RSS 使用量。如果非常接近机器内存，通常是由于 JVM 分配不合理。请参考[JVM 参数配置建议](#jvm-parameter-configuration-recommendations)章节。

### 2. FE leader 堆内存过高，触发 Full GC，导致 leader 切换

当旧 leader 因 leader 切换而退出时，`fe.out` 中会打印以下信息：

```
transfer FE type from LEADER to UNKNOWN. exit
```

或

```
transfer FE type from LEADER to FOLLOWER. exit
```

大多数 leader 切换是由 Full GC 引起的；少数情况下，是由 leader CPU 使用率过高或执行 `jstack` 触发的。

**分析 GC 日志：**

将 GC 日志上传至[https://gceasy.io/](https://gceasy.io/)并点击**暂停 GC 持续时间**。如果单次暂停 GC 的持续时间超过 30 秒，或频繁出现超过 10 秒的暂停 GC，将会触发 leader 切换。

点击**GC 前堆内存**以找到堆内存上升的时间点。利用该时间范围定位该时段的内存 profile 文件，从而确定当时是哪个模块在申请内存。

### 3. FE 程序内部 bug 导致崩溃

此情况不在本文讨论范围内。请参考[恢复元数据](../administration/Meta_recovery.md)文档。

### 4. JVM bug 导致 FE 崩溃

此情况极为罕见。遇到后，可通过查看崩溃日志来排查原因，日志位于 `log/hs_err_pid%p.log`，其中 `%p` 为进程 ID。

## FE 进程正常但内存占用过高

在 FE 长期运行过程中，堆内内存使用量可能异常增加，频繁触发 Full GC，导致查询或导入变慢。问题可能表现为堆内存**突然增加**或持续**缓慢增长**。

### 现场排查流程

打开您的监控工具：[监控与告警](../administration/management/monitoring/Monitor_and_Alert.md)

**第一步：检查以下监控**

- **FE JVM 面板**：JVM 堆内存使用量指标

**第二步：识别内存增长类型**

| 类型 | 特征 | 后续排查路径 |
|------|---------|--------------------------------|
| **突然增长** | 堆内存在短时间内（秒到分钟级）急剧飙升 | 优先查看 QPS、连接数和 SQL 请求 |
| **缓慢上升** | 内存持续增长，GC 后回收效果越来越差 | 优先排查内存泄漏或缓存积累 |

### 内存飙升排查

短时间内请求压力是否增大——高 QPS 或高并发的元数据操作可能导致内存骤增。

**1. 检查以下监控：**

- **QPS 面板**：FE 接收的请求数是否突然增加？
- **连接数面板**：FE 的并发连接数是否瞬间飙升？

**2. 通过审计表或日志确认异常 SQL：**

查询最近 5-10 分钟内的高频 SQL 语句：

```sql
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE feip = 'IP of memory abnormal node'
  AND `timestamp` >= now() - INTERVAL 1000 MINUTE
ORDER BY `timestamp` DESC;
```

重点关注：

- 含有深层子查询、多重 JOIN 及复杂聚合的语句
- 非常规业务 SQL（例如 BI 平台生成的数据集并发 SQL）
- 在 3.3.13+ 版本中，可以查看 FE 审计日志中的 `queryFeMemory` 字段，该字段表示某个查询在 FE 中总共申请了多少内存。
- 当 FE 进程因 FE OOM 退出时，会自动打印正在执行的查询及对应的 `QueryFEAllocatedMemory`。

在 FE 日志中搜索关键词：

```
QueryFEAllocatedMemory
```

**3. 通过 FE 日志检查是否存在大量元数据操作（建表和删表）：**

```bash
# 创建表
grep "Begin to unprotect create table" fe.log

# 删除表
grep "Finished drop table" fe.log
```

### 内存缓慢增长

**4. JVM 配置的内存过小**

监控 FE 进程的 GC 状态，判断是否频繁发生 Full GC。通过 `jstat` 命令查看 JVM 内存使用情况：

```bash
jstat -gcutil $pid 1000 1000
```

示例输出：

```
 S0     S1     E      O      M     CCS    YGC     YGCT    FGC    FGCT     GCT
  0.00 100.00  27.78  95.45  97.77  94.45   24    0.226     1    0.065    0.291
  0.00 100.00  44.44  95.45  97.77  94.45   24    0.226     1    0.065    0.291
  0.00 100.00  55.56  95.45  97.77  94.45   24    0.226     1    0.065    0.291
```

如果 `O`（老年代）占比一直较高，说明 JVM 内存配置存在问题，需要增大 JVM 内存。

**5. 通过 FE 的内存追踪器（3.3.7+）观察是否存在泄漏**

可以通过查看 `MemoryUsageTracker` 的日志来定位哪个模块存在内存泄漏，该日志每小时打印一次各模块的内存消耗：

```
2025-06-06 16:37:50.633+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():161] (6ms) Module Dict -
CacheDictManager estimated 0B of memory. Contains ColumnDict with 0 object(s).

2025-06-06 16:37:50.657+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():161] (21ms) Module LocalMetastore -
LocalMetastore estimated 3.8MB of memory. Contains Partition with 17473 object(s).

2025-06-06 16:37:50.667+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():161] (0ms) Module TabletInvertedIndex -
TabletInvertedIndex estimated 37.7MB of memory. Contains TabletMeta with 353459 object(s).

2025-06-06 16:37:50.741+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():108] total tracked memory: 46.8MB,
jvm: Process used: 18.6GB, heap used: 4.4GB, non heap used: 289.1MB, direct buffer used: 395.5MB
```

对比最近几条日志中各模块的内存增长情况，找出持续增长的模块。

### 现场信息采集

**如果进程尚未重启，请立即采集 `jmap` 信息。**

#### jmap 内存排查流程

**第一步：确认 FE 的进程 PID**

```bash
ps aux | grep FE
```

**第二步：使用 jmap**

:::warning[注意事项]

- `jmap -dump` 会短暂暂停 FE 进程。在对线上稳定性要求较高时请谨慎使用，建议提前告知相关人员。
- `jmap -histo` 通常对进程无影响；触发 GC 可能需要数十毫秒。
- 不建议频繁使用 `-dump` 导出堆文件，因为这会带来较大的开销。
- `histo` 足以对大对象进行初步判断，不一定需要进行 dump。

:::

**`jmap -histo:live`（在生产环境中请谨慎使用）**

通过强制执行 GC 来捕获当前堆内对象分布。使用 `live` 参数可能有助于解决内存占用过高的问题：

```bash
jmap -histo:live <pid> > jmap_histo_$(date +%s).log
```

建议采集两次样本并对比差异：

```bash
jmap -histo:live <pid> > histo_1.log
sleep 60
jmap -histo:live <pid> > histo_2.log
```

**`jmap -histo pid`（轻量级，不触发 Full GC）**

```bash
# 获取当前 JVM 堆中的所有对象，可能包含
# 已被清理的对象——会影响结果分析。
jmap -histo <pid> > histo.txt
```

**第三步：分析 Top N 对象占用**

查看文件的头部条目：

```bash
head -n 30 histo_2.log
```

字段说明：

| 列名        | 含义           |
|------------|----------------|
| num        | 类编号         |
| #instances | 实例数量       |
| #bytes     | 总字节数       |
| class name | 对象名称       |

对比 `histo_1` 和 `histo_2`，以识别哪些类的实例数量或内存占用显著增加。

示例：以 `java` 开头的类是被业务类引用的工具类。从上到下查看，排在前面的相关类占比更大。例如，`com.starrocks.lake.LakeTablet` 占用大量内存表明 tablet 使用过多。

**第四步：导出完整堆转储（若 histo 不足以分析）**

```bash
# 将触发 Full GC，请谨慎使用。
jmap -dump:live,format=b,file=heap_$(date +%s).hprof <pid>
```

该文件可能非常大（数 GB），需要使用 **MAT** 或 **VisualVM** 打开并进行分析。

**第五步：配置 OOM 时自动 dump**

在 `fe.conf` 的 JVM 配置中添加以下内容，以便在 FE 内存耗尽时自动生成 dump 文件：

```bash
JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true \
  -Xmx8192m \
  -XX:+UseG1GC \
  -Xlog:gc*:${LOG_DIR}/fe.gc.log.$DATE:time \
  -XX:ErrorFile=${LOG_DIR}/hs_err_pid%p.log \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=${LOG_DIR}/heap_dump_oom.hprof \
  -Djava.security.policy=${STARROCKS_HOME}/conf/udf_security.policy \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -Dsun.security.krb5.debug=true \
  -Dsun.security.spnego.debug=true \
  -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8113"
```

注意：`HeapDumpPath` 指定的是目录路径，而非文件名。

#### 保留内存分析文件以供分析

从 3.3.6 版本开始，内存分析文件输出至 `log/proc_profile` 目录，格式为 HTML，并使用 tgz 压缩。

找到与内存增长时间对应的文件，解压后在浏览器中打开，即可查看内存分配的火焰图。堆栈中较宽的部分表示分配了更多内存。

如果在内存增长期间未打印内存分析文件，可以关闭 CPU 分析并将打印间隔调整为 5 分钟：

```properties
proc_profile_cpu_enable = false
proc_profile_collect_interval_s = 300
```

这些是动态参数，可直接修改而无需重启：

```sql
ADMIN SET FRONTEND CONFIG ("proc_profile_cpu_enable" = "false");
ADMIN SET FRONTEND CONFIG ("proc_profile_collect_interval_s" = "300");
```

## 紧急恢复

**1. 停止 FE 服务**

**2. 将堆内存配置调整为更大的值并重启**

修改 `fe.conf` 中的 `-Xmx` 值：

```bash
JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC \
  -Xlog:gc*:${LOG_DIR}/fe.gc.log.$DATE:time \
  -XX:ErrorFile=${LOG_DIR}/hs_err_pid%p.log \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=${LOG_DIR}/heap_dump_oom.hprof \
  -Djava.security.policy=${STARROCKS_HOME}/conf/udf_security.policy \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8113"
```

**3. 如果机器本身内存有限**，重启后仍可能出现 OOM 问题。请考虑扩充机器内存，然后增大 JVM 配置后再重启。

**4. 如果在案例中找到了历史对应的 PR**，建议升级至对应的修复版本。

## 案例集锦

### 案例一：频繁且大规模的元数据操作导致 FE 内存占用过高

**背景**：某客户业务需要定期建表和清理，顺序操作每秒创建数十张表，同步操作将其删除，整个过程中累计创建了数十万张表。

**问题表现**：堆内存和机器 CPU 持续上升，查询响应变慢。查看 FE 日志后，发现大量建表和删表操作，导致服务压力急剧增加。

**恢复方法**：停止建表和删除表的作业，重启服务，恢复正常使用。

**解决方案**：针对此业务场景，使用 `DROP TABLE` 时必须添加 `FORCE`，否则元数据将持续被占用。同时，降低建表的并发频率。

***

### 案例 2：复杂并发 SQL 导致 FE 内存突然增加

**问题表现**：堆内存突然增加，定时导入任务明显延迟。QPS 和连接数在正常变化范围内。通过审计日志和 FE 日志发现，并发定时 SQL 中存在大量高度复杂的查询，导致查询计划解析过程中占用了大量内存。

**恢复方法**：扩展 FE 服务的内存——停机扩容内存，增大 JVM XMX 配置，并重启服务。

**解决方案**：拆分复杂 SQL，观察作业高峰期的内存使用情况，确保其保持在 80% 以下。

***

### 案例 3：MV 刷新过于频繁，导致 leader FE 内存缓慢增长并持续偏高

**背景**：用户存在 MV 刷新任务，且许多任务配置了分钟级或秒级刷新。

**问题表现**：FE leader 内存持续上升，查询出现卡顿。检查 FE 日志发现大量慢锁，导致占用的资源无法释放。

慢锁日志示例：

```
2025-06-06 15:41:43.813+08:00 WARN (AutoStatistic|41)
[LockManager.logSlowLockTrace():423] LockManager detects slow lock :
{"owners":[{"id":13479085,"name":"starrocks-taskrun-pool-22075","type":"INTENTION_SHARED","heldFor":6935,"queryId":"ab976f93-42a9-11f0-98e6-fa163e3710f8","waitTime":0,"stack":["java.base@11.0.20.1/java.net.SocketInputStream.socketRead0(Native Method)","java.base@11.0.20.1/java.net.SocketInputStream.socketRead(SocketInputStream.java:115)","java.base@11.0.20.1/java.net.SocketInputStream.read(SocketInputStream.java:168)","java.base@11.0.20.1/java.net.SocketInputStream.read(SocketInputStream.java:140)","app//org.postgresql.core.VisibleBufferedInputStream.readMore(VisibleBufferedInputStream.java:161)","app//org.postgresql.core.VisibleBufferedInputStream.ensureBytes(VisibleBufferedInputStream.java:128)","app//org.postgresql.core.VisibleBufferedInputStream.ensureBytes(VisibleBufferedInputStream.java:113)","app//org.postgresql.core.VisibleBufferedInputStream.read(VisibleBufferedInputStream.java:73)","app//org.postgresql.core.PGStream.receiveChar(PGStream.java:453)","app//org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:2120)","app//org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:356)","app//org.postgresql.jdbc.PgStatement.executeInternal(PgStatement.java:496)","app//org.postgresql.jdbc.PgStatement.execute(PgStatement.java:413)","app//org.postgresql.jdbc.PgStatement.executeWithFlags(PgStatement.java:333)","app//org.postgresql.jdbc.PgStatement.executeCachedSql(PgStatement.java:319)","app//org.postgresql.jdbc.PgStatement.executeWithFlags(PgStatement.java:295)","app//org.postgresql.jdbc.PgStatement.executeQuery(PgStatement.java:244)","app//org.postgresql.jdbc.PgDatabaseMetaData.getColumns(PgDatabaseMetaData.java:1584)","app//com.starrocks.connector.jdbc.PostgresSchemaResolver.getColumns(PostgresSchemaResolver.java:49)","app//com.starrocks.connector.jdbc.JDBCMetadata.lambda$getTable$1(JDBCMetadata.java:200)","app//com.starrocks.connector.jdbc.JDBCMetadata$$Lambda$1340/0x000014e17aa20960.apply(Unknown Source)","app//com.starrocks.connector.jdbc.JDBCMetaCache.get(JDBCMetaCache.java:73)","app//com.starrocks.connector.jdbc.JDBCMetadata.getTable(JDBCMetadata.java:197)","app//com.starrocks.connector.CatalogConnectorMetadata.getTable(CatalogConnectorMetadata.java:136)","app//com.starrocks.server.MetadataMgr.lambda$getTable$5(MetadataMgr.java:501)","app//com.starrocks.server.MetadataMgr$$Lambda$513/0x000014e2116f0cb0.apply(Unknown Source)","java.base@11.0.20.1/java.util.Optional.map(Optional.java:265)","app//com.starrocks.server.MetadataMgr.getTable(MetadataMgr.java:501)","app//com.starrocks.sql.analyzer.QueryAnalyzer.resolveTable(QueryAnalyzer.java:1391)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.resolveTableRef(QueryAnalyzer.java:482)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.resolveTableRef(QueryAnalyzer.java:420)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.resolveTableRef(QueryAnalyzer.java:420)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:363)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SelectRelation.accept(SelectRelation.java:232)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryRelation(QueryAnalyzer.java:303)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:293)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.QueryStatement.accept(QueryStatement.java:70)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:907)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SubqueryRelation.accept(SubqueryRelation.java:66)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:370)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SelectRelation.accept(SelectRelation.java:232)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryRelation(QueryAnalyzer.java:303)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:293)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.QueryStatement.accept(QueryStatement.java:70)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:907)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SubqueryRelation.accept(SubqueryRelation.java:66)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitJoin(QueryAnalyzer.java:739)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitJoin(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.JoinRelation.accept(JoinRelation.java:134)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:370)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SelectRelation.accept(SelectRelation.java:232)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryRelation(QueryAnalyzer.java:303)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:293)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.QueryStatement.accept(QueryStatement.java:70)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer.analyze(QueryAnalyzer.java:121)","app//com.starrocks.sql.analyzer.InsertAnalyzer.analyzeWithDeferredLock(InsertAnalyzer.java:74)","app//com.starrocks.sql.analyzer.InsertAnalyzer.analyze(InsertAnalyzer.java:62)","app//com.starrocks.sql.analyzer.Analyzer$AnalyzerVisitor.visitInsertStatement(Analyzer.java:398)","app//com.starrocks.sql.analyzer.Analyzer$AnalyzerVisitor.visitInsertStatement(Analyzer.java:176)","app//com.starrocks.sql.ast.InsertStmt.accept(InsertStmt.java:304)","app//com.starrocks.sql.ast.AstVisitor.visit(AstVisitor.java:80)","app//com.starrocks.sql.analyzer.Analyzer.analyze(Analyzer.java:173)","app//com.starrocks.sql.StatementPlanner.analyzeStatement(StatementPlanner.java:218)","app//com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:116)","app//com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:95)","app//com.starrocks.load.InsertOverwriteJobRunner.executeInsert(InsertOverwriteJobRunner.java:351)","app//com.starrocks.load.InsertOverwriteJobRunner.doLoad(InsertOverwriteJobRunner.java:171)","app//com.starrocks.load.InsertOverwriteJobRunner.handle(InsertOverwriteJobRunner.java:151)","app//com.starrocks.load.InsertOverwriteJobRunner.transferTo(InsertOverwriteJobRunner.java:212)","app//com.starrocks.load.InsertOverwriteJobRunner.prepare(InsertOverwriteJobRunner.java:256)","app//com.starrocks.load.InsertOverwriteJobRunner.handle(InsertOverwriteJobRunner.java:148)","app//com.starrocks.load.InsertOverwriteJobRunner.run(InsertOverwriteJobRunner.java:136)","app//com.starrocks.load.InsertOverwriteJobMgr.executeJob(InsertOverwriteJobMgr.java:91)","app//com.starrocks.qe.StmtExecutor.handleInsertOverwrite(StmtExecutor.java:2152)","app//com.starrocks.qe.StmtExecutor.handleDMLStmt(StmtExecutor.java:2244)","app//com.starrocks.qe.StmtExecutor.handleDMLStmtWithProfile(StmtExecutor.java:2161)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.refreshMaterializedView(PartitionBasedMvRefreshProcessor.java:1271)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.doRefreshMaterializedView(PartitionBasedMvRefreshProcessor.java:464)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.doRefreshMaterializedViewWithRetry(PartitionBasedMvRefreshProcessor.java:373)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.doMvRefresh(PartitionBasedMvRefreshProcessor.java:332)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.processTaskRun(PartitionBasedMvRefreshProcessor.java:200)","app//com.starrocks.scheduler.TaskRun.executeTaskRun(TaskRun.java:285)","app//com.starrocks.scheduler.TaskRunExecutor.lambda$executeTaskRun$0(TaskRunExecutor.java:60)","app//com.starrocks.scheduler.TaskRunExecutor$$Lambda$3311/0x000014db15a06cb0.get(Unknown Source)","java.base@11.0.20.1/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1700)","java.base@11.0.20.1/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)","java.base@11.0.20.1/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)","java.base@11.0.20.1/java.lang.Thread.run(Thread.java:829)"]}],"waiter":[{"id":13446845,"name":"thrift-server-pool-367701","type":"WRITE","waitTime":6895},{"id":5268046,"name":"lake-publish-task-8973","type":"INTENTION_EXCLUSIVE","waitTime":6895},{"id":5268447,"name":"lake-publish-task-9181","type":"INTENTION_EXCLUSIVE","waitTime":6874},{"id":5267898,"name":"lake-publish-task-8911","type":"INTENTION_EXCLUSIVE","waitTime":6874},{"id":5267726,"name":"lake-publish-task-8827","type":"INTENTION_EXCLUSIVE","waitTime":6874},{"id":13494208,"name":"thrift-server-pool-369089","type":"INTENTION_EXCLUSIVE","waitTime":6857},{"id":13492395,"name":"starrocks-taskrun-pool-22096","type":"INTENTION_EXCLUSIVE","waitTime":6854},{"id":41,"name":"AutoStatistic","type":"INTENTION_SHARED","waitTime":6854,"queryId":"ad195549-42a9-11f0-98e6-fa163e3710f8"},{"id":486,"name":"nioEventLoopGroup-6-33","type":"INTENTION_SHARED","waitTime":6852},{"id":5267924,"name":"lake-publish-task-8925","type":"INTENTION_SHARED","waitTime":6849},{"id":5271934,"name":"lake-publish-task-9265","type":"INTENTION_SHARED","waitTime":6849},{"id":5344442,"name":"lake-publish-task-9312","type":"INTENTION_EXCLUSIVE","waitTime":6786},{"id":13471238,"name":"thrift-server-pool-368401","type":"INTENTION_SHARED","waitTime":55},{"id":13494164,"name":"thrift-server-pool-369065","type":"INTENTION_SHARED","waitTime":55},{"id":13481141,"name":"thrift-server-pool-368676","type":"INTENTION_SHARED","waitTime":54}]}
```

**恢复方法**：leader GC 后，切换为主节点并自动恢复。

**解决方案**：放宽刷新频率。将非实时强依赖场景调整为小时级甚至天级，避免大量分钟级 MV 同时触发。慢锁消失后，问题即可解决。

***

### 案例 4：查询 Iceberg catalog 中的外部表导致 FE 内存增加

**背景**：用户对 Iceberg 表执行查询作业。

**问题表现**：提交 SQL 后，FE leader 的内存显著增加，CPU 也持续上升。其他查询和导入任务报错。检查进程状态发现频繁触发 GC，导致服务状态异常。

**恢复方法**：重启 leader 节点，并定期管理对应 Iceberg 表的 delete files。

***

### 案例 5：Insert 内存泄漏问题导致 FE 内存异常增加

**背景**：用户在操作过程中发现 FE 内存异常增加，进程重启，且 leader 未能正常切换。客户端集群的主要任务为导入任务。

**问题表现**：导出 leader 节点进程的 `jmap` 以进行排查：

```bash
jmap -histo <FEpid> > jmap.txt  # This operation will not trigger Full GC
```

在导出文件中发现：

```bash
14: 521358 154321968 com.starrocks.load.loadv2.InsertLoadJob
```

`InsertLoadJob` 实例数量为 52 万。当该数量超过 1 万时，表明已发生内存泄漏。

- [Issue](https://github.com/StarRocks/starrocks/issues/538100
- [修复 PR](https://github.com/StarRocks/starrocks/pull/53809)

**受影响版本**：

- 3.1.0 - 3.1.16
- 3.2.0 - 3.2.12
- 3.3.0 - 3.3.7

**修复版本**：3.1.17+、3.2.13+、3.3.8+

**恢复方法**：升级集群版本。

***

### 案例 6：async-profiler 导致 JVM 崩溃，进而引发 FE 崩溃

**问题表现**：FE 进程崩溃或重启。

**问题排查**：当进程异常且日志目录中生成了 `hs_err_pid$pid.log` 文件时，FE 崩溃可能是由 JVM 崩溃引起的。

```
--------------- S U M M A R Y ------------
...
--------------- T H R E A D ---------------
Current thread: JavaThread "tablet scheduler" ...

Native frames:
V  [libjvm.so] frame::entry_frame_is_first() const
V  [libjvm.so] forte_fill_call_trace_given_top(...)
V  [libjvm.so] AsyncGetCallTrace
C  [libasyncProfiler.so] Profiler::getJavaTraceAsync(...)
C  [libasyncProfiler.so] Profiler::recordSample(...)
C  [libasyncProfiler.so] PerfEvents::signalHandler(...)
```

崩溃断点 `PerfEvents::signalHandler` 由 `async-profiler` 引起。该工具在尝试获取 Java 调用栈时，在 JVM 内部触发了段错误。在企业版中，该工具默认部署，用于定期获取进程信息。

**处理方法**：停止 `async-profiler` CPU 信息采集：

```sql
ADMIN SET FRONTEND CONFIG ("proc_profile_cpu_enable" = "false");
```

同时在 `fe.conf` 中添加以下内容：

```properties
proc_profile_cpu_enable = false
```

***

### 案例 7：JVM 堆配置不足导致 OOM

正常情况下，3 个 FE 中，若 Leader 节点因内存不足崩溃，剩余 2 个 FE 可以选举出新的 Leader 并提供服务。若 Leader 崩溃后 2 个 Follower 节点无法正常通信，即将晋升为 Leader 的 FE 也可能因元数据副本不足而退出，导致 2 个 FE 同时崩溃。

**问题排查**：

FE Leader 日志显示：

```
java.lang.OutOfMemoryError: Java heap space
2025-08-09 08:22:30.926-06:00 WARN (thrift-server-pool-9443633|65019425)
[TIOStreamTransport.close():153] Error closing output stream.
java.net.SocketException: Socket closed
    at java.net.SocketOutputStream.socketWrite(SocketOutputStream.java:113) ~[?:?]
    at java.net.SocketOutputStream.write(SocketOutputStream.java:150) ~[?:?]
    at java.io.BufferedOutputStream.flushBuffer(BufferedOutputStream.java:81) ~[?:?]
    at java.io.BufferedOutputStream.flush(BufferedOutputStream.java:142) ~[?:?]
    at java.io.FilterOutputStream.close(FilterOutputStream.java:182) ~[?:?]
    at org.apache.thrift.transport.TIOStreamTransport.close(TIOStreamTransport.java:151) ~[libthrift-0.20.0.jar:0.20.0]
    at org.apache.thrift.transport.TSocket.close(TSocket.java:238) ~[libthrift-0.20.0.jar:0.20.0]
    at com.starrocks.common.SRTThreadPoolServer$WorkerProcess.run(SRTThreadPoolServer.java:326) ~[starrocks-fe.jar:?]
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) ~[?:?]
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) ~[?:?]
    at java.lang.Thread.run(Thread.java:829) ~[?:?]
2025-08-09 08:22:30.927-06:00 ERROR (thrift-server-accept|144)
[SRTThreadPoolServer.execute():221] ExecutorService threw error:
java.lang.OutOfMemoryError: Java heap space
java.lang.OutOfMemoryError: Java heap space
2025-08-09 08:22:30.931-06:00 WARN (starrocks-mysql-nio-pool-42252|65019486)
[StmtExecutor.execute():595] New planner error: ...
com.starrocks.sql.common.StarRocksPlannerException: StarRocks planner use long time 3000 ms in memo phase,
This probably because 1. FE Full GC, 2. Hive external table fetch metadata took a long time, 3. The SQL is
very complex. You could 1. adjust FE JVM config, 2. try query again, 3. enlarge new_planner_optimize_timeout
session variable
    at com.starrocks.sql.optimizer.task.SeriallyTaskScheduler.executeTasks(SeriallyTaskScheduler.java:50) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.optimizer.Optimizer.memoOptimize(Optimizer.java:900) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.optimizer.Optimizer.optimizeByCost(Optimizer.java:272) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.optimizer.Optimizer.optimize(Optimizer.java:196) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.StatementPlanner.createQueryPlanWithReTry(StatementPlanner.java:348) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:138) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:95) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.StmtExecutor.execute(StmtExecutor.java:580) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.ConnectProcessor.handleQuery(ConnectProcessor.java:389) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.ConnectProcessor.dispatch(ConnectProcessor.java:598) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.ConnectProcessor.processOnce(ConnectProcessor.java:936) ~[starrocks-fe.jar:?]
    at com.starrocks.mysql.nio.ReadListener.lambda$handleEvent$0(ReadListener.java:69) ~[starrocks-fe.jar:?]
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) ~[?:?]
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) ~[?:?]
    at java.lang.Thread.run(Thread.java:829) ~[?:?]
2025-08-09 08:22:30.932-06:00 INFO (TaskCleaner|111)
[TaskManager.dropTasks():392] drop tasks:[]
2025-08-09 08:22:35.431-06:00 INFO (main|1)
[StarRocksFE.start():123] StarRocks FE starting, version: 3.3.14-ee-5b29ea9
```

正在晋升为 Leader 的 FE 失败：

```
    at com.starrocks.common.util.Daemon.run(Daemon.java:109) ~[starrocks-fe.jar:?]
2025-08-09 08:16:07.844-06:00 ERROR (stateChangeExecutor|86)
[GlobalStateMgr.transferToLeader():1333] failed to init journal after transfer to leader! will exit
com.starrocks.journal.JournalException: catch exception after retried 3 times
    at com.starrocks.journal.bdbje.BDBJEJournal.open(BDBJEJournal.java:222) ~[starrocks-fe.jar:?]
    at com.starrocks.server.GlobalStateMgr.transferToLeader(GlobalStateMgr.java:1323) ~[starrocks-fe.jar:?]
    at com.starrocks.server.GlobalStateMgr$1.transferToLeader(GlobalStateMgr.java:795) ~[starrocks-fe.jar:?]
    at com.starrocks.ha.StateChangeExecutor.runOneCycle(StateChangeExecutor.java:125) ~[starrocks-fe.jar:?]
    at com.starrocks.common.util.Daemon.run(Daemon.java:109) ~[starrocks-fe.jar:?]
Caused by: com.sleepycat.je.rep.InsufficientReplicasException: (JE 18.3.20) Commit policy:
SIMPLE_MAJORITY required 1 replica. But none were active with this master.
    at com.sleepycat.je.rep.impl.node.DurabilityQuorum.ensureReplicasForCommit(DurabilityQuorum.java:116) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.impl.RepImpl.txnBeginHook(RepImpl.java:1171) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn.txnBeginHook(MasterTxn.java:195) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.initTxn(Txn.java:384) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.<init>(Txn.java:288) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.<init>(Txn.java:267) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn.<init>(MasterTxn.java:146) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn$1.create(MasterTxn.java:117) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn.create(MasterTxn.java:435) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.impl.RepImpl.createRepUserTxn(RepImpl.java:1145) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.createAutoTxn(Txn.java:334) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.LockerFactory.getWritableLocker(LockerFactory.java:79) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.Environment.setupDatabase(Environment.java:816) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.Environment.openDatabase(Environment.java:668) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.starrocks.journal.bdbje.BDBEnvironment.openDatabase(BDBEnvironment.java:446) ~[starrocks-fe.jar:?]
    at com.starrocks.journal.bdbje.BDBJEJournal.open(BDBJEJournal.java:213) ~[starrocks-fe.jar:?]
    ... 4 more
2025-08-09 08:16:07.845-06:00 INFO (Thread-69|145)
[StarRocksFE.lambda$addShutdownHook$1():374] start to execute shutdown hook
2025-08-09 08:16:07.852-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 69224
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 31128
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:SET NAMES utf8, QueryFEAllocatedMemory: 64464
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:SET NAMES utf8, QueryFEAllocatedMemory: 119040
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 83760
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 69080
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 83760
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 84072
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:SET NAMES utf8, QueryFEAllocatedMemory: 10048
```

**处理方法**：增加 FE JVM 内存配置，生产环境建议最低 16GB，后续根据元数据增量进行调整。恢复步骤请参考 [元数据恢复](../administration/Meta_recovery.md) 文档中的步骤 8 和 9。

***

### 案例 8：FE 响应缓慢导致导入任务耗时增加

**问题现象**：Agent 在与 FE 通信时，若遇到 FE 未及时响应的情况，会打印 `jstack`。两个 Follower FE 的日志中包含打印消息 "notify new FE type"。Agent 日志中存在大量 `jstack` 采集记录。

`jstack` 采集加剧了 FE 无响应状态的持续时间。

**处理方法**：关闭 `jstack` 采集。3.3+ 版本已默认关闭。可在集群配置中移除 `jstack` 命令。

***

### 案例 9：FE 内存缓慢增长（版本 3.3.13 - 3.3.18）

**版本范围**：此问题出现在 3.3.13 至 3.3.18 版本之间。

**问题现象**：若发现 FE 堆内存持续缓慢增长，可能由此问题引起。

获取 `jmap` 并检查后发现，`com.starrocks.catalog.Replica` 占用了过多内存。所有涉及大量 `DROP`、`SWAP` 和 `INSERT OVERWRITE` 的操作均可能触发此问题。

此问题源于 3.3.13 版本对 tablet 删除路径的优化，该优化引入了内存泄漏，已在 3.3.18 版本中修复。

- 3.3.13 中的优化 PR：[BugFix] Fix recycle bin missing to delete lake mv's expired partitions after mv refreshed
- 修复 PR 位于 [3.3.18](https://github.com/StarRocks/starrocks/pull/61582)

**处理方法**:

- 临时解决方案：重启 FE
- 根本解决方案：升级到已修复的版本

**已修复版本**：3.3.18、3.4.7、3.5.4

***

## 附录：工具使用命令

### jstat — 监控 JVM GC 状态

```bash
jstat -gcutil <pid> 1000 10
```

每隔 1 秒打印一次 FE 进程的 GC 使用情况，连续打印 10 次。

**常用指标：**

| 指标 | 含义 |
|-----------|-----------------------------------------------|
| S0, S1 | Survivor 区使用率（年轻代） |
| E | Eden 区使用率（年轻代） |
| O | 老年代使用率 |
| YGC, YGCT | 年轻代 GC 次数与耗时 |
| FGC, FGCT | Full GC 次数与耗时 |

**用途**：快速判断 GC 是否频繁、老年代是否接近满载，以及是否存在 Full GC。

### jmap — 查看 JVM 内存对象分布

**方法一：查看当前堆中的对象（包括垃圾对象）**

```bash
jmap -histo <pid> | head -n 30
```

统计所有对象类型的实例数量和占用大小。用于检查堆中的大对象是什么，例如 `byte[]`、`String`、`HashMap` 等。

**方法二：强制 GC 后检查存活对象**

```bash
jmap -histo:live <pid> | head -n 30
```

触发一次 Full GC，然后仅显示 GC 后仍然存活的对象。用于排查对象泄漏或缓存未释放等问题。
