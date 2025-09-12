---
displayed_sidebar: docs
keywords: ['shen ji ri zhi']
---

在部署和操作StarRocks时，理解并正确使用日志系统对于故障排除、性能分析和系统调优至关重要。本文详细介绍了StarRocks的Frontend（FE）和Backend（BE或CN）组件的日志文件类型、典型内容、配置方法以及日志滚动和保留策略。

本文档中的信息基于StarRocks版本3.5.x。

## FE日志详解

### `fe.log`

主要的FE日志包括启动过程、集群状态变化、DML/DQL请求和调度相关信息。这些日志主要记录FE在运行时的行为。

#### 配置

- `sys_log_dir`: 日志存储目录。默认是`${STARROCKS_HOME}/log`
- `sys_log_level`: 日志级别。默认是`INFO`
- `sys_log_roll_num`: 控制保留的日志文件数量，以防止无限增长消耗过多磁盘空间。默认是10
- `sys_log_roll_interval`: 指定轮换频率。默认是`DAY`，意味着日志每天轮换
- `sys_log_delete_age`: 控制旧日志文件在删除前保留的时间。默认是7天
- `sys_log_roll_mode`: 日志轮换模式。默认是`SIZE-MB-1024`，意味着当前日志文件达到1024 MB时将创建新文件。与sys_log_roll_interval结合使用，表示FE日志可以按天或文件大小轮换
- `sys_log_enable_compress`: 控制是否启用日志压缩。默认是false，意味着压缩未启用

### `fe.warn.log`

`fe.warn.log`是用于系统监控和故障排除的重要日志文件：
- 操作监控——监控系统健康状态
- 故障诊断——快速定位关键问题
- 性能分析——识别系统瓶颈和异常
- 安全审计——记录权限和访问错误
与记录所有级别日志的`fe.log`相比，`fe.warn.log`专注于需要注意的警告和错误，帮助运维人员快速识别和解决系统问题。

### `fe.gc.log`

`fe.gc.log`是StarRocks FE的Java垃圾回收日志，用于监控和分析JVM垃圾回收行为。

需要注意的是，该日志文件使用本机JVM日志轮换机制。例如，可以通过以下配置启用基于文件大小和文件数量的自动轮换：

```bash
-Xlog:gc*:${LOG_DIR}/fe.gc.log:time,tags:filecount=7,filesize=100M
```

### `fe.out`

`fe.out`是StarRocks FE的标准输出日志文件。它记录了FE进程运行期间打印到标准输出（stdout）和标准错误（stderr）的内容。主要内容包括：

- FE启动期间的控制台输出
  - JVM启动信息（例如，堆设置，GC参数）
  - FE模块初始化顺序（Catalog, Scheduler, RPC, HTTP Server等）
- 来自stderr的错误堆栈跟踪
  - Java异常（Exception/StackTrace）
  - 未捕获的错误，如ClassNotFound, NullPointerException等
- 未被其他日志系统捕获的输出
  - 一些使用`System.out.println()`或`e.printStackTrace()`的第三方库

在以下场景中应检查`fe.out`：

- FE启动失败：检查`fe.out`中的Java异常或无效参数消息。
- FE意外崩溃：查找未捕获的异常堆栈跟踪。
- 日志系统不确定性：如果`fe.out`是FE日志目录中唯一有输出的日志文件，则可能是`log4j`初始化失败或配置不正确。
默认情况下，`fe.out`不支持自动日志轮换。

### `fe.profile.log`

`fe.profile.log`的目的是记录详细的查询执行信息以进行性能分析。其主要功能包括：

- 查询性能分析：记录每个查询的详细执行数据，包括：
  - 查询ID、用户、数据库、SQL语句
  - 执行时间（startTime, endTime, latency）
  - 资源使用情况（CPU、内存、扫描行数/大小）
  - 执行状态（RUNNING, FINISHED, FAILED, CANCELLED）
- 运行时指标跟踪：通过QueryDetail类捕获关键指标：
  - `scanRows / scanBytes`: 扫描的数据量
  - `returnRows`: 返回的结果行数
  - `cpuCostNs`: 消耗的CPU时间（纳秒）
  - `memCostBytes`: 内存使用量
  - `spillBytes`: 溢出到磁盘的数据量
- 错误诊断：记录失败查询的错误消息和堆栈跟踪
- 资源组监控：跟踪不同资源组的查询执行指标
`fe.profile.log`以JSON格式存储。

#### 配置

- `enable_profile_log`: 是否启用profile日志
- `profile_log_dir`: 存储profile日志的目录
- `profile_log_roll_size_mb`: 日志轮换大小（MB）
- `profile_log_roll_num`: 控制保留的profile日志文件数量，以防止无限增长和过多磁盘使用。默认是5
- `profile_log_roll_interval`: 指定轮换频率。默认是DAY，意味着每天轮换。当满足轮换条件时，保留最新的5个文件，删除旧文件
- `profile_log_delete_age`: 控制旧文件在删除前保留的时间。默认是1天
- `enable_profile_log_compress`: 控制是否启用profile日志压缩。默认是false，意味着压缩未启用

### `fe.internal.log`

`fe.internal.log`的目的是记录专用于FE（Frontend）内部操作的日志，主要用于系统级审计和调试。其主要功能包括：
1. 内部操作审计：将系统发起的内部SQL执行与用户查询分开记录
2. 统计跟踪：专门记录与统计收集相关的操作
3. 调试支持：提供详细日志以支持内部操作问题的故障排除
日志记录包括以下条目：
- 统计模块（internal.statistic）
- 核心系统模块（internal.base）

此日志特别有助于分析StarRocks的内部统计收集过程和诊断与内部操作相关的问题。

#### 配置

- `internal_log_dir`: 控制此日志的存储目录
- `internal_log_modules`: 配置内部日志模块的数组，定义需要记录在`fe.internal.log`文件中的内部操作模块。默认是`{"base", "statistic"}`
- `internal_log_roll_num`: 保留的文件数量。默认是90
- `internal_log_roll_interval`: 指定轮换频率。默认是DAY，意味着每天轮换。当满足轮换条件时，保留最新的90个文件，删除旧文件
- `internal_log_delete_age`: 控制旧文件在删除前保留的时间。默认是7天

### `fe.audit.log`

这是StarRocks的查询审计日志，记录所有用户查询和连接的详细信息。用于监控、分析和审计。其主要目的包括：

1. 查询监控：记录所有SQL查询的执行状态和性能指标
2. 用户审计：跟踪用户行为和数据库访问
3. 性能分析：提供查询执行时间和资源消耗等指标
4. 问题诊断：记录错误状态和错误代码以便于故障排除

#### 配置

- `audit_log_dir`: 控制此日志的存储目录
- `audit_log_roll_num`: 保留的文件数量。默认是90
- `audit_log_roll_interval`: 指定轮换频率。默认是DAY，意味着每天轮换。当满足轮换条件时，保留最新的90个文件，删除旧文件
- `audit_log_delete_age`: 控制旧文件在删除前保留的时间。默认是7天
- `audit_log_json_format`: 是否以JSON格式记录。默认是false
- `audit_log_enable_compress`: 是否启用压缩

### `fe.big_query.log`

这是StarRocks专用的大查询日志文件，用于监控和分析高资源消耗的查询。其结构类似于审计日志，但包括三个附加字段：
- `bigQueryLogCPUSecondThreshold`: CPU时间阈值
- `bigQueryLogScanBytesThreshold`: 扫描大小（字节）阈值
- `bigQueryLogScanRowsThreshold`: 扫描行数阈值

#### 配置

- `big_query_log_dir`: 控制此日志的存储目录
- `big_query_log_roll_num`: 保留的文件数量。默认是10
- `big_query_log_modules`: 内部日志模块类型。默认是query
- `big_query_log_roll_interval`: 指定轮换频率。默认是DAY，意味着每天轮换。当满足轮换条件时，保留最新的10个文件，删除旧文件
- `big_query_log_delete_age`: 控制旧文件在删除前保留的时间。默认是7天

### `fe.dump.log`

这是StarRocks的查询转储日志，专门用于详细的查询调试和问题诊断。其主要目的包括：
- 异常调试：在查询执行遇到异常时自动记录完整的查询上下文
- 问题重现：提供足够详细的信息以重现查询问题
- 深入诊断：包含调试信息，如元数据、统计信息、执行计划等
- 技术支持：为技术支持团队提供全面的数据以分析问题
可以通过以下命令启用：

```bash
SET enable_query_dump = true;
```

#### 配置

- `dump_log_dir`: 控制此日志的存储目录
- `dump_log_roll_num`: 保留的文件数量。默认是10
- `dump_log_modules`: 内部日志模块类型。默认是query
- `dump_log_roll_interval`: 指定轮换频率。默认是DAY，意味着每天轮换。当满足轮换条件时，保留最新的10个文件，删除旧文件
- `dump_log_delete_age`: 控制旧文件在删除前保留的时间。默认是7天

### `fe.features.log`
这是StarRocks的查询计划特征日志，用于收集和记录查询执行计划的特征信息。主要服务于机器学习和查询优化分析。关键目的包括：
1. 查询计划特征收集：从查询执行计划中提取各种特征
2. 机器学习数据源：为查询成本预测模型提供训练数据
3. 查询模式分析：分析执行模式和特征分布
4. 优化器改进：提供数据以支持CBO优化器的增强

可以通过配置启用。

```bash
// 启用计划特征收集  
enable_plan_feature_collection = false  // 默认禁用  

// 启用查询成本预测  
enable_query_cost_prediction = false  // 默认禁用  
```

#### 配置

- `feature_log_dir`: 控制此日志的存储目录
- `feature_log_roll_num`: 保留的文件数量。默认是5
- `feature_log_roll_interval`: 指定轮换频率。默认是DAY，意味着每天轮换。当满足轮换条件时，保留最新的5个文件，删除旧文件
- `feature_log_delete_age`: 控制旧文件在删除前保留的时间。默认是3天
- `feature_log_roll_size_mb`: 日志轮换大小。默认是1024 MB，意味着每1 GB创建一个新文件

## BE/CN日志详解

### `{be or cn}.INFO.log`

主要记录由BE/CN节点生成的各种运行时行为日志，这些日志处于`INFO`级别。例如：

- 系统启动信息：
  - BE进程启动和初始化
  - 硬件资源检测（CPU、内存、磁盘）
  - 配置参数加载
- 查询执行信息：
  - 查询接收和分发
  - 片段执行状态
- 存储相关信息：
  - Tablet加载和卸载
  - Compaction执行过程
  - 数据导入状态
  - 存储空间管理

#### 配置

- `sys_log_level`: 日志级别，默认是`INFO`
- `sys_log_dir`: 日志存储目录，默认是`${STARROCKS_HOME}/log`
- `sys_log_roll_mode`: 日志轮换模式，默认是`SIZE-MB-1024`，意味着当前日志文件达到1024 MB时将创建新文件
- `sys_log_roll_num`: 保留的日志文件数量，默认是10

### `{be or cn}.WARN.log`

`be.WARN.log`存储警告级别及以上的日志条目。示例包括：
查询执行警告：
- 查询执行时间过长
- 内存分配失败警告
- Operator执行异常
存储相关警告：
- 异常Tablet状态
- 慢速Compaction执行
- 数据文件损坏警告
- 存储I/O异常

#### 配置

- `sys_log_level`: 日志级别，默认是INFO
- `sys_log_dir`: 日志存储目录，默认是`${STARROCKS_HOME}/log`
- `sys_log_roll_mode`: 日志轮换模式，默认是`SIZE-MB-1024`，意味着当前日志文件达到1024 MB时将创建新文件
- `sys_log_roll_num`: 保留的日志文件数量，默认是10

### `{be or cn}.ERROR.log`

`be.ERROR.log`存储`ERROR`级别及以上的日志条目。典型的错误日志内容：
查询执行错误
- 查询超时或取消
- 由于内存不足导致的查询失败
数据处理错误
- 数据导入失败（例如，格式错误，约束违规）
- 数据写入失败
- 数据读取错误（例如，文件损坏，I/O错误）
存储系统错误
- Tablet加载失败
- Compaction执行失败
- 数据文件损坏
- 磁盘I/O错误

#### 配置

- `sys_log_level`: 日志级别，默认是`INFO`
- `sys_log_dir`: 日志存储目录，默认是`${STARROCKS_HOME}/log`
- `sys_log_roll_mode`: 日志轮换模式，默认是`SIZE-MB-1024`，意味着当前日志文件达到1024 MB时将创建新文件
- `sys_log_roll_num`: 保留的日志文件数量，默认是10

### `{be or cn}.FATAL.log`

主要记录由BE/CN节点生成的各种运行时行为日志，这些日志处于`FATAL`级别。一旦生成此类日志，BE/CN节点进程将退出。

#### 配置

- `sys_log_level`: 日志级别，默认是`INFO`
- `sys_log_dir`: 日志存储目录，默认是`${STARROCKS_HOME}/log`
- `sys_log_roll_mode`: 日志轮换模式，默认是`SIZE-MB-1024`，意味着当前日志文件达到1024 MB时将创建新文件
- `sys_log_roll_num`: 保留的日志文件数量，默认是10

### `error_log`

此日志主要记录BE/CN节点在数据导入过程中遇到的各种错误、被拒记录和ETL问题。用户可以通过`http://be_ip:be_port/api/get_log_file`获取导入错误的主要原因。日志文件存储在`${STARROCKS_HOME}/storage/error_log`目录中。

#### 配置
- `load_error_log_reserve_hours`: 错误日志文件保留时间。默认是48小时，意味着日志文件将在创建48小时后删除。
