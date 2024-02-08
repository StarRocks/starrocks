---
displayed_sidebar: "Chinese"
---

# Query Profile 结构与详细指标

## Query Profile 的结构

Query Profile 的结构与执行引擎的设计密切相关，由以下五部分组成：

- Fragment：执行树。一个查询由一个或多个 Fragment 组成。
- FragmentInstance：每个 Fragment 可以有多个实例，每个实例称为 FragmentInstance，分别由不同的计算节点来执行。
- Pipeline：一个 FragmentInstance 会被拆分成多个 Pipeline。Pipeline 是一个执行链，由一组首尾相接的 Operator 构成。
- PipelineDriver：一个 Pipeline 可以有多个实例，每个实例称为 PipelineDriver，以充分利用多个计算核心。
- Operator：算子。一个 PipelineDriver 由多个 Operator 组成。

![img](../assets/Profile/profile-hierarchy.png)

### Query Profile 的合并策略

通过分析上述结构，您可以轻易观察到，同一个 Fragment 关联的多个 FragmentInstance 在结构上具有高度相似性。同样的，归属于同一 Pipeline 的多个 PipelineDriver 也展现出类似的结构特征。为了减少 Query Profile 的体积，您可以考虑将 FragmentInstance 层进行合并，同样的方法也适用于 PipelineDriver 层。经过这样的合并处理，原本的五层结构便简化为三层，具体表现为：

- Fragment
- Pipeline
- Operator

您可以通过一个 Session 变量 `pipeline_profile_level` 来控制这个合并行为，其可选值有2个：

- `1`：合并，即三层结构。默认值。
- `2`：不合并，即保留原始的五层结构。
- 其他任何数值都会被当成默认值 `1`。

通常，我们并不推荐将该参数设置为 `2`。原因在于，五层结构的 Query Profile 面临众多限制，如无法借助任何工具进行可视化分析，只能依靠人工直接观察来分析。因此，除非合并过程中导致了关键信息的丢失，否则通常没有必要调整这个参数。

### Metric 合并以及 MIN/MAX 值

在 FragmentInstance 和 PipelineDriver 进行合并时，需要对所有同名的指标进行合并，并记录所有并发实例中每个指标的最小值和最大值。不同种类的指标采用了不同的合并策略：

- 时间类指标求平均值。例如：
  - `OperatorTotalTime` 是所有并发实例的平均耗时。
  - `__MAX_OF_OperatorTotalTime` 是所有并发实例中的最大耗时。
  - `__MIN_OF_OperatorTotalTime` 是所有并发实例中的最小耗时。

```SQL
             - OperatorTotalTime: 2.192us
               - __MAX_OF_OperatorTotalTime: 2.502us
               - __MIN_OF_OperatorTotalTime: 1.882us
```

- 非时间类指标求和。例如：
  - `PullChunkNum` 是该指标在所有并发实例的和。
  - `__MAX_OF_PullChunkNum` 是该指标在所有并发实例中的最大值。
  - `__MIN_OF_PullChunkNum` 是该指标在所有并发实例中的最小值。

```SQL
             - PullChunkNum: 146.66K (146660)
               - __MAX_OF_PullChunkNum: 24.45K (24450)
               - __MIN_OF_PullChunkNum: 24.435K (24435)
```

- 个别没有最值的指标在所有并发实例中的值相同，例如：`DegreeOfParallelism`。

通常，MIN 和 MAX 值之间如果有明显差异，则表明数据有很大几率存在倾斜。可能的场景包括聚合和 Join 等。

```SQL
             - OperatorTotalTime: 2m48s
               - __MAX_OF_OperatorTotalTime: 10m30s
               - __MIN_OF_OperatorTotalTime: 279.170us
```

## Query Profile 指标清单

Query Profile 包含大量查询执行详细信息的指标。在大多数情况下，您只需关注运算符的执行时间以及处理的数据量大小即可。找到瓶颈后，您就可以有针对性地解决它们。

### Summary 指标

##### Total

描述：查询消耗的总时间，包括 Planning、Executing 以及 Profiling 阶段耗时。

##### Query State

描述：查询状态，可能状态包括 Finished、Error 以及 Running。 

### Execution Overview 指标

##### FrontendProfileMergeTime

描述：Query Profile 在 FE 侧的处理时间。

##### QueryAllocatedMemoryUsage

描述：所有计算节点，累计分配内存之和。

##### QueryDeallocatedMemoryUsage

描述：所有计算节点，累计释放内存之和。

##### QueryPeakMemoryUsage

描述：所有计算节点中，峰值内存的最大值。

##### QueryExecutionWallTime

描述：执行的墙上时间。

##### QueryCumulativeCpuTime

描述：所有计算节点，累计 CPU 耗时之和。

##### QueryCumulativeOperatorTime

描述：所有节点耗时之和。这里是简单的线性累加，但实际上，不同的算子的执行时间可能是有重叠的。该参数作为计算算子时间占比的分母。

##### QueryCumulativeNetworkTime

描述：所有 Exchange 节点的网络时间之和。这里是简单的线性累加，但实际上，不同 Exchange 的执行时间可能是有重叠的。

##### QueryCumulativeScanTime

描述：所有 Scan 节点的 IO 时间之和。这里是简单的线性累加，但实际上，不同 Scan 的执行时间可能是有重叠的。

##### QueryPeakScheduleTime

描述：所有Pipeline中，ScheduleTime 指标的最大值。

##### QuerySpillBytes

描述：Spill 到本地磁盘的字节数量。

##### ResultDeliverTime

描述：传输结果的额外耗时，对于查询语句，这个参数是指数据传回客户端的时间；对于插入语句，这个参数是指数据写入到存储层的时间。

### Fragment 指标

##### InstanceNum

描述：该 Fragment 的所有 FragmentInstance 的数量。

##### InstanceIds

描述：该 Fragment 的所有 FragmentInstance ID。

##### BackendNum

描述：参与该 Fragment 执行的 BE 的数量。

##### BackendAddresses

描述：参与该 Fragment 执行的所有 BE 的地址信息。

##### FragmentInstancePrepareTime

描述：Fragment Prepare 阶段的耗时。

##### InstanceAllocatedMemoryUsage

描述：该 Fragment 下所有 FragmentInstance 的累计分配内存。

##### InstanceDeallocatedMemoryUsage

描述：该 Fragment 下所有 FragmentInstance 的累计释放内存。

##### InstancePeakMemoryUsage

描述：该 Fragment 下所有 FragmentInstance 中，峰值内存的最大值。

### Pipeline 指标

核心指标的关系如下图所示：

- DriverTotalTime = ActiveTime + PendingTime + ScheduleTime
- ActiveTime = ∑ OperatorTotalTime + OverheadTime
- PendingTime = InputEmptyTime + OutputFullTime + PreconditionBlockTime + PendingFinishTime
- InputEmptyTime = FirstInputEmptyTime + FollowupInputEmptyTime

![img](../assets/Profile/profile_pipeline_time_relationship.jpeg)

##### DegreeOfParallelism

描述：Pipeline 执行的并行度。

##### TotalDegreeOfParallelism

描述：并行度之和。由于同一个 Pipeline 会在多个机器执行，这里指的是把所有并行度累加起来的值。

##### DriverPrepareTime

描述：执行 Prepare 的时间，该时间不包括在 DriverTotalTime 中。

##### DriverTotalTime

描述：Pipeline 的执行总时间。不包括 prepare 阶段的耗时。

##### ActiveTime

描述：Pipeline 的执行时间，包括各个算子的执行时间，以及整个框架的 Overhead，包括调用 has_output，need_input 这些方法的时间。

##### PendingTime

描述：Pipeline 由于各种原因，无法被调度执行而阻塞的时间。

##### InputEmptyTime

描述：Pipeline 由于输入队列为空而导致被阻塞的时间。

##### FirstInputEmptyTime

描述：Pipeline 第一次由于输入队列为空导致的阻塞时间。单独把第一次提出来是因为，第一次等待，大概率是由于 Pipeline 的依赖关系产生的。

##### FollowupInputEmptyTime

描述：Pipeline 后续（第二次开始）所有因为输入队列为空导致的阻塞时间。

##### OutputFullTime

描述：Pipeline 由于输出队列满而导致被阻塞的时间。

##### PreconditionBlockTime

描述：Pipeline 由于依赖条件未满足而导致被阻塞的时间。

##### PendingFinishTime

描述：Pipeline 由于等待异步任务结束执行而导致被阻塞的时间。

##### ScheduleTime

描述：Pipeline 的调度时间，即进入就绪队列，到被调度执行的这段时间。

##### BlockByInputEmpty

描述：由于 InputEmpty 从而被 Block 的次数。

##### BlockByOutputFull

描述：由于 OutputFull 从而被 Block 的次数。

##### BlockByPrecondition

描述：由于前置依赖未就绪从而被 Block 的次数。

### Operator 通用指标

##### OperatorAllocatedMemoryUsage

描述：Operator 累计分配的内存。

##### OperatorDeallocatedMemoryUsage

描述：Operator 累计释放的内存。

##### OperatorPeakMemoryUsage

描述：所有计算节点中，该 Operator 的峰值内存。该指标仅对于部分物化算子有意义，例如聚合、排序、Join 等。而对于 Project、Scan 等算子无意义，因为内存在当前算子分配，在后续算子释放，对于当前算子来说，峰值内存就等同于累计分配的内存。在 v3.1.8 和 v3.2.3 之前的版本中，该指标的含义为 “所有 *PipelineDriver* 中，该 Operator 的峰值内存”。

##### PrepareTime

描述：Prepare 的时间。

##### OperatorTotalTime

描述：Operator 消耗的总时间。且满足：OperatorTotalTime = PullTotalTime + PushTotalTime + SetFinishingTime + SetFinishedTime + CloseTime。不包含 Prepare 的时间。

##### PullTotalTime

描述：Operator 执行 push_chunk 的总时间。

##### PushTotalTime

描述：Operator 执行 pull_chunk 的总时间。

##### SetFinishingTime

描述：Operator 执行 set_finishing 的总时间。

##### SetFinishedTime

描述：Operator 执行 set_finished 的总时间。

##### PushRowNum

描述：Operator 累积输入行数。

##### PullRowNum

描述：Operator 累积输出行数。

##### JoinRuntimeFilterEvaluate

描述：Join Runtime Filter 执行的次数。

##### JoinRuntimeFilterHashTime

描述：Join Runtime Filter 计算Hash的时间。

##### JoinRuntimeFilterInputRows

描述：Join Runtime Filter 的输入行数。

##### JoinRuntimeFilterOutputRows

描述：Join Runtime Filter 的输出行数。

##### JoinRuntimeFilterTime

描述：Join Runtime Filter 的耗时。

### Unique 指标

### Scan Operator

Scan Operator 会使用一个额外的线程池来执行 IO 任务，因此该节点的时间指标的关系如下：

![img](../assets/Profile/profile_scan_time_relationship.jpeg)

#### OLAP Scan Operator

为了帮助大家更好地理解 Scan Operator 中的各项指标，以下图形将清晰展示这些指标与存储结构之间的关联。

![img](../assets/Profile/profile_scan_relationship.jpeg)

##### Table

- 描述：表名称。
- 级别：一级指标

##### Rollup

- 描述：物化视图名称。如果没有命中物化视图的话，等同于表名称。
- 级别：一级指标

##### SharedScan

- 描述：是否启用了 enable_shared_scan Session 变量。
- 级别：一级指标

##### TabletCount

- 描述：Tablet 数量。
- 级别：一级指标

##### MorselsCount

- 描述：Morsel 数量。
- 级别：一级指标

##### PushdownPredicates

- 描述：下推的谓词数量。
- 级别：一级指标

##### Predicates

- 描述：谓词表达式。
- 级别：一级指标

##### BytesRead

- 描述：读取数据的大小。
- 级别：一级指标

##### CompressedBytesRead

- 描述：从磁盘上读取的压缩数据的大小。
- 级别：一级指标

##### UncompressedBytesRead

- 描述：从磁盘上读取的未压缩数据的大小。
- 级别：一级指标

##### RowsRead

- 描述：读取的行数（谓词过滤后的行数）。
- 级别：一级指标

##### RawRowsRead

- 描述：读取的原始行数（谓词过滤前的行数）。
- 级别：一级指标

##### ReadPagesNum

- 描述：读取 Page 的数量。
- 级别：一级指标

##### CachedPagesNum

- 描述：缓存的 Page 数量。
- 级别：一级指标

##### ChunkBufferCapacity

- 描述：Chunk Buffer 的容量。
- 级别：一级指标

##### DefaultChunkBufferCapacity

- 描述：Chunk Buffer 的默认容量。
- 级别：一级指标

##### PeakChunkBufferMemoryUsage

- 描述：Chunk Buffer 的峰值内存。
- 级别：一级指标

##### PeakChunkBufferSize

- 描述：Chunk Buffer 的峰值大小。
- 级别：一级指标

##### PrepareChunkSourceTime

- 描述：Chunk Source 的 Prepare 时间。
- 级别：一级指标

##### ScanTime

- 描述：Scan 累计时间。Scan 操作在异步 I/O 线程池中完成。
- 级别：一级指标

##### IOTaskExecTime

- 描述：IO 任务的执行时间。
- 级别：一级指标
- 下属指标：CreateSegmentIter、DictDecode、GetDelVec、GetDeltaColumnGroup、GetRowsets、IOTime、LateMaterialize、ReadPKIndex、SegmentInit、SegmentRead

##### CreateSegmentIter

- 描述：创建 Segment 迭代器的时间。
- 级别：二级指标

##### DictDecode

- 描述：低基数优化字典解码的时间。
- 级别：二级指标

##### GetDelVec

- 描述：加载 DelVec（删除向量）的时间。
- 级别：二级指标

##### GetDeltaColumnGroup

- 描述：加载 DelVecColumnGroup 的时间。
- 级别：二级指标

##### GetRowsets

- 描述：加载 RowSet 的时间。
- 级别：二级指标

##### IOTime

- 描述：文件 IO 的时间。
- 级别：二级指标

##### LateMaterialize

- 描述：延迟物化的时间。
- 级别：二级指标

##### ReadPKIndex

- 描述：读取 PrimaryKey 索引的时间。
- 级别：二级指标

##### SegmentInit

- 描述：Segment 初始化的时间。
- 级别：二级指标
- 下属指标：BitmapIndexFilter、BitmapIndexFilterRows、BloomFilterFilter、BloomFilterFilterRows、ColumnIteratorInit、ShortKeyFilter、ShortKeyFilterRows、ShortKeyRangeNumber、RemainingRowsAfterShortKeyFilter、ZoneMapIndexFiter、ZoneMapIndexFilterRows、SegmentZoneMapFilterRows、SegmentRuntimeZoneMapFilterRows

##### BitmapIndexFilter

- 描述：Bitmap 索引过滤时间。
- 级别：三级指标

##### BitmapIndexFilterRows

- 描述：Bitmap 索引过滤行数。
- 级别：三级指标

##### BloomFilterFilter

- 描述：Bloom 索引过滤时间。
- 级别：三级指标

##### BloomFilterFilterRows

- 描述：Bloom 索引过滤行数。
- 级别：三级指标

##### ColumnIteratorInit

- 描述：Column 迭代器初始化的时间。
- 级别：三级指标

##### ShortKeyFilter

- 描述：ShortKey 索引的过滤时间。
- 级别：三级指标

##### ShortKeyFilterRows

- 描述：ShortKey 索引的过滤行数。
- 级别：三级指标

##### ShortKeyRangeNumber

- 描述：ShortKey Range 的数量。
- 级别：三级指标

##### RemainingRowsAfterShortKeyFilter

- 描述：ShortKey 索引过滤后的剩余行数。
- 级别：三级指标

##### ZoneMapIndexFiter

- 描述：ZoneMap 索引过滤时间。
- 级别：三级指标

##### ZoneMapIndexFilterRows

- 描述：ZoneMap 索引过滤行数。
- 级别：三级指标

##### SegmentZoneMapFilterRows

- 描述：Segment ZoneMap 索引过滤行数。
- 级别：三级指标

##### SegmentRuntimeZoneMapFilterRows

- 描述：Segment Runtime ZoneMap 索引过滤行数。
- 级别：三级指标

##### SegmentRead

- 描述：Segment 读取时间。
- 级别：二级指标
- 下属指标：BlockFetch、BlockFetchCount、BlockSeek、BlockSeekCount、ChunkCopy、DecompressT、DelVecFilterRows、PredFilter、PredFilterRows、RowsetsReadCount、SegmentsReadCount、TotalColumnsDataPageCount

##### BlockFetch

- 描述：Block 读取时间。
- 级别：三级指标

##### BlockFetchCount

- 描述：Block 读取次数。
- 级别：三级指标

##### BlockSeek

- 描述：Block 搜索时间。
- 级别：三级指标

##### BlockSeekCount

- 描述：Block 搜索次数。
- 级别：三级指标

##### ChunkCopy

- 描述：Chunk 拷贝时间。
- 级别：三级指标

##### DecompressT

- 描述：解压缩的时间。
- 级别：三级指标

##### DelVecFilterRows

- 描述：DELETE vector 过滤行数。
- 级别：三级指标

##### PredFilter

- 描述：谓词过滤时间。
- 级别：三级指标

##### PredFilterRows

- 描述：谓词过滤行数。
- 级别：三级指标

##### RowsetsReadCount

- 描述：Rowset 读取次数。
- 级别：三级指标

##### SegmentsReadCount

- 描述：Segment 读取次数。
- 级别：三级指标

##### TotalColumnsDataPageCount

- 描述：Column Data Page 的数量。
- 级别：三级指标

##### IOTaskWaitTime

- 描述：IO 任务从投递成功到被调度执行的等待时间。
- 级别：一级指标

##### SubmitTaskCount

- 描述：提交 IO 任务的次数。
- 级别：一级指标

##### SubmitTaskTime

- 描述：提交任务的耗时。
- 级别：一级指标

##### PeakIOTasks

- 描述：IO 任务的峰值数量。
- 级别：一级指标

##### PeakScanTaskQueueSize

- 描述：IO 任务队列的峰值大小。
- 级别：一级指标

#### Connector Scan Operator

##### DataSourceType

- 描述：数据源类型，可以是 HiveDataSource，ESDataSource 等等。
- 级别：一级指标

##### Table

- 描述：表名称。
- 级别：一级指标

##### TabletCount

- 描述：Tablet 数量。
- 级别：一级指标

##### MorselsCount

- 描述：Morsel 数量。
- 级别：一级指标

##### Predicates

- 描述：谓词表达式。
- 级别：一级指标

##### PredicatesPartition

- 描述：作用在分区上的谓词表达式。
- 级别：一级指标

##### SharedScan

- 描述：是否启用了 enable_shared_scan Session 变量。
- 级别：一级指标

##### ChunkBufferCapacity

- 描述：Chunk Buffer 的容量。
- 级别：一级指标

##### DefaultChunkBufferCapacity

- 描述：Chunk Buffer 的默认容量。
- 级别：一级指标

##### PeakChunkBufferMemoryUsage

- 描述：Chunk Buffer 的峰值内存。
- 级别：一级指标

##### PeakChunkBufferSize

- 描述：Chunk Buffer 的峰值大小。
- 级别：一级指标

##### PrepareChunkSourceTime

- 描述：Chunk Source 的 Prepare 时间。
- 级别：一级指标

##### ScanTime

- 描述：Scan 累计时间。Scan 操作在异步 I/O 线程池中完成。
- 级别：一级指标

##### IOTaskExecTime

- 描述：IO 任务的执行时间。
- 级别：一级指标
- 下属指标：ColumnConvertTime、ColumnReadTime、ExprFilterTime、InputStream、ORC、OpenFile、ReaderInit、RowsRead、RowsSkip、ScanRanges、SharedBuffered

##### ColumnConvertTime

- 描述：Column 转换的耗时。
- 级别：二级指标

##### ColumnReadTime

- 描述：Reader 读取和解析数据时间。
- 级别：二级指标

##### ExprFilterTime

- 描述：表达式过滤时间。
- 级别：二级指标

##### InputStream

- 描述：仅用于分类，无具体含义。
- 级别：二级指标
- 下属指标：AppIOBytesRead、AppIOCounter、AppIOTime、FSIOBytesRead、FSIOCounter、FSIOTime

##### AppIOBytesRead

- 描述：应用层读取的数据量。
- 级别：三级指标

##### AppIOCounter

- 描述：应用层读取的 I/O 次数。
- 级别：三级指标

##### AppIOTime

- 描述：应用层累计读取时间。
- 级别：三级指标

##### FSIOBytesRead

- 描述：存储系统读取的数据量。
- 级别：三级指标

##### FSIOCounter

- 描述：存储层读取的 I/O 次数。
- 级别：三级指标

##### FSIOTime

- 描述：存储层累计读取时间。
- 级别：三级指标

##### ORC

- 描述：仅用于分类，无具体含义。
- 级别：二级指标
- 下属指标：IcebergV2FormatTimer、StripeNumber、StripeSizes

##### IcebergV2FormatTimer

- 描述：格式转换的耗时。
- 级别：三级指标

##### StripeNumber

- 描述：ORC 文件的数量。
- 级别：三级指标

##### StripeSizes

- 描述：ORC 文件每个 stripe 的平均大小。
- 级别：三级指标

##### OpenFile

- 描述：打开文件的耗时。
- 级别：二级指标

##### ReaderInit

- 描述：初始化 Reader的耗时。
- 级别：二级指标

##### RowsRead

- 描述：读取数据的行数。
- 级别：二级指标

##### RowsSkip

- 描述：跳过的行数。
- 级别：二级指标

##### ScanRanges

- 描述：扫描的数据分片总数。
- 级别：二级指标

##### SharedBuffered

- 描述：仅用于分类，无具体含义。
- 级别：二级指标
- 下属指标：DirectIOBytes、DirectIOCount、DirectIOTime、SharedIOBytes、SharedIOCount、SharedIOTime

##### DirectIOBytes

- 描述：直接 IO 读取的数据量。
- 级别：三级指标

##### DirectIOCount

- 描述：直接 IO 的次数。
- 级别：三级指标

##### DirectIOTime

- 描述：直接 IO 的耗时。
- 级别：三级指标

##### SharedIOBytes

- 描述：共享 IO 读取的数据量
- 级别：三级指标

##### SharedIOCount

- 描述：共享 IO 的次数。
- 级别：三级指标

##### SharedIOTime

- 描述：共享 IO 的耗时。
- 级别：三级指标

##### IOTaskWaitTime

- 描述：IO 任务从投递成功到被调度执行的等待时间。
- 级别：一级指标

##### SubmitTaskCount

- 描述：提交 IO 任务的次数。
- 级别：一级指标

##### SubmitTaskTime

- 描述：提交任务的耗时。
- 级别：一级指标

##### PeakIOTasks

- 描述：IO 任务的峰值数量。
- 级别：一级指标

##### PeakScanTaskQueueSize

- 描述：IO 任务队列的峰值大小。
- 级别：一级指标

### Exchange Operator

#### Exchange Sink Operator

##### ChannelNum

描述：Channel 数量。一般来说，有几个接收端，就有几个Channel。

##### DestFragments

描述：接收端 FragmentInstance ID 列表。

##### DestID

描述：接收端节点 ID。

##### PartType

描述：数据分布模式，包括：UNPARTITIONED、RANDOM、HASH_PARTITIONED 以及 BUCKET_SHUFFLE_HASH_PARTITIONED。

##### SerializeChunkTime

描述：序列化 Chunk 的耗时。

##### SerializedBytes

描述：序列化的数据大小。

##### ShuffleChunkAppendCounter

描述：当 PartType 为 HASH_PARTITIONED 或 BUCKET_SHUFFLE_HASH_PARTITIONED 时，Chunk Append 的次数。

##### ShuffleChunkAppendTime

描述：当 PartType 为 HASH_PARTITIONED 或 BUCKET_SHUFFLE_HASH_PARTITIONED 时，Chunk Append 的耗时。

##### ShuffleHashTime

描述：当 PartType 为 HASH_PARTITIONED 或 BUCKET_SHUFFLE_HASH_PARTITIONED 时，计算 Hash 的耗时。

##### RequestSent

描述：发送的数据包的数量。

##### RequestUnsent

描述：未发送的数据包的数量。存在短路逻辑时，该指标不为0。其他情况下，该指标为0。

##### BytesSent

描述：发送的数据大小。

##### BytesUnsent

描述：未发送数据大小。存在短路逻辑时，该指标不为0。其他情况下，该指标为0。

##### BytesPassThrough

描述：当目的节点就是当前节点时，不再通过网络传输数据，即PassThrough的数据大小。通过 enable_exchange_pass_through 开启 PassThrough。

##### PassThroughBufferPeakMemoryUsage

描述：PassThrough Buffer的峰值内存。

##### CompressTime

描述：压缩时间。

##### CompressedBytes

描述：压缩数据大小。

##### OverallThroughput

描述：吞吐速率。

##### NetworkTime

描述：数据包传输时间（不包括接收后处理时间）。

##### NetworkBandwidth

描述：网络带宽估算值。

##### WaitTime

描述：由于发送端队列满而导致的等待时间。

##### OverallTime

描述：整个传输过程的总耗时。即，发送第一个数据包，到确认最后一个数据包已被正确接收的这段时间。

##### RpcAvgTime

描述：Rpc的平均耗时。

##### RpcCount

描述：Rpc的总次数。

#### Exchange Source Operator

##### RequestReceived

描述：接收的数据包的大小。

##### BytesReceived

描述：接收的数据大小。

##### DecompressChunkTime

描述：解压缩的耗时。

##### DeserializeChunkTime

描述：反序列化的耗时。

##### ClosureBlockCount

描述：阻塞的 Closure 的数量。

##### ClosureBlockTime

描述：Closure 的阻塞时间。

##### ReceiverProcessTotalTime

描述：接收端处理的耗时。

##### WaitLockTime

描述：锁的等待时间。

### Aggregate Operator

##### GroupingKeys

描述：GROUP BY 列。

##### AggregateFunctions

描述：聚合函数计算耗时。

##### AggComputeTime

描述：AggregateFunctions +  Group By耗时。

##### ChunkBufferPeakMem

描述：Chunk Buffer 峰值内存。

##### ChunkBufferPeakSize

描述：Chunk Buffer 峰值大小。

##### ExprComputeTime

描述：表达式计算耗时。

##### ExprReleaseTime

描述：表达式 Release 耗时。

##### GetResultsTime

描述：提取聚合结果的耗时。

##### HashTableSize

描述：Hash Table 大小。

##### HashTableMemoryUsage

描述：Hash Table 的内存大小。

##### InputRowCount

描述：输入行数。

##### PassThroughRowCount

描述：Auto 模式下，由于聚合度不高，导致退化成 Streaming 模式后，处理数据的行数。

##### ResultAggAppendTime

描述：聚合结果列 Append 的耗时。

##### ResultGroupByAppendTime

描述：Group By 列 Append 的耗时。

##### ResultIteratorTime

描述：迭代 Hash Table 的耗时

##### StreamingTime

描述：Streaming 模式下的处理耗时。

### Join Operator

##### DistributionMode

描述：分布类型。包括：BROADCAST，PARTITIONED，COLOCATE等等。

##### JoinPredicates

描述：Join 谓词。

##### JoinType

描述：Join 类型。

##### BuildBuckets

描述：Hash Table 的 Bucket 数量。

##### BuildKeysPerBucket

描述：每个 Bucket 中 Key 的数量。

##### BuildConjunctEvaluateTime

描述：Conjunct 的耗时。

##### BuildHashTableTime

描述：构建 Hash Table 的耗时。

##### ProbeConjunctEvaluateTime

描述：Probe Conjunct 的耗时。

##### SearchHashTableTimer

描述：查询 Hash Table 的耗时。

##### CopyRightTableChunkTime

描述：拷贝右表 Chunk 的耗时。

##### HashTableMemoryUsage

描述：Hash Table 内存的占用。

##### RuntimeFilterBuildTime

描述：构建 Runtime Filter 的耗时。

##### RuntimeFilterNum

描述：Runtime Filter 的数量。

### Window Function Operator

##### ProcessMode

描述：执行模式。包含两部分：第一个部分包括：Materializing，Streaming；第二部分包括：Cumulative，RemovableCumulative，ByDefinition。

##### ComputeTime

描述：窗口函数计算耗时。

##### PartitionKeys

描述：分区列。

##### AggregateFunctions

描述：聚合函数。

##### ColumnResizeTime

描述：Column 缩容或扩容的耗时。

##### PartitionSearchTime

描述：搜索 Partition 边界的耗时。

##### PeerGroupSearchTime

描述：搜索 Peer Group 边界的耗时。仅在窗口类型为 RANGE 时有意义。

##### PeakBufferedRows

描述：Buffer 的峰值行数。

##### RemoveUnusedRowsCount

描述：移除无用 Buffer 的次数。

##### RemoveUnusedTotalRows

描述：移除无用 Buffer 的行数。

### Sort Operator

##### SortKeys

描述：排序键。

##### SortType

描述：查询结果排序方式：全排序或者排序 Top N 个结果。

##### MaxBufferedBytes

描述：缓存的数据的峰值大小。

##### MaxBufferedRows

描述：缓存的数据的峰值行数。

##### NumSortedRuns

描述：有序片段的数量。

##### BuildingTime

描述：维护排序时所用到的一些内部数据结构的耗时。

##### MergingTime

描述：排序时，会将一个大的序列，切分成多个小序列，然后对每个小序列进行排序。最后再将这些序列合并起来。因此，这个指标表示的是合并的耗时。

##### SortingTime

描述：排序的耗时。

##### OutputTime

描述：构建输出有序序列的耗时。

### Merge Operator

为了方便理解各项指标，Merge 可以表示成如下状态机制：

```
                   ┌────────── PENDING ◄──────────┐
                   │                              │
                   │                              │
                   ├──────────────◄───────────────┤
                   │                              │
                   ▼                              │
       INIT ──► PREPARE ──► SPLIT_CHUNK ──► FETCH_CHUNK ──► FINISHED
                   ▲
                   |
                   | one traverse from leaf to root
                   |
                   ▼
                PROCESS
```

##### Limit

- 描述：Limit。
- 级别：一级指标

##### Offset

- 描述：Offset。
- 级别：一级指标

##### StreamingBatchSize

- 描述：Merge 以 Streaming 模式进行，每次处理的数据量的大小。
- 级别：一级指标

##### LateMaterializationMaxBufferChunkNum

- 描述：启用延迟物化时，Buffer 的最大 Chunk 数量。
- 级别：一级指标

##### OverallStageCount

- 描述：各阶段的执行次数总和。
- 级别：一级指标
- 下属指标：1-InitStageCount、2-PrepareStageCount、3-ProcessStageCount、4-SplitChunkStageCount、5-FetchChunkStageCount、6-PendingStageCount、7-FinishedStageCount

##### 1-InitStageCount

- 描述：Init 阶段执行次数。
- 级别：二级指标

##### 2-PrepareStageCount

- 描述：Prepare 阶段执行次数。
- 级别：二级指标

##### 3-ProcessStageCount

- 描述：Process 阶段执行次数。
- 级别：二级指标

##### 4-SplitChunkStageCount

- 描述：Split 阶段执行次数。
- 级别：二级指标

##### 5-FetchChunkStageCount

- 描述：Fetch 阶段执行次数。
- 级别：二级指标

##### 6-PendingStageCount

- 描述：Pending 阶段执行次数。
- 级别：二级指标

##### 7-FinishedStageCount

- 描述：Finished 阶段执行次数。
- 级别：二级指标

##### OverallStageTime

- 描述：各个阶段执行总耗时。
- 级别：一级指标
- 下属指标：1-InitStageTime、2-PrepareStageTime、3-ProcessStageTime、4-SplitChunkStageTime、5-FetchChunkStageTime、6-PendingStageTime、7-FinishedStageTime

##### 1-InitStageTime

- 描述：Init 阶段执行耗时。
- 级别：二级指标

##### 2-PrepareStageTime

- 描述：Prepare 阶段执行耗时。
- 级别：二级指标

##### 3-ProcessStageTime

- 描述：Process 阶段执行耗时。
- 级别：二级指标
- 下属指标：LateMaterializationGenerateOrdinalTime、SortedRunProviderTime

##### LateMaterializationGenerateOrdinalTime

- 描述：延迟物化构建 ID 列的耗时。
- 级别：三级指标

##### SortedRunProviderTime

- 描述：从 Provider 获取数据的耗时。
- 级别：三级指标

##### 4-SplitChunkStageTime

- 描述：Split 阶段的耗时。
- 级别：二级指标

##### 5-FetchChunkStageTime

- 描述：Fetch 阶段的耗时。
- 级别：二级指标

##### 6-PendingStageTime

- 描述：Pending 阶段的耗时。
- 级别：二级指标

##### 7-FinishedStageTime

- 描述：Finished 阶段的耗时。
- 级别：二级指标

### TableFunction Operator

##### TableFunctionExecTime

描述：Table Function 计算耗时。

##### TableFunctionExecCount

描述：Table Function 执行次数。

### Project Operator

##### ExprComputeTime

描述：表达式计算耗时。

##### CommonSubExprComputeTime

描述：公共子表达式计算耗时。

### LocalExchange Operator

##### Type

描述：Local Exchange 类型，包括：`Passthrough`、`Partition` 以及 `Broadcast`。

##### ShuffleNum

描述：Shuffle 数量。该指标仅当 `Type` 为 `Partition` 时有效。

##### LocalExchangePeakMemoryUsage

描述：峰值内存。
