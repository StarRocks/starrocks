---
displayed_sidebar: docs
description: "为 Flink 作业启用多表事务，以便在一个处理周期内向同一数据库中的多个表写入数据。"
---

# 使用 Apache Flink® 通过多表事务加载数据

StarRocks Flink Connector 支持多表事务，可将 Flink 中的数据原子性地加载到多个表中。

## 使用场景

当单个 Flink 作业在一个处理周期内向同一 StarRocks 数据库中的多个表写入数据时，启用多表事务可保证：

- **跨表原子提交**：在同一提交周期内写入不同表的数据以原子方式可见——要么全部成功，要么全部失败。
- **源事务完整性**：完整的上游事务（例如来自 Kafka 的事务）不会被拆分到两个 StarRocks 事务中。
- **亚秒级数据新鲜度**：数据通过 `/api/transaction/load` 持续流入 StarRocks，并按 `sink.buffer-flush.interval-ms` 配置的间隔进行提交。

典型场景：

- 同步写入汇总表和明细表（例如 `orders` 和 `order_items`）
- 将事件路由到不同的分区表（例如 `events_202601`、`events_202602`）
- 单个作业维护多个相互关联的下游结果表

:::tip[前提条件]
要启用多表事务，您必须在 StarRocks v4.0 及以上版本（支持多表事务 Stream Load）上运行集群，并使用 v1.2.9 及以上版本的 StarRocks Flink Connector。
:::

## 核心能力

| 能力                         | 描述                                                         |
| ---------------------------- | ------------------------------------------------------------ |
| 跨表原子提交                 | 同一刷新周期内的所有表共享一个 StarRocks 事务标签，Prepare 和 Commit 操作统一执行。 |
| 源事务完整性                 | 提交时机由 `transactionEnd` 标志控制，仅在完整的源事务边界处进行提交。 |
| 亚秒级数据可见性             | 数据定期刷新到 StarRocks（`/api/transaction/load`），当满足 `transactionEnd` 和定时器条件时进行提交。 |
| N:1 事务映射                 | 多个源事务可以在单个 StarRocks 事务中累积，无需按 1:1 映射。 |
| 分区内有序性                 | `keyBy(sourcePartition)` 确保来自同一分区的事务在同一 sink 子任务中按顺序处理。 |

## 配置项

### 多表事务配置

#### `sink.transaction.multi-table.enabled`

- 类型：Boolean
- 默认值：`false`
- 描述：是否启用多表原子事务模式。

#### `sink.transaction.multi-table.buffer-size`

- 类型：Long
- 默认值：`134217728`（128 MB）
- 单位：字节
- 描述：多表事务模式下的全局缓冲区大小（字节）。当所有表的缓冲数据总量达到此阈值时，触发刷新。

### 加载相关配置

#### `sink.version`

- 推荐值：`V2`
- 描述：必填项。`V1` 不支持事务 Stream Load 接口。

#### `sink.semantic`

- 推荐值：`at-least-once`
- 描述：多表模式当前仅支持 `at-least-once`。

#### `database-name`

- 推荐值：`*`
- 描述：通配符，用于启用动态多表路由。

#### `table-name`

- 推荐值：`*`
- 描述：通配符，用于启用动态多表路由。

#### `sink.buffer-flush.interval-ms`

- 推荐值：`1000`
- 描述：控制提交周期。可将其设置为 `1000` 以实现约一秒的数据新鲜度。

#### `sink.properties.format`

- 推荐值：`json`
- 描述：数据格式。

#### `sink.properties.strip_outer_array`

- 推荐值：`true`
- 描述：是否去除最外层的数组结构。

## 接口

### `StarRocksRowData`

```java
public interface StarRocksRowData {
    String getUniqueKey();    // Region routing key (nullable; auto-derived from database.table)
    String getDatabase();     // Target database
    String getTable();        // Target table
    String getRow();          // Row data in JSON format

    /**
     * Indicates this is the last row of a source transaction batch.
     * Used by multi-table transaction mode to determine safe commit points:
     * the connector only commits when the most recent write had this flag set,
     * ensuring no partial source transaction is committed.
     */
    default boolean isTransactionEnd() {
        return false;
    }

    /**
     * Returns the source partition ID for this row.
     * Used by multi-table transaction mode to track per-partition transaction
     * boundaries. Returns -1 when partition tracking is not applicable.
     */
    default int getSourcePartition() {
        return -1;
    }
}
```

### `DefaultStarRocksRowData`

```java
public class DefaultStarRocksRowData implements StarRocksRowData {
    // 基本字段
    private String uniqueKey;
    private String database;
    private String table;
    private String row;

    // 多表事务字段
    private boolean transactionEnd;       // Source transaction end marker
    private int sourcePartition = -1;     // Source partition ID (for keyBy ordering)

    // 构造函数
    public DefaultStarRocksRowData();
    public DefaultStarRocksRowData(String database, String table);
    public DefaultStarRocksRowData(String uniqueKey, String database, String table, String row);

    // Setter 方法
    public void setUniqueKey(String uniqueKey);
    public void setDatabase(String database);
    public void setTable(String table);
    public void setRow(String row);
    public void setTransactionEnd(boolean transactionEnd);
    public void setSourcePartition(int sourcePartition);

    // Getter 方法（继承自 StarRocksRowData）
    public String getUniqueKey();
    public String getDatabase();
    public String getTable();
    public String getRow();
    public boolean isTransactionEnd();
    public int getSourcePartition();
}
```

### 用户实现的组件

用户需要实现一个 `KeyedProcessFunction`（在本文档中称为 `TransactionAssembler`），它：

1. 按源分区作为键并在事务中缓冲数据行
2. 仅在源事务关闭时（例如，收到 `TXN_END` 时）才发出所有行
3. 在最后一行设置 `transactionEnd=true`
4. 在每一行上设置 `sourcePartition`

无需自定义 `SinkFunction` — 标准连接器 API（`SinkFunctionFactory.createSinkFunction()`）可处理一切。

## 完整示例

### StarRocks 表 DDL

```sql
CREATE DATABASE `test`;

CREATE TABLE `test`.`orders` (
    `order_id` BIGINT NOT NULL,
    `customer_id` BIGINT NOT NULL,
    `total_amount` DECIMAL(10,2) DEFAULT "0",
    `order_status` VARCHAR(32) DEFAULT ""
) ENGINE=OLAP PRIMARY KEY(`order_id`)
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES("replication_num" = "1");

CREATE TABLE `test`.`order_items` (
    `item_id` BIGINT NOT NULL,
    `order_id` BIGINT NOT NULL,
    `product_name` VARCHAR(128) DEFAULT "",
    `quantity` INT DEFAULT "0",
    `price` DECIMAL(10,2) DEFAULT "0"
) ENGINE=OLAP PRIMARY KEY(`item_id`)
DISTRIBUTED BY HASH(`item_id`)
PROPERTIES("replication_num" = "1");
```

### Flink 作业代码

```java
import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class WriteMultipleTablesWithTransaction {

    // =====================================================
    // 1. 源事件模型（根据业务逻辑自定义）
    // =====================================================

    enum EventType { TXN_BEGIN, DATA, TXN_END }

    static class TxnEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        int partition;
        String txnId;
        EventType type;
        String database;
        String table;
        String json;

        TxnEvent() {}

        TxnEvent(int partition, String txnId, EventType type,
                 String database, String table, String json) {
            this.partition = partition;
            this.txnId = txnId;
            this.type = type;
            this.database = database;
            this.table = table;
            this.json = json;
        }

        static TxnEvent begin(int partition, String txnId) {
            return new TxnEvent(partition, txnId, EventType.TXN_BEGIN, null, null, null);
        }

        static TxnEvent data(int partition, String txnId, String db, String table, String json) {
            return new TxnEvent(partition, txnId, EventType.DATA, db, table, json);
        }

        static TxnEvent end(int partition, String txnId) {
            return new TxnEvent(partition, txnId, EventType.TXN_END, null, null, null);
        }
    }

    // =====================================================
    // 2. TransactionAssembler — 核心用户组件
    // =====================================================

    /**
     * Buffers DATA events per partition; on TXN_END emits the complete
     * transaction's rows as individual DefaultStarRocksRowData records.
     *
     * Only emits when a source transaction is fully closed (TXN_END received).
     * All rows are emitted synchronously within one processElement() call,
     * so they enter the downstream sink buffer without intervening checkpoint barriers.
     *
     * Multiple source transactions accumulate in the sink's buffer between
     * flush cycles — the connector handles grouping them into StarRocks transactions.
     */
    static class TransactionAssembler
            extends KeyedProcessFunction<Integer, TxnEvent, DefaultStarRocksRowData> {

        private transient ListState<TxnEvent> pendingRows;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<TxnEvent> descriptor = new ListStateDescriptor<>(
                    "pending-txn-rows", Types.POJO(TxnEvent.class));
            pendingRows = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(TxnEvent event, Context ctx,
                                   Collector<DefaultStarRocksRowData> out) throws Exception {
            switch (event.type) {
                case TXN_BEGIN:
                    pendingRows.clear();
                    break;

                case DATA:
                    pendingRows.add(event);
                    break;

                case TXN_END:
                    List<TxnEvent> rows = new ArrayList<>();
                    for (TxnEvent row : pendingRows.get()) {
                        rows.add(row);
                    }
                    int partition = ctx.getCurrentKey();
                    for (int i = 0; i < rows.size(); i++) {
                        TxnEvent row = rows.get(i);
                        DefaultStarRocksRowData rowData = new DefaultStarRocksRowData(
                                null, row.database, row.table, row.json);
                        rowData.setSourcePartition(partition);
                        // 将最后一行标记为事务结束
                        if (i == rows.size() - 1) {
                            rowData.setTransactionEnd(true);
                        }
                        out.collect(rowData);
                    }
                    pendingRows.clear();
                    break;
            }
        }
    }

    // =====================================================
    // 3. 主程序
    // =====================================================

    public static void main(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        String jdbcUrl  = params.get("jdbcUrl", "jdbc:mysql://127.0.0.1:9030");
        String loadUrl  = params.get("loadUrl", "127.0.0.1:8030");
        String userName = params.get("userName", "root");
        String password = params.get("password", "");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);  // Checkpoint is for recovery only; does not affect commit cycle

        // --- 数据源 ---
        // 替换为实际的 Kafka Source，将数据反序列化为 TxnEvent
        DataStream<TxnEvent> events = env.addSource(/* KafkaSource or MockTxnEventSource */);

        // --- 步骤 1：按分区组装完整事务 ---
        // TransactionAssembler 仅在收到 TXN_END 后才发出行。
        // 多个已关闭的源事务在两次刷新之间累积在 sink 缓冲区中。
        DataStream<DefaultStarRocksRowData> rows = events
                .keyBy(e -> e.partition)
                .process(new TransactionAssembler());

        // --- 步骤 2：按分区亲和性路由到 sink ---
        // keyBy(sourcePartition) 将同一分区的数据路由到同一个 sink 子任务。
        // 连接器在内部使用按分区划分的 region，因此即使多个
        // 分区落在同一个 sink 子任务上，事务边界也会通过 PartitionCommitTracker
        // 按分区独立跟踪。
        DataStream<DefaultStarRocksRowData> partitionedRows = rows
                .keyBy(DefaultStarRocksRowData::getSourcePartition);

        // --- 步骤 3：配置连接器 ---
        // sink.transaction.multi-table.enabled=true 激活每分区的 region
        // 在 StreamLoadManagerV2 内部进行跟踪：每个分区的 region 在其 txnEnd 到达时独立切换，
        // 仅当所有活跃分区均已切换后才触发提交。
        // 活跃分区均已完成切换。
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcUrl)
                .withProperty("load-url", loadUrl)
                .withProperty("database-name", "*")               // Wildcard for dynamic multi-table routing
                .withProperty("table-name", "*")                  // Wildcard for dynamic multi-table routing
                .withProperty("username", userName)
                .withProperty("password", password)
                .withProperty("sink.version", "V2")               // Required: V2
                .withProperty("sink.semantic", "at-least-once")
                .withProperty("sink.transaction.multi-table.enabled", "true")  // Enable multi-table txn
                .withProperty("sink.buffer-flush.interval-ms", "1000")         // ~1s data freshness
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        // 可选：每张表的 Stream Load 属性
        StreamLoadTableProperties orderItemsProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("order_items")
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build();
        options.addTableProperties(orderItemsProps);

        // --- 步骤 4：创建并附加 Sink ---
        // 标准连接器 API — 无需自定义 SinkFunction。
        // 在经过 keyBy 的流上调用 addSink 可确保分区亲和性。
        SinkFunction<DefaultStarRocksRowData> sink = SinkFunctionFactory.createSinkFunction(options);
        partitionedRows.addSink(sink);

        env.execute("WriteMultipleTablesWithTransaction");
    }
}
```

### 数据流拓扑

```
Kafka (60 partitions)
  |
  v
keyBy(partition) ———— Ensures same-partition events go to the same subtask
  |
  v
TransactionAssembler (KeyedProcessFunction)
  |  Buffers DATA events
  |  On TXN_END: emits all rows (last row has transactionEnd=true)
  |  Every row carries sourcePartition
  |
  v
keyBy(sourcePartition) ———— Ensures same-partition rows go to the same sink subtask
  |
  v
StarRocksDynamicSinkFunctionV2 (via SinkFunctionFactory.createSinkFunction)
  |
  |  +——————————— StreamLoadManagerV2 (multi-table txn mode) —————————-——+
  |  |                                                                   |
  |  |  Per-partition, per-table regions:                                |
  |  |    Region(P0, orders), Region(P0, order_items)                    |
  |  |    Region(P2, orders), Region(P2, order_items)                    |
  |  |                                                                   |
  |  |  Each region tracks:                                              |
  |  |    - activeChunk / inactiveChunks                                 |
  |  |    - lastSwitchTimeMs      (for miniInterval batching)            |
  |  |    - activeChunkCleanBoundary (true iff last task event is txnEnd)|
  |  |                                                                   |
  |  |  write(partition, db, table, row)  [task thread]                  |
  |  |    -> routes to Region(partition, db, table)                      |
  |  |    -> write0 sets activeChunkCleanBoundary = false                |
  |  |    -> In multi-table mode write0 does NOT switchChunk, so         |
  |  |       activeChunk only freezes at a txnEnd boundary               |
  |  |                                                                   |
  |  |  setCommitAllowed(partition, txnEnd=true)  [task thread]          |
  |  |    -> region.tryMiniIntervalSwitch():                             |
  |  |         sets activeChunkCleanBoundary = true                      |
  |  |         if (now - lastSwitchTimeMs >= miniInterval                |
  |  |             && activeChunk has data): switchChunkForCommit        |
  |  |         else: data batches into activeChunk with subsequent       |
  |  |               completed source transactions (N:1 mapping)         |
  |  |    -> PartitionCommitTracker.onTxnEnd(partition)                  |
  |  |                                                                   |
  |  |  SharedTransactionCoordinator:                                    |
  |  |    -> eagerly opens shared txn before any flush                   |
  |  |    -> all autonomous flushes use the shared label                 |
  |  |    -> recycles idle txn at 80% of server timeout                  |
  |  |                                                                   |
  |  |  Manager thread (every scanningFrequency):                        |
  |  |    -> tryForceCleanSwitch per region:                             |
  |  |         if cleanBoundary && has data && miniInterval elapsed      |
  |  |             -> switchChunkForCommit (source-idle fallback)        |
  |  |    -> tryStartTimerDrivenCommit:                                  |
  |  |         if commitInterval elapsed && hasDataLoaded                |
  |  |             -> set commitInFlight = true                          |
  |  |    -> autonomous flush: drain inactiveChunks via streamLoad       |
  |  |       (never touches activeChunk in multi-table mode)             |
  |  |                                                                   |
  |  |  Manager thread (commitInFlight=true):                            |
  |  |    -> triggerLoadIfNeeded per region (HTTP /api/transaction/load) |
  |  |    -> wait all loads complete                                     |
  |  |    -> unified commit via SharedTransactionCoordinator             |
  |  |    -> reset tracker; open next shared txn                         |
  |  +———————————————————————————————————————————————————————————————————+
  |
  v
StarRocks (test.orders + test.order_items)
```

## 工作原理

### 块生命周期与 `miniInterval` 批处理

每个 `(partition, table)` 区域有一个 `activeChunk`（当前接受写入）和一个 `inactiveChunks` 的 FIFO（冻结数据，等待 HTTP 加载）。在多表事务模式下，**仅** 数据从 `activeChunk` 移入 `inactiveChunks` 的方式是通过 `switchChunkForCommit`，它恰好从三个地方被调用：

1. **任务线程，在 txnEnd 时** — 通过 `region.tryMiniIntervalSwitch()` 在 `setCommitAllowed(partition, true)` 内部。这是常见路径。

2. **管理线程，源空闲回退** — 在每个扫描周期中，通过 `region.tryForceCleanSwitch()` 对那些 `activeChunk` 一直处于干净事务边界空闲状态的区域执行此操作。

3. **管理线程，保存点/回收** — 在验证每个区域都处于干净边界后，强制切换所有区域。

为避免在高吞吐量 CDC 中每个源事务产生一次 HTTP 加载，任务线程仅在距同一区域上次切换已过去至少 `miniSwitchIntervalMs` 时才执行切换。`miniSwitchIntervalMs` 的计算方式为 `min(1000 ms, max(100 ms, commitInterval / 10))`，因此 1 秒的提交间隔在 100 ms 时进行批处理，而 30 秒的间隔则将批处理上限设为 1 秒。在一个 miniInterval 窗口内，多个已完成的源事务会累积到同一个 `activeChunk` 中（N:1 映射），并在下次切换时一起冻结。

每个区域包含两个驱动这些决策的字段：

- `lastSwitchTimeMs` — 最近一次切换的纪元毫秒数。初始值为 0，因此区域创建后的第一个 txnEnd 始终会触发切换。

- `activeChunkCleanBoundary` — 如果此区域上最近的任务线程事件是 `onTxnEnd` 或 `switchChunk`，则为 `true`。在任何 `write()` 之后为 `false`。管理线程的 `tryForceCleanSwitch` 仅在此标志为 `true` 时运行，因此它永远不会冻结部分源事务数据。

### 任务线程 — 写入与 txnEnd

```
invoke(record)                                           [Flink task thread]
  |
  |  if record is a data row:
  |      write(partition, db, table, row)
  |          region.write(row) → addRow(); cleanBoundary = false
  |
  |  if record carries transactionEnd=true:
  |      setCommitAllowed(partition, true)
  |          for each region owned by this partition:
  |              region.tryMiniIntervalSwitch():
  |                  cleanBoundary = true       // always (txnEnd observed)
  |                  if (now - lastSwitchTimeMs >= miniInterval
  |                      && activeChunk has data):
  |                      switchChunkForCommit()  // freezes activeChunk
  |          partitionTracker.onTxnEnd(partition)  // safety bookkeeping only
```

任务线程是 `write()` 和 `setCommitAllowed()` 事件的唯一序列化器，这也是为什么 `cleanBoundary` 标记和条件切换都必须在此处执行（而非延迟到管理线程）：`cleanBoundary` 必须始终反映最新的任务线程事件，否则管理器的空闲回退可能与写入操作产生竞争，导致部分数据被冻结。

### 6.3 管理线程 — 时间驱动提交

管理线程以 `scanningFrequency` 的频率运行扫描循环。每次迭代：

1. **Ensure shared transaction**: 打开一个新连接（即时），或在当前连接接近 StarRocks 服务端超时时主动回收该连接（`timeout` 标头的 80%，默认 480 秒）。

2. **源空闲回退**: call `region.tryForceCleanSwitch()` on every region to freeze any `activeChunk` that is clean and has been idle for at least `miniInterval`. This handles the "source paused after a few txnEnds" case where the task thread stopped before issuing a fresh switch.

3. **时间驱动的提交触发器**: 调用 `tryStartTimerDrivenCommit()`，如果满足条件则设置 `commitInFlight=true`**两者** 条件成立：

   - `now - lastCommitTimeMs >= commitInterval`（已配置的 `sink.buffer-flush.interval-ms`）。

   - 存在待提交的数据——`txnCoordinator.hasDataLoaded()` 为 true，或至少有一个 region 仍有待处理的 inactiveChunks。

4. **自主刷新**：通过 `FlushAndCommitStrategy` 排空任何 `inactiveChunks` 非空的 region。多表模式的 `flush()` 仅流出已冻结的非活跃 chunk——它**从不**不会触碰 `activeChunk`。这保证了在共享标签下到达 StarRocks 的每个 chunk 均来自已完成的源事务这一不变量。

当 `commitInFlight=true` 时，主循环进入 `processMultiTableCommit`，等待飞行中的加载完成，为所有剩余的非活跃 chunk 触发加载，通过 `SharedTransactionCoordinator` 执行统一提交，更新 `lastCommitTimeMs`，重置追踪器，并为下一个周期开启新的共享事务。

### 共享事务协调

同一提交周期内的所有表共享一个由 `SharedTransactionCoordinator` 管理的 StarRocks 事务：

1. **提前开启事务**：在任何自主刷新之前，共享事务会被提前开启，因此所有 HTTP 加载均使用共享标签。这消除了独立标签刷新可能在共享事务随后覆盖标签时成为孤儿的数据丢失窗口。

2. **统一提交**：在所有 region 的数据加载完成后，针对共享标签执行单次 `commit`。多表事务跳过 `prepare` 步骤，因为 StarRocks 在多表模式下不支持 `TXN_PREPARE`。

3. **空闲事务回收**：如果共享事务保持开启的时间超过 StarRocks 服务端超时时间的 80%（默认：600s 超时对应 480s），则会主动回收（提交或回滚 + 重新开启），以防止服务端超时错误。若任何 region 存在进行中的事务数据（违反清洁边界），或任何分区已写入数据但从未收到 txnEnd，则回收操作将快速失败。

### 6.5 PartitionCommitTracker（安全记账）

在当前设计中，提交时机完全由提交间隔和每个 region 的清洁边界标志驱动。`PartitionCommitTracker` 被简化为信息/安全辅助工具：

- `onWrite(partition)` 在首次写入时将分区注册为 `ACTIVE`。

- `onTxnEnd(partition)` 将分区转换为 `TXN_END_SEEN`（粘性——后续写入**不会**不会将其降级回 `ACTIVE`）。

- `getPartitionsWithoutTxnEnd()` 列出已写入数据但从未收到 txnEnd 的分区。用于保存点和回收操作在上游合约违规（从未关闭的源事务）时快速失败。

- `reset()` 在提交周期结束时清除所有分区。

追踪器不再驱动切换/提交决策，不追踪 `SWITCHED` 状态，也不管理待处理的 txnEnd 信号。提交后停止产生数据的分区由 `reset()` 简单清除；若之后恢复，下一次 `onWrite` 会重新注册它们。

### 使用共享标签的自主刷新

当 region 的 `inactiveChunks` 变为非空时，管理线程的自主刷新循环通过 `/api/transaction/load` 在**当前共享标签**当前共享标签下将其流式传输到 StarRocks。由于共享事务始终在任何数据加载之前开启，因此不存在数据在独立（孤儿）标签下被加载的窗口。在多表模式下，`flush()` 从不触发 `switchChunk`——它仅排空已冻结的非活跃 chunk——因此「在共享标签下到达 StarRocks 的每个 chunk 均来自已完成的源事务」这一不变量无条件成立。

### 安全保证

| 保证                                               | 机制                                                                          |
| ------------------------------------------------- | ----------------------------------------------------------------------------- |
| switchChunk 不会拆分源事务                          | `switchChunkForCommit` 仅在清洁事务边界处调用（在 txnEnd 时或当 `activeChunkCleanBoundary` 为 `true` 时）。 |
| 提交从不包含部分源事务                              | 到达 StarRocks 的每个 chunk 均源自 `switchChunkForCommit`。自主刷新在多表模式下从不切换 `activeChunk`。 |
| 按分区隔离                                         | 每个 `(partition, table)` 拥有自己的 region。一个分区的切换不会影响另一个分区的数据。 |
| 分区内有序性                                       | `keyBy(sourcePartition)` 将相同分区的行路由到同一 sink 子任务。 |
| 任务线程非阻塞                                     | `tryMiniIntervalSwitch` 的时间复杂度为 O(regions-in-partition)。HTTP 工作在管理线程上异步执行。 |
| 源空闲时的数据可见性                               | 管理线程的 `tryForceCleanSwitch` 在空闲 `miniInterval` 后冻结干净的 `activeChunk`，因此即使源暂停，数据在 `commitInterval + miniInterval` 内仍保持可见。 |
| 自主刷新是事务安全的                               | 每次加载均使用共享标签。每个冻结的 chunk 均来自已完成的源事务。 |
| 空闲事务不会超时                                   | 共享事务在服务端超时时间的 80% 时被回收。回收在进行中的数据上快速失败。 |
| 按分区独立提交                                     | 具有已完成源事务的分区在下一个提交间隔提交，与其他分区的进行中事务无关。 |

### N:1 事务映射

多个源事务可通过 miniInterval 批处理机制在单个 StarRocks 事务中累积：

```
Source txn K1 (3 rows) -> write -> activeChunk -> txnEnd (1st switch)
Source txn K2 (2 rows) -> write -> activeChunk -> txnEnd (inside miniInterval: no switch)
Source txn K3 (4 rows) -> write -> activeChunk -> txnEnd (inside miniInterval: no switch)
                                                  (miniInterval elapsed → next switch batches K2+K3)
                                                  -> ...
commitInterval elapsed
                                                  -> commit(label=A)
                                                  -> K1 + K2 + K3 atomically committed to StarRocks
```

由于提交决策是时间驱动的（不与特定的 txnEnd 绑定），连接器通过摊销 HTTP 加载及 begin/commit 开销来处理大量小型源事务，无需任何配置更改。对于以每秒 100 个 txnEnd 速率发送的 CDC 源，配置 `commitInterval=1 s`、`miniInterval=100 ms` 时，连接器每秒最多发出约 10 次加载调用，而非约 100 次。

## 限制

- **需要 `sink.version=V2`**：V1 不支持事务流式加载。

- **仅至少一次**：失败的重试可能产生重复写入。多表模式保证同一批次内的所有表一起成功或失败，但不提供全局精确一次语义。对于 PRIMARY KEY 表，重复写入是幂等的（upsert）。

- **所有表必须在同一数据库中**：StarRocks 多表事务是数据库范围的；不支持跨数据库事务。

- **事务范围为每个 sink 子任务 + 每个分区**：每个 sink 子任务独立维护其自身的 StarRocks 事务。原子性在**在单个源事务内**（某一分区上单次 txnEnd 涉及的所有行，跨该分区写入的所有表）范围内得到保证。不同**不同**源分区的数据可见性可以交错：一旦分区 P0 的源事务已完全到达且提交间隔已过，P0 的数据即会被提交，即使分区 P1 的源事务仍在进行中。需要在 StarRocks 层面实现跨分区原子性的应用，必须使用单一源分区或在上游协调提交。

- **数据可见性延迟**：由 `sink.buffer-flush.interval-ms` 和内部 `miniInterval = min(1000, max(100, commitInterval/10))` 控制。在持续流动的 CDC 流中，单行数据在其源提交后约 `commitInterval + miniInterval` 内即可在 StarRocks 中可见。在源暂停期间，已提交的数据保持可见，而上次切换与暂停之间的任何行将在再经过一个 `miniInterval` 后可见（由管理线程的清洁边界回退机制处理）。

- **取决于 StarRocks 集群事务设置**：监控运行中的事务数量限制、prepared 超时（默认 600 秒）以及 label 保留情况。确保 `sink.buffer-flush.interval-ms` 显著短于 StarRocks 事务超时时间。

- **长源事务下 `activeChunk` 内存增长**：由于多表模式禁用了由 chunk 大小触发的内部切换（以保持清洁事务边界不变性），`activeChunk` 可能持续增长直到下一个 txnEnd 到达。内存受 `sink.transaction.multi-table.buffer-size`（软限制）和 `2 × buffer-size`（通过 `blockIfCacheFull` 实现的硬限制）约束。异常大的源事务将通过背压限制任务线程；若此情况频繁发生，请在上游拆分源事务或增大 `sink.transaction.multi-table.buffer-size`。

- **跨数据库写入将被拒绝**：多表事务会验证所有 region 是否属于同一数据库。在同一提交周期内向不同数据库的表写入数据将抛出错误。

- **与合并提交不兼容**：`sink.properties.enable_merge_commit=true` 不能与 `sink.transaction.multi-table.enabled=true` 组合使用。合并提交将写入路由通过 `MergeCommitManager`，而该路径缺少多表模式用于事务边界的分区感知 `write(int, ...)` / `setCommitAllowed(int, ...)` 钩子。若两者同时启用，连接器将在验证阶段快速失败。

## 监控与故障排查

推荐指标：

- **Flink 侧**
  - Checkpoint 成功率
  - Checkpoint 持续时间
  - Sink 刷新/提交延迟
- **StarRocks 侧**
  - 运行中/已 prepared 的事务数量
  - 事务超时发生次数
  - Label 冲突

### 常见问题

#### `transaction not existed`

- 原因：StarRocks 事务超时
- 解决方案：连接器会在服务器超时时间的 80% 时自动回收空闲事务。若问题仍然发生，请检查 prepared 超时是否过短或刷新间隔是否过大。

#### `too many running txns`

- 原因：并发事务数量过多。
- 解决方案：降低 sink 并行度，或增大 StarRocks FE 配置项 `max_running_txn_num_per_db` 的值。

#### `Transaction start failed`

- 原因：`beginTransaction` HTTP 调用失败
- 解决方案：验证 load-url 的连通性以及 StarRocks 版本（需要 v4.0 或更高版本）。

#### 数据可见性延迟过高

- 原因：提交条件未满足。
- 解决方案：验证上游数据是否包含正确的 `transactionEnd=true` 标记；每行数据预期最多有 `commitInterval + miniInterval` 的延迟。若延迟超出此预算，请检查管理线程是否卡在回收或飞行中的加载操作（参见 `StarRocks-Sink-Manager` 日志）。

#### 跨数据库写入错误

- 原因：不同数据库中的表处于同一提交周期内。
- 解决方案：确保同一作业中写入的所有表均属于同一 StarRocks 数据库。

## 最佳实践

1. **TransactionAssembler 合约**:

   - 仅在源事务完全关闭后才发出行。
   - 最后一行必须包含 `setTransactionEnd(true)`。
   - 每一行必须包含 `setSourcePartition(partition)`。
   - 所有行必须在单个 `processElement()` 调用中同步发出。

2. **sink 前的 keyBy 是强制要求**：`rows.keyBy(DefaultStarRocksRowData::getSourcePartition).addSink(sink)` — 省略此项会破坏分区内的事务顺序。

3. **Checkpoint 与提交解耦**：Checkpoint 间隔可以设置为较大的值（例如 60 秒）用于故障恢复。数据可见性由 `sink.buffer-flush.interval-ms` 控制（例如 1000ms）。

4. **保持路由策略稳定**：避免单个事务写入过多不同的表，这会增加事务持续时间和失败概率。

5. **上线前进行故障注入测试**：终止 TaskManager / 引入网络抖动，并在 checkpoint 恢复后验证数据正确性。
