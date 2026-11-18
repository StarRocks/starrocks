---
sidebar_position: 120
displayed_sidebar: docs
description: "为 Flink 作业启用多表事务，以便在一个处理周期内向同一数据库中的多个表写入数据。"
products: [oss, enterprise, anywhere]
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
| 跨表原子提交                 | 同一刷新周期内的所有表共享一个 StarRocks 事务标签，并针对该标签统一发出一次 `POST /api/transaction/commit`（不调用 prepare——StarRocks 不支持对多表事务执行 prepare）。 |
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

#### `sink.transaction.multi-table.mini-switch-interval-ms`

- Type: Long
- Default: `-1`（自动）
- Unit: ms
- Description: 每个分区执行 Chunk 切换之间的最小间隔（毫秒）。在此间隔内，源事务会被批量合并到一次 Stream Load 中，从而限制 HTTP 请求数量。`-1` 表示自动计算为 `min(1000, max(500, sink.buffer-flush.interval-ms/4))`；设置正数时则使用指定值。

#### `sink.transaction.multi-table.min-switch-bytes`

- Type: Long
- Default: `1048576`（1 MB）
- Unit: Bytes
- Description: 基于时间间隔触发的 Chunk 切换仅在某个 Region 的活动 Chunk 至少累积了此字节数后才会执行。这样，低流量分区可以将更多源事务批量合并到一次 Stream Load 中，而不是产生大量微小请求。达到半满时的预留空间、缓冲区大小导致的内存压力，或者自该分区上次切换后经过完整的 `sink.buffer-flush.interval-ms`，都会强制执行切换，不受此大小阈值限制。因此，即使设置了较大的阈值，持续的低流量数据流仍会大约按照 flush 间隔进行切换，不会被无限期地延迟。设置为 `<=0` 可禁用大小限制，恢复之前按时间间隔切换的行为。当 `mini-switch-interval-ms` 等于（或大于）`sink.buffer-flush.interval-ms` 时，此选项对 txnEnd 时的切换不起作用（参见块生命周期与 `miniInterval` 批处理）。

#### `sink.transaction.multi-table.max-txn-bytes`

- Type: Long
- Default: `0`（无限制）
- Unit: Bytes
- Description: 在等待 `txnEnd` 期间，单个 sink 子任务可以缓冲的正在进行中的源事务数据的硬上限（字节）。对于在 `txnEnd` 之前无法 flush 的数据，Writer 不会阻塞（参见限制 7），因此单个源事务可能增长到超过 `2 × buffer-size`。此选项用于限制这种增长；超过限制时，任务将因明确的错误而失败。`0` 表示不限制大小，此时唯一的限制是 TaskManager 的堆内存。

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

#### `sink.http.keep-alive.enabled`

- 推荐值：`true`
- 描述：推荐开启。加载请求以及 BEGIN / PREPARE / COMMIT / ROLLBACK 请求复用 HTTP 连接，而不是为每个请求新建一个连接。默认关闭（参见最佳实践 6）。

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
        // 仅支持对齐 Checkpoint。每次 Checkpoint 还会排空并提交
        // 当前开启的共享事务（参见最佳实践 3）。
        env.enableCheckpointing(10_000);

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
        // 在 StreamLoadManagerV2 内部进行跟踪：每个分区的所有 region 在其 txnEnd 边界处
        // 一起切换，且与其他分区相互独立。
        // 定时器驱动的提交不会等待每个分区：它切换处于干净边界的分区，
        // 而处于事务中途的分区只贡献其之前已切换的 chunk——
        // 其进行中的行会进入下一个共享事务。
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
                .withProperty("sink.http.keep-alive.enabled", "true")          // Recommended: reuse HTTP connections
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
  |  +——————————— StreamLoadManagerV2 (multi-table txn mode) ———————————+
  |  |                                                                   |
  |  |  Per-partition, per-table regions:                                 |
  |  |    Region(P0, orders), Region(P0, order_items)                    |
  |  |    Region(P2, orders), Region(P2, order_items)                    |
  |  |                                                                   |
  |  |  Each region tracks:                                              |
  |  |    - activeChunk / inactiveChunks                                 |
  |  |    - activeChunkCleanBoundary (true iff last task event is txnEnd)|
  |  |    - commit watermark (set at the commit cut, see below)          |
  |  |  Each partition tracks:                                           |
  |  |    - last switch time         (for miniInterval batching)         |
  |  |                                                                   |
  |  |  write(partition, db, table, row)  [task thread]                  |
  |  |    -> routes to Region(partition, db, table)                      |
  |  |    -> write0 sets activeChunkCleanBoundary = false                |
  |  |    -> In multi-table mode write0 does NOT switchChunk, so         |
  |  |       activeChunk only freezes at a txnEnd boundary               |
  |  |                                                                   |
  |  |  setCommitAllowed(partition, txnEnd=true)  [task thread]          |
  |  |    -> marks every region of the partition clean                   |
  |  |    -> if (miniInterval elapsed && a region >= minSwitchBytes)     |
  |  |          || commitInterval elapsed || half-full || cache pressure:|
  |  |           lockstepSwitchPartition  (freshness-bounded)            |
  |  |       else: batch into activeChunk with later txns (N:1)          |
  |  |    -> PartitionCommitTracker.onTxnEnd(partition)                  |
  |  |                                                                   |
  |  |  SharedTransactionCoordinator:                                    |
  |  |    -> eagerly opens shared txn before any flush                   |
  |  |    -> all autonomous flushes use the shared label                 |
  |  |    -> recycles the txn at 80% of the BEGIN timeout                |
  |  |                                                                   |
  |  |  Manager thread (every scanningFrequency):                        |
  |  |    -> force-switch clean partitions (source-idle):                |
  |  |         if all-clean && has data && (pressure                     |
  |  |             || (bytes>=minSwitchBytes && miniInterval elapsed))   |
  |  |             -> lockstepSwitchPartition                            |
  |  |    -> tryStartTimerDrivenCommit:                                  |
  |  |         if commitInterval elapsed && committable data             |
  |  |             -> set commitInFlight = true                          |
  |  |    -> autonomous flush: drain inactiveChunks via streamLoad       |
  |  |       (never touches activeChunk in multi-table mode)             |
  |  |                                                                   |
  |  |  Manager thread (commitInFlight=true):                            |
  |  |    -> commit cut, per partition:                                  |
  |  |         clean: switch activeChunk, then set watermark             |
  |  |         dirty: set watermark at its last switch; its              |
  |  |                activeChunk goes into the next shared txn          |
  |  |    -> triggerLoadIfNeeded for chunks <= watermark                 |
  |  |       (HTTP /api/transaction/load)                                |
  |  |    -> wait all loads complete                                     |
  |  |    -> unified commit via SharedTransactionCoordinator             |
  |  |    -> lift watermarks; reset tracker; open next shared txn        |
  |  +———————————————————————————————————————————————————————————————————+
  |
  v
StarRocks (test.orders + test.order_items)
```

## 工作原理

### 块生命周期与 `miniInterval` 批处理

每个 `(partition, table)` 区域有一个 `activeChunk`（当前接受写入）和一个 `inactiveChunks` 的 FIFO（冻结数据，等待 HTTP 加载）。在多表事务模式下，数据从 `activeChunk` 移入 `inactiveChunks` 的**唯一**方式是提交切换：它仅在干净的事务边界处进行，并且除保存点外，会同时作用于一个分区的所有区域（`lockstepSwitchPartition`）。提交切换恰好发生在以下四个位置：

1. **任务线程，在 txnEnd 时** — 在 `setCommitAllowed(partition, true)` 内部进行。该方法先将分区的每个区域标记为干净，然后为整个分区做出一次切换决策（规则见下文）。这是常见路径。

2. **管理线程，源空闲强制切换** — 在每个扫描周期中调用 `managerForceSwitchCleanBoundaryRegions()`，作用于所有区域**都**处于干净边界、且至少有一个区域包含数据行的分区。存在压力时（缓冲的字节数达到 `buffer-size`，或某个区域的 `activeChunk` 已半满），该分区会被立即切换；否则，仅当某个区域至少包含 `min-switch-bytes` 的数据，**并且**自该分区上次切换以来已经过 `miniSwitchIntervalMs` 时，才会切换。低于 `min-switch-bytes` 的空闲数据留给提交切分处理。

3. **管理线程，提交切分** — 在每次定时器驱动的提交以及每次回收开始时进行（参见管理线程 — 时间驱动提交，以及共享事务协调）：切换每个干净分区，并在每个区域上冻结提交水位线。

4. **管理线程，保存点** — Checkpoint 刷新在验证每个区域都处于干净边界后，强制切换所有区域（参见最佳实践 3）。

为避免在高吞吐量 CDC 中每个源事务产生一次 HTTP 加载，txnEnd 路径仅在满足以下**任一**条件时才切换该分区；否则，已完成的事务会留在 `activeChunk` 中，与后续事务一起批处理：

- 自该分区上次切换以来已经过至少 `miniSwitchIntervalMs`，**并且**其至少一个区域的 `activeChunk` 中的数据量达到 `sink.transaction.multi-table.min-switch-bytes`（默认 1 MB；`<= 0` 表示禁用此大小检查）；

- 自该分区上次切换以来已经过完整的 `commitInterval`（新鲜度上限，确保大小限制永远不会无限期地推迟已完成的数据）；

- 某个区域的 `activeChunk` 达到写阻塞上限的一半（即 `buffer-size` 字节）；

- 所有区域的缓冲字节总数达到 `buffer-size`。

自动计算时，`miniSwitchIntervalMs` 为 `min(1000 ms, max(500 ms, commitInterval / 4))`，因此 2 秒的提交间隔按 500 ms 进行批处理，而 4 秒或更长的间隔则将批处理上限设为 1 秒（显式设置的 `sink.transaction.multi-table.mini-switch-interval-ms` 会覆盖此值）。在该窗口内，多个已完成的源事务会累积到同一个 `activeChunk` 中（N:1 映射），并在下次切换时一起冻结。

大小限制仅在 `miniSwitchIntervalMs` 小于 `commitInterval` 时才起作用。如果将 `mini-switch-interval-ms` 设置为等于（或大于）`sink.buffer-flush.interval-ms`——或者在提交间隔不超过 500 ms 时自动计算为 500 ms——则新鲜度上限最迟在该间隔结束时就会达到，因此在该间隔过后，每个 txnEnd 都会触发切换（与数据大小无关），`min-switch-bytes` 对 txnEnd 路径不起作用。

以下两项状态驱动这些决策：

- 分区的上次切换时间（由管理器按分区维护，因此同一分区的所有区域一起进行批处理）。初始值为 0，因此分区的第一个 txnEnd 始终会触发切换。

- `activeChunkCleanBoundary`（按区域维护）— 如果此区域上最近的任务线程事件是 txnEnd 或切换，则为 `true`。在任何 `write()` 之后为 `false`。管理线程执行的切换会在持有各区域写锁的情况下，重新检查该分区每个区域的此标志，只要有任一区域为脏就跳过该分区，因此永远不会冻结部分源事务数据。

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
  |              cleanBoundary = true           // always (txnEnd observed)
  |          if (activeChunk has data) and (
  |                (miniInterval elapsed && bytes >= minSwitchBytes)
  |             || commitInterval elapsed since last switch   // freshness bound
  |             || half-full headroom || memory pressure):
  |              lockstepSwitchPartition()      // freezes all regions together
  |          partitionTracker.onTxnEnd(partition)  // safety bookkeeping only
```

任务线程是 `write()` 和 `setCommitAllowed()` 事件的唯一序列化器，这也是为什么 `cleanBoundary` 标记和条件切换都必须在此处执行（而非延迟到管理线程）：`cleanBoundary` 必须始终反映最新的任务线程事件，否则管理器的空闲回退可能与写入操作产生竞争，导致部分数据被冻结。

### 6.3 管理线程 — 时间驱动提交

管理线程以 `scanningFrequency` 的频率运行扫描循环。每次迭代：

1. **Ensure shared transaction**: 打开一个新连接（即时），或在当前连接接近 StarRocks 服务端超时时主动回收该连接（`timeout` 标头的 80%，默认 480 秒）。

2. **源空闲回退**: 对于所有 region **都**处于干净边界的分区，一旦其累积的数据达到 `min-switch-bytes`，且自其上次切换以来已经过 `miniSwitchIntervalMs`，就强制切换该分区；在内存压力下则立即强制切换（参见块生命周期与 `miniInterval` 批处理）。低于大小阈值时，空闲数据不会在此处切换——而是由下文的时间驱动提交对其进行切分并提交（参见步骤 3），因此即使源暂停，其数据仍会在一个 `commitInterval` 内变为可见。这用于处理「源在若干次 txnEnd 后暂停」的情况，即任务线程在发起新一次切换之前就已停止。

3. **时间驱动的提交触发器**: 调用 `tryStartTimerDrivenCommit()`，如果满足条件则设置 `commitInFlight=true`**两者** 条件成立：

   - `now - lastCommitTimeMs >= commitInterval`（已配置的 `sink.buffer-flush.interval-ms`）。

   - 存在待提交的数据——`txnCoordinator.hasDataLoaded()` 为 true，至少有一个 region 有待处理的 inactiveChunks，或者某个分区整体处于干净边界，且有缓冲的活动行（因低于大小限制而被推迟），提交切分会将这些行冻结。最后这项检查在分区范围内进行，因此单独一个干净的 region（其兄弟表仍处于事务中途）不会触发一个无法推进的提交周期。

4. **自主刷新**：通过 `FlushAndCommitStrategy` 排空任何 `inactiveChunks` 非空的 region。多表模式的 `flush()` 仅流出已冻结的非活跃 chunk——它**从不**不会触碰 `activeChunk`。这保证了在共享标签下到达 StarRocks 的每个 chunk 均来自已完成的源事务这一不变量。

当 `commitInFlight=true` 时，主循环进入 `processMultiTableCommit`。其第一步是**提交切分**（commit cut），用于决定哪些数据进入当前标签。切分按分区进行，不会等待每个分区都到达干净边界。在持有该分区各 region 写锁的情况下（这样并发的 txnEnd 切换就无法插入其间）：

- **干净**分区（每个 region 都处于干净边界，且至少有一个 region 有数据）会以 lockstep 方式切换，然后将每个 region 的提交水位线设置为其最新切换出的 chunk。

- **脏**分区（任一 region 处于事务中途）不会被切换；其各 region 的水位线设置在各自的上次切换处。该分区之前切换出的 chunk 会在本次提交，而其 `activeChunk`（进行中的源事务，以及与之一起批处理的已完成事务）会进入下一个共享事务。

如果任一分区的锁正被占用，则在下一次扫描时重试切分。只有位于水位线及以下的 chunk 会在当前标签下加载；切分之后才切换出的 chunk 则等待下一次切分。随后，提交会等待飞行中的加载完成，为剩余符合条件的 chunk 触发加载，通过 `SharedTransactionCoordinator` 执行统一提交，解除水位线，更新 `lastCommitTimeMs`，重置追踪器，并为下一个周期开启新的共享事务。

### 共享事务协调

同一提交周期内的所有表共享一个由 `SharedTransactionCoordinator` 管理的 StarRocks 事务：

1. **提前开启事务**：在任何自主刷新之前，共享事务会被提前开启，因此所有 HTTP 加载均使用共享标签。这消除了独立标签刷新可能在共享事务随后覆盖标签时成为孤儿的数据丢失窗口。

2. **统一提交**：在所有 region 的数据加载完成后，针对共享标签执行单次 `POST /api/transaction/commit`。多表事务跳过 `prepare` 步骤，因为 StarRocks 在多表模式下不支持 `TXN_PREPARE`。

3. **事务回收**：如果共享事务的开启时长（从其 BEGIN 开始计算，而不是从最后一次加载开始计算）超过 BEGIN 请求 `timeout` 标头的 80%（默认：连接器发送的 600s 对应 480s），则会主动回收（提交或回滚 + 重新开启），以防止服务端超时错误。回收使用与定时器驱动的提交相同的按分区提交切分，因此处于事务中途的分区只会被顺延到下一个标签。仅当某个分区自上次提交以来写入了数据却未收到任何 txnEnd 时，回收操作才会快速失败。回收是尽力而为的：当有提交周期正在进行，或任一 region 仍在加载或重试时，回收会等待，因此一个永不返回的加载（在默认 `sink.socket.timeout-ms=-1` 下可能发生）仍可能使标签到达其截止时间。请设置有限的 `sink.socket.timeout-ms` 来限制这种情况。

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
| txnEnd 路径上无 HTTP I/O                         | `setCommitAllowed` 中的分区切换决策的时间复杂度为 O(regions-in-partition)，所有 HTTP 工作都在管理线程上异步执行。这并不是非阻塞保证：任务线程可能会等待由管理线程切换持有的 region 写锁，可能在自身的切换冻结了一个过大的 chunk 时逐行重新拆分该 chunk（参见限制 7），可能在 `blockIfCacheFull` 中挂起，也可能在 Checkpoint 期间阻塞（参见最佳实践 3）。 |
| 源空闲时的数据可见性                               | 如果源在数据量低于 `min-switch-bytes` 时暂停，时间驱动提交的分区范围清洁边界检测会切分并提交空闲的 `activeChunk`，因此即使源不再恢复，数据也会在大约一个 `commitInterval` 内变为可见。 |
| 自主刷新是事务安全的                               | 每次加载均使用共享标签。每个冻结的 chunk 均来自已完成的源事务。 |
| 共享事务在截止时间前被回收（尽力而为）                      | 共享事务在 BEGIN `timeout` 的 80% 时被回收。处于事务中途的分区会被顺延到下一个标签，且仅当某个分区写入了数据却没有任何 txnEnd 时，回收才会快速失败。当有提交周期或加载正在进行时，回收会等待，因此一个永不返回的加载（默认 `sink.socket.timeout-ms=-1`）可能使标签超过其截止时间；设置有限的 socket 超时时间可以限制这种情况。 |
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

由于提交决策是时间驱动的（不与特定的 txnEnd 绑定），连接器通过摊销 HTTP 加载及 begin/commit 开销来处理大量小型源事务，无需任何配置更改。对于以每秒 100 个 txnEnd 速率发送的 CDC 源，配置 `commitInterval=1 s`（因此自动计算出的 `miniInterval=500 ms`）时，连接器每秒最多发出约 2 次加载调用，而非约 100 次；在默认的 `min-switch-bytes` 大小限制下，低流量数据流会进一步批处理，降至大约每个 `commitInterval` 一次加载（即低于阈值的 chunk 仍会被切换的新鲜度上限）。

## 限制

1. **需要 `sink.version=V2`**：V1 不支持事务流式加载。

2. **仅至少一次**：失败的重试可能产生重复写入。多表模式保证同一批次内的所有表一起成功或失败，但不提供全局精确一次语义。对于 PRIMARY KEY 表，重复写入是幂等的（upsert）。

3. **所有表必须在同一数据库中**：StarRocks 多表事务是数据库范围的；不支持跨数据库事务。

4. **事务范围为每个 sink 子任务 + 每个分区**：每个 sink 子任务独立维护其自身的 StarRocks 事务。原子性在**在单个源事务内**（某一分区上单次 txnEnd 涉及的所有行，跨该分区写入的所有表）范围内得到保证。不同**不同**源分区的数据可见性可以交错：一旦分区 P0 的源事务已完全到达且提交间隔已过，P0 的数据即会被提交，即使分区 P1 的源事务仍在进行中。需要在 StarRocks 层面实现跨分区原子性的应用，必须使用单一源分区或在上游协调提交。简而言之，源事务保证仅在每个 lane 内成立，其中一个 lane 指一个 sink 子任务上的一个 `sourcePartition`。在同一个 Label 下提交的所有内容（包括来自多个 lane 的行）会原子地变为可见，但无法控制不同 lane 的哪些源事务会进入同一个 Label：另一个 lane 上的相关事务可能落入更早或更晚的 Label，而不同 sink 子任务上的 lane 永远不会共享同一个 Label。

5. **数据可见性延迟**：由 `sink.buffer-flush.interval-ms`（提交间隔）控制。已完成的源事务首先会从 `activeChunk` 中冻结出来——要么在分区达到 `sink.transaction.multi-table.min-switch-bytes`（默认 1 MB）时立即冻结，要么对于低于该阈值的低流量分区，最迟在其上次切换后一个 `commitInterval` 时冻结（即任务线程在 txnEnd 时强制执行的新鲜度上限）——然后由下一次时间驱动的提交发布。因此，在持续流动的 CDC 流中，如果分区足够繁忙、能够达到大小限制，单行数据会在大约一个 `commitInterval` 内可见；在低流量的最坏情况下，则最多需要两个 `commitInterval` 窗口（一个用于冻结低于阈值的 Chunk，一个用于提交它）。在源暂停期间，已提交的数据保持可见，而最后一个低于阈值的空闲 Chunk 会通过时间驱动提交的干净边界检测，在最多再经过一个 `commitInterval` 后被提交。设置 `min-switch-bytes <= 0` 可在每个间隔都执行切换（延迟最低，但加载次数更多、单次数据量更小）。

6. **取决于 StarRocks 集群事务设置**：监控运行中的事务数量限制、事务超时时间以及 Label 保留情况。事务超时时间是指连接器在 BEGIN 请求中发送的 `timeout` 标头——除非被 `sink.properties.timeout` 覆盖，否则为 600 秒——StarRocks 将其视为从 BEGIN 开始计算的绝对截止时间，而非空闲超时：向该 Label 加载数据不会延长它。连接器会在达到该超时时间的 80% 时（默认 480 秒）回收共享事务。确保 `sink.buffer-flush.interval-ms` 显著短于此超时时间。

7. **长源事务下 `activeChunk` 内存增长**：由于多表模式会禁用基于 Chunk 大小触发的内部切换（以保持干净的事务边界不变量），因此 `activeChunk` 可能一直增长到下一个 `txnEnd` 到达。`sink.transaction.multi-table.buffer-size`（软限制）和 `2 × buffer-size`（通过 `blockIfCacheFull` 实现的硬限制）仅限制缓冲区中*可 flush 的*部分。冻结的 Chunk 和所有 clean 分区不受此限制。无法在 `txnEnd` 之前从缓冲区中移出的数据（正在进行中的事务本身，以及同一分区中因 lockstep 规则而被暂时保留的兄弟表已完成数据）不会触发等待：当缓冲区达到硬上限且没有任何可 flush 数据时，Writer 会记录 WARN（`Write-block cap ... reached with nothing flushable`），并继续缓冲直到 `txnEnd` 到达；之后，该分区会被切换并排空。因此，单个源事务可能超过 `2 × buffer-size`；其大小仅受 `sink.transaction.multi-table.max-txn-bytes`（默认为无限制）和 TaskManager 堆内存的限制。应根据预期的最大源事务大小 × 每个 TaskManager 上的 sink 子任务数来规划堆内存；如果希望在内存压力过大时直接明确失败，请设置 `max-txn-bytes`。对于无界事务（批量初始化），应在上游拆分事务。在切换时，如果冻结的 Chunk 大于 Load Body 限制（默认为 `sink.chunk-limit` 的 3 GB 与 `sink.transaction.multi-table.buffer-size` 两者中的较小值），则会在相同的共享 Label 下重新切分为多个 Load 请求。因此，除了单行数据本身就超过该限制的情况外，单个 `/api/transaction/load` 请求体都不会超过此限制。

8. **跨数据库写入将被拒绝**：多表事务会验证所有 region 是否属于同一数据库。在同一提交周期内向不同数据库的表写入数据将抛出错误。

9. **与合并提交不兼容**：`sink.properties.enable_merge_commit=true` 不能与 `sink.transaction.multi-table.enabled=true` 组合使用。合并提交将写入路由通过 `MergeCommitManager`，而该路径缺少多表模式用于事务边界的分区感知 `write(int, ...)` / `setCommitAllowed(int, ...)` 钩子。若两者同时启用，连接器将在验证阶段快速失败。

10. **每个 `sourcePartition` 只能有一个上游生产者**：干净边界是基于位置判断的——当且仅当 sink 子任务为某个分区收到的最后一行带有 txnEnd 时，该分区才处于干净边界；连接器不会跟踪某一行属于哪个源事务。如果两个生产者（两个 assembler 子任务，或分区编号冲突的两个源，例如两个 Kafka Topic 的分区 0）发出具有相同 `sourcePartition` 的行，这些行会在 sink 端交错，一个生产者的 txnEnd 会将该分区标记为干净，而另一个生产者的事务仍在进行中，下一次切换就可能部分提交该事务。请让 TransactionAssembler 与 sink 按相同的值进行 keyBy，并确保 `sourcePartition` 在所有源中唯一。

11. **仅支持对齐 Checkpoint**：Checkpoint 刷新要求 barrier 到达 sink 时每个 region 都处于干净边界（参见最佳实践 3）。对齐 Checkpoint 可以保证这一点，因为 assembler 会在一次 `processElement()` 调用中发出整个源事务，barrier 不会落在该事务的行之间。非对齐 Checkpoint（`execution.checkpointing.unaligned.enabled`；Flink 1.20 之前为 `execution.checkpointing.unaligned`）允许 barrier 越过在途的行，在事务中途到达 sink。这也包括先对齐后非对齐的模式，即同时启用非对齐 Checkpoint 和 `execution.checkpointing.aligned-checkpoint-timeout`，Checkpoint 会在超时后切换为非对齐；在禁用非对齐 Checkpoint 时，该超时设置不起作用。在这种情况下，Checkpoint 会失败，当前开启的 Label 会被回滚，作业发生故障转移。

12. **数据质量拒绝会中止整个 Label**：对于多表事务，StarRocks 在提交时（而不是在每次 `/api/transaction/load` 时）应用严格模式和 `max_filter_ratio`。一旦被拒绝，提交就会失败，并中止整个 Label——即该提交周期内所有表的行，而不仅仅是包含问题行的那张表。连接器会报告提交失败，作业发生故障转移；由于会重放相同的行，作业会持续失败，直到修复这些行或加载设置。

13. **StarRocks v4.1.6 及更早版本可能将失败的多表提交报告为 OK**：在这些版本中，服务端已失败的多表提交可能会以 `OK` 返回给客户端。连接器随后会将该 Label 视为已提交，因此该提交周期的行可能会静默丢失。该问题已由 StarRocks PR [#80351](https://github.com/StarRocks/starrocks/pull/80351) 修复（CelerData backport：commit `1ceb0763cf`）。

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
- **连接器侧**
  - `sharedTxnLatencyMs` 直方图：从共享事务的最早一行进入 sink 缓冲区到该事务提交所经过的时间。每次提交还会按 Label 将其记录到日志中：`[MultiTxn] SharedTransaction committed successfully: label=<label>, firstRowTimeMs=<epoch ms>, latencyMs=<ms>`（未知时两者均为 `-1`）。

### 常见问题

#### `transaction not existed`

- 原因：StarRocks 事务超时
- 解决方案：连接器会在 BEGIN `timeout` 的 80% 时（从 BEGIN 开始计算）自动回收共享事务。若问题仍然发生，请检查 `sink.properties.timeout` 是否过短、刷新间隔是否过大，或者是否有加载挂起超过了截止时间（请设置有限的 `sink.socket.timeout-ms`）。

#### `too many running txns`

- 原因：并发事务数量过多。
- 解决方案：降低 sink 并行度，或增大 StarRocks FE 配置项 `max_running_txn_num_per_db` 的值。

#### `Transaction start failed`

- 原因：`beginTransaction` HTTP 调用失败
- 解决方案：验证 load-url 的连通性以及 StarRocks 版本（需要 v4.0 或更高版本）。

#### 数据可见性延迟过高

- 原因：提交条件未满足。
- 解决方案：验证上游数据是否包含正确的 `transactionEnd=true` 标记；对于低于 `min-switch-bytes` 的低流量分区，每行数据预期最多有约 2×`commitInterval` 的延迟（一个间隔用于冻结低于阈值的 Chunk，一个间隔用于提交它）。若延迟超出此预算，请检查管理线程是否卡在回收或飞行中的加载操作（参见 `StarRocks-Sink-Manager` 日志），或者调低 `min-switch-bytes`。

#### `Write-block cap ... reached with nothing flushable` (WARN)

- 原因：一个源事务以及在其 `txnEnd` 到达前被暂时保留的兄弟表已完成数据，其总大小超过了 `2 × buffer-size`。Writer 不会阻塞，而是继续缓冲直到 `txnEnd` 到达。
- 解决方案：对于大型源事务，这是预期行为。`txnEnd` 到达后，数据即会提交。请监控 TaskManager 的堆内存。如果希望超过限制时直接失败，请设置 `sink.transaction.multi-table.max-txn-bytes`，或者在上游拆分事务。

#### `would exceed sink.transaction.multi-table.max-txn-bytes`

- 原因：源事务大小超过了配置的硬限制。
- 解决方案：提高 `max-txn-bytes`（或者设置为 `0` 表示无限制），或者在上游拆分源事务。

#### 跨数据库写入错误

- 原因：不同数据库中的表处于同一提交周期内。
- 解决方案：确保同一作业中写入的所有表均属于同一 StarRocks 数据库。

## 最佳实践

1. **TransactionAssembler 合约**:

   - 仅在源事务完全关闭后才发出行。
   - 最后一行必须包含 `setTransactionEnd(true)`。
   - 每一行必须包含 `setSourcePartition(partition)`。
   - 所有行必须在单个 `processElement()` 调用中同步发出。

2. **sink 前的 keyBy 是强制要求**：`rows.keyBy(DefaultStarRocksRowData::getSourcePartition).addSink(sink)` — 省略此项会破坏分区内的事务顺序。每个 `sourcePartition` 值还必须只来自一个上游生产者（参见限制 10）。

3. **Checkpoint 也会提交**：数据可见性由 `sink.buffer-flush.interval-ms` 控制（例如 1000ms），但 Checkpoint 并未与提交解耦。在 at-least-once 模式下，`snapshotState` 会调用 `flush()`，该调用会阻塞任务线程，直到整个缓冲区——包括仍在 `activeChunk` 中批处理的已完成事务——全部加载完毕，且当前开启的共享 Label 已提交。该 flush 的时间预算为事务超时时间的 1.1 倍（默认 660 秒），但该预算仅在各次等待之间检查，并非硬性上限：排空过程会等待每个飞行中的 `/api/transaction/load`，且没有截止时间；在默认 `sink.socket.timeout-ms=-1` 的情况下，一个始终收不到响应的加载可能会使 Checkpoint 以及任务线程无限期阻塞。请设置一个有限的 `sink.socket.timeout-ms`，并使其长于预期最慢的加载耗时，这样挂起的加载会转而导致作业失败。如果 barrier 到达时有任一分区处于事务中途，flush 会抛出异常，当前开启的 Label 会被回滚，作业发生故障转移（参见限制 10 和 11）。Checkpoint 间隔越大（例如 60 秒），这类强制提交就越少。

4. **保持路由策略稳定**：避免单个事务写入过多不同的表，这会增加事务持续时间和失败概率。

5. **上线前进行故障注入测试**：终止 TaskManager / 引入网络抖动，并在 checkpoint 恢复后验证数据正确性。

6. **启用 HTTP keep-alive**：设置 `sink.http.keep-alive.enabled=true`。一个共享事务会发送一次 BEGIN、多次加载和一次 COMMIT，如果不启用 keep-alive，其中每个请求都会新建一个 TCP（以及 TLS）连接。在任一请求过程中出现连接故障，都会导致整个共享事务失败，从而回滚其中的所有表并使作业发生故障转移；因此，新建连接越少，此类失败就越少，例如位于可能重置新连接的负载均衡器后方时。启用 keep-alive 后，如果某个请求所用的复用连接已被服务器或负载均衡器关闭，则在不会导致数据被写入两次的情况下，该请求会在新连接上重新发送。请确保 `sink.http.keep-alive.idle-timeout-ms`（默认 60000）短于 Flink 与 StarRocks 之间每个代理或负载均衡器的空闲超时时间；AWS Network Load Balancer 默认会断开空闲 350 秒的连接。
