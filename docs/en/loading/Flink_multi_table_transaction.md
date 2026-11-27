---
sidebar_position: 120
displayed_sidebar: docs
description: "Enable Multi-table Transaction for a Flink job to write to multiple tables within the same database in one processing cycle."
products: [oss, enterprise, anywhere]
---

# Load data from Apache Flink® with Multi-table Transaction

StarRocks Flink Connector supports Multi-table Transaction to load data from Flink into multiple tables atomically.

## Use Cases

When a single Flink job writes to multiple tables within the same StarRocks database in one processing cycle, enabling multi-table transaction guarantees:

- **Cross-table atomic commit**: Data written to different table within the same commit cycle becomes visible atomically — all or nothing.
- **Source transaction integrity**: A complete upstream transaction (for example, from Kafka) is never split across two StarRocks transactions.
- **Sub-second data freshness**: Data continuously flows into StarRocks via `/api/transaction/load`, and is committed at the interval configured by `sink.buffer-flush.interval-ms`.

Typical scenarios:

- Synchronous writes to a general table and a detail table (for example, `orders` and `order_items`)
- Event routing to different partition tables (for example, `events_202601`, `events_202602`)
- A single job maintaining multiple interrelated downstream result tables

:::tip[Prerequisites]
To enable Multi-table Transaction, you must running your cluster on StarRocks v4.0 and later (with the Multi-table Transaction Stream Load support), and StarRocks Flink Connector on v1.2.9 and later.
:::

## Core Capabilities

| Capability                   | Description                                                  |
| ---------------------------- | ------------------------------------------------------------ |
| Cross-table atomic commit    | All tables within the same flush cycle share one StarRocks transaction label. One unified `POST /api/transaction/commit` is issued for the label (no prepare call — StarRocks does not support prepare for multi-table transactions). |
| Source transaction integrity | Commit timing is controlled by the `transactionEnd` flag. Commit only occurs at complete source transaction boundaries. |
| Sub-second data visibility   | Data is periodically flushed to StarRocks (`/api/transaction/load`). It is committed when the `transactionEnd` and the timer conditions are met |
| N:1 transaction mapping      | Multiple source transactions can accumulate in a single StarRocks transaction. They do not have to be mapped 1:1. |
| Within-partition ordering    | `keyBy(sourcePartition)` ensures transactions from the same partition are processed in order within the same sink subtask. |

## Configurations

### Multi-table Transaction Configurations

#### `sink.transaction.multi-table.enabled`

- Type: Boolean
- Default: `false`
- Description: Whether to enable the Multi-table Atomic Transaction mode.

#### `sink.transaction.multi-table.buffer-size`

- Type: Long
- Default: `134217728` (128 MB)
- Unit: Bytes
- Description: Global buffer size in bytes for the Multi-table Transaction mode. When the total buffered data across all tables reaches this threshold, a flush is triggered.

#### `sink.transaction.multi-table.mini-switch-interval-ms`

- Type: Long
- Default: `-1` (auto)
- Unit: ms
- Description: Minimum interval (ms) between per-partition chunk switches. Within the interval, source transactions are batched into a single stream-load, bounding HTTP request count. `-1` auto-derives `min(1000, max(500, sink.buffer-flush.interval-ms/4))`; a positive value overrides it.

#### `sink.transaction.multi-table.min-switch-bytes`

- Type: Long
- Default: `1048576` (1 MB)
- Unit: Bytes
- Description: An interval-elapsed chunk switch only fires once a region's active chunk has accumulated at least this many bytes, so low-volume partitions batch more source transactions into one stream-load instead of emitting many tiny requests. Half-full headroom, buffer-size memory pressure, or a full `sink.buffer-flush.interval-ms` elapsing since the partition last switched all force a switch regardless — so even with a large threshold a continuous low-volume stream is still switched at roughly the flush interval and never withheld indefinitely. Set `<=0` to disable the size gate and restore the prior per-interval switching behavior. When `mini-switch-interval-ms` is equal to (or larger than) `sink.buffer-flush.interval-ms`, this option has no effect on the txnEnd switch (see Chunk Lifecycle and `miniInterval` Batching).

#### `sink.transaction.multi-table.max-txn-bytes`

- Type: Long
- Default: `0` (unlimited)
- Unit: Bytes
- Description: Hard limit in bytes on the in-progress source-transaction data one sink subtask may buffer while waiting for txnEnd. The writer never blocks on bytes that cannot be flushed before txnEnd (see Limitation 7), so a single source transaction may grow past `2 × buffer-size`; this option bounds that growth and fails the job with a clear error when exceeded. `0` means no limit: the TaskManager heap is the only bound.

### Load-related Configurations

#### `sink.version`

-  Recommended Value: `V2`
-  Description: Required. `V1` does not support the transaction Stream Load interface.

#### `sink.semantic`

-  Recommended Value: `at-least-once`
-  Description: Multi-table mode currently supports `at-least-once` only.

#### `database-name`

-  Recommended Value: `*`
-  Description: Wildcard to enable dynamic multi-table routing.

#### `table-name`

-  Recommended Value: `*`
-  Description: Wildcard to enable dynamic multi-table routing.

#### `sink.buffer-flush.interval-ms`

-  Recommended Value: `1000`
-  Description: Controls the commit cycle. You can set it to `1000` to achieve the freshness of approximately one second.

#### `sink.http.keep-alive.enabled`

-  Recommended Value: `true`
-  Description: Recommended. Loads and BEGIN / PREPARE / COMMIT / ROLLBACK reuse HTTP connections instead of opening a new connection for every request. Off by default (see Best Practices 6).

#### `sink.properties.format`

-  Recommended Value: `json`
-  Description: The data format.

#### `sink.properties.strip_outer_array`

-  Recommended Value: `true`
-  Description: Whether to strip the outermost array structure.

## Interfaces

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
    // Basic fields
    private String uniqueKey;
    private String database;
    private String table;
    private String row;

    // Multi-table transaction fields
    private boolean transactionEnd;       // Source transaction end marker
    private int sourcePartition = -1;     // Source partition ID (for keyBy ordering)

    // Constructors
    public DefaultStarRocksRowData();
    public DefaultStarRocksRowData(String database, String table);
    public DefaultStarRocksRowData(String uniqueKey, String database, String table, String row);

    // Setters
    public void setUniqueKey(String uniqueKey);
    public void setDatabase(String database);
    public void setTable(String table);
    public void setRow(String row);
    public void setTransactionEnd(boolean transactionEnd);
    public void setSourcePartition(int sourcePartition);

    // Getters (inherited from StarRocksRowData)
    public String getUniqueKey();
    public String getDatabase();
    public String getTable();
    public String getRow();
    public boolean isTransactionEnd();
    public int getSourcePartition();
}
```

### User-Implemented Component

Users need to implement a `KeyedProcessFunction` (referred to as `TransactionAssembler` in this document) that:

1. Keys by source partition and buffers data rows within a transaction
2. Emits all rows only when the source transaction is closed (for example, upon receiving `TXN_END`)
3. Sets `transactionEnd=true` on the last row
4. Sets `sourcePartition` on every row

No custom `SinkFunction` is needed — the standard connector API (`SinkFunctionFactory.createSinkFunction()`) handles everything.

## Complete Example

### StarRocks Table DDL

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

### Flink Job Code

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
    // 1. Source Event Model (define per your business logic)
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
    // 2. TransactionAssembler — Core User Component
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
                        // Mark the last row as transaction end
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
    // 3. Main Program
    // =====================================================

    public static void main(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        String jdbcUrl  = params.get("jdbcUrl", "jdbc:mysql://127.0.0.1:9030");
        String loadUrl  = params.get("loadUrl", "127.0.0.1:8030");
        String userName = params.get("userName", "root");
        String password = params.get("password", "");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Aligned checkpoints only. Each checkpoint also drains and commits the
        // open shared transaction (see Best Practices 3).
        env.enableCheckpointing(10_000);

        // --- Source ---
        // Replace with an actual Kafka Source that deserializes into TxnEvent
        DataStream<TxnEvent> events = env.addSource(/* KafkaSource or MockTxnEventSource */);

        // --- Step 1: Assemble complete transactions per partition ---
        // TransactionAssembler emits rows only after TXN_END.
        // Multiple closed source txns accumulate in the sink buffer between flushes.
        DataStream<DefaultStarRocksRowData> rows = events
                .keyBy(e -> e.partition)
                .process(new TransactionAssembler());

        // --- Step 2: Partition-affinity routing to sink ---
        // keyBy(sourcePartition) routes same-partition data to the same sink subtask.
        // The connector uses per-partition regions internally, so even when multiple
        // partitions land on the same sink subtask, transaction boundaries are tracked
        // independently per partition via PartitionCommitTracker.
        DataStream<DefaultStarRocksRowData> partitionedRows = rows
                .keyBy(DefaultStarRocksRowData::getSourcePartition);

        // --- Step 3: Configure the connector ---
        // sink.transaction.multi-table.enabled=true activates per-partition region
        // tracking inside StreamLoadManagerV2: each partition's regions are switched
        // together, independently of other partitions, at its txnEnd boundaries.
        // The timer-driven commit does not wait for every partition: it switches
        // the partitions that are at a clean boundary, and a partition that is
        // mid-transaction contributes only the chunks it switched earlier — its
        // in-progress rows go into the next shared transaction.
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

        // Optional: per-table stream load properties
        StreamLoadTableProperties orderItemsProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("order_items")
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build();
        options.addTableProperties(orderItemsProps);

        // --- Step 4: Create and attach the sink ---
        // Standard connector API — no custom SinkFunction needed.
        // addSink on the keyBy'd stream ensures partition affinity.
        SinkFunction<DefaultStarRocksRowData> sink = SinkFunctionFactory.createSinkFunction(options);
        partitionedRows.addSink(sink);

        env.execute("WriteMultipleTablesWithTransaction");
    }
}
```

### Data Flow Topology

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

## How It Works

### Chunk Lifecycle and `miniInterval` Batching

Each `(partition, table)` region has one `activeChunk` (currently accepting writes) and a FIFO of `inactiveChunks` (frozen data, pending HTTP load). In multi-table transaction mode, the **only** way data moves from `activeChunk` into `inactiveChunks` is a commit switch, taken only at a clean transaction boundary and, except for the savepoint, applied to all regions of a partition together (`lockstepSwitchPartition`). It happens at exactly four sites:

1. **Task thread, on txnEnd** — inside `setCommitAllowed(partition, true)`, which marks every region of the partition clean and then makes one switch decision for the whole partition (rule below). This is the common path.

2. **Manager thread, source-idle force-switch** — `managerForceSwitchCleanBoundaryRegions()` on every scan cycle, for partitions whose regions are **all** at a clean boundary and at least one holds rows. Under pressure (buffered bytes reached `buffer-size`, or a region's `activeChunk` is half-full) the partition is switched at once; otherwise only when a region holds at least `min-switch-bytes` **and** `miniSwitchIntervalMs` has elapsed since the partition's last switch. Idle data below `min-switch-bytes` is left for the commit cut.

3. **Manager thread, commit cut** — at the start of every timer-driven commit and every recycle (see Manager Thread — Time-Driven Commit and Shared Transaction Coordination): switches every clean partition and freezes a commit watermark on every region.

4. **Manager thread, savepoint** — the checkpoint flush force-switches every region after verifying every region is at a clean boundary (see Best Practices 3).

To avoid one HTTP load per source transaction in high-throughput CDC, the txnEnd path switches the partition only when **any** of these holds; otherwise the completed transaction stays in `activeChunk` and is batched with later ones:

- at least `miniSwitchIntervalMs` has elapsed since the partition's last switch **and** at least one of its regions' `activeChunk` holds at least `sink.transaction.multi-table.min-switch-bytes` (default 1 MB; `<= 0` disables this size check);

- a full `commitInterval` has elapsed since the partition's last switch (the freshness upper bound, so the size gate never defers completed data indefinitely);

- a region's `activeChunk` reached half of the write-block cap (that is, `buffer-size` bytes);

- buffered bytes across all regions reached `buffer-size`.

When auto-derived, `miniSwitchIntervalMs` is `min(1000 ms, max(500 ms, commitInterval / 4))`, so a 2-second commit interval batches at 500 ms while a 4-second-or-longer interval caps batching at 1 second (an explicit `sink.transaction.multi-table.mini-switch-interval-ms` overrides this). Within that window multiple completed source transactions accumulate into the same `activeChunk` (N:1 mapping) and are frozen together on the next switch.

The size gate only matters while `miniSwitchIntervalMs` is shorter than `commitInterval`. If `mini-switch-interval-ms` is set equal to (or larger than) `sink.buffer-flush.interval-ms` — or is auto-derived to 500 ms for a commit interval of 500 ms or less — the freshness bound is reached no later than the interval, so every txnEnd after the interval switches regardless of size and `min-switch-bytes` has no effect on the txnEnd path.

Two pieces of state drive these decisions:

- The partition's last switch time (kept per partition by the manager, so all regions of a partition batch together). Initially 0, so the very first txnEnd of a partition always triggers a switch.

- `activeChunkCleanBoundary` (per region) — `true` if the most recent task-thread event on this region was either a txnEnd or a switch. `false` after any `write()`. The manager-thread switches re-check this flag on every region of the partition under the regions' write locks and skip the partition if any region is dirty, so they can never freeze partial source-transaction data.

### Task Thread — Write and txnEnd

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

The task thread is the sole serializer of `write()` and `setCommitAllowed()` events, which is why both the `cleanBoundary` mark and the conditional switch must run here (not deferred to the manager thread): `cleanBoundary` must reflect the most recent task-thread event at all times, or the manager's idle-fallback could race a write and freeze partial data.

### 6.3 Manager Thread — Time-Driven Commit

The manager thread runs a scan loop at `scanningFrequency`. Each iteration:

1. **Ensure shared transaction**: open a new one (eager) or proactively recycle the current one if it is approaching the StarRocks server-side timeout (80% of `timeout` header, default 480 s).

2. **Source-idle fallback**: force-switch any partition whose regions are **all** at a clean boundary once it has accumulated `min-switch-bytes` and `miniSwitchIntervalMs` has elapsed since its last switch, or at once under memory pressure (see Chunk Lifecycle and `miniInterval` Batching). Below the size threshold the idle data is not switched here — the time-driven commit below cuts and commits it instead (see step 3), so a source that pauses still becomes visible within one `commitInterval`. This handles the "source paused after a few txnEnds" case where the task thread stopped before issuing a fresh switch.

3. **Time-driven commit trigger**: call `tryStartTimerDrivenCommit()`, which sets `commitInFlight=true` if **both** conditions hold:

   - `now - lastCommitTimeMs >= commitInterval` (the configured `sink.buffer-flush.interval-ms`).

   - There is data to commit — `txnCoordinator.hasDataLoaded()` is true, at least one region has pending inactiveChunks, or some partition is entirely at a clean boundary with buffered active rows (deferred below the size gate) that the commit cut will freeze. This last check is evaluated at partition scope, so a lone clean region whose sibling table is still mid-transaction does not arm a commit cycle that cannot make progress.

4. **Autonomous flush**: drain any region whose `inactiveChunks` is non-empty via the `FlushAndCommitStrategy`. Multi-table mode's `flush()` only streams out already-frozen inactive chunks — it **never** touches `activeChunk`. This preserves the invariant that every chunk reaching StarRocks under the shared label comes from completed source transactions.

When `commitInFlight=true` the main loop enters `processMultiTableCommit`. Its first step is the **commit cut**, which decides what goes into this label. The cut works per partition and does not wait for every partition to reach a clean boundary. Under the partition's region write locks (so a concurrent txnEnd switch cannot land in between):

- A **clean** partition (every region at a clean boundary, at least one with rows) is switched in lockstep, then every region's commit watermark is set to its newest switched chunk.

- A **dirty** partition (any region mid-transaction) is not switched; its regions' watermarks are set at their last switch. The chunks it switched earlier are committed now, and its `activeChunk` (the in-progress source transaction plus any completed transactions batched with it) goes into the next shared transaction.

If any partition's locks are busy, the cut is retried on the next scan. Only chunks at or below the watermark are loaded under this label; chunks switched after the cut wait for the next one. The commit then waits for in-flight loads, triggers loads for the remaining eligible chunks, runs the unified commit via `SharedTransactionCoordinator`, lifts the watermarks, updates `lastCommitTimeMs`, resets the tracker, and opens a new shared transaction for the next cycle.

### Shared Transaction Coordination

All tables within the same commit cycle share a single StarRocks transaction managed by `SharedTransactionCoordinator`:

1. **Eager transaction opening**: A shared transaction is opened eagerly before any autonomous flush, so all HTTP loads use the shared label. This eliminates the data-loss window where an independent-label flush could be orphaned when a shared transaction later overwrites the label.

2. **Unified commit**: After all regions' data is loaded, a single `POST /api/transaction/commit` is executed for the shared label. Multi-table transactions skip the `prepare` step because StarRocks does not support `TXN_PREPARE` in multi-table mode.

3. **Transaction recycling**: If the shared transaction has been open (measured from its BEGIN, not from its last load) longer than 80% of the BEGIN `timeout` header (default: 480s for the 600s the connector sends), it is proactively recycled (commit-or-rollback + reopen) to prevent server-side timeout errors. The recycle uses the same per-partition commit cut as the timer-driven commit, so a partition that is mid-transaction is simply carried into the next label. Recycle fails fast only if a partition has written data without receiving any txnEnd since the last commit. Recycling is best-effort: it waits while a commit cycle is in flight or any region is still loading or retrying, so a load that never returns (possible with the default `sink.socket.timeout-ms=-1`) can still let the label reach its deadline. Set a finite `sink.socket.timeout-ms` to bound this.

### 6.5 PartitionCommitTracker (Safety Bookkeeping)

In the current design, commit timing is driven entirely by the commit interval and the per-region clean-boundary flags. `PartitionCommitTracker` is reduced to an informational/safety aid:

- `onWrite(partition)` registers a partition as `ACTIVE` on first write.

- `onTxnEnd(partition)` transitions the partition to `TXN_END_SEEN` (sticky — subsequent writes do **not** demote it back to `ACTIVE`).

- `getPartitionsWithoutTxnEnd()` lists partitions that have written data but never received a txnEnd. Used by savepoint and recycle to fail fast on upstream contract violations (a source transaction that never closed).

- `reset()` clears all partitions at the end of a commit cycle.

The tracker no longer drives switch/commit decisions, does not track a `SWITCHED` state, and does not manage pending txnEnd signals. Partitions that stop producing data after a commit are simply cleared by `reset()`; if they resume later, the next `onWrite` re-registers them.

### Autonomous Flush with Shared Labels

When a region's `inactiveChunks` becomes non-empty, the manager thread's autonomous-flush loop streams it to StarRocks via `/api/transaction/load` under the **current shared label**. Because the shared transaction is always opened before any data is loaded, there is no window where data could be loaded under an independent (orphaned) label. In multi-table mode, `flush()` never triggers a `switchChunk` — it only drains already-frozen inactive chunks — so the invariant "every chunk reaching StarRocks under the shared label comes from completed source transactions" holds unconditionally.

### Safety Guarantees

| Guarantee                                         | Mechanism                                                                     |
| ------------------------------------------------- | ----------------------------------------------------------------------------- |
| switchChunk does not split source transactions    | `switchChunkForCommit` is only called at a clean transaction boundary (on txnEnd or when `activeChunkCleanBoundary` is `true`). |
| Commit never includes partial source transactions | Every chunk reaching StarRocks originates from `switchChunkForCommit`. Autonomous flush never switches `activeChunk` in multi-table mode. |
| Per-partition isolation                           | Each `(partition, table)` has its own region. One partition's switch never affects another's data. |
| Within-partition ordering                         | `keyBy(sourcePartition)` routes same-partition rows to the same sink subtask. |
| No HTTP I/O on the txnEnd path                    | The partition switch decision in `setCommitAllowed` is O(regions-in-partition), and all HTTP work happens asynchronously on the manager thread. This is not a non-blocking guarantee: the task thread can wait for a region write lock held by a manager-thread switch, re-split an oversized frozen chunk row by row when its own switch freezes one (see Limitation 7), park in `blockIfCacheFull`, and block during a checkpoint (see Best Practices 3). |
| Source-idle data visibility                       | If the source pauses below `min-switch-bytes`, the time-driven commit's partition-scope clean-boundary detection cuts and commits the idle `activeChunk`, so data becomes visible within about one `commitInterval` even if the source never resumes. |
| Autonomous flushes are transaction-safe           | Every load uses the shared label. Every frozen chunk is from completed source transactions. |
| Shared transactions are recycled before their deadline (best effort) | Shared transactions are recycled at 80% of the BEGIN `timeout`. A mid-transaction partition is carried into the next label, and recycle fails fast only on a partition that wrote data without any txnEnd. Recycling waits while a commit cycle or a load is in flight, so a load that never returns (default `sink.socket.timeout-ms=-1`) can let the label pass its deadline; a finite socket timeout bounds this. |
| Per-partition independent commit                  | A partition with a completed source transaction commits on the next commit interval, independent of other partitions' in-progress transactions. |

### N:1 Transaction Mapping

Multiple source transactions can accumulate in a single StarRocks transaction via the miniInterval batching mechanism:

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

Because the commit decision is time-driven (not tied to a specific txnEnd), the connector amortizes HTTP-load and begin/commit overhead across many small source transactions without any configuration changes. For a CDC source emitting 100 txnEnds per second with `commitInterval=1 s` (so the auto-derived `miniInterval=500 ms`), the connector issues at most ~2 load calls per second instead of ~100; with the default `min-switch-bytes` size gate a low-volume stream batches even further, down to about one load per `commitInterval` (the freshness bound at which a below-threshold chunk is still switched).

## Limitations

1. **Requires `sink.version=V2`**: V1 does not support transaction stream load.

2. **At-least-once only**: Failed retries may produce duplicate writes. Multi-table mode guarantees all tables within the same batch succeed or fail together, but does not provide global exactly-once. For PRIMARY KEY tables, duplicate writes are idempotent (upsert).

3. **All tables must be in the same database**: StarRocks multi-table transactions are database-scoped; cross-database transactions are not supported.

4. **Transaction scope is per sink subtask + per partition**: Each sink subtask maintains its own StarRocks transaction independently. Atomicity is guaranteed **within a single source transaction** (all rows for one txnEnd on one partition, across all tables that partition writes to). Data visibility across **different** source partitions can interleave: once partition P0's source transaction has fully arrived and the commit interval has elapsed, P0's data is committed even if partition P1's source transaction is still in progress. Applications that require cross-partition atomicity at the StarRocks level must either use a single source partition or coordinate commits upstream. In short, the source-transaction guarantee holds per lane only, where a lane is one `sourcePartition` on one sink subtask. Everything committed under one label, including rows from several lanes, becomes visible atomically, but nothing controls which source transactions of different lanes end up in the same label: a related transaction on another lane can land in an earlier or later label, and lanes on different sink subtasks never share a label.

5. **Data visibility latency**: Governed by `sink.buffer-flush.interval-ms` (the commit interval). A completed source transaction is first frozen out of the `activeChunk` — either as soon as the partition reaches `sink.transaction.multi-table.min-switch-bytes` (default 1 MB), or, for a low-volume partition below that threshold, at most one `commitInterval` after its last switch (the freshness bound enforced on the task thread at txnEnd) — and is then published by the next time-driven commit. So in a continuously flowing CDC stream an individual row becomes visible within roughly one `commitInterval` when the partition is busy enough to reach the size gate, and within up to two `commitInterval` windows in the low-volume worst case (one to freeze the below-threshold chunk, one to commit it). During a source pause, previously-committed data remains visible and the last idle sub-threshold chunk is committed within one more `commitInterval` via the time-driven commit's clean-boundary detection. Set `min-switch-bytes <= 0` to switch on every interval (lowest latency, more/smaller loads).

6. **Depends on StarRocks cluster transaction settings**: Monitor running txn limits, the transaction timeout, and label retention. The transaction timeout is the `timeout` header the connector sends on BEGIN — 600 s unless `sink.properties.timeout` overrides it — and StarRocks treats it as an absolute deadline measured from BEGIN, not an idle timeout: loads into the label do not extend it. The connector recycles the shared transaction at 80% of it (480 s by default). Ensure `sink.buffer-flush.interval-ms` is significantly shorter than this timeout.

7. **activeChunk memory growth under large source transactions**: Because multi-table mode disables chunk-size-triggered internal switching (to preserve the clean-transaction-boundary invariant), `activeChunk` can grow until the next txnEnd arrives. `sink.transaction.multi-table.buffer-size` (soft) and `2 × buffer-size` (hard, via `blockIfCacheFull`) only bound the *flushable* part of the buffer — frozen chunks and all-clean partitions. Bytes that cannot leave the buffer before a txnEnd (the in-progress transaction itself, plus completed rows of sibling tables in the same partition that the lockstep rule holds back) are never waited on: once the buffer is at the hard cap and nothing is flushable, the writer logs a WARN (`Write-block cap ... reached with nothing flushable`) and keeps buffering until txnEnd, after which the partition is switched and drained. So a single source transaction may exceed `2 × buffer-size`; its size is bounded only by `sink.transaction.multi-table.max-txn-bytes` (default unlimited) and the TaskManager heap. Size the heap for the largest expected source transaction times the number of sink subtasks per TaskManager, set `max-txn-bytes` if you prefer a clear failure over heap pressure, and split unbounded transactions (bulk bootstraps) upstream. At switch time a frozen chunk larger than the load body limit — the smaller of `sink.chunk-limit` (3 GB by default) and `sink.transaction.multi-table.buffer-size` — is re-cut into several load requests under the same shared label, so no single `/api/transaction/load` body exceeds that limit, except for a single row that is itself larger than it.

8. **Cross-database writes are rejected**: Multi-table transactions validate that all regions belong to the same database. Writing to tables in different databases within the same commit cycle will throw an error.

9. **Incompatible with merge commit**: `sink.properties.enable_merge_commit=true` cannot be combined with `sink.transaction.multi-table.enabled=true`. Merge commit routes writes through `MergeCommitManager`, which lacks the partition-aware `write(int, ...)` / `setCommitAllowed(int, ...)` hooks that multi-table mode relies on for transaction boundaries. The connector fails fast at validation time if both are enabled.

10. **Exactly one upstream producer per `sourcePartition`**: Clean boundaries are positional — a partition is at a clean boundary iff the last row the sink subtask received for it carried txnEnd; the connector does not track which source transaction a row belongs to. If two producers (two assembler subtasks, or two sources whose partition numbers collide, such as partition 0 of two Kafka topics) emit rows with the same `sourcePartition`, their rows interleave at the sink, one producer's txnEnd marks the partition clean while the other's transaction is still in progress, and the next switch can commit that transaction partially. Key the TransactionAssembler by the same value the sink keys by, and make `sourcePartition` unique across all sources.

11. **Aligned checkpoints only**: The checkpoint flush requires every region to be at a clean boundary when the barrier reaches the sink (see Best Practices 3). Aligned checkpoints guarantee this, because the assembler emits a whole source transaction within one `processElement()` call and the barrier cannot fall between its rows. Unaligned checkpoints (`execution.checkpointing.unaligned.enabled`; `execution.checkpointing.unaligned` before Flink 1.20) let the barrier overtake in-flight rows and reach the sink mid-transaction. This includes the aligned-then-unaligned mode, where unaligned checkpoints are enabled together with `execution.checkpointing.aligned-checkpoint-timeout` and a checkpoint switches to unaligned once the timeout expires; the timeout has no effect while unaligned checkpoints are disabled. The checkpoint then fails, the open label is rolled back and the job fails over.

12. **Data-quality rejections abort the whole label**: For a multi-table transaction, StarRocks applies strict mode and `max_filter_ratio` at commit, not at each `/api/transaction/load`. A rejection fails the commit and aborts the whole label — every table's rows in that commit cycle, not only the table with the bad rows. The connector reports a commit failure and the job fails over; the same rows are replayed, so it keeps failing until the rows or the load settings are fixed.

13. **StarRocks v4.1.6 and earlier can report a failed multi-table commit as OK**: On these versions a multi-table commit that failed on the server can be returned to the client as `OK`. The connector then treats the label as committed, so the rows of that commit cycle can be silently missing. Fixed by StarRocks PR [#80351](https://github.com/StarRocks/starrocks/pull/80351) (CelerData backport: commit `1ceb0763cf`).

## Monitoring and Troubleshooting

Recommended metrics:

- **On the Flink Side**
  - Checkpoint success rate
  - Checkpoint duration
  - Sink flush/commit latency
- **On the StarRocks Side**
  - Running/prepared txn count
  - Txn timeout occurrences
  - Label conflicts
- **On the Connector Side**
  - The `sharedTxnLatencyMs` histogram: the time from when the earliest row of a shared transaction entered the sink buffer until the transaction committed. Each commit also logs it per label: `[MultiTxn] SharedTransaction committed successfully: label=<label>, firstRowTimeMs=<epoch ms>, latencyMs=<ms>` (both `-1` when unknown).

### Common issues

#### `transaction not existed`

- Cause: StarRocks transaction timeout
- Solution: The connector automatically recycles the shared transaction at 80% of the BEGIN `timeout` (measured from BEGIN). If this still occurs, check whether `sink.properties.timeout` is too short, the flush interval is too large, or a load hung past the deadline (set a finite `sink.socket.timeout-ms`).

#### `too many running txns`

- Cause: There are excessive concurrent transactions.
- Solution: Reduce sink parallelism or increase the value of StarRocks FE configuration `max_running_txn_num_per_db`.

#### `Transaction start failed`

- Cause: The `beginTransaction` HTTP call failed
- Solution: Verify load-url connectivity and the StarRocks version (v4.0 or later is required).

#### High data visibility latency

- Cause: Commit conditions are not met.
- Solution: Verify upstream data has correct `transactionEnd=true` markers; for a low-volume partition below `min-switch-bytes`, expect up to ~2×`commitInterval` per row (one interval to freeze the below-threshold chunk, one to commit it). If latency exceeds this budget, check the manager thread is not stuck in a recycle or in-flight load (see `StarRocks-Sink-Manager` logs), or lower `min-switch-bytes`.

#### `Write-block cap ... reached with nothing flushable` (WARN)

- Cause: A source transaction, plus completed rows of sibling tables held back until its txnEnd, exceeds `2 × buffer-size`; the writer keeps buffering until txnEnd instead of blocking.
- Solution: Expected under a large source transaction; the data is committed once its txnEnd arrives. Watch TaskManager heap. Set `sink.transaction.multi-table.max-txn-bytes` if you prefer a hard failure, or split the transaction upstream.

#### `would exceed sink.transaction.multi-table.max-txn-bytes`

- Cause: A source transaction is larger than the configured hard limit.
- Solution: Raise `max-txn-bytes` (or set it to `0` for unlimited) or split the source transaction upstream.

#### Cross-database write error

- Cause: Tables in different databases are in same commit cycle.
- Solution: Ensure all tables written in the same job belong to the same StarRocks database.

## Best Practices

1. **TransactionAssembler contract**:

   - Emit rows only after the source transaction is fully closed.
   - The last row must have `setTransactionEnd(true)`.
   - Every row must have `setSourcePartition(partition)`.
   - All rows must be emitted synchronously within a single `processElement()` call.

2. **keyBy before sink is mandatory**: `rows.keyBy(DefaultStarRocksRowData::getSourcePartition).addSink(sink)` — omitting this breaks within-partition transaction ordering. Each `sourcePartition` value must also come from exactly one upstream producer (see Limitation 10).

3. **Checkpoints also commit**: Data visibility is governed by `sink.buffer-flush.interval-ms` (for example, 1000ms), but checkpoints are not decoupled from commit. In at-least-once mode `snapshotState` calls `flush()`, which blocks the task thread until the whole buffer — including completed transactions still batched in `activeChunk` — is loaded and the open shared label is committed. The flush has a budget of 1.1 × the transaction timeout (660 s by default), but it is checked only between waits and is not a hard cap: the drain waits for every in-flight `/api/transaction/load` without a deadline, and with the default `sink.socket.timeout-ms=-1` a load whose response never arrives can keep the checkpoint, and the task thread, blocked indefinitely. Set a finite `sink.socket.timeout-ms`, longer than the slowest expected load, so that a hung load fails the job instead. If the barrier arrives while any partition is mid-transaction, the flush throws, the open label is rolled back and the job fails over (see Limitations 10 and 11). A larger checkpoint interval (for example, 60 seconds) means fewer of these forced commits.

4. **Keep routing strategies stable**: Avoid single transactions writing to an excessive number of distinct tables, which increases transaction duration and failure probability.

5. **Fault injection testing before production**: Kill TaskManagers / introduce network jitter and verify data correctness after checkpoint recovery.

6. **Enable HTTP keep-alive**: Set `sink.http.keep-alive.enabled=true`. A shared transaction sends a BEGIN, many loads and a COMMIT, and without keep-alive each of them opens a new TCP (and TLS) connection. A connection that fails during any of these requests fails the whole shared transaction, which rolls back every table in it and fails the job over, so fewer new connections means fewer such failures, for example behind a load balancer that can reset new connections. With keep-alive on, a request on a reused connection that the server or a load balancer closed is sent again on a new connection where that cannot write data twice. Keep `sink.http.keep-alive.idle-timeout-ms` (default 60000) shorter than the idle timeout of every proxy or load balancer between Flink and StarRocks; an AWS Network Load Balancer drops connections idle for 350 seconds by default.
