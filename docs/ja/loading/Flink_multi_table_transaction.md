---
displayed_sidebar: docs
description: "Flinkジョブのマルチテーブルトランザクションを有効にして、1回の処理サイクルで同じデータベース内の複数のテーブルに書き込みます。"
---

# Apache Flink®からマルチテーブルトランザクションでデータをロードする

StarRocks Flink Connectorはマルチテーブルトランザクションをサポートしており、FlinkからStarRocksの複数のテーブルにアトミックにデータをロードできます。

## ユースケース

単一のFlinkジョブが1つの処理サイクルで同じStarRocksデータベース内の複数のテーブルに書き込む場合、マルチテーブルトランザクションを有効にすることで以下が保証されます：

- **クロステーブルアトミックコミット**：同じコミットサイクル内で異なるテーブルに書き込まれたデータはアトミックに可視化されます — すべて成功するか、すべて失敗するかのどちらかです。
- **ソーストランザクションの整合性**：完全なアップストリームトランザクション（例：Kafkaからのもの）が2つのStarRocksトランザクションに分割されることはありません。
- **サブ秒のデータ鮮度**：データは`/api/transaction/load`を介してStarRocksに継続的に流れ込み、`sink.buffer-flush.interval-ms`で設定された間隔でコミットされます。

典型的なシナリオ：

- 一般テーブルと詳細テーブルへの同期書き込み（例：`orders`と`order_items`）
- 異なるパーティションテーブルへのイベントルーティング（例：`events_202601`、`events_202602`）
- 複数の相互関連するダウンストリーム結果テーブルを管理する単一ジョブ

:::tip[前提条件]
マルチテーブルトランザクションを有効にするには、StarRocks v4.0（マルチテーブルトランザクションStream Loadサポートを含む）でクラスターを実行し、StarRocks Flink Connectorをv1.2.9以降で使用する必要があります。
:::

## 主要機能

| 機能                         | 説明                                                         |
| ---------------------------- | ------------------------------------------------------------ |
| クロステーブルアトミックコミット | 同じフラッシュサイクル内のすべてのテーブルは1つのStarRocksトランザクションラベルを共有します。PrepareおよびCommit操作は統一されています。 |
| ソーストランザクションの整合性 | コミットタイミングは`transactionEnd`フラグによって制御されます。コミットは完全なソーストランザクション境界でのみ発生します。 |
| サブ秒のデータ可視性         | データは定期的にStarRocksにフラッシュされます（`/api/transaction/load`）。`transactionEnd`とタイマー条件が満たされたときにコミットされます。 |
| N:1トランザクションマッピング | 複数のソーストランザクションを単一のStarRocksトランザクションに蓄積できます。1:1でマッピングする必要はありません。 |
| パーティション内の順序保証   | `keyBy(sourcePartition)`は、同じパーティションからのトランザクションが同じシンクサブタスク内で順番に処理されることを保証します。 |

## 設定

### マルチテーブルトランザクションの設定

#### `sink.transaction.multi-table.enabled`

- 型：Boolean
- デフォルト：`false`
- 説明：マルチテーブルアトミックトランザクションモードを有効にするかどうか。

#### `sink.transaction.multi-table.buffer-size`

- 型：Long
- デフォルト：`134217728`（128 MB）
- 単位：バイト
- 説明：マルチテーブルトランザクションモードのグローバルバッファサイズ（バイト単位）。すべてのテーブルにわたるバッファリングされたデータの合計がこのしきい値に達すると、フラッシュがトリガーされます。

### ロード関連の設定

#### `sink.version`

- 推奨値：`V2`
- 説明：必須。`V1`はトランザクションStream Loadインターフェースをサポートしていません。

#### `sink.semantic`

- 推奨値：`at-least-once`
- 説明：マルチテーブルモードは現在`at-least-once`のみをサポートしています。

#### `database-name`

- 推奨値：`*`
- 説明：動的マルチテーブルルーティングを有効にするためのワイルドカード。

#### `table-name`

- 推奨値：`*`
- 説明：動的マルチテーブルルーティングを有効にするためのワイルドカード。

#### `sink.buffer-flush.interval-ms`

- 推奨値：`1000`
- 説明：コミットサイクルを制御します。約1秒の鮮度を実現するために`1000`に設定できます。

#### `sink.properties.format`

- 推奨値：`json`
- 説明：データフォーマット。

#### `sink.properties.strip_outer_array`

- 推奨値：`true`
- 説明: 最外部の配列構造を取り除くかどうか。

## インターフェース

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
    // 基本フィールド
    private String uniqueKey;
    private String database;
    private String table;
    private String row;

    // マルチテーブルトランザクションフィールド
    private boolean transactionEnd;       // Source transaction end marker
    private int sourcePartition = -1;     // Source partition ID (for keyBy ordering)

    // コンストラクタ
    public DefaultStarRocksRowData();
    public DefaultStarRocksRowData(String database, String table);
    public DefaultStarRocksRowData(String uniqueKey, String database, String table, String row);

    // セッター
    public void setUniqueKey(String uniqueKey);
    public void setDatabase(String database);
    public void setTable(String table);
    public void setRow(String row);
    public void setTransactionEnd(boolean transactionEnd);
    public void setSourcePartition(int sourcePartition);

    // ゲッター（StarRocksRowDataから継承）
    public String getUniqueKey();
    public String getDatabase();
    public String getTable();
    public String getRow();
    public boolean isTransactionEnd();
    public int getSourcePartition();
}
```

### ユーザー実装コンポーネント

ユーザーは `KeyedProcessFunction`（本ドキュメントでは `TransactionAssembler` と呼ぶ）を実装する必要があります。これは以下の条件を満たすものです:

1. ソースパーティションをキーとし、トランザクション内のデータ行をバッファリングする
2. ソーストランザクションがクローズされたとき（例: `TXN_END` を受信したとき）にのみ、すべての行を出力する
3. 最後の行に `transactionEnd=true` を設定する
4. すべての行に `sourcePartition` を設定する

カスタム `SinkFunction` は不要です — 標準コネクタ API（`SinkFunctionFactory.createSinkFunction()`）がすべてを処理します。

## 完全な例

### StarRocks テーブル DDL

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

### Flink ジョブコード

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
    // 1. ソースイベントモデル（ビジネスロジックに応じて定義）
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
    // 2. TransactionAssembler — コアユーザーコンポーネント
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
                        // 最後の行をトランザクション終了としてマーク
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
    // 3. メインプログラム
    // =====================================================

    public static void main(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        String jdbcUrl  = params.get("jdbcUrl", "jdbc:mysql://127.0.0.1:9030");
        String loadUrl  = params.get("loadUrl", "127.0.0.1:8030");
        String userName = params.get("userName", "root");
        String password = params.get("password", "");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);  // Checkpoint is for recovery only; does not affect commit cycle

        // --- ソース ---
        // TxnEventにデシリアライズする実際のKafkaソースに置き換えてください
        DataStream<TxnEvent> events = env.addSource(/* KafkaSource or MockTxnEventSource */);

        // --- ステップ1：パーティションごとに完全なトランザクションを組み立てる ---
        // TransactionAssemblerはTXN_END後にのみ行を出力します。
        // 複数のクローズされたソーストランザクションがフラッシュ間にシンクバッファに蓄積されます。
        DataStream<DefaultStarRocksRowData> rows = events
                .keyBy(e -> e.partition)
                .process(new TransactionAssembler());

        // --- ステップ2：シンクへのパーティションアフィニティルーティング ---
        // keyBy(sourcePartition)は同じパーティションのデータを同じシンクサブタスクにルーティングします。
        // コネクタは内部でパーティションごとのリージョンを使用するため、複数の
        // パーティションが同じシンクサブタスクに配置された場合でも、トランザクション境界は
        // PartitionCommitTrackerを介してパーティションごとに独立して追跡されます。
        DataStream<DefaultStarRocksRowData> partitionedRows = rows
                .keyBy(DefaultStarRocksRowData::getSourcePartition);

        // --- ステップ3: コネクタを設定する ---
        // sink.transaction.multi-table.enabled=true はパーティションごとのリージョンを有効にします
        // StreamLoadManagerV2 内でのトラッキング: 各パーティションのリージョンは切り替えられます
        // txnEnd が到着したときに独立して切り替わり、すべての
        // アクティブなパーティションが切り替えられたときにのみコミットがトリガーされます。
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

        // オプション: テーブルごとのストリームロードプロパティ
        StreamLoadTableProperties orderItemsProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("order_items")
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build();
        options.addTableProperties(orderItemsProps);

        // --- ステップ4: シンクを作成してアタッチする ---
        // 標準コネクタ API — カスタム SinkFunction は不要です。
        // keyBy されたストリームに addSink を使用することで、パーティションアフィニティが確保されます。
        SinkFunction<DefaultStarRocksRowData> sink = SinkFunctionFactory.createSinkFunction(options);
        partitionedRows.addSink(sink);

        env.execute("WriteMultipleTablesWithTransaction");
    }
}
```

### データフロートポロジー

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

## 動作の仕組み

### チャンクのライフサイクルと `miniInterval` バッチ処理

各 `(partition, table)` リージョンには、1 つの `activeChunk`（現在書き込みを受け付けている）と、`inactiveChunks`（凍結済みデータ、HTTP ロード待ち）の FIFO キューがあります。マルチテーブルトランザクションモードでは、**のみ** `activeChunk` から `inactiveChunks` へデータを移動する方法は `switchChunkForCommit` を介して行われ、これは正確に 3 か所から呼び出されます:

1. **タスクスレッド、txnEnd 時** — `setCommitAllowed(partition, true)` 内の `region.tryMiniIntervalSwitch()` を介して。これが一般的なパスです。

2. **マネージャースレッド、ソースアイドルフォールバック** — すべてのスキャンサイクルで `region.tryForceCleanSwitch()` を介して、`activeChunk` がクリーンなトランザクション境界でアイドル状態になっているリージョンに対して実行されます。

3. **マネージャースレッド、セーブポイント/リサイクル** — すべてのリージョンがクリーンな境界にあることを確認した後、すべてのリージョンを強制切り替えします。

高スループット CDC において 1 ソーストランザクションごとに 1 回の HTTP ロードが発生するのを避けるため、タスクスレッドは同一リージョンで前回の切り替えから少なくとも `miniSwitchIntervalMs` が経過した場合にのみ切り替えを実行します。`miniSwitchIntervalMs` は `min(1000 ms, max(100 ms, commitInterval / 10))` として計算されるため、コミット間隔が 1 秒の場合は 100 ms でバッチ処理され、30 秒の場合は最大 1 秒でバッチ処理が上限となります。miniInterval ウィンドウ内では、複数の完了済みソーストランザクションが同じ `activeChunk` に蓄積され（N:1 マッピング）、次の切り替え時にまとめて凍結されます。

各リージョンには、これらの判断を駆動する 2 つのフィールドがあります:

- `lastSwitchTimeMs` — 最後の切り替えのエポック ms。初期値は 0 であるため、リージョン作成後の最初の txnEnd は常に切り替えをトリガーします。

- `activeChunkCleanBoundary` — このリージョンに対する最新のタスクスレッドイベントが `onTxnEnd` または `switchChunk` のいずれかであった場合は `true`。`write()` の後は `false`。マネージャースレッドの `tryForceCleanSwitch` はこのフラグが `true` のときにのみ実行されるため、ソーストランザクションの部分的なデータを凍結することはありません。

### タスクスレッド — 書き込みと txnEnd

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

タスクスレッドは `write()` および `setCommitAllowed()` イベントの唯一のシリアライザーであるため、`cleanBoundary` マークと条件付き切り替えの両方をここで実行する必要があります（マネージャースレッドに委譲してはなりません）: `cleanBoundary` は常に最新のタスクスレッドイベントを反映していなければならず、そうでなければマネージャーのアイドルフォールバックが書き込みと競合して部分的なデータを凍結する可能性があります。

### 6.3 マネージャースレッド — 時間駆動コミット

マネージャースレッドは `scanningFrequency` でスキャンループを実行します。各イテレーションでは:

1. **共有トランザクションの確保**: 新しいものを開く（先行）か、StarRocks サーバー側タイムアウト（`timeout` ヘッダーの 80%、デフォルト 480 秒）に近づいている場合は現在のものを積極的にリサイクルします。

2. **ソースアイドルフォールバック**: すべてのリージョンに対して `region.tryForceCleanSwitch()` を呼び出し、クリーンかつ少なくとも `miniInterval` の間アイドル状態にある `activeChunk` を凍結します。これは、タスクスレッドが新しい切り替えを発行する前に停止した「数回の txnEnd 後にソースが一時停止した」ケースを処理します。

3. **時間駆動コミットトリガー**: `tryStartTimerDrivenCommit()` を呼び出し、**両方** 条件が成立する場合に `commitInFlight=true` を設定します:

   - `now - lastCommitTimeMs >= commitInterval`（設定された`sink.buffer-flush.interval-ms`）。

   - コミットするデータが存在します — `txnCoordinator.hasDataLoaded()`がtrueであるか、少なくとも1つのリージョンにまだ保留中のinactiveChunksがあります。

4. **自律フラッシュ**：`inactiveChunks`が空でないリージョンを`FlushAndCommitStrategy`経由でドレインします。マルチテーブルモードの`flush()`は、既にフリーズされた非アクティブチャンクのみをストリームアウトします — それは**決して**`activeChunk`に触れます。これにより、共有ラベルの下でStarRocksに到達するすべてのチャンクが完了したソーストランザクションから来るという不変条件が保持されます。

`commitInFlight=true`のとき、メインループは`processMultiTableCommit`に入り、インフライトのロードを待機し、残りの非アクティブチャンクのロードをトリガーし、`SharedTransactionCoordinator`を介して統合コミットを実行し、`lastCommitTimeMs`を更新し、トラッカーをリセットして、次のサイクルのために新しい共有トランザクションを開きます。

### 共有トランザクションの調整

同じコミットサイクル内のすべてのテーブルは、`SharedTransactionCoordinator`によって管理される単一のStarRocksトランザクションを共有します：

1. **積極的なトランザクション開始**：共有トランザクションは、自律フラッシュの前に積極的に開かれるため、すべてのHTTPロードは共有ラベルを使用します。これにより、独立ラベルのフラッシュが後で共有トランザクションがラベルを上書きする際に孤立する可能性があるデータ損失ウィンドウが排除されます。

2. **統合コミット**：すべてのリージョンのデータがロードされた後、共有ラベルに対して単一の`commit`が実行されます。マルチテーブルトランザクションは、StarRocksがマルチテーブルモードで`TXN_PREPARE`をサポートしていないため、`prepare`ステップをスキップします。

3. **アイドルトランザクションのリサイクル**：共有トランザクションがStarRocksサーバー側タイムアウトの80%（デフォルト：600sタイムアウトに対して480s）を超えて開いたままの場合、サーバー側タイムアウトエラーを防ぐために積極的にリサイクル（コミットまたはロールバック＋再オープン）されます。いずれかのリージョンに進行中のトランザクションデータがある場合（クリーン境界違反）、またはいずれかのパーティションがtxnEndを受け取ることなくデータを書き込んだ場合、リサイクルは即座に失敗します。

### 6.5 PartitionCommitTracker（安全性の記録管理）

現在の設計では、コミットのタイミングはコミット間隔とリージョンごとのクリーン境界フラグによって完全に制御されます。`PartitionCommitTracker`は情報提供/安全補助に縮小されています：

- `onWrite(partition)`は最初の書き込み時にパーティションを`ACTIVE`として登録します。

- `onTxnEnd(partition)`はパーティションを`TXN_END_SEEN`に遷移させます（スティッキー — 後続の書き込みは**しない**`ACTIVE`に降格しません）。

- `getPartitionsWithoutTxnEnd()`は、データを書き込んだがtxnEndを受け取ったことがないパーティションを一覧表示します。セーブポイントおよびリサイクルが、上流のコントラクト違反（クローズされなかったソーストランザクション）で即座に失敗するために使用されます。

- `reset()`はコミットサイクルの終了時にすべてのパーティションをクリアします。

トラッカーはスイッチ/コミットの決定を駆動しなくなり、`SWITCHED`状態を追跡せず、保留中のtxnEndシグナルを管理しません。コミット後にデータの生成を停止したパーティションは`reset()`によって単純にクリアされます。後で再開した場合、次の`onWrite`が再登録します。

### 共有ラベルを使用した自律フラッシュ

リージョンの`inactiveChunks`が空でなくなると、マネージャースレッドの自律フラッシュループは、`/api/transaction/load`を介して**現在の共有ラベル**の下でStarRocksにストリームします。共有トランザクションは常にデータがロードされる前に開かれるため、データが独立した（孤立した）ラベルの下でロードされる可能性のあるウィンドウは存在しません。マルチテーブルモードでは、`flush()`は`switchChunk`をトリガーしません — 既にフリーズされた非アクティブチャンクのみをドレインします — そのため、「共有ラベルの下でStarRocksに到達するすべてのチャンクは完了したソーストランザクションから来る」という不変条件は無条件に成立します。

### 安全性の保証

| 保証                                               | メカニズム                                                                     |
| ------------------------------------------------- | ----------------------------------------------------------------------------- |
| switchChunkはソーストランザクションを分割しない    | `switchChunkForCommit`はクリーントランザクション境界（txnEnd時、または`activeChunkCleanBoundary`が`true`の場合）でのみ呼び出されます。 |
| コミットには部分的なソーストランザクションが含まれない | StarRocksに到達するすべてのチャンクは`switchChunkForCommit`から発生します。自律フラッシュはマルチテーブルモードで`activeChunk`を切り替えません。 |
| パーティションごとの分離                           | 各`(partition, table)`は独自のリージョンを持ちます。あるパーティションのスイッチは別のパーティションのデータに影響しません。 |
| パーティション内の順序付け                         | `keyBy(sourcePartition)`は同じパーティションの行を同じシンクサブタスクにルーティングします。 |
| タスクスレッドはノンブロッキング                   | `tryMiniIntervalSwitch`はO(パーティション内のリージョン数)です。HTTPの作業はマネージャースレッドで非同期に行われます。 |
| ソースアイドル時のデータ可視性                     | マネージャースレッドの`tryForceCleanSwitch`は、`miniInterval`のアイドル後にクリーンな`activeChunk`をフリーズするため、ソースが一時停止しても`commitInterval + miniInterval`内でデータが可視のままになります。 |
| 自律フラッシュはトランザクションセーフ             | すべてのロードは共有ラベルを使用します。すべてのフリーズされたチャンクは完了したソーストランザクションからのものです。 |
| アイドルトランザクションはタイムアウトしない       | 共有トランザクションはサーバータイムアウトの80%でリサイクルされます。リサイクルは進行中のデータで即座に失敗します。 |
| パーティションごとの独立したコミット              | 完了したソーストランザクションを持つパーティションは、他のパーティションの進行中のトランザクションとは独立して、次のコミット間隔でコミットされます。 |

### N:1 トランザクションマッピング

複数のソーストランザクションは、miniIntervalバッチングメカニズムを介して単一のStarRocksトランザクションに蓄積できます：

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

コミットの決定は時間駆動（特定のtxnEndに紐付けられていない）であるため、コネクターは設定変更なしに多くの小さなソーストランザクション全体でHTTPロードおよびbegin/commitのオーバーヘッドを分散します。`commitInterval=1 s`、`miniInterval=100 ms`で毎秒100件のtxnEndを発行するCDCソースの場合、コネクターは毎秒約100回ではなく最大約10回のロード呼び出しを発行します。

## 制限事項

- **`sink.version=V2`が必要**：V1はトランザクションストリームロードをサポートしていません。

- **少なくとも1回のみ**：失敗した再試行により重複書き込みが発生する可能性があります。マルチテーブルモードは同じバッチ内のすべてのテーブルが一緒に成功または失敗することを保証しますが、グローバルな正確に1回の保証は提供しません。PRIMARY KEYテーブルの場合、重複書き込みはべき等（upsert）です。

- **すべてのテーブルは同じデータベースに存在する必要があります**：StarRocksのマルチテーブルトランザクションはデータベーススコープです。クロスデータベーストランザクションはサポートされていません。

- **トランザクションスコープはシンクサブタスクごと＋パーティションごと**：各シンクサブタスクは、独自のStarRocksトランザクションを独立して維持します。アトミック性は保証されています**単一のソーストランザクション内**（1つのパーティション上の1つのtxnEndに対するすべての行、そのパーティションが書き込むすべてのテーブルにわたって）。**異なる**ソースパーティション間のデータ可視性はインターリーブする可能性があります：パーティションP0のソーストランザクションが完全に到着し、コミット間隔が経過すると、パーティションP1のソーストランザクションがまだ進行中であっても、P0のデータはコミットされます。StarRocksレベルでクロスパーティションのアトミック性を必要とするアプリケーションは、単一のソースパーティションを使用するか、上流でコミットを調整する必要があります。

- **データ可視性レイテンシ**：`sink.buffer-flush.interval-ms` および内部の `miniInterval = min(1000, max(100, commitInterval/10))` によって制御されます。継続的に流れるCDCストリームでは、個々の行はソースコミットからおよそ `commitInterval + miniInterval` 以内にStarRocksで可視になります。ソースが一時停止している間、以前にコミットされたデータは引き続き可視であり、最後のスイッチと一時停止の間の行は、さらに1回の `miniInterval` の後に可視になります（マネージャースレッドのクリーンバウンダリフォールバックによって処理されます）。

- **StarRocksクラスターのトランザクション設定に依存**：実行中のトランザクション制限、準備済みタイムアウト（デフォルト600秒）、およびラベルの保持を監視してください。`sink.buffer-flush.interval-ms` がStarRocksのトランザクションタイムアウトよりも大幅に短いことを確認してください。

- **長いソーストランザクション下での `activeChunk` メモリ増大**：マルチテーブルモードはチャンクサイズによる内部スイッチングを無効にするため（クリーントランザクション境界の不変条件を保持するため）、`activeChunk` は次のtxnEndが到着するまで増加し続ける可能性があります。メモリは `sink.transaction.multi-table.buffer-size`（ソフト）および `2 × buffer-size`（`blockIfCacheFull` によるハード）によって制限されます。非常に大きなソーストランザクションはバックプレッシャーによってタスクスレッドをスロットルします。これが常態化する場合は、上流でソーストランザクションを分割するか、`sink.transaction.multi-table.buffer-size` を増やしてください。

- **クロスデータベース書き込みは拒否されます**：マルチテーブルトランザクションは、すべてのリージョンが同じデータベースに属していることを検証します。同じコミットサイクル内で異なるデータベースのテーブルに書き込むとエラーが発生します。

- **マージコミットとは互換性がありません**：`sink.properties.enable_merge_commit=true` は `sink.transaction.multi-table.enabled=true` と組み合わせることができません。マージコミットは `MergeCommitManager` を通じて書き込みをルーティングしますが、これはマルチテーブルモードがトランザクション境界のために依存するパーティション対応の `write(int, ...)` / `setCommitAllowed(int, ...)` フックを持っていません。両方が有効になっている場合、コネクターは検証時に即座に失敗します。

## 監視とトラブルシューティング

推奨メトリクス：

- **Flink側**
  - チェックポイント成功率
  - チェックポイント所要時間
  - シンクのフラッシュ/コミットレイテンシ
- **StarRocks側**
  - 実行中/準備済みトランザクション数
  - トランザクションタイムアウトの発生
  - ラベルの競合

### よくある問題

#### `transaction not existed`

- 原因：StarRocksのトランザクションタイムアウト
- 解決策：コネクターはサーバータイムアウトの80%でアイドルトランザクションを自動的にリサイクルします。それでも発生する場合は、準備済みタイムアウトが短すぎるか、フラッシュ間隔が大きすぎないか確認してください。

#### `too many running txns`

- 原因：同時トランザクションが過剰です。
- 解決策：シンクの並列度を下げるか、StarRocks FE設定 `max_running_txn_num_per_db` の値を増やしてください。

#### `Transaction start failed`

- 原因：`beginTransaction` HTTPコールが失敗しました
- 解決策：load-urlの接続性とStarRocksのバージョン（v4.0以降が必要）を確認してください。

#### データ可視性レイテンシが高い

- 原因：コミット条件が満たされていません。
- 解決策：上流データに正しい `transactionEnd=true` マーカーがあることを確認してください。行ごとに最大 `commitInterval + miniInterval` のレイテンシを想定してください。レイテンシがこの予算を超える場合は、マネージャースレッドがリサイクルまたはインフライトロードで停止していないか確認してください（`StarRocks-Sink-Manager` ログを参照）。

#### クロスデータベース書き込みエラー

- 原因：異なるデータベースのテーブルが同じコミットサイクルに含まれています。
- 解決策：同じジョブで書き込まれるすべてのテーブルが同じStarRocksデータベースに属していることを確認してください。

## ベストプラクティス

1. **TransactionAssemblerコントラクト**:

   - ソーストランザクションが完全にクローズされた後にのみ行を出力します。
   - 最後の行には`setTransactionEnd(true)`が必要です。
   - すべての行には`setSourcePartition(partition)`が必要です。
   - すべての行は単一の`processElement()`呼び出し内で同期的に出力される必要があります。

2. **シンク前のkeyByは必須**: `rows.keyBy(DefaultStarRocksRowData::getSourcePartition).addSink(sink)` — これを省略するとパーティション内のトランザクション順序が壊れます。

3. **チェックポイントはコミットから切り離されています**: チェックポイント間隔はフォールト回復のために大きな値（例：60秒）に設定できます。データの可視性は`sink.buffer-flush.interval-ms`（例：1000ms）によって制御されます。

4. **ルーティング戦略を安定させる**: 過剰な数の異なるテーブルへの書き込みを行う単一トランザクションを避けてください。これによりトランザクション時間と失敗確率が増加します。

5. **本番前のフォールトインジェクションテスト**: TaskManagersを強制終了したり、ネットワークジッターを導入し、チェックポイント回復後のデータ正確性を検証してください。
