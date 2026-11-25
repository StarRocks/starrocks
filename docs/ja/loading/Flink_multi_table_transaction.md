---
sidebar_position: 120
displayed_sidebar: docs
description: "Flinkジョブのマルチテーブルトランザクションを有効にして、1回の処理サイクルで同じデータベース内の複数のテーブルに書き込みます。"
products: [oss, enterprise, anywhere]
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
マルチテーブルトランザクションを有効にするには、StarRocks v4.0以降（マルチテーブルトランザクションStream Loadサポートを含む）でクラスターを実行し、StarRocks Flink Connectorをv1.2.9以降で使用する必要があります。
:::

## 主要機能

| 機能                         | 説明                                                         |
| ---------------------------- | ------------------------------------------------------------ |
| クロステーブルアトミックコミット | 同じフラッシュサイクル内のすべてのテーブルは1つのStarRocksトランザクションラベルを共有します。そのラベルに対して、統合された`POST /api/transaction/commit`が1回発行されます（prepare呼び出しは行いません — StarRocksはマルチテーブルトランザクションのprepareをサポートしていません）。 |
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

#### `sink.transaction.multi-table.mini-switch-interval-ms`

- 型：Long
- デフォルト：`-1`（自動）
- 単位：ms
- 説明：パーティションごとのチャンク切り替え間の最小間隔（ms）。この間隔内では、ソーストランザクションを 1 つの Stream Load にまとめ、HTTP リクエスト数を抑制します。`-1` の場合は `min(1000, max(500, sink.buffer-flush.interval-ms/4))` が自動的に適用され、正の値を指定するとその値で上書きされます。

#### `sink.transaction.multi-table.min-switch-bytes`

- 型：Long
- デフォルト：`1048576`（1 MB）
- 単位：バイト
- 説明：時間間隔に基づくチャンク切り替えは、あるリージョンのアクティブチャンクに少なくともこのバイト数が蓄積されている場合にのみ実行されます。これにより、低ボリュームのパーティションでは、複数のソーストランザクションを 1 つの Stream Load にまとめ、多数の小さなリクエストが発生するのを防ぎます。半分まで埋まった場合のヘッドルーム、バッファサイズによるメモリプレッシャー、またはパーティションが最後に切り替えられてから `sink.buffer-flush.interval-ms` が経過した場合は、サイズしきい値に関係なく切り替えが強制されます。そのため、大きなしきい値を設定しても、継続的な低ボリュームのストリームはおおむねフラッシュ間隔で切り替えられ、無期限に保持されることはありません。`<=0` に設定するとサイズゲートが無効になり、従来のインターバルごとの切り替え動作に戻ります。`mini-switch-interval-ms` が `sink.buffer-flush.interval-ms` と等しい（またはそれより大きい）場合、このオプションは txnEnd 時の切り替えには影響しません（「チャンクのライフサイクルと `miniInterval` バッチ処理」を参照）。

#### `sink.transaction.multi-table.max-txn-bytes`

- 型：Long
- デフォルト：`0`（無制限）
- 単位：バイト
- 説明：`txnEnd` を待機している間に、1 つのシンクサブタスクがバッファリングできる処理中のソーストランザクションデータ量のハード上限（バイト単位）。`txnEnd` より前にフラッシュできないデータについて、Writer がブロックすることはありません（制限事項 7 を参照）。そのため、1 つのソーストランザクションが `2 × buffer-size` を超えて増大する可能性があります。このオプションを使用するとその増大を制限し、上限を超えた場合は明確なエラーでジョブを失敗させます。`0` は上限なしを意味し、TaskManager のヒープのみが上限となります。

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

#### `sink.http.keep-alive.enabled`

- 推奨値：`true`
- 説明：推奨。ロードおよびBEGIN / PREPARE / COMMIT / ROLLBACKで、リクエストごとに新しい接続を開く代わりにHTTP接続を再利用します。デフォルトでは無効です（ベストプラクティス 6 を参照）。

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
        // アラインドチェックポイントのみ。各チェックポイントでは、開いている共有
        // トランザクションのドレインとコミットも行われます（ベストプラクティス 3 を参照）。
        env.enableCheckpointing(10_000);

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
        // sink.transaction.multi-table.enabled=true は、StreamLoadManagerV2 内での
        // パーティションごとのリージョントラッキングを有効にします: 各パーティションの
        // リージョンは、他のパーティションとは独立して、txnEnd 境界でまとめて切り替えられます。
        // タイマー駆動コミットはすべてのパーティションを待機しません: クリーンな境界にある
        // パーティションを切り替え、トランザクションの途中にあるパーティションからは、
        // それ以前に切り替えられたチャンクのみが含まれます — 進行中の行は
        // 次の共有トランザクションに入ります。
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

## 動作の仕組み

### チャンクのライフサイクルと `miniInterval` バッチ処理

各 `(partition, table)` リージョンには、1 つの `activeChunk`（現在書き込みを受け付けている）と、`inactiveChunks`（凍結済みデータ、HTTP ロード待ち）の FIFO キューがあります。マルチテーブルトランザクションモードでは、`activeChunk` から `inactiveChunks` へデータを移動する**唯一の**方法はコミット切り替えです。コミット切り替えはクリーンなトランザクション境界でのみ行われ、セーブポイントの場合を除き、パーティションのすべてのリージョンに対してまとめて適用されます（`lockstepSwitchPartition`）。コミット切り替えが発生するのは、正確に次の 4 か所です:

1. **タスクスレッド、txnEnd 時** — `setCommitAllowed(partition, true)` 内で行われます。この処理は、パーティションのすべてのリージョンをクリーンとしてマークした後、パーティション全体に対して 1 回の切り替え判定を行います（ルールは後述）。これが一般的なパスです。

2. **マネージャースレッド、ソースアイドル時の強制切り替え** — すべてのスキャンサイクルで `managerForceSwitchCleanBoundaryRegions()` によって実行され、リージョンが**すべて**クリーンな境界にあり、少なくとも 1 つのリージョンが行を保持しているパーティションが対象です。プレッシャーがある場合（バッファリングされたバイト数が `buffer-size` に達した場合、またはリージョンの `activeChunk` が半分まで埋まっている場合）、パーティションは即座に切り替えられます。それ以外の場合は、あるリージョンが少なくとも `min-switch-bytes` を保持し、**かつ**パーティションの最後の切り替えから `miniSwitchIntervalMs` が経過した場合にのみ切り替えられます。`min-switch-bytes` 未満のアイドルデータは、コミットカットに委ねられます。

3. **マネージャースレッド、コミットカット** — すべてのタイマー駆動コミットおよびすべてのリサイクルの開始時に実行されます（「マネージャースレッド — 時間駆動コミット」および「共有トランザクションの調整」を参照）。すべてのクリーンなパーティションを切り替え、すべてのリージョンでコミットウォーターマークを固定します。

4. **マネージャースレッド、セーブポイント** — チェックポイントのフラッシュが、すべてのリージョンがクリーンな境界にあることを確認した後、すべてのリージョンを強制切り替えします（ベストプラクティス 3 を参照）。

高スループット CDC において 1 ソーストランザクションごとに 1 回の HTTP ロードが発生するのを避けるため、txnEnd パスでは、以下の**いずれか**が成立する場合にのみパーティションを切り替えます。それ以外の場合、完了したトランザクションは `activeChunk` に残り、後続のトランザクションとまとめてバッチ処理されます:

- パーティションの最後の切り替えから少なくとも `miniSwitchIntervalMs` が経過し、**かつ**そのリージョンの少なくとも 1 つで `activeChunk` が `sink.transaction.multi-table.min-switch-bytes`（デフォルト 1 MB。`<= 0` の場合、このサイズチェックは無効）以上のデータを保持している。

- パーティションの最後の切り替えから `commitInterval` 全体が経過した（鮮度の上限であり、これによりサイズゲートが完了済みデータを無期限に先送りすることはありません）。

- リージョンの `activeChunk` が書き込みブロック上限の半分（つまり `buffer-size` バイト）に達した。

- すべてのリージョンにわたるバッファリング済みのバイト数が `buffer-size` に達した。

自動導出される場合、`miniSwitchIntervalMs` は `min(1000 ms, max(500 ms, commitInterval / 4))` となるため、コミット間隔が 2 秒の場合は 500 ms でバッチ処理され、4 秒以上の場合はバッチ処理が最大 1 秒に制限されます（`sink.transaction.multi-table.mini-switch-interval-ms` を明示的に指定すると、この値は上書きされます）。このウィンドウ内では、複数の完了済みソーストランザクションが同じ `activeChunk` に蓄積され（N:1 マッピング）、次の切り替え時にまとめて凍結されます。

サイズゲートが意味を持つのは、`miniSwitchIntervalMs` が `commitInterval` より短い場合のみです。`mini-switch-interval-ms` が `sink.buffer-flush.interval-ms` と等しい（またはそれより大きい）値に設定されている場合、またはコミット間隔が 500 ms 以下で 500 ms に自動導出される場合は、遅くともこの間隔が経過する時点で鮮度の上限に達します。そのため、間隔の経過後はすべての txnEnd でサイズに関係なく切り替えが行われ、`min-switch-bytes` は txnEnd パスに影響しません。

これらの判断を駆動する状態は 2 つあります:

- パーティションの最後の切り替え時刻（マネージャーによってパーティションごとに保持されるため、パーティションのすべてのリージョンはまとめてバッチ処理されます）。初期値は 0 であるため、パーティションの最初の txnEnd は常に切り替えをトリガーします。

- `activeChunkCleanBoundary`（リージョンごと） — このリージョンに対する最新のタスクスレッドイベントが txnEnd または切り替えのいずれかであった場合は `true`。`write()` の後は `false`。マネージャースレッドによる切り替えでは、リージョンの書き込みロックを保持した状態で、パーティションのすべてのリージョンについてこのフラグを再確認し、いずれかのリージョンがダーティであればそのパーティションをスキップします。そのため、ソーストランザクションの部分的なデータを凍結することはありません。

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
  |              cleanBoundary = true           // always (txnEnd observed)
  |          if (activeChunk has data) and (
  |                (miniInterval elapsed && bytes >= minSwitchBytes)
  |             || commitInterval elapsed since last switch   // freshness bound
  |             || half-full headroom || memory pressure):
  |              lockstepSwitchPartition()      // freezes all regions together
  |          partitionTracker.onTxnEnd(partition)  // safety bookkeeping only
```

タスクスレッドは `write()` および `setCommitAllowed()` イベントの唯一のシリアライザーであるため、`cleanBoundary` マークと条件付き切り替えの両方をここで実行する必要があります（マネージャースレッドに委譲してはなりません）: `cleanBoundary` は常に最新のタスクスレッドイベントを反映していなければならず、そうでなければマネージャーのアイドルフォールバックが書き込みと競合して部分的なデータを凍結する可能性があります。

### 6.3 マネージャースレッド — 時間駆動コミット

マネージャースレッドは `scanningFrequency` でスキャンループを実行します。各イテレーションでは:

1. **共有トランザクションの確保**: 新しいものを開く（先行）か、StarRocks サーバー側タイムアウト（`timeout` ヘッダーの 80%、デフォルト 480 秒）に近づいている場合は現在のものを積極的にリサイクルします。

2. **ソースアイドルフォールバック**: リージョンが**すべて**クリーンな境界にあるパーティションについて、`min-switch-bytes` が蓄積され、かつ最後の切り替えから `miniSwitchIntervalMs` が経過した時点で、またはメモリプレッシャーがある場合は即座に、強制切り替えを行います（「チャンクのライフサイクルと `miniInterval` バッチ処理」を参照）。サイズしきい値を下回る場合、アイドルデータはここでは切り替えられません — 代わりに後述の時間駆動コミットがカットしてコミットします（ステップ 3 を参照）。そのため、ソースが一時停止した場合でも、1 回の `commitInterval` 以内に可視になります。これは、タスクスレッドが新しい切り替えを発行する前に停止した「数回の txnEnd 後にソースが一時停止した」ケースを処理します。

3. **時間駆動コミットトリガー**: `tryStartTimerDrivenCommit()` を呼び出し、**両方の**条件が成立する場合に `commitInFlight=true` を設定します:

   - `now - lastCommitTimeMs >= commitInterval`（設定された`sink.buffer-flush.interval-ms`）。

   - コミットするデータが存在します — `txnCoordinator.hasDataLoaded()`がtrueである、少なくとも1つのリージョンに保留中のinactiveChunksがある、またはいずれかのパーティションが全体としてクリーンな境界にあり、コミットカットによって凍結されるバッファ済みのアクティブな行（サイズゲートを下回るため保留されたもの）を持っている、のいずれかです。この最後のチェックはパーティション単位で評価されるため、兄弟テーブルがまだトランザクションの途中にある単独のクリーンなリージョンが、進行できないコミットサイクルを開始させることはありません。

4. **自律フラッシュ**：`inactiveChunks`が空でないリージョンを`FlushAndCommitStrategy`経由でドレインします。マルチテーブルモードの`flush()`は、既にフリーズされた非アクティブチャンクのみをストリームアウトします — それは**決して**`activeChunk`に触れません。これにより、共有ラベルの下でStarRocksに到達するすべてのチャンクが完了したソーストランザクションから来るという不変条件が保持されます。

`commitInFlight=true`のとき、メインループは`processMultiTableCommit`に入ります。その最初のステップは**コミットカット**であり、このラベルに何を含めるかを決定します。カットはパーティションごとに行われ、すべてのパーティションがクリーンな境界に達するのを待ちません。パーティションのリージョンの書き込みロックを保持した状態で（並行するtxnEndの切り替えが途中に割り込めないようにするため）、次のように処理します：

- **クリーン**なパーティション（すべてのリージョンがクリーンな境界にあり、少なくとも1つのリージョンが行を持つ）は、ロックステップで切り替えられます。その後、各リージョンのコミットウォーターマークが、そのリージョンで最も新しく切り替えられたチャンクに設定されます。

- **ダーティ**なパーティション（いずれかのリージョンがトランザクションの途中にある）は切り替えられません。そのリージョンのウォーターマークは、それぞれの最後の切り替えの位置に設定されます。それ以前に切り替えられたチャンクは今回コミットされ、その`activeChunk`（進行中のソーストランザクションと、それとまとめてバッチ処理された完了済みトランザクション）は次の共有トランザクションに入ります。

いずれかのパーティションのロックが使用中の場合、カットは次のスキャンで再試行されます。ウォーターマーク以下のチャンクのみがこのラベルでロードされ、カット後に切り替えられたチャンクは次のカットを待ちます。その後、コミットはインフライトのロードを待機し、残りの対象チャンクのロードをトリガーし、`SharedTransactionCoordinator`を介して統合コミットを実行し、ウォーターマークを解除し、`lastCommitTimeMs`を更新し、トラッカーをリセットして、次のサイクルのために新しい共有トランザクションを開きます。

### 共有トランザクションの調整

同じコミットサイクル内のすべてのテーブルは、`SharedTransactionCoordinator`によって管理される単一のStarRocksトランザクションを共有します：

1. **積極的なトランザクション開始**：共有トランザクションは、自律フラッシュの前に積極的に開かれるため、すべてのHTTPロードは共有ラベルを使用します。これにより、独立ラベルのフラッシュが後で共有トランザクションがラベルを上書きする際に孤立する可能性があるデータ損失ウィンドウが排除されます。

2. **統合コミット**：すべてのリージョンのデータがロードされた後、共有ラベルに対して単一の`POST /api/transaction/commit`が実行されます。マルチテーブルトランザクションは、StarRocksがマルチテーブルモードで`TXN_PREPARE`をサポートしていないため、`prepare`ステップをスキップします。

3. **トランザクションのリサイクル**：共有トランザクションが開かれている時間（最後のロードからではなく、BEGINから計測）がBEGINの`timeout`ヘッダーの80%（デフォルト：コネクターが送信する600sに対して480s）を超えた場合、サーバー側タイムアウトエラーを防ぐために積極的にリサイクル（コミットまたはロールバック＋再オープン）されます。リサイクルはタイマー駆動コミットと同じパーティションごとのコミットカットを使用するため、トランザクションの途中にあるパーティションは単に次のラベルに持ち越されます。リサイクルが即座に失敗するのは、前回のコミット以降、いずれかのパーティションがtxnEndを1つも受け取らずにデータを書き込んだ場合のみです。リサイクルはベストエフォートです：コミットサイクルが実行中の間、またはいずれかのリージョンがまだロード中もしくはリトライ中の間は待機するため、応答が返らないロード（デフォルトの`sink.socket.timeout-ms=-1`では起こり得ます）によって、ラベルが期限に達してしまう可能性があります。これに上限を設けるには、有限の`sink.socket.timeout-ms`を設定してください。

### 6.5 PartitionCommitTracker（安全性の記録管理）

現在の設計では、コミットのタイミングはコミット間隔とリージョンごとのクリーン境界フラグによって完全に制御されます。`PartitionCommitTracker`は情報提供/安全補助に縮小されています：

- `onWrite(partition)`は最初の書き込み時にパーティションを`ACTIVE`として登録します。

- `onTxnEnd(partition)`はパーティションを`TXN_END_SEEN`に遷移させます（スティッキー — 後続の書き込みによって`ACTIVE`に降格されることは**ありません**）。

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
| txnEndパスでHTTP I/Oを行わない                     | `setCommitAllowed`でのパーティション切り替え判定はO(パーティション内のリージョン数)であり、すべてのHTTPの作業はマネージャースレッドで非同期に行われます。これはノンブロッキングの保証ではありません：タスクスレッドは、マネージャースレッドの切り替えが保持しているリージョンの書き込みロックを待機する、自身の切り替えでサイズ超過のチャンクをフリーズした際にそのチャンクを行単位で再分割する（制限事項 7 を参照）、`blockIfCacheFull`で待機する、チェックポイント中にブロックする（ベストプラクティス 3 を参照）、といった場合があります。 |
| ソースアイドル時のデータ可視性                     | ソースが`min-switch-bytes`未満で一時停止した場合、時間駆動コミットのパーティション単位のクリーン境界検出がアイドル状態の`activeChunk`をカットしてコミットするため、ソースが再開しなくても約1回の`commitInterval`以内にデータが可視になります。 |
| 自律フラッシュはトランザクションセーフ             | すべてのロードは共有ラベルを使用します。すべてのフリーズされたチャンクは完了したソーストランザクションからのものです。 |
| 共有トランザクションは期限前にリサイクルされる（ベストエフォート） | 共有トランザクションはBEGINの`timeout`の80%でリサイクルされます。トランザクションの途中にあるパーティションは次のラベルに持ち越され、リサイクルが即座に失敗するのは、txnEndなしでデータを書き込んだパーティションがある場合のみです。リサイクルはコミットサイクルまたはロードが実行中の間は待機するため、応答が返らないロード（デフォルトの`sink.socket.timeout-ms=-1`）によってラベルが期限を過ぎる可能性があります。有限のソケットタイムアウトによって、これに上限を設けられます。 |
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

コミットの決定は時間駆動（特定のtxnEndに紐付けられていない）であるため、コネクターは設定変更なしに多くの小さなソーストランザクション全体でHTTPロードおよびbegin/commitのオーバーヘッドを分散します。`commitInterval=1 s`（したがって自動導出される`miniInterval=500 ms`）で毎秒100件のtxnEndを発行するCDCソースの場合、コネクターは毎秒約100回ではなく最大約2回のロード呼び出しを発行します。デフォルトの`min-switch-bytes`サイズゲートでは、低ボリュームのストリームはさらにまとめられ、`commitInterval`ごとに約1回のロードまで減ります（しきい値未満のチャンクでも切り替えられる鮮度の上限）。

## 制限事項

1. **`sink.version=V2`が必要**：V1はトランザクションストリームロードをサポートしていません。

2. **少なくとも1回のみ**：失敗した再試行により重複書き込みが発生する可能性があります。マルチテーブルモードは同じバッチ内のすべてのテーブルが一緒に成功または失敗することを保証しますが、グローバルな正確に1回の保証は提供しません。PRIMARY KEYテーブルの場合、重複書き込みはべき等（upsert）です。

3. **すべてのテーブルは同じデータベースに存在する必要があります**：StarRocksのマルチテーブルトランザクションはデータベーススコープです。クロスデータベーストランザクションはサポートされていません。

4. **トランザクションスコープはシンクサブタスクごと＋パーティションごと**：各シンクサブタスクは、独自のStarRocksトランザクションを独立して維持します。アトミック性は**単一のソーストランザクション内**（1つのパーティション上の1つのtxnEndに対するすべての行、そのパーティションが書き込むすべてのテーブルにわたって）で保証されています。**異なる**ソースパーティション間のデータ可視性はインターリーブする可能性があります：パーティションP0のソーストランザクションが完全に到着し、コミット間隔が経過すると、パーティションP1のソーストランザクションがまだ進行中であっても、P0のデータはコミットされます。StarRocksレベルでクロスパーティションのアトミック性を必要とするアプリケーションは、単一のソースパーティションを使用するか、上流でコミットを調整する必要があります。要するに、ソーストランザクションの保証はレーンごとにのみ成立します。ここでのレーンとは、1つのシンクサブタスク上の1つの`sourcePartition`を指します。1つのラベルでコミットされたものは、複数のレーンの行も含めてすべてアトミックに可視になりますが、異なるレーンのどのソーストランザクションが同じラベルに入るかを制御するものはありません：別のレーン上の関連するトランザクションが、それより前または後のラベルに入る可能性があり、異なるシンクサブタスク上のレーンがラベルを共有することはありません。

5. **データ可視性レイテンシ**：`sink.buffer-flush.interval-ms`（コミット間隔）によって制御されます。完了したソーストランザクションは、まず `activeChunk` から凍結されて切り出されます。これは、パーティションが `sink.transaction.multi-table.min-switch-bytes`（デフォルト 1 MB）に達した時点で即座に行われるか、そのしきい値を下回る低ボリュームのパーティションの場合は、最後の切り替えから最大 1 回の `commitInterval` 後（txnEnd 時にタスクスレッドで適用される鮮度の上限）に行われます。その後、次の時間駆動コミットによって公開されます。したがって、継続的に流れるCDCストリームでは、パーティションがサイズゲートに達するほど活発な場合、個々の行はおよそ 1 回の `commitInterval` 以内に可視になり、低ボリュームの最悪ケースでは最大 2 回分の `commitInterval` ウィンドウ以内に可視になります（1 回はしきい値未満のチャンクを凍結するため、もう 1 回はそれをコミットするため）。ソースが一時停止している間、以前にコミットされたデータは引き続き可視であり、最後のアイドル状態のしきい値未満のチャンクは、時間駆動コミットのクリーン境界検出によって、さらに 1 回の `commitInterval` 以内にコミットされます。`min-switch-bytes <= 0` に設定すると、間隔ごとに切り替えが行われます（レイテンシは最小になりますが、ロードの回数が増え、1 回あたりのサイズは小さくなります）。

6. **StarRocksクラスターのトランザクション設定に依存**：実行中のトランザクション制限、トランザクションタイムアウト、およびラベルの保持を監視してください。トランザクションタイムアウトとは、コネクターがBEGIN時に送信する `timeout` ヘッダーの値です（`sink.properties.timeout` で上書きしない限り600秒）。StarRocksはこれをアイドルタイムアウトではなく、BEGINから計測される絶対的な期限として扱います：ラベルへのロードによって期限が延長されることはありません。コネクターは、その80%（デフォルトでは480秒）で共有トランザクションをリサイクルします。`sink.buffer-flush.interval-ms` がこのタイムアウトよりも大幅に短いことを確認してください。

7. **大規模なソーストランザクションにおける `activeChunk` のメモリ使用量の増加**: マルチテーブルモードでは、チャンクサイズによる内部切り替えが無効化されています（クリーンなトランザクション境界の不変条件を維持するため）。そのため、`activeChunk` は次の `txnEnd` が到着するまで増大する可能性があります。`sink.transaction.multi-table.buffer-size`（ソフト上限）と `2 × buffer-size`（`blockIfCacheFull` によるハード上限）は、バッファのうち*フラッシュ可能な*部分のみを制限します。フラッシュ可能な部分とは、フリーズされたチャンクと、全体がクリーンなパーティションを指します。`txnEnd` より前にバッファから排出できないデータ（処理中のトランザクション自体、および同じパーティション内でロックステップルールによって保持される兄弟テーブルの完了済み行）については、Writer は待機しません。バッファがハード上限に達し、フラッシュ可能なデータが何もない場合、Writer は WARN（`Write-block cap ... reached with nothing flushable`）をログに出し、`txnEnd` までバッファリングを継続します。その後、パーティションが切り替えられて排出されます。そのため、1 つのソーストランザクションが `2 × buffer-size` を超える可能性があります。そのサイズは `sink.transaction.multi-table.max-txn-bytes`（デフォルトは無制限）と TaskManager のヒープによってのみ制限されます。ヒープサイズは、想定される最大ソーストランザクションサイズ × 1 つの TaskManager 上のシンクサブタスク数を基準に設定してください。ヒーププレッシャーよりも明確な失敗を優先する場合は、`max-txn-bytes` を設定してください。また、サイズに上限のないトランザクション（バルクブートストラップ）は上流で分割してください。切り替え時に、フリーズされたチャンクがロード本文の上限（デフォルトでは `sink.chunk-limit` の 3 GB と `sink.transaction.multi-table.buffer-size` の小さい方）を超えている場合は、同じ共有ラベルを使用して複数のロードリクエストに再分割されます。そのため、単一の行自体がこの上限を超える場合を除き、単一の `/api/transaction/load` 本文がこの上限を超えることはありません。

8. **クロスデータベース書き込みは拒否されます**：マルチテーブルトランザクションは、すべてのリージョンが同じデータベースに属していることを検証します。同じコミットサイクル内で異なるデータベースのテーブルに書き込むとエラーが発生します。

9. **マージコミットとは互換性がありません**：`sink.properties.enable_merge_commit=true` は `sink.transaction.multi-table.enabled=true` と組み合わせることができません。マージコミットは `MergeCommitManager` を通じて書き込みをルーティングしますが、これはマルチテーブルモードがトランザクション境界のために依存するパーティション対応の `write(int, ...)` / `setCommitAllowed(int, ...)` フックを持っていません。両方が有効になっている場合、コネクターは検証時に即座に失敗します。

10. **`sourcePartition`ごとに上流のプロデューサーは1つのみ**：クリーンな境界は位置に基づいて判定されます — パーティションがクリーンな境界にあるのは、シンクサブタスクがそのパーティションについて最後に受信した行がtxnEndを持っていた場合に限られます。コネクターは、行がどのソーストランザクションに属するかを追跡しません。2つのプロデューサー（2つのアセンブラーサブタスク、またはKafkaの2つのトピックのパーティション0のようにパーティション番号が衝突する2つのソース）が同じ`sourcePartition`を持つ行を出力すると、それらの行はシンクでインターリーブします。一方のプロデューサーのtxnEndによって、もう一方のトランザクションがまだ進行中であるにもかかわらずパーティションがクリーンとマークされ、次の切り替えでそのトランザクションが部分的にコミットされる可能性があります。TransactionAssemblerは、シンクがキーとして使用する値と同じ値をキーとし、`sourcePartition`をすべてのソース間で一意にしてください。

11. **アラインドチェックポイントのみ**：チェックポイントのフラッシュでは、バリアがシンクに到達した時点で、すべてのリージョンがクリーンな境界にある必要があります（ベストプラクティス 3 を参照）。アラインドチェックポイントではこれが保証されます。アセンブラーはソーストランザクション全体を1回の`processElement()`呼び出し内で出力するため、バリアがその行の間に入ることはないからです。アンアラインドチェックポイント（`execution.checkpointing.unaligned.enabled`。Flink 1.20より前では`execution.checkpointing.unaligned`）では、バリアがインフライトの行を追い越し、トランザクションの途中でシンクに到達することがあります。これには、アラインドからアンアラインドに切り替わるモードも含まれます。このモードでは、アンアラインドチェックポイントが`execution.checkpointing.aligned-checkpoint-timeout`とともに有効化され、タイムアウトが経過するとチェックポイントがアンアラインドに切り替わります。アンアラインドチェックポイントが無効な場合、このタイムアウトは効果がありません。バリアがトランザクションの途中でシンクに到達した場合、チェックポイントは失敗し、開いているラベルはロールバックされ、ジョブはフェイルオーバーします。

12. **データ品質による拒否はラベル全体を中止させる**：マルチテーブルトランザクションの場合、StarRocksはストリクトモードと`max_filter_ratio`を、各`/api/transaction/load`ではなくコミット時に適用します。拒否されるとコミットが失敗し、ラベル全体が中止されます — 不正な行を含むテーブルだけでなく、そのコミットサイクル内のすべてのテーブルの行が中止されます。コネクターはコミットの失敗を報告し、ジョブはフェイルオーバーします。同じ行がリプレイされるため、行またはロード設定が修正されるまで失敗し続けます。

13. **StarRocks v4.1.6以前では、失敗したマルチテーブルコミットがOKとして報告される場合がある**：これらのバージョンでは、サーバー上で失敗したマルチテーブルコミットが、クライアントに`OK`として返される場合があります。その場合、コネクターはラベルをコミット済みとして扱うため、そのコミットサイクルの行がサイレントに欠落する可能性があります。StarRocks PR [#80351](https://github.com/StarRocks/starrocks/pull/80351)（CelerDataのバックポート：コミット`1ceb0763cf`）で修正されています。

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
- **コネクター側**
  - `sharedTxnLatencyMs`ヒストグラム：共有トランザクションの最も早い行がシンクバッファに入ってから、そのトランザクションがコミットされるまでの時間。各コミットでは、この値もラベルごとにログに出力されます：`[MultiTxn] SharedTransaction committed successfully: label=<label>, firstRowTimeMs=<epoch ms>, latencyMs=<ms>`（不明な場合はどちらも`-1`）。

### よくある問題

#### `transaction not existed`

- 原因：StarRocksのトランザクションタイムアウト
- 解決策：コネクターは、BEGINの`timeout`の80%（BEGINから計測）で共有トランザクションを自動的にリサイクルします。それでも発生する場合は、`sink.properties.timeout`が短すぎないか、フラッシュ間隔が大きすぎないか、またはロードがハングして期限を過ぎていないか（有限の`sink.socket.timeout-ms`を設定してください）を確認してください。

#### `too many running txns`

- 原因：同時トランザクションが過剰です。
- 解決策：シンクの並列度を下げるか、StarRocks FE設定 `max_running_txn_num_per_db` の値を増やしてください。

#### `Transaction start failed`

- 原因：`beginTransaction` HTTPコールが失敗しました
- 解決策：load-urlの接続性とStarRocksのバージョン（v4.0以降が必要）を確認してください。

#### データ可視性レイテンシが高い

- 原因：コミット条件が満たされていません。
- 解決策：上流データに正しい `transactionEnd=true` マーカーがあることを確認してください。`min-switch-bytes` を下回る低ボリュームのパーティションでは、行ごとに最大約 2×`commitInterval` のレイテンシを想定してください（1 回の間隔でしきい値未満のチャンクを凍結し、もう 1 回の間隔でそれをコミットします）。レイテンシがこの予算を超える場合は、マネージャースレッドがリサイクルまたはインフライトロードで停止していないか確認するか（`StarRocks-Sink-Manager` ログを参照）、`min-switch-bytes` を小さくしてください。

#### `Write-block cap ... reached with nothing flushable` (WARN)

- 原因：ソーストランザクションと、`txnEnd` まで保持される兄弟テーブルの完了済み行の合計が `2 × buffer-size` を超えています。Writer はブロックせず、`txnEnd` までバッファリングを継続します。
- 解決策：大きなソーストランザクションでは想定される動作です。`txnEnd` が到着するとデータがコミットされます。TaskManager のヒープを監視してください。ハード失敗を優先する場合は `sink.transaction.multi-table.max-txn-bytes` を設定するか、上流でトランザクションを分割してください。

#### `would exceed sink.transaction.multi-table.max-txn-bytes`

- 原因：ソーストランザクションが設定されたハード上限を超えています。
- 解決策：`max-txn-bytes` を引き上げる（または無制限にするため `0` に設定する）か、上流でソーストランザクションを分割してください。

#### クロスデータベース書き込みエラー

- 原因：異なるデータベースのテーブルが同じコミットサイクルに含まれています。
- 解決策：同じジョブで書き込まれるすべてのテーブルが同じStarRocksデータベースに属していることを確認してください。

## ベストプラクティス

1. **TransactionAssemblerコントラクト**:

   - ソーストランザクションが完全にクローズされた後にのみ行を出力します。
   - 最後の行には`setTransactionEnd(true)`が必要です。
   - すべての行には`setSourcePartition(partition)`が必要です。
   - すべての行は単一の`processElement()`呼び出し内で同期的に出力される必要があります。

2. **シンク前のkeyByは必須**: `rows.keyBy(DefaultStarRocksRowData::getSourcePartition).addSink(sink)` — これを省略するとパーティション内のトランザクション順序が壊れます。また、各`sourcePartition`の値は、上流の1つのプロデューサーのみから出力される必要があります（制限事項 10 を参照）。

3. **チェックポイントでもコミットされる**: データの可視性は`sink.buffer-flush.interval-ms`（例：1000ms）によって制御されますが、チェックポイントはコミットから切り離されていません。at-least-onceモードでは、`snapshotState`が`flush()`を呼び出し、バッファ全体（`activeChunk`内でまだバッチ処理中の完了済みトランザクションを含む）がロードされ、開いている共有ラベルがコミットされるまで、タスクスレッドをブロックします。フラッシュにはトランザクションタイムアウトの1.1倍（デフォルトでは660秒）の予算がありますが、これは待機の合間にのみチェックされ、ハード上限ではありません：ドレインはインフライトのすべての`/api/transaction/load`を期限なしで待機するため、デフォルトの`sink.socket.timeout-ms=-1`では、応答が返らないロードによって、チェックポイントとタスクスレッドが無期限にブロックされたままになる可能性があります。ハングしたロードによってジョブが失敗するように、想定される最も遅いロードよりも長い有限の`sink.socket.timeout-ms`を設定してください。いずれかのパーティションがトランザクションの途中にある間にバリアが到着すると、フラッシュは例外をスローし、開いているラベルはロールバックされ、ジョブはフェイルオーバーします（制限事項 10 および 11 を参照）。チェックポイント間隔を大きくする（例：60秒）と、このような強制コミットが少なくなります。

4. **ルーティング戦略を安定させる**: 過剰な数の異なるテーブルへの書き込みを行う単一トランザクションを避けてください。これによりトランザクション時間と失敗確率が増加します。

5. **本番前のフォールトインジェクションテスト**: TaskManagersを強制終了したり、ネットワークジッターを導入し、チェックポイント回復後のデータ正確性を検証してください。

6. **HTTP keep-aliveを有効にする**: `sink.http.keep-alive.enabled=true`を設定してください。共有トランザクションは1回のBEGIN、多数のロード、1回のCOMMITを送信し、keep-aliveを使用しない場合は、それぞれが新しいTCP（およびTLS）接続を開きます。これらのリクエストのいずれかの実行中に接続が失敗すると、共有トランザクション全体が失敗し、その中のすべてのテーブルがロールバックされてジョブがフェイルオーバーします。そのため、新しい接続が少ないほど、このような失敗は少なくなります（たとえば、新しい接続をリセットする可能性のあるロードバランサーの背後にある場合）。keep-aliveが有効な場合、サーバーまたはロードバランサーによってクローズされた再利用接続上のリクエストは、データが二重に書き込まれることがない場合に限り、新しい接続で再送信されます。`sink.http.keep-alive.idle-timeout-ms`（デフォルト60000）は、FlinkとStarRocksの間にあるすべてのプロキシまたはロードバランサーのアイドルタイムアウトよりも短くしてください。AWS Network Load Balancerは、デフォルトで350秒間アイドル状態の接続を切断します。
