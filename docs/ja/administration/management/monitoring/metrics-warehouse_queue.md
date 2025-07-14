---
displayed_sidebar: docs
---

# ウェアハウスのモニタリングメトリクス

v3.5 以降、StarRocks はウェアハウスを管理し、Query Queue 機能がウェアハウスに対して有効化されている場合にクエリキューをモニタリングするためのさまざまなメトリクスを提供します。

ウェアハウスに対して Query Queue 機能を有効化するには、以下の SQL コマンドを実行します。

```SQL
ALTER WAREHOUSE <warehouse_name> SET("enable_query_queue" = "true");
```

## メトリクス項目

### warehouse_query_queue

- タイプ: Gauge
- 説明: ウェアハウスのクエリ処理のさまざまな側面をモニタリングするための異なるフィールドラベルを持つウェアハウスクエリキューメトリクス。

#### フィールドラベル

##### query_pending_length

- タイプ: Gauge
- 説明: ウェアハウスのクエリキューで現在保留中のクエリの数。

##### query_running_length

- タイプ: Gauge
- 説明: ウェアハウスで現在実行中のクエリの数。

##### max_query_queue_length

- タイプ: Gauge
- 説明: ウェアハウスのクエリキューの最大長。

##### earliest_query_wait_time

- タイプ: Gauge
- 説明: キュー内で最も早いクエリの待ち時間（秒単位）。設定されていない場合は `0.0` が返されます。

##### max_query_pending_time_second

- タイプ: Gauge
- 説明: ウェアハウスのクエリキューでクエリが保留状態にあった最大時間（秒単位）。

##### max_required_slots

- タイプ: Gauge
- 説明: まだ割り当てられていないクエリが必要とする最大スロット数。

##### sum_required_slots

- タイプ: Gauge
- 説明: まだ割り当てられていないクエリが必要とするスロットの合計。

##### remain_slots

- タイプ: Gauge
- 説明: ウェアハウスに残っている利用可能なスロットの数。

##### max_slots

- タイプ: Gauge
- 説明: ウェアハウスで利用可能な最大スロット数。

## 使用例

### ウェアハウスのクエリキューの状態をモニタリングする

これらのメトリクスを使用して、ウェアハウスの健康状態とパフォーマンスをモニタリングできます。

```promql
# すべてのウェアハウスで保留中のクエリを確認
warehouse_query_queue{field="query_pending_length"}

# すべてのウェアハウスで実行中のクエリを確認
warehouse_query_queue{field="query_running_length"}

# スロットの利用状況をモニタリング
warehouse_query_queue{field="remain_slots"} / warehouse_query_queue{field="max_slots"}
```

## メトリクスラベル

すべてのウェアハウスメトリクスには以下のラベルが含まれます。

- `warehouse_id`: ウェアハウスの一意の識別子
- `warehouse_name`: ウェアハウスの名前
- `field`: 測定されている特定のフィールド（上記のリスト参照）

これらのラベルを使用して、特定のウェアハウスごとにメトリクスをフィルタリングおよびグループ化し、それぞれのパフォーマンス特性をモニタリングできます。

## メトリクス例

例 1: ウェアハウスクエリキューメトリクスのデモ:

```Plain
{"tags":{"metric":"warehouse_query_queue","field":"query_pending_length","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"query_running_length","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"max_query_queue_length","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":1024},
{"tags":{"metric":"warehouse_query_queue","field":"earliest_query_wait_time","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0.0},
{"tags":{"metric":"warehouse_query_queue","field":"max_query_pending_time_second","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":600},
{"tags":{"metric":"warehouse_query_queue","field":"max_required_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"sum_required_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"remain_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":208},
{"tags":{"metric":"warehouse_query_queue","field":"max_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":208},
```
