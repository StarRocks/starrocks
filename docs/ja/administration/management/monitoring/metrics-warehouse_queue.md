---
displayed_sidebar: docs
description: "ウェアハウスの管理と各ウェアハウスのクエリキューのモニタリングに使用するメトリクス。"
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

### ウェアハウスごとのクエリメトリクス

以下のメトリクスは、クラスタ全体の FE クエリメトリクス（`request_total`、`query_total`、`query_err`、`query_timeout`、`query_analysis_err`、`query_internal_err`、`slow_query`、`qps`、`rps`、`query_latency`、`query_latency_ms`）のウェアハウスごとの版です。各 FE ノードが自身で処理したリクエストについて個別に保持し、`warehouse_id` と `warehouse_name` のラベルを持ち、FE 設定項目 `enable_per_warehouse_query_metrics` で制御されます。ウェアハウスが最初のリクエストを処理した時点で、そのウェアハウスのすべてのメトリクスが（値 0 で）作成され、ウェアハウスが削除されると削除されます。`SET_VAR(warehouse='...')` ヒントでウェアハウスを切り替えたクエリは、実際に実行したウェアハウスに帰属します。一方、`warehouse_request_total` は常にセッションのウェアハウスに帰属します。

#### warehouse_request_total

- タイプ: Counter
- 説明: この FE ノード上でウェアハウスのために処理したリクエストの総数。

#### warehouse_query_total

- タイプ: Counter
- 説明: この FE ノード上でウェアハウス内に完了したクエリの総数（成功したクエリと失敗したクエリの両方を含む）。

#### warehouse_query_err

- タイプ: Counter
- 説明: この FE ノード上でウェアハウス内に失敗したクエリの総数。

#### warehouse_query_timeout

- タイプ: Counter
- 説明: この FE ノード上でウェアハウス内にタイムアウトで失敗したクエリの総数。

#### warehouse_query_analysis_err

- タイプ: Counter
- 説明: この FE ノード上でウェアハウス内に解析段階で失敗したクエリの総数（構文エラーや存在しないオブジェクトの参照など）。

#### warehouse_query_internal_err

- タイプ: Counter
- 説明: この FE ノード上でウェアハウス内に内部エラーで失敗したクエリの総数（解析エラーでもタイムアウトでもないすべての失敗）。

#### warehouse_slow_query

- タイプ: Counter
- 説明: この FE ノード上でウェアハウス内で、レイテンシが FE 設定項目 `qe_slow_log_ms` を超えた成功クエリの総数。

#### warehouse_qps

- タイプ: Gauge
- 説明: この FE ノード上のウェアハウスの 1 秒あたりのクエリ数。直近の計算間隔（15 秒）における `warehouse_query_total` の増分から算出されます。

#### warehouse_rps

- タイプ: Gauge
- 説明: この FE ノード上のウェアハウスの 1 秒あたりのリクエスト数。直近の計算間隔（15 秒）における `warehouse_request_total` の増分から算出されます。

#### warehouse_query_latency

- タイプ: Gauge
- 説明: この FE ノード上でウェアハウス内に直近の計算間隔（15 秒）に完了した成功クエリのレイテンシ（ミリ秒単位）。`type` ラベルの値はクラスタ全体の `query_latency` メトリクスと同じです。
  - `mean`: 平均レイテンシ。
  - `50_quantile`: 中央値のレイテンシ。
  - `75_quantile`: 75 パーセンタイルのレイテンシ。
  - `90_quantile`: 90 パーセンタイルのレイテンシ。
  - `95_quantile`: 95 パーセンタイルのレイテンシ。
  - `99_quantile`: 99 パーセンタイルのレイテンシ。
  - `999_quantile`: 99.9 パーセンタイルのレイテンシ。

#### warehouse_query_latency_ms

- タイプ: Summary
- 説明: FE の起動以降にこの FE ノード上でウェアハウス内に成功したすべてのクエリのレイテンシ（ミリ秒単位）。`quantile` ラベル（`0.75`、`0.95`、`0.98`、`0.99`、`0.999`）に加えて `_sum` と `_count` の系列を公開し、`rate()` と組み合わせて任意の時間窓の平均レイテンシを算出できます。

## 使用例

### ウェアハウスのクエリキューの状態をモニタリングする

これらのメトリクスを使用して、ウェアハウスの健康状態とパフォーマンスをモニタリングできます。

FE の HTTP ポートの Prometheus エンドポイントでは、このメトリクスは `starrocks_fe_warehouse_query_queue` という名前で公開されます。接頭辞のない `warehouse_query_queue` は内部メトリクス名であり、[メトリクス例](#メトリクス例) に示す JSON 出力にのみ現れます。

```promql
# すべてのウェアハウスで保留中のクエリを確認
starrocks_fe_warehouse_query_queue{field="query_pending_length"}

# すべてのウェアハウスで実行中のクエリを確認
starrocks_fe_warehouse_query_queue{field="query_running_length"}

# スロットの利用状況をモニタリング
starrocks_fe_warehouse_query_queue{field="remain_slots"} / starrocks_fe_warehouse_query_queue{field="max_slots"}
```

### ウェアハウスのクエリスループットとレイテンシをモニタリングする

Prometheus エンドポイントでは、ウェアハウスごとのクエリメトリクスは `starrocks_fe_` の接頭辞付きで公開されます（例: `starrocks_fe_warehouse_query_total`）。カウンターは FE ノードごとの値なので、ウェアハウスのクラスタ全体の値を得るには FE ノード間で合計します。

```promql
# 各ウェアハウスの QPS（カウンターから算出、すべての FE ノードを合計）
sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_total[1m]))

# 各ウェアハウスの QPS（事前計算された Gauge、FE ノードごとに 1 系列）
starrocks_fe_warehouse_qps

# 各ウェアハウスのクエリ失敗率
sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_err[1m]))
  / sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_total[1m]))

# 各ウェアハウスの直近の計算間隔における P95 クエリレイテンシ
starrocks_fe_warehouse_query_latency{type="95_quantile"}

# 各ウェアハウスの直近 5 分間の平均クエリレイテンシ（Summary から算出）
sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_latency_ms_sum[5m]))
  / sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_latency_ms_count[5m]))
```

## 収集範囲

このページのメトリクスは 2 つの異なる方法で収集されるため、混ぜて `sum()` しないでください。

- `warehouse_query_queue` と [`warehouse_cngroup`](./metrics-warehouse_cngroup.md) は **Leader FE 上のみ** で収集され、`enable_collect_warehouse_metrics` で制御されます。クエリスケジューラに由来し、ウェアハウス全体を表すため、FE ノード間で合計してはいけません。
- ウェアハウスごとのクエリメトリクス（`warehouse_request_total`、`warehouse_query_*`、`warehouse_slow_query`、`warehouse_qps`、`warehouse_rps`）は **各 FE ノード上** でそのノードが実行したステートメントについて収集され、`enable_per_warehouse_query_metrics` で制御されます。FE ノード間で合計するとウェアハウスの合計になります。

`warehouse_cngroup` のクエリ数とレイテンシ（`success_queries_count`、`failed_queries_count`、`query_avg_latency_ms` など）とウェアハウスごとのクエリメトリクスは測定対象が異なり、一致することは期待できません。`warehouse_cngroup` は CN グループに割り当てられたクエリの完了時にスケジューラが記録し、`cngroup_name` で分割されます。ウェアハウスごとのクエリメトリクスは FE のステートメント層ですべてのステートメントについて記録され、CN グループに到達しないもの（解析エラー、メタデータのみのクエリ、SHOW ステートメント。これらは空の `cngroup_name` で報告されます）も含みます。スケジューラ視点の CN グループの状態（ノード数、実行中クエリ、CPU）には `warehouse_cngroup` を、クライアントが観測するスループット・エラー率・レイテンシにはウェアハウスごとのクエリメトリクスを、ウェアハウス単位または CN グループ単位で使用してください。

## メトリクスラベル

すべてのウェアハウスメトリクスには以下のラベルが含まれます。

- `warehouse_id`: ウェアハウスの一意の識別子
- `warehouse_name`: ウェアハウスの名前

ウェアハウスごとのクエリメトリクス（`warehouse_query_*`、`warehouse_slow_query`、`warehouse_qps`）には追加で `cngroup_name` ラベルが含まれます。また、`warehouse_query_queue` には `field` ラベルが、`warehouse_query_latency` には `type` ラベルが、`warehouse_query_latency_ms` には `quantile` ラベルが含まれます。

- `cngroup_name`: ステートメントを実行した CN グループ。CN グループを経由せずに実行されたステートメント（メタデータのみのステートメントや、コンピュートノードのないウェアハウスなど）では空です。`warehouse_request_total` と `warehouse_rps` はステートメントの解析前にカウントされるためこのラベルを持ちません。ウェアハウスレベルの値を得るには、クエリメトリクスを `cngroup_name` で合計してください。
- `field`: 測定されている特定のフィールド（上記のリスト参照）
- `type`: 報告されるレイテンシの統計量（上記のリスト参照）
- `quantile`: Summary のパーセンタイル（`0.75`、`0.95`、`0.98`、`0.99`、`0.999`）

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

例 2: ウェアハウスごとのクエリメトリクスのデモ:

```Plain
{"tags":{"metric":"warehouse_request_total","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"requests","value":1610},
{"tags":{"metric":"warehouse_query_total","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":1532},
{"tags":{"metric":"warehouse_query_err","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":7},
{"tags":{"metric":"warehouse_query_timeout","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":1},
{"tags":{"metric":"warehouse_query_analysis_err","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":5},
{"tags":{"metric":"warehouse_query_internal_err","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":1},
{"tags":{"metric":"warehouse_slow_query","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":3},
{"tags":{"metric":"warehouse_qps","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":12.6},
{"tags":{"metric":"warehouse_rps","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":13.2},
{"tags":{"metric":"warehouse_query_latency","type":"mean","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":42.5},
{"tags":{"metric":"warehouse_query_latency","type":"50_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":31.0},
{"tags":{"metric":"warehouse_query_latency","type":"75_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":58.0},
{"tags":{"metric":"warehouse_query_latency","type":"90_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":95.0},
{"tags":{"metric":"warehouse_query_latency","type":"95_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":120.0},
{"tags":{"metric":"warehouse_query_latency","type":"99_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":250.0},
{"tags":{"metric":"warehouse_query_latency","type":"999_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":900.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.75"},"unit":"milliseconds","value":58.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.95"},"unit":"milliseconds","value":120.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.98"},"unit":"milliseconds","value":180.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.99"},"unit":"milliseconds","value":250.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.999"},"unit":"milliseconds","value":900.0},
{"tags":{"metric":"warehouse_query_latency_ms_sum","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":64812.5},
{"tags":{"metric":"warehouse_query_latency_ms_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":1525},
```
