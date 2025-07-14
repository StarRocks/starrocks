---
displayed_sidebar: docs
---

# ウェアハウス CNGroup メトリクス

v4.0 以降、StarRocks は倉庫内の Compute Node Group (CN Group) を監視および管理するためのさまざまなメトリクスを提供しています。

## メトリクス項目

### warehouse_cngroup

- タイプ: Gauge/Counter
- 説明: CN Group のパフォーマンスと健康状態のさまざまな側面を監視するための異なるフィールドラベルを持つ Warehouse Compute Node Group メトリクス。

#### フィールドラベル

##### cngroup_nodes_count

- タイプ: Gauge
- 説明: CN Group 内のコンピュートノードの総数。

##### cngroup_alive_nodes_count

- タイプ: Gauge
- 説明: CN Group 内で稼働中のコンピュートノードの数。

##### running_queries_count

- タイプ: Gauge
- 説明: 現在の FE で CN Group 内で実行中のクエリの数。

##### cngroup_status

- タイプ: Gauge
- 説明: CN Group のステータス。 有効な値: `0` (無効) および `1` (有効)。

##### scheduled_queries_count

- タイプ: Counter
- 説明: CN Group にスケジュールされたクエリの総数。

##### success_queries_count

- タイプ: Counter
- 説明: CN Group 内で正常に実行されたクエリの総数。

##### failed_queries_count

- タイプ: Counter
- 説明: CN Group 内で失敗したクエリの総数。

##### query_max_latency_ms

- タイプ: Gauge
- 説明: CN Group の最大クエリ遅延時間（ミリ秒単位）。

##### query_avg_latency_ms

- タイプ: Gauge
- 説明: CN Group の平均クエリ遅延時間（ミリ秒単位）。

##### avg_cpu_used_permille

- タイプ: Gauge
- 説明: CN Group 内のすべてのコンピュートノードにおける平均 CPU 使用率（パーミル単位）。 値が無効または利用できない場合は `-1.0` が返されます。

##### max_compute_node_running_queries_count

- タイプ: Gauge
- 説明: CN Group 内のすべてのコンピュートノードで実行中のクエリの最大数。 値が無効または利用できない場合は `-1` が返されます。

## 使用例

### CN Group の健康状態とパフォーマンスの監視

これらのメトリクスを使用して、倉庫 CN Groups の健康状態とパフォーマンスを監視できます。

```promql
# CN Group 内のノードの可用性を確認
warehouse_cngroup{field="cngroup_alive_nodes_count"} / warehouse_cngroup{field="cngroup_nodes_count"}

# CN Group のステータスを監視
warehouse_cngroup{field="cngroup_status"}

# クエリの成功率を確認
warehouse_cngroup{field="success_queries_count"} / warehouse_cngroup{field="scheduled_queries_count"}

# クエリ遅延を監視
warehouse_cngroup{field="query_avg_latency_ms"}

# CPU 使用率を確認
warehouse_cngroup{field="avg_cpu_used_permille"} / 10
```

## メトリクスラベル

すべての倉庫 CN Group メトリクスには以下のラベルが含まれています。

- `warehouse_id`: 倉庫の一意の識別子
- `warehouse_name`: 倉庫の名前
- `cngroup_name`: CN Group の名前
- `field`: 測定されている特定のフィールド（上記のリストに記載）

これらのラベルを使用して、特定の倉庫や CN Groups ごとにメトリクスをフィルタリングおよびグループ化し、それぞれのパフォーマンス特性を監視できます。

## パフォーマンスに関する考慮事項

- CN Group リソース使用メトリクスは、過剰な計算を避けるために 1 秒間キャッシュされます
- CPU 使用メトリクスは、値が無効、null、または NaN の場合に `-1.0` を返します
- 最大実行クエリ数は、値が無効または利用できない場合に `-1` を返します
- クエリ遅延メトリクスは、スレッドセーフを確保するために原子的に更新されます

## メトリクス例

例 1: Warehouse CN Group メトリクスデモ:

```Plain
{"tags":{"metric":"warehouse_cngroup","field":"cngroup_nodes_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":2},
{"tags":{"metric":"warehouse_cngroup","field":"cngroup_alive_nodes_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":2},
{"tags":{"metric":"warehouse_cngroup","field":"running_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_cngroup","field":"cngroup_status","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":1},
{"tags":{"metric":"warehouse_cngroup","field":"scheduled_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":98},
{"tags":{"metric":"warehouse_cngroup","field":"success_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":83},
{"tags":{"metric":"warehouse_cngroup","field":"failed_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":15},
{"tags":{"metric":"warehouse_cngroup","field":"query_max_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":1485.0},
{"tags":{"metric":"warehouse_cngroup","field":"query_avg_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":54.255102040816325},
{"tags":{"metric":"warehouse_cngroup","field":"avg_cpu_used_permille","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":54.255102040816325},
{"tags":{"metric":"warehouse_cngroup","field":"max_compute_node_running_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":0},
```
