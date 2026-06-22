---
displayed_sidebar: docs
description: "v3.1以降、StarRocks は非同期マテリアライズドビューのメトリクスを Prometheus で監視できます。"
sidebar_position: 20
---

# 非同期マテリアライズドビューの監視メトリクス

v3.1以降、StarRocksは非同期マテリアライズドビューのメトリクスをサポートしています。

Prometheusがクラスター内のマテリアライズドビューのメタデータにアクセスできるようにするには、Prometheusの設定ファイル **prometheus/prometheus.yml** に以下の設定を追加する必要があります。

```YAML
global:
....
scrape_configs:

  - job_name: 'dev' 
    metrics_path: '/metrics'    
    # 以下の設定を追加します。
    basic_auth:
      username: 'root'
      password: ''
    params:
      'with_materialized_view_metrics' : ['all']   
....
```

- `username`: StarRocksクラスターにログインするためのユーザー名。rootアカウントを使用しない場合、ユーザーには`user_admin`と`db_admin`の両方のロールが付与されている必要があります。
- `password`: StarRocksクラスターにログインするためのパスワード。
- `'with_materialized_view_metrics'`: 収集するメトリクスの範囲。有効な値:
  - `'all'`: マテリアライズドビューに関連するすべてのメトリクスが収集されます。
  - `'minified'`: ゲージメトリクスと値が`0`のメトリクスは収集されません。

## メトリクス項目

### mv_refresh_jobs

- Type: Counter
- Description: マテリアライズドビューに対してトリガーされたリフレッシュジョブの総数。1つのリフレッシュジョブはユーザー起動またはスケジュールによる1回のリフレッシュに対応し、内部で複数の Task Run が実行される場合がある。各ジョブは終了状態に達した時点で1回カウントされる。MERGED 状態の Task Run（後のバッチにマージされたサブタスク）はカウントされない。

### mv_refresh_total_success_jobs

- Type: Counter
- Description: 正常完了したリフレッシュジョブの数。ジョブ成功時に1回カウントされる。

### mv_refresh_total_failed_jobs

- Type: Counter
- Description: 失敗したリフレッシュジョブの数。ジョブ失敗時に1回カウントされる。

### mv_refresh_total_empty_jobs

- Type: Counter
- Description: リフレッシュするデータが空のためキャンセルされたマテリアライズドビューのリフレッシュジョブの数。

### mv_refresh_total_retry_meta_count

- Type: Counter
- Description: マテリアライズドビューのリフレッシュジョブがベーステーブルの更新を確認する回数。

### mv_query_total_count

- Type: Counter
- Description: クエリの前処理でマテリアライズドビューが使用された回数。

### mv_query_total_hit_count

- Type: Counter
- Description: クエリプランでマテリアライズドビューがクエリを書き換え可能と見なされた回数。この値は、最終的なクエリプランが高コストのため書き換えをスキップする場合があるため、より高く表示されることがあります。

### mv_query_total_considered_count

- Type: Counter
- Description: マテリアライズドビューがクエリを書き換えた回数（マテリアライズドビューに対する直接クエリを除く）。

### mv_query_total_matched_count

- Type: Counter
- Description: マテリアライズドビューがクエリの最終プランに関与した回数（マテリアライズドビューに対する直接クエリを含む）。

### mv_refresh_pending_jobs

- Type: Gauge
- Description: 現在保留中のマテリアライズドビューのリフレッシュジョブの数。

### mv_refresh_running_jobs

- Type: Gauge
- Description: 現在実行中のマテリアライズドビューのリフレッシュジョブの数。

### mv_row_count

- Type: Gauge
- Description: マテリアライズドビューの行数。

### mv_storage_size

- Type: Gauge
- Description: マテリアライズドビューのサイズ。単位: バイト。

### mv_inactive_state

- Type: Gauge
- Description: マテリアライズドビューのステータス。有効な値: `0`（アクティブ）および`1`（非アクティブ）。

### mv_partition_count

- Type: Gauge
- Description: マテリアライズドビューのパーティション数。マテリアライズドビューがパーティション化されていない場合、値は`0`です。

### mv_refresh_duration

- Type: Histogram
- Description: リフレッシュジョブの実時間（ミリ秒）。マルチバッチジョブの場合、最初の Task Run 開始から最後の Task Run 完了までを計測する。

### mv_global_count

- Type: Gauge
- Description: クラスター内の非同期マテリアライズドビューの現在の数。ラベル `refresh_mode`（マテリアライズドビューのリフレッシュモード）と `status`（`ACTIVE` または `INACTIVE`）を持ちます。このメトリクスは、マテリアライズドビューごとのメトリクス権限に関係なく常に出力されます。

### mv_global_query_rewrite_queries_total

- Type: Counter
- Description: マテリアライズドビューの書き換え結果でグループ化されたクエリ数。ラベル `state`：`HIT`（クエリがマテリアライズドビューを使用するように書き換えられた）、`NO_HIT`（書き換えは有効だがマテリアライズドビューが使用されなかった）、`DISABLED`（セッション変数または FE 設定でマテリアライズドビューの書き換えが無効化されている）。クエリごとに 1 回カウントされます。

### mv_global_query_mv_usage_total

- Type: Counter
- Description: クエリによってマテリアライズドビューが使用された回数。ラベル `usage_type`（`REWRITE` はクエリがマテリアライズドビューを使用するように書き換えられた場合、`DIRECT` はマテリアライズドビューを直接クエリする場合）と `refresh_mode` を持ちます。
