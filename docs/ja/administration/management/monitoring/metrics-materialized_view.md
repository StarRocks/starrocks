---
displayed_sidebar: docs
---

# 非同期マテリアライズドビューの監視メトリクス

v3.1以降、StarRocksは非同期マテリアライズドビューのメトリクスをサポートしています。

Prometheusがクラスター内のマテリアライズドビューのメタデータにアクセスできるようにするには、Prometheusの設定ファイル **prometheus/prometheus.yml** に次の設定を追加する必要があります。

```YAML
global:
....
scrape_configs:

  - job_name: 'dev' 
    metrics_path: '/metrics'    
    # 次の設定を追加します。
    basic_auth:
      username: 'root'
      password: ''
    params:
      'with_materialized_view_metrics' : ['all']   
....
```

- `username`: StarRocksクラスターにログインするためのユーザー名。このユーザーには`user_admin`ロールが付与されている必要があります。
- `password`: StarRocksクラスターにログインするためのパスワード。
- `'with_materialized_view_metrics'`: 収集するメトリクスの範囲。有効な値:
  - `'all'`: マテリアライズドビューに関連するすべてのメトリクスが収集されます。
  - `'minified'`: ゲージメトリクスおよび値が`0`のメトリクスは収集されません。

## メトリクス項目

### mv_refresh_jobs

- Type: Counter
- Description: マテリアライズドビューのリフレッシュジョブの総数。

### mv_refresh_total_success_jobs

- Type: Counter
- Description: マテリアライズドビューの成功したリフレッシュジョブの数。

### mv_refresh_total_failed_jobs

- Type: Counter
- Description: マテリアライズドビューの失敗したリフレッシュジョブの数。

### mv_refresh_total_empty_jobs

- Type: Counter
- Description: リフレッシュするデータが空であるためにキャンセルされたマテリアライズドビューのリフレッシュジョブの数。

### mv_refresh_total_retry_meta_count

- Type: Counter
- Description: マテリアライズドビューのリフレッシュジョブがベーステーブルが更新されているかどうかを確認する回数。

### mv_query_total_count

- Type: Counter
- Description: クエリの前処理でマテリアライズドビューが使用された回数。

### mv_query_total_hit_count

- Type: Counter
- Description: クエリプランでマテリアライズドビューがクエリをリライトできると見なされた回数。この値は、最終的なクエリプランが高コストのためにリライトをスキップすることがあるため、高く表示されることがあります。

### mv_query_total_considered_count

- Type: Counter
- Description: マテリアライズドビューがクエリをリライトした回数（マテリアライズドビューに対する直接のクエリを除く）。

### mv_query_total_matched_count

- Type: Counter
- Description: クエリの最終プランにマテリアライズドビューが関与した回数（マテリアライズドビューに対する直接のクエリを含む）。

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
- Description: 成功したマテリアライズドビューのリフレッシュジョブの期間。