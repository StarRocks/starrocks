---
displayed_sidebar: docs
---

# 共有データクラスタの監視メトリクス

StarRocks は、共有データクラスタ用に 2 つのダッシュボードテンプレートを提供しています。

- [共有データダッシュボード](#shared-data-dashboard)
- [Starlet ダッシュボード](#starlet-dashboard)

## 共有データダッシュボード

共有データダッシュボードには、次のカテゴリの監視メトリクスが含まれています。

- [Publish Version](#publish-version)
- [Metadata](#metadata)
- [Metacache](#metacache)
- [Vacuum](#vacuum)
- [Loading](#loading)

### Publish Version

#### レイテンシー / QPS

- 説明: Public Version タスクの分位レイテンシー、平均レイテンシー、および QPS。

#### キューに入ったタスク

- 説明: キュー内の Public Version タスクの数。

### Metadata

#### Get Tablet Metadata

- 説明: Get Tablet Metadata タスクの分位レイテンシー、平均レイテンシー、および QPS。

#### Put Tablet Metadata

- 説明: Put Tablet Metadata タスクの分位レイテンシー、平均レイテンシー、および QPS。

#### Get Txn Log

- 説明: Get Txn Log タスクの分位レイテンシー、平均レイテンシー、および QPS。

#### Put Txn Log

- 説明: Put Txn Log タスクの分位レイテンシー、平均レイテンシー、および QPS。

### Metacache

#### Metacache 使用率

- 説明: Metacache の利用率。

#### Delvec Cache ミス毎分

- 説明: 毎分の Delvec Cache のキャッシュミスの数。

#### Metadata Cache ミス毎分

- 説明: 毎分の Metadata Cache のキャッシュミスの数。

#### Txn Log Cache ミス毎分

- 説明: 毎分の Txn Log Cache のキャッシュミスの数。

#### Segment Cache ミス毎分

- 説明: 毎分の Segment Cache のキャッシュミスの数。

### Vacuum

#### Vacuum Deletes

- 説明: Vacuum Deletes タスクの分位レイテンシー、平均レイテンシー、および QPS。

#### エラー

- 説明: 失敗した Vacuum Deletes 操作の数。

### Loading

#### キューサイズ

- 説明: BE Async Delta Writer のキューサイズ。

## Starlet ダッシュボード

Starlet ダッシュボードには、次のカテゴリの監視メトリクスが含まれています。

- [FSLIB READ IO METRICS](#fslib-read-io-metrics)
- [FSLIB WRITE IO METRICS](#fslib-write-io-metrics)
- [S3 IO METRICS](#s3-io-metrics)
- [FSLIB CACHE METRICS](#fslib-cache-metrics)
- [FSLIB FS METRICS](#fslib-fs-metrics)

### FSLIB READ IO METRICS

#### fslib read io_latency (quantile)

- タイプ: ヒストグラム
- 説明: S3 読み取りの分位レイテンシー。

#### fslib read io_latency (average)

- タイプ: カウンター
- 説明: S3 読み取りの平均レイテンシー。

#### fslib total read data

- タイプ: カウンター
- 説明: S3 読み取りの合計データサイズ。

#### fslib read iosize (quantile)

- タイプ: ヒストグラム
- 説明: S3 読み取りの分位 I/O サイズ。

#### fslib read iosize (average)

- タイプ: カウンター
- 説明: S3 読み取りの平均 I/O サイズ。

#### fslib read throughput

- タイプ: カウンター
- 説明: S3 読み取りの毎秒 I/O スループット。

#### fslib read iops

- タイプ: カウンター
- 説明: S3 読み取りの毎秒 I/O 操作数。

### FSLIB WRITE IO METRICS

#### fslib write io_latency (quantile)

- タイプ: ヒストグラム
- 説明: アプリケーション書き込みの分位レイテンシー。この値はバッファに書き込まれたデータのみを監視するため、低く表示される場合があります。

#### fslib write io_latency (average)

- タイプ: カウンター
- 説明: アプリケーション書き込みの平均レイテンシー。この値はバッファに書き込まれたデータのみを監視するため、低く表示される場合があります。

#### fslib total write data

- タイプ: カウンター
- 説明: アプリケーション書き込みの合計データサイズ。

#### fslib write iosize (quantile)

- タイプ: ヒストグラム
- 説明: アプリケーション書き込みの分位 I/O サイズ。

#### fslib write iosize (average)

- タイプ: カウンター
- 説明: アプリケーション書き込みの平均 I/O サイズ。

#### fslib write throughput

- タイプ: カウンター
- 説明: アプリケーション書き込みの毎秒 I/O スループット。

### S3 IO METRICS

#### fslib s3 single upload iops

- タイプ: カウンター
- 説明: S3 Put Object の毎秒呼び出し数。

#### fslib s3 single upload iosize (quantile)

- タイプ: ヒストグラム
- 説明: S3 Put Object の分位 I/O サイズ。

#### fslib s3 single upload latency (quantile)

- タイプ: ヒストグラム
- 説明: S3 Put Object の分位レイテンシー。

#### fslib s3 multi upload iops

- タイプ: カウンター
- 説明: S3 Multi Upload Object の毎秒呼び出し数。

#### fslib s3 multi upload iosize (quantile)

- タイプ: ヒストグラム
- 説明: S3 Multi Upload Object の分位 I/O サイズ。

#### fslib s3 multi upload latency (quantile)

- タイプ: ヒストグラム
- 説明: S3 Multi Upload Object の分位レイテンシー。

#### fslib s3 complete multi upload latency (quantile)

- タイプ: ヒストグラム
- 説明: S3 Complete Multi Upload Object の分位レイテンシー。

### FSLIB CACHE METRICS

#### fslib cache hit ratio

- タイプ: カウンター
- 説明: キャッシュヒット率。

#### fslib cache hits/misses

- タイプ: カウンター
- 説明: 毎秒のキャッシュヒット/ミスの数。

### FSLIB FS METRICS

#### fslib alive fs instances count

- タイプ: ゲージ
- 説明: 生存しているファイルシステムインスタンスの数。

#### fslib open files

- タイプ: カウンター
- 説明: 開かれたファイルの累積数。

#### fslib create files

- タイプ: カウンター
- 説明: 毎秒作成されるファイルの平均数。

#### filesystem meta operations

- タイプ: カウンター
- 説明: 毎秒のディレクトリリスト操作の平均数。

#### fslib async caches

- タイプ: カウンター
- 説明: 非同期キャッシュ内のファイルの累積数。

#### fslib create files (TOTAL)

- タイプ: カウンター
- 説明: 作成されたファイルの累積数。

#### fslib async tasks

- タイプ: カウンター
- 説明: キュー内の非同期タスクの累積数。