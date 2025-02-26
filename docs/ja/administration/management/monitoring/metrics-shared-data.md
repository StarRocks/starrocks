---
displayed_sidebar: docs
---

# 共有データクラスタの監視メトリクス

StarRocks は、共有データクラスタ用に 2 つのダッシュボードテンプレートを提供しています。

- [Shared-data Dashboard](#shared-data-dashboard)
- [Starlet Dashboard](#starlet-dashboard)

## Shared-data Dashboard

Shared-data Dashboard には、次のカテゴリの監視メトリクスが含まれています。

- [Publish Version](#publish-version)
- [Metadata](#metadata)
- [Metacache](#metacache)
- [Vacuum](#vacuum)
- [Loading](#loading)

### Publish Version

#### Latency / QPS

- 説明: Public Version タスクの分位数レイテンシー、平均レイテンシー、および QPS。

#### Queued Tasks

- 説明: キュー内の Public Version タスクの数。

### Metadata

#### Get Tablet Metadata

- 説明: Get Tablet Metadata タスクの分位数レイテンシー、平均レイテンシー、および QPS。

#### Put Tablet Metadata

- 説明: Put Tablet Metadata タスクの分位数レイテンシー、平均レイテンシー、および QPS。

#### Get Txn Log

- 説明: Get Txn Log タスクの分位数レイテンシー、平均レイテンシー、および QPS。

#### Put Txn Log

- 説明: Put Txn Log タスクの分位数レイテンシー、平均レイテンシー、および QPS。

### Metacache

#### Metacache Usage

- 説明: Metacache の利用率。

#### Delvec Cache Miss Per Minute

- 説明: Delvec Cache における 1 分あたりのキャッシュミスの数。

#### Metadata Cache Miss Per Minute

- 説明: Metadata Cache における 1 分あたりのキャッシュミスの数。

#### Txn Log Cache Miss Per Minute

- 説明: Txn Log Cache における 1 分あたりのキャッシュミスの数。

#### Segment Cache Miss Per Minute

- 説明: Segment Cache における 1 分あたりのキャッシュミスの数。

### Vacuum

#### Vacuum Deletes

- 説明: Vacuum Deletes タスクの分位数レイテンシー、平均レイテンシー、および QPS。

#### Errors

- 説明: 失敗した Vacuum Deletes 操作の数。

### Loading

#### Queue Size

- 説明: BE Async Delta Writer のキューサイズ。

## Starlet Dashboard

Starlet Dashboard には、次のカテゴリの監視メトリクスが含まれています。

- [FSLIB READ IO METRICS](#fslib-read-io-metrics)
- [FSLIB WRITE IO METRICS](#fslib-write-io-metrics)
- [S3 IO METRICS](#s3-io-metrics)
- [FSLIB CACHE METRICS](#fslib-cache-metrics)
- [FSLIB FS METRICS](#fslib-fs-metrics)

### FSLIB READ IO METRICS

#### fslib read io_latency (quantile)

- タイプ: ヒストグラム
- 説明: S3 読み取りの分位数レイテンシー。

#### fslib read io_latency (average)

- タイプ: カウンター
- 説明: S3 読み取りの平均レイテンシー。

#### fslib total read data

- タイプ: カウンター
- 説明: S3 読み取りのデータ総サイズ。

#### fslib read iosize (quantile)

- タイプ: ヒストグラム
- 説明: S3 読み取りの分位数 I/O サイズ。

#### fslib read iosize (average)

- タイプ: カウンター
- 説明: S3 読み取りの平均 I/O サイズ。

#### fslib read throughput

- タイプ: カウンター
- 説明: S3 読み取りの 1 秒あたりの I/O スループット。

#### fslib read iops

- タイプ: カウンター
- 説明: S3 読み取りの 1 秒あたりの I/O 操作数。

### FSLIB WRITE IO METRICS

#### fslib write io_latency (quantile)

- タイプ: ヒストグラム
- 説明: アプリケーション書き込みの分位数レイテンシー。この値は、バッファに書き込まれたデータのみを監視するため、低く見える場合があります。

#### fslib write io_latency (average)

- タイプ: カウンター
- 説明: アプリケーション書き込みの平均レイテンシー。この値は、バッファに書き込まれたデータのみを監視するため、低く見える場合があります。

#### fslib total write data

- タイプ: カウンター
- 説明: アプリケーション書き込みのデータ総サイズ。

#### fslib write iosize (quantile)

- タイプ: ヒストグラム
- 説明: アプリケーション書き込みの分位数 I/O サイズ。

#### fslib write iosize (average)

- タイプ: カウンター
- 説明: アプリケーション書き込みの平均 I/O サイズ。

#### fslib write throughput

- タイプ: カウンター
- 説明: アプリケーション書き込みの 1 秒あたりの I/O スループット。

### S3 IO METRICS

#### fslib s3 single upload iops

- タイプ: カウンター
- 説明: S3 Put Object の 1 秒あたりの呼び出し回数。

#### fslib s3 single upload iosize (quantile)

- タイプ: ヒストグラム
- 説明: S3 Put Object の分位数 I/O サイズ。

#### fslib s3 single upload latency (quantile)

- タイプ: ヒストグラム
- 説明: S3 Put Object の分位数レイテンシー。

#### fslib s3 multi upload iops

- タイプ: カウンター
- 説明: S3 Multi Upload Object の 1 秒あたりの呼び出し回数。

#### fslib s3 multi upload iosize (quantile)

- タイプ: ヒストグラム
- 説明: S3 Multi Upload Object の分位数 I/O サイズ。

#### fslib s3 multi upload latency (quantile)

- タイプ: ヒストグラム
- 説明: S3 Multi Upload Object の分位数レイテンシー。

#### fslib s3 complete multi upload latency (quantile)

- タイプ: ヒストグラム
- 説明: S3 Complete Multi Upload Object の分位数レイテンシー。

### FSLIB CACHE METRICS

#### fslib cache hit ratio

- タイプ: カウンター
- 説明: キャッシュヒット率。

#### fslib cache hits/misses

- タイプ: カウンター
- 説明: 1 秒あたりのキャッシュヒット/ミスの数。

### FSLIB FS METRICS

#### fslib alive fs instances count

- タイプ: ゲージ
- 説明: 稼働中のファイルシステムインスタンスの数。

#### fslib open files

- タイプ: カウンター
- 説明: 開かれたファイルの累積数。

#### fslib create files

- タイプ: カウンター
- 説明: 1 秒あたりに作成されるファイルの平均数。

#### filesystem meta operations

- タイプ: カウンター
- 説明: 1 秒あたりのディレクトリリスト操作の平均数。

#### fslib async caches

- タイプ: カウンター
- 説明: 非同期キャッシュ内のファイルの累積数。

#### fslib create files (TOTAL)

- タイプ: カウンター
- 説明: 作成されたファイルの累積数。

#### fslib async tasks

- タイプ: カウンター
- 説明: キュー内の非同期タスクの累積数。