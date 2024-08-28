---
displayed_sidebar: docs
---

# 存算分离集群监控指标

StarRocks 为存算分离集群提供以下两种 Dashboard 模板：

- [Shared-data Dashboard](#shared-data-dashboard-监控项)
- [Starlet Dashboard](#starlet-dashboard-监控项)

## Shared-data Dashboard 监控项

Shared-data Dashboard 包括以下监控指标类别：

- [Publish Version](#publish-version)
- [Metadata](#metadata)
- [Metacache](#metacache)
- [Vacuum](#vacuum)
- [Loading](#loading)

### Publish Version

#### Latency / QPS

- 描述 Public Version 任务的 Quantile 分位数延迟、平均延迟和 QPS。

#### Queued Tasks

- 描述 Public Version 队列中的任务数量。

### Metadata

#### Get Tablet Metadata

- 描述 Get Tablet Metadata 任务的 Quantile 分位数延迟、平均延迟和 QPS。

#### Put Tablet Metadata

- 描述 Put Tablet Metadata 任务的 Quantile 分位数延迟、平均延迟和 QPS。

#### Get Txn Log

- 描述 Get Txn Log 任务的 Quantile 分位数延迟、平均延迟和 QPS。

#### Put Txn Log

- 描述 Put Txn Log任务的 Quantile 分位数延迟、平均延迟和 QPS。

### Metacache

#### Metacache Usage

- 描述 Metacache 利用率。

#### Delvec Cache Miss Per Minute

- 描述 Delvec Cache 每分钟 Miss 的次数。

#### Metadata Cache Miss Per Minute

- 描述 Metadata Cache 每分钟 Miss 的次数。

#### Txn Log Cache Miss Per Minute

- 描述 Txn Log Cache 每分钟 Miss 的次数。

#### Segment Cache Miss Per Minute

- 描述 Segment Cache 每分钟 Miss 的次数。

### Vacuum

#### Vacuum Deletes

- 描述 Vacuum Deletes 任务的 Quantile 分位数延迟、平均延迟和 QPS。

#### Errors

- 描述 Vacuum Deletes 操作失败的次数。

### Loading

#### Queue Size

- 描述 BE Async Delta Writer 的队列长度。

## Starlet Dashboard 监控项

Starlet Dashboard 包括以下监控指标类别：

- [FSLIB READ IO METRICS](#fslib-read-io-metrics)
- [FSLIB WRITE IO METRICS](#fslib-write-io-metrics)
- [S3 IO METRICS](#s3-io-metrics)
- [FSLIB CACHE METRICS](#fslib-cache-metrics)
- [FSLIB FS METRICS](#fslib-fs-metrics)

### FSLIB READ IO METRICS

#### fslib read io_latency (quantile)

- 类型：Histogram
- 描述 S3 读取的 Quantile 分位延迟。

#### fslib read io_latency (average)

- 类型：Counter
- 描述 S3 读取的平均延迟。

#### fslib total read data

- 类型：Counter
- 描述 S3 读取的总数据大小。

#### fslib read iosize (quantile)

- 类型：Histogram
- 描述 S3 读取的 Quantile 分位 I/O 大小。

#### fslib read iosize (average)

- 类型：Counter
- 描述 S3 读取的平均 I/O 大小。

#### fslib read throughput

- 类型：Counter
- 描述 S3 读取的每秒 I/O 吞吐量。

#### fslib read iops

- 类型：Counter
- 描述 S3 读取的每秒 I/O 操作次数。

### FSLIB WRITE IO METRICS

#### fslib write io_latency (quantile)

- 类型：Histogram
- 描述 应用写入的 Quantile 分位延迟。请注意，该值仅监控写入缓冲区的数据，因此观察到的值会相比实际较低。

#### fslib write io_latency (average)

- 类型：Counter
- 描述 应用写入的平均延迟。请注意，该值仅监控写入缓冲区的数据，因此观察到的值会相比实际较低。

#### fslib total write data

- 类型：Counter
- 描述 应用写入的总数据大小。

#### fslib write iosize (quantile)

- 类型：Histogram
- 描述 应用写入的 Quantile 分位 I/O 大小。

#### fslib write iosize (average)

- 类型：Counter
- 描述 应用写入的平均 I/O 大小。

#### fslib write throughput

- 类型：Counter
- 描述 应用写入的每秒 I/O 吞吐量。

### S3 IO METRICS

#### fslib s3 single upload iops

- 类型：Counter
- 描述 S3 Put Object 的每秒调用次数。

#### fslib s3 single upload iosize (quantile)

- 类型：Histogram
- 描述 S3 Put Object 的 Quantile 分位 I/O 大小。

#### fslib s3 single upload latency (quantile)

- 类型：Histogram
- 描述 S3 Put Object 的 Quantile 分位延迟。

#### fslib s3 multi upload iops

- 类型：Counter
- 描述 S3 Multi Upload Object 的每秒调用次数。

#### fslib s3 multi upload iosize (quantile)

- 类型：Histogram
- 描述 S3 Multi Upload Object 的 Quantile 分位 I/O 大小。

#### fslib s3 multi upload latency (quantile)

- 类型：Histogram
- 描述 S3 Multi Upload Object 的 Quantile 分位延迟。

#### fslib s3 complete multi upload latency (quantile)

- 类型：Histogram
- 描述 S3 Complete Multi Upload Object 的 Quantile 分位延迟。

### FSLIB CACHE METRICS

#### fslib cache hit ratio

- 类型：Counter
- 描述 缓存命中率。

#### fslib cache hits/misses

- 类型：Counter
- 描述 每秒的缓存命中/未命中次数。

### FSLIB FS METRICS

#### fslib alive fs instances count

- 类型：Gauge
- 描述 存活的文件系统实例数量。

#### fslib open files

- 类型：Counter
- 描述 累计打开文件的数量。

#### fslib create files

- 类型：Counter
- 描述 每秒平均创建的文件数量。

#### filesystem meta operations

- 类型：Counter
- 描述 每秒平均 List 文件目录操作的次数。

#### fslib async caches

- 类型：Counter
- 描述 异步缓存中的文件累计数量。

#### fslib create files (TOTAL)

- 类型：Counter
- 描述 累计创建的文件数量。

#### fslib async tasks

- 类型：Counter
- 描述 队列中异步任务的累计数量。
