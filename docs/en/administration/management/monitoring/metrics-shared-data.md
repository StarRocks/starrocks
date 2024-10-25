---
displayed_sidebar: docs
---

# Monitoring Metrics for Shared-data Clusters

StarRocks provides two Dashboard templates for shared-data clusters:

- [Shared-data Dashboard](#shared-data-dashboard)
- [Starlet Dashboard](#starlet-dashboard)

## Shared-data Dashboard

Shared-data Dashboard includes the following categories of monitoring metrics:

- [Publish Version](#publish-version)
- [Metadata](#metadata)
- [Metacache](#metacache)
- [Vacuum](#vacuum)
- [Loading](#loading)

### Publish Version

#### Latency / QPS

- Description: Quantile latency, average latency, and QPS of Public Version tasks.

#### Queued Tasks

- Description: The number of Public Version tasks in the queue.

### Metadata

#### Get Tablet Metadata

- Description: Quantile latency, average latency, and QPS of Get Tablet Metadata tasks.

#### Put Tablet Metadata

- Description: Quantile latency, average latency, and QPS of Put Tablet Metadata tasks.

#### Get Txn Log

- Description: Quantile latency, average latency, and QPS of Get Txn Log tasks.

#### Put Txn Log

- Description: Quantile latency, average latency, and QPS of Put Txn Log tasks.

### Metacache

#### Metacache Usage

- Description: Metacache utilization rate.

#### Delvec Cache Miss Per Minute

- Description: Number of cache misses in Delvec Cache per minute.

#### Metadata Cache Miss Per Minute

- Description: Number of cache misses in Metadata Cache per minute.

#### Txn Log Cache Miss Per Minute

- Description: Number of cache misses in Txn Log Cache per minute.

#### Segment Cache Miss Per Minute

- Description: Number of cache misses in Segment Cache per minute.

### Vacuum

#### Vacuum Deletes

- Description: Quantile latency, average latency, and QPS of Vacuum Deletes tasks.

#### Errors

- Description: Number of failed Vacuum Deletes operations.

### Loading

#### Queue Size

- Description: Queue size of BE Async Delta Writer.

## Starlet Dashboard

Starlet Dashboard includes the following categories of monitoring metrics:

- [FSLIB READ IO METRICS](#fslib-read-io-metrics)
- [FSLIB WRITE IO METRICS](#fslib-write-io-metrics)
- [S3 IO METRICS](#s3-io-metrics)
- [FSLIB CACHE METRICS](#fslib-cache-metrics)
- [FSLIB FS METRICS](#fslib-fs-metrics)

### FSLIB READ IO METRICS

#### fslib read io_latency (quantile)

- Type: Histogram
- Description: Quantile latency for S3 reads.

#### fslib read io_latency (average)

- Type: Counter
- Description: Average latency for S3 reads.

#### fslib total read data

- Type: Counter
- Description: Total data size for S3 reads.

#### fslib read iosize (quantile)

- Type: Histogram
- Description: Quantile I/O size for S3 reads.

#### fslib read iosize (average)

- Type: Counter
- Description: Average I/O size for S3 reads.

#### fslib read throughput

- Type: Counter
- Description: I/O throughput per second for S3 reads.

#### fslib read iops

- Type: Counter
- Description: Number of I/O operations per second for S3 reads.

### FSLIB WRITE IO METRICS

#### fslib write io_latency (quantile)

- Type: Histogram
- Description: Quantile latency for application writes. Please note that this value may appear lower because this metric monitors only data written to the buffer.

#### fslib write io_latency (average)

- Type: Counter
- Description: Average latency for application writes. Please note that this value may appear lower because this metric monitors only data written to the buffer.

#### fslib total write data

- Type: Counter
- Description: Total data size for application writes.

#### fslib write iosize (quantile)

- Type: Histogram
- Description: Quantile I/O size for application writes.

#### fslib write iosize (average)

- Type: Counter
- Description: Average I/O size for application writes.

#### fslib write throughput

- Type: Counter
- Description: I/O throughput per second for application writes.

### S3 IO METRICS

#### fslib s3 single upload iops

- Type: Counter
- Description: Number of invocations per second for S3 Put Object.

#### fslib s3 single upload iosize (quantile)

- Type: Histogram
- Description: Quantile I/O size for S3 Put Object.

#### fslib s3 single upload latency (quantile)

- Type: Histogram
- Description: Quantile latency for S3 Put Object.

#### fslib s3 multi upload iops

- Type: Counter
- Description: Number of invocations per second for S3 Multi Upload Object.

#### fslib s3 multi upload iosize (quantile)

- Type: Histogram
- Description: Quantile I/O size for S3 Multi Upload Object.

#### fslib s3 multi upload latency (quantile)

- Type: Histogram
- Description: Quantile latency for S3 Multi Upload Object.

#### fslib s3 complete multi upload latency (quantile)

- Type: Histogram
- Description: Quantile latency for S3 Complete Multi Upload Object.

### FSLIB CACHE METRICS

#### fslib cache hit ratio

- Type: Counter
- Description: Cache hit ratio.

#### fslib cache hits/misses

- Type: Counter
- Description: Number of cache hits/misses per second.

### FSLIB FS METRICS

#### fslib alive fs instances count

- Type: Gauge
- Description: Number of file system instances that are alive.

#### fslib open files

- Type: Counter
- Description: Cumulative number of opened files.

#### fslib create files

- Type: Counter
- Description: Average number of files created per second.

#### filesystem meta operations

- Type: Counter
- Description: Average number of directory listing operations per second.

#### fslib async caches

- Type: Counter
- Description: Cumulative number of files in asynchronous cache.

#### fslib create files (TOTAL)

- Type: Counter
- Description: Cumulative number of files created.

#### fslib async tasks

- Type: Counter
- Description: Cumulative number of asynchronous tasks in the queue.
