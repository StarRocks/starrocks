# Data lake-related FAQ

This topic describes some commonly asked questions (FAQ) about data lake and provides solutions to these issues. Some metrics mentioned in this topic can be obtained only from the profiles of the SQL queries. To obtain the profiles of SQL queries, you must specify `set enable_profile=true`.

## Slow HDFS nodes

### Problem description

When you access the data files stored in your HDFS cluster, you may find a huge difference between the values of the `__MAX_OF_FSIOTime` and `__MIN_OF_FSIOTime` metrics from the profiles of the SQL queries you run, which indicates that queries are slow on specific HDFS nodes. The following example is a typical profile that indicates an HDFS node slowdown issue:

```plaintext
 - InputStream: 0
   - AppIOBytesRead: 22.72 GB
     - __MAX_OF_AppIOBytesRead: 187.99 MB
     - __MIN_OF_AppIOBytesRead: 64.00 KB
   - AppIOCounter: 964.862K (964862)
     - __MAX_OF_AppIOCounter: 7.795K (7795)
     - __MIN_OF_AppIOCounter: 1
   - AppIOTime: 1s372ms
     - __MAX_OF_AppIOTime: 4s358ms
     - __MIN_OF_AppIOTime: 1.539ms
   - FSBytesRead: 15.40 GB
     - __MAX_OF_FSBytesRead: 127.41 MB
     - __MIN_OF_FSBytesRead: 64.00 KB
   - FSIOCounter: 1.637K (1637)
     - __MAX_OF_FSIOCounter: 12
     - __MIN_OF_FSIOCounter: 1
   - FSIOTime: 9s357ms
     - __MAX_OF_FSIOTime: 60s335ms
     - __MIN_OF_FSIOTime: 1.536ms
```

### Solution

You can use one of the following solutions to resolve this issue:

- **[Recommended]** Enable the [data cache](../data_source/data_cache.md) feature, which eliminates the impact of slow HDFS nodes on queries by automatically caching the data from external storage systems.
- Enable the [Hedged Read](https://hadoop.apache.org/docs/r2.8.3/hadoop-project-dist/hadoop-common/release/2.4.0/RELEASENOTES.2.4.0.html) feature. With this feature enabled, if a read from a block is slow, StarRocks starts up a new read, which runs in parallel to the original read, to read against a different block replica. Whenever one of the two reads returns, the other read is cancelled. **The Hedged Read feature can help accelerate reads, but it also significantly increases heap memory consumption on Java virtual machines (JVMs). Therefore, if your physical machines provide a small memory capacity, we recommend that you do not enable the Hedged Read feature.**

#### [Recommended] Data Cache

参见 [Data Cache](../data_source/data_cache.md)。

#### Hedged Read

Use the following parameters (supported from v3.1 onwards) in the BE configuration file `be.conf` to enable and configure the Hedged Read feature in your HDFS cluster.

| Parameter                                | Default value | Description                                                         |
| ---------------------------------------- | ------------- | ------------------------------------------------------------------- |
| hdfs_client_enable_hedged_read           | false         | Specifies whether to enable the hedged read feature.                                    |
| hdfs_client_hedged_read_threadpool_size  | 128           | Specifies the size of the Hedged Read thread pool on your client. The thread pool size limits the number of threads to dedicate to the running of hedged reads in your client. This parameter is equivalent to the `dfs.client.hedged.read.threadpool.size` parameter in the `hdfs-site.xml` file of your HDFS cluster. |
| hdfs_client_hedged_read_threshold_millis | 2500          | Specifies the number of milliseconds to wait before starting up a hedged read. For example, you have set this parameter to `30`. In this situation, if a read from a block has not returned within 30 milliseconds, StarRocks will start up a hedged read against a different block replica. This parameter is equivalent to the `dfs.client.hedged.read.threshold.millis` parameter in the `hdfs-site.xml` file of your HDFS cluster. |

If the value of any of the following metrics in your query profiles exceeds `0`, the Hedged Read feature is enabled.

| Metric                         | Description                                                  |
| ------------------------------ | ------------------------------------------------------------ |
| TotalHedgedReadOps             | The number of hedged reads that are started up.                 |
| TotalHedgedReadOpsInCurThread  | The number of times that StarRocks has to start up a hedged read in the current thread instead of in a new thread because the Hedged Read thread pool has reached its maximum size specified by the `hdfs_client_hedged_read_threadpool_size` parameter. |
| TotalHedgedReadOpsWin          | The number of times that a hedged read beats its original read. |
