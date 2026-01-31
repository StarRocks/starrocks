---
displayed_sidebar: docs
---

# Data Cache の可観測性

以前のバージョンでは、[Data Cache](./data_cache.md) のパフォーマンス、使用状況、健康状態を監視するための豊富なメトリクスや効率的な方法がありませんでした。

v3.3 では、StarRocks は Data Cache の可観測性を向上させ、効率的な監視方法とより多くのメトリクスを提供します。ユーザーは、データキャッシュのディスクとメモリの使用状況、関連メトリクスを確認でき、キャッシュ使用状況の監視が強化されます。

> **NOTE**
>
> v3.4.0 以降、external catalogs とクラウドネイティブテーブル（共有データクラスタ内）に対するクエリは、統一された Data Cache インスタンスを使用します。したがって、特に指定がない限り、以下の方法は、external catalogs とクラウドネイティブテーブルに対するクエリのキャッシュ使用量を含む、Data Cache インスタンス自体のメトリクスを表示することをデフォルトとします。

## SQL コマンド

各 BE ノードで Data Cache の容量と使用状況を確認するために SQL コマンドを実行できます。

### SHOW BACKENDS

`DataCacheMetrics` フィールドは、特定の BE 上の Data Cache の使用ディスクとメモリスペースを記録します。

```SQL
mysql> show backends\G
*************************** 1. row ***************************
            BackendId: 10004
                   IP: XXX.XX.XX.XXX
        HeartbeatPort: 4450
               BePort: 4448
             HttpPort: 4449
             BrpcPort: 4451
        LastStartTime: 2023-12-13 20:09:30
        LastHeartbeat: 2023-12-13 20:10:43
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 48
     DataUsedCapacity: 0.000 B
        AvailCapacity: 280.103 GB
        TotalCapacity: 1.968 TB
              UsedPct: 86.10 %
       MaxDiskUsedPct: 86.10 %
               ErrMsg:
              Version: datacache-heartbeat-c68caf7
               Status: {"lastSuccessReportTabletsTime":"2023-12-13 20:10:38"}
    DataTotalCapacity: 280.103 GB
          DataUsedPct: 0.00 %
             CpuCores: 104
    NumRunningQueries: 0
           MemUsedPct: 0.00 %
           CpuUsedPct: 0.0 %
     -- highlight-start
     DataCacheMetrics: Status: Normal, DiskUsage: 0.00GB/2.00GB, MemUsage: 0.00GB/30.46GB
     -- highlight-end
1 row in set (1.90 sec)
```

### information_schema

`information_schema` の `be_datacache_metrics` ビューは、以下の Data Cache に関連する情報を記録します。

```Bash
mysql> select * from information_schema.be_datacache_metrics;
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
| BE_ID | STATUS | DISK_QUOTA_BYTES | DISK_USED_BYTES | MEM_QUOTA_BYTES | MEM_USED_BYTES | META_USED_BYTES | DIR_SPACES                                                                                   |
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
| 10004 | Normal |       2147483648 |               0 |     32706263420 |              0 |               0 | [{"Path":"/home/disk1/datacache","QuotaBytes":2147483648}] |
+-------+--------+------------------+-----------------+-----------------+----------------+-----------------+----------------------------------------------------------------------------------------------+
1 row in set (5.41 sec)
```

- `BE_ID`: BE ID
- `STATUS`: BE のステータス
- `DISK_QUOTA_BYTES`: ユーザーが設定したディスクキャッシュ容量（バイト単位）
- `DISK_USED_BYTES`: 使用されたディスクキャッシュスペース（バイト単位）
- `MEM_QUOTA_BYTES`: ユーザーが設定したメモリキャッシュ容量（バイト単位）
- `MEM_USED_BYTES`: 使用されたメモリキャッシュスペース（バイト単位）
- `META_USED_BYTES`: メタデータをキャッシュするために使用されたスペース
- `DIR_SPACES`: キャッシュパスとそのキャッシュサイズ

## API コール

v3.3.2 以降、StarRocks は異なるレベルでキャッシュ状態を反映するキャッシュメトリクスを取得するための 2 つの API を提供します。

- `/api/datacache/app_stat`: Block Cache と Page Cache のヒット率をクエリします。
- `/api/datacache/stat`: Data Cache の基礎的な実行状態。このインターフェースは主に Data Cache のメンテナンスとボトルネックの特定に使用されます。クエリの実際のヒット率を反映しません。一般ユーザーはこのインターフェースに注意を払う必要はありません。

### キャッシュヒットメトリクスの表示

以下の API インターフェースにアクセスしてキャッシュヒットメトリクスを表示します。

```bash
http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/app_stat
```

返り値:

```bash
{
    "block_cache_hit_bytes": 1642106883,
    "block_cache_miss_bytes": 8531219739,
    "block_cache_hit_rate": 0.16,
    "block_cache_hit_bytes_last_minute": 899037056,
    "block_cache_miss_bytes_last_minute": 4163253265,
    "block_cache_hit_rate_last_minute": 0.18,
    "page_cache_hit_count": 15048,
    "page_cache_miss_count": 10032,
    "page_cache_hit_rate": 0.6,
    "page_cache_hit_count_last_minute": 10032,
    "page_cache_miss_count_last_minute": 5016,
    "page_cache_hit_rate_last_minute": 0.67
}
```

| **Metric**                         | **Description**                                                                                     |
|------------------------------------|-----------------------------------------------------------------------------------------------------|
| block_cache_hit_bytes              | Block Cache から読み取られたバイト数。                                                                    |
| block_cache_miss_bytes             | リモートストレージから読み取られたバイト数（Block Cache のミス）。                                        |
| block_cache_hit_rate               | Block Cache のヒット率、`(block_cache_hit_bytes / (block_cache_hit_bytes + block_cache_miss_bytes))`。|
| block_cache_hit_bytes_last_minute  | 最後の 1 分間に Block Cache から読み取られたバイト数。                                                  |
| block_cache_miss_bytes_last_minute | 最後の 1 分間にリモートストレージから読み取られたバイト数。                                              |
| block_cache_hit_rate_last_minute   | 最後の 1 分間の Block Cache のヒット率。                                                              |
| page_cache_hit_count               | Page Cache から読み取られたページ数。                                                                 |
| page_cache_miss_count              | Page Cache でミスしたページ数。                                                                       |
| block_cache_hit_rate               | Page Cache のヒット率: `(page_cache_hit_count / (page_cache_hit_count + page_cache_miss_count))`。     |
| page_cache_hit_count_last_minute   | 最後の 1 分間に Page Cache から読み取られたページ数。                                                  |
| page_cache_miss_count_last_minute  | 最後の 1 分間に Page Cache でミスしたページ数。                                                        |
| page_cache_hit_rate_last_minute    | 最後の 1 分間の Page Cache のヒット率。                                                               |

### Data Cache の基礎的な実行状態の表示

以下の API インターフェースにアクセスして、Data Cache の詳細なメトリクスを取得できます。

```Bash
http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/stat
```

結果は以下の通りです。

```json
{
    "page_cache_mem_quota_bytes": 10679976935,
    "page_cache_mem_used_bytes": 10663052377,
    "page_cache_mem_used_rate": 1.0,
    "page_cache_hit_count": 276890,
    "page_cache_miss_count": 153126,
    "page_cache_hit_rate": 0.64,
    "page_cache_hit_count_last_minute": 11196,
    "page_cache_miss_count_last_minute": 9982,
    "page_cache_hit_rate_last_minute": 0.53,
    "block_cache_status": "NORMAL",
    "block_cache_disk_quota_bytes": 214748364800,
    "block_cache_disk_used_bytes": 11371020288,
    "block_cache_disk_used_rate": 0.05,
    "block_cache_disk_spaces": "/disk1/sr/be/storage/datacache:107374182400;/disk2/sr/be/storage/datacache:107374182400",
    "block_cache_meta_used_bytes": 11756727,
    "block_cache_hit_count": 57707,
    "block_cache_miss_count": 2556,
    "block_cache_hit_rate": 0.96,
    "block_cache_hit_bytes": 15126253744,
    "block_cache_miss_bytes": 620687633,
    "block_cache_hit_count_last_minute": 18108,
    "block_cache_miss_count_last_minute": 2449,
    "block_cache_hit_bytes_last_minute": 4745613488,
    "block_cache_miss_bytes_last_minute": 607536783,
    "block_cache_read_disk_bytes": 15126253744,
    "block_cache_write_bytes": 11338218093,
    "block_cache_write_success_count": 43377,
    "block_cache_write_fail_count": 36394,
    "block_cache_remove_bytes": 0,
    "block_cache_remove_success_count": 0,
    "block_cache_remove_fail_count": 0,
    "block_cache_current_reading_count": 0,
    "block_cache_current_writing_count": 0,
    "block_cache_current_removing_count": 0
}
```

### メトリクスの説明

| **Metric**                         | **Description**                                                                                                                                                                                                                                                        |
|------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| page_cache_mem_quota_bytes         | Page Cache の現在のメモリ制限。                                                                                                                                                                                                                                    |
| page_cache_mem_used_bytes	         | Page Cache によって使用されている現在の実際のメモリ。                                                                                                                                                                                                                              |
| page_cache_mem_used_rate	          | Page Cache の現在のメモリ使用率。                                                                                                                                                                                                                               |
| page_cache_hit_count	              | Page Cache のヒット数。                                                                                                                                                                                                                                             |
| page_cache_miss_count	             | Page Cache のミス数。                                                                                                                                                                                                                                           |
| page_cache_hit_rate	               | Page Cache のヒット率。                                                                                                                                                                                                                                                |
| page_cache_hit_count_last_minute	  | 最後の 1 分間の Page Cache のヒット数。                                                                                                                                                                                                                          |
| page_cache_miss_count_last_minute  | 最後の 1 分間の Page Cache のミス数。                                                                                                                                                                                                                        |
| page_cache_hit_rate_last_minute	   | 最後の 1 分間の Page Cache のヒット率。                                                                                                                                                                                                                             |
| block_cache_status                 | Block Cache のステータス、含む:`NORMAL`: インスタンスは正常に動作しています。`ABNORMAL`: データをキャッシュに読み書きできません。問題はログを使用して特定する必要があります。`UPDATING`: インスタンスが更新中、例えばオンラインスケーリング中の更新状態。 |
| block_cache_disk_quota_bytes       | ユーザーが設定した Block Cache のディスクキャッシュ容量（バイト単位）。                                                                                                                                                                                                  |
| block_cache_disk_used_bytes        | Block Cache によって使用されたディスクキャッシュスペース（バイト単位）。                                                                                                                                                                                                      |
| block_cache_disk_used_rate         | Block Cache の実際のディスクキャッシュ使用率（パーセンテージ）。                                                                                                                                                                                                       |
| block_cache_disk_spaces            | ユーザーが設定した Block Cache のディスクキャッシュ情報、各キャッシュパスとキャッシュサイズを含む。                                                                                                                                                           |
| block_cache_meta_used_bytes        | Block Cache メタデータをキャッシュするために使用されたメモリスペース（バイト単位）。                                                                                                                                                                                                         |
| block_cache_hit_count              | Block Cache のヒット数。                                                                                                                                                                                                                                            |
| block_cache_miss_count             | キャッシュミスの数。                                                                                                                                                                                                                                                |
| block_cache_hit_rate               | Block Cache のヒット率。                                                                                                                                                                                                                                               |
| block_cache_hit_bytes              | Block Cache でヒットしたバイト数。                                                                                                                                                                                                                       |
| block_cache_miss_bytes             | Block Cache でミスしたバイト数。                                                                                                                                                                                                                    |
| block_cache_hit_count_last_minute  | 最後の 1 分間の Block Cache のヒット数。                                                                                                                                                                                                                         |
| block_cache_miss_count_last_minute | 最後の 1 分間の Block Cache のミス数。                                                                                                                                                                                                                       |
| block_cache_hit_bytes_last_minute  | 最後の 1 分間に Block Cache でヒットしたタイプ数。                                                                                                                                                                                                                 |
| block_cache_miss_bytes_last_minute | 最後の 1 分間に Block Cache でミスしたタイプ数。                                                                                                                                                                                                              |
| block_cache_buffer_item_count      | Block Cache 内の Buffer インスタンスの現在の数。 Buffer インスタンスは、リモートファイルから生データの一部を読み取って直接メモリまたはディスクにキャッシュするような一般的なデータキャッシュを指します。                                             |
| block_cache_buffer_item_bytes      | Block Cache 内の Buffer インスタンスをキャッシュするために使用されたタイプ数。                                                                                                                                                                                                      |
| block_cache_read_disk_bytes        | Block Cache から読み取られたバイト数。                                                                                                                                                                                                                                 |
| block_cache_write_bytes            | Block Cache に書き込まれたバイト数。                                                                                                                                                                                                                                |
| block_cache_write_success_count    | 成功した Block Cache 書き込みの数。                                                                                                                                                                                                                               |
| block_cache_write_fail_count       | 失敗した Block Cache 書き込みの数。                                                                                                                                                                                                                                   |
| block_cache_remove_bytes           | Block Cache から削除されたバイト数。                                                                                                                                                                                                                              |
| block_cache_remove_success_count   | Block Cache からの削除操作の成功数。                                                                                                                                                                                                               |
| block_cache_remove_fail_count      | Block Cache からの削除操作の失敗数。                                                                                                                                                                                                                   |
| block_cache_current_reading_count  | Block Cache で現在実行中の読み取り操作の数。                                                                                                                                                                                                     |
| block_cache_current_writing_count  | Block Cache で現在実行中の書き込み操作の数。                                                                                                                                                                                                    |
| block_cache_current_removing_count | Block Cache で現在実行中の削除操作の数。                                                                                                                                                                                                   |