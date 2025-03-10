---
displayed_sidebar: docs
sidebar_position: 9
---

# 実行中のクエリを表示

この記事では、StarRocks で実行中のクエリを表示し、そのリソース消費を分析する方法について説明します。

## current_queries

`show proc '/current_queries'` を使用すると、現在の FE ノードで実行中のクエリを表示できます。以下の情報が含まれます。

| **列名** | **説明** |
| --- | --- |
| StartTime | クエリの開始時間 |
| feIp | クエリを実行している FE ノードの IP アドレス |
| QueryId | クエリ ID |
| ConnectionId | 接続 ID |
| Database | クエリが実行されているデータベース |
| User | クエリを実行しているユーザー |
| ScanBytes | スキャンされたデータ量 |
| ScanRows | スキャンされた行数 |
| MemoryUsage | クエリで使用されたメモリ量 |
| DiskSpillSize | ディスクにスピルされたデータ量 |
| CPUTime | クエリで消費された CPU 時間 |
| ExecTime | クエリの実行時間 |
| Warehouse | クエリで使用されたウェアハウス |
| CustomQueryId | ユーザー定義のクエリ ID |
| ResourceGroup | クエリで使用されたリソースグループ |

**例**:
```sql
MySQL > show proc '/current_queries'\G
***************************[ 1. row ]***************************
StartTime     | 2025-03-07 02:16:04
feIp          | 172.26.92.227
QueryId       | 10db481c-fab7-11ef-8063-461f20abc3f0
ConnectionId  | 13
Database      | tpcds_2
User          | root
ScanBytes     | 120.573 MB
ScanRows      | 5859503 rows
MemoryUsage   | 225.893 MB
DiskSpillSize | 0.000 B
CPUTime       | 47.878 s
ExecTime      | 4.077 s
Warehouse     | default_warehouse
CustomQueryId |
ResourceGroup | rg1
```

## global_current_queries

`current_queries` と同様に、`show proc '/global_current_queries'` はすべての FE ノードで実行中のクエリ情報を表示します。このコマンドはバージョン 3.4 からサポートされています。

**例**:
```sql
MySQL root@127.1:(none)> show proc '/global_current_queries'\G
***************************[ 1. row ]***************************
StartTime     | 2025-03-07 02:21:48
feIp          | 172.26.92.227
QueryId       | de516505-fab7-11ef-8063-461f20abc3f0
ConnectionId  | 14
Database      | tpcds_2
User          | root
ScanBytes     | 120.573 MB
ScanRows      | 5859503 rows
MemoryUsage   | 346.915 MB
DiskSpillSize | 0.000 B
CPUTime       | 33.265 s
ExecTime      | 3.032 s
Warehouse     | default_warehouse
CustomQueryId |
ResourceGroup | rg1
```

## 実行中のクエリ

`SHOW RUNNING QUERIES` は主にクエリキューの状態を表示するために使用されます。クエリがキューにある場合、その状態は `PENDING` になります。

| フィールド名 | 説明 |
|---|---|
| QueryId | クエリ ID |
| ResourceGroupId | リソースグループ ID |
| StartTime | クエリの開始時間 |
| PendingTimeout | 待機タイムアウト |
| QueryTimeout | クエリタイムアウト |
| State | クエリの状態、PENDING と RUNNING を含む |
| Slots | 占有しているスロット数 |
| Fragments | クエリプラン内のフラグメント数 |
| DOP | 並行性の度合い |
| Frontend | FE ノード情報 |
| FeStartTime | FE の開始時間 |

**例**:
```sql
MySQL root@127.1:(none)> show running queries\G
***************************[ 1. row ]***************************
QueryId         | 50029ec1-fab8-11ef-8063-461f20abc3f0
ResourceGroupId | 562275
StartTime       | 2025-03-07 02:24:59
PendingTimeout  | 2025-03-07 02:27:29
QueryTimeout    | 2025-03-07 02:27:29
State           | RUNNING
Slots           | 1
Fragments       | 11
DOP             | 0
Frontend        | 172.26.92.227_8034_1709578860161
FeStartTime     | 2025-03-06 23:39:00
```