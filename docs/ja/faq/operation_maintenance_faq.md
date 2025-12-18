---
displayed_sidebar: docs
---

# 運用とメンテナンス

このトピックでは、運用とメンテナンスに関連するいくつかの質問に対する回答を提供します。

## `trash` ディレクトリをクリーンアップできますか？

FE パラメータ `catalog_trash_expire_second` を設定して、FE `trash` ディレクトリ内のファイルがどのくらいの期間保持されるかを指定できます（デフォルト: 24 時間）。

BE パラメータ `trash_file_expire_time_sec` は、BE のゴミ箱のクリーンアップ間隔を制御します（デフォルト: 24 時間）。

DROP TABLE または DROP DATABASE の後、データは最初に FE のゴミ箱に入り、1 日間保持され、その間に RECOVER で復元できます。その後、BE のゴミ箱に移動し、こちらも 24 時間保持されます。

## タブレットには主従関係がありますか？レプリカがいくつか欠けている場合、クエリにどのように影響しますか？

テーブルプロパティ `replicated_storage` が `true` に設定されている場合、書き込みは主従メカニズムを使用します。データは最初に主レプリカに書き込まれ、次に他のレプリカに同期されます。複数のレプリカは通常、クエリパフォーマンスに大きな影響を与えません。

## タスクの CPU とメモリ使用量を監視インターフェースで測定できますか？

`fe.audit.log` の `cpucostns` と `memcostbytes` フィールドを確認できます。

## ユニークキーテーブルにマテリアライズドビューを作成すると、「The aggregation type of column[now_time] must be same as the aggregate type of base column in aggregate table」というエラーが返されます。ユニークキーテーブルはマテリアライズドビューをサポートしていますか？

このエラーは、マテリアライズドビューの集計タイプがベーステーブルの集計タイプと一致する必要があることを示しています。ユニークキーテーブルでは、ソートキーの順序を調整するためにのみマテリアライズドビューを使用できます。
例えば、ベーステーブル `tableA` が `k1`、`k2`、`k3` の列を持ち、`k1` と `k2` がソートキーである場合、クエリに `WHERE k3=x` という句が含まれ、プレフィックスインデックスを加速する必要がある場合、`k3` を最初の列として使用するマテリアライズドビューを作成できます。

```SQL
CREATE MATERIALIZED VIEW k3_as_key AS
SELECT k3, k2, k1
FROM tableA;
```

## 正確なタイムスタンプでインポートボリュームのメトリクスを取得するにはどうすればよいですか？`query_latency` は平均応答時間のメトリクスですか？

テーブルレベルのインポートメトリクスは、`http://user:password@fe_host:http_port/metrics?type=json&with_table_metrics=all` から取得できます。

クラスター レベルのデータは、`http://fe_host:http_port/api/show_data` から取得できます（インクリメントは手動で計算する必要があります）。

`query_latency` はパーセンタイルクエリ応答時間を提供します。

## SHOW PROC '/backends' と SHOW BACKENDS の違いは何ですか？

`SHOW PROC '/backends'` は現在の FE からメタデータを取得し、遅延する可能性があります。一方、`SHOW BACKENDS` は Leader FE からメタデータを取得し、権威があります。

## StarRocks にはタイムアウトメカニズムがありますか？なぜ一部のクライアント接続が長時間持続するのですか？

はい。システム変数 `wait_timeout`（デフォルト: 8 時間）を設定して接続タイムアウトを調整できます。

例:

```SQL
SET GLOBAL wait_timeout = 3600;
```

## GRANT で複数のテーブルに一度に権限を付与できますか？

いいえ。`GRANT <priv> on db1.tb1, db1.tb2` のようなステートメントはサポートされていません。

## ALL TABLES 権限が付与されている場合、特定のテーブルの権限を取り消すことはできますか？

部分的な取り消しはサポートされていません。データベースまたはテーブルレベルで権限を付与することをお勧めします。

## StarRocks はテーブルレベルおよび行レベルの権限をサポートしていますか？

テーブルレベルのアクセス制御はサポートされています。行および列レベルのアクセス制御はオープンソース版ではサポートされていません。

## 主キーテーブルがパーティション分割されていない場合、ホット・コールドデータの分離は機能しますか？

いいえ。ホット・コールド分離はパーティションに基づいています。

## データが誤ってルートディレクトリに配置された場合、データストレージパスを変更できますか？

はい。`be.conf` の `storage_root_path` を更新して新しいディスクを追加し、パスをセミコロンで区切ります。

## FE の StarRocks バージョンを確認するにはどうすればよいですか？

`SHOW FRONTENDS;` を実行し、`Version` フィールドを確認します。

## StarRocks はネストされたサブクエリを使用した DELETE をサポートしていますか？

バージョン 2.3 以降、主キーテーブルは完全な DELETE WHERE 構文をサポートしています。
詳細は [Reference - DELETE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/table_bucket_part_index/DELETE/) を参照してください。

## 動的パーティションで古いパーティションを自動的にクリーンアップしたくない場合、dynamic_partition.start を省略するだけでよいですか？

いいえ。非常に大きな値に設定してください。

## BE マシンにメモリの故障があり、メンテナンスが必要な場合、何をすべきですか？

BE を [Decommission](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md#be) します。修理後、クラスターに再追加します。

## すべての将来のパーティションをデフォルトで SSD を使用するように強制できますか？

いいえ。デフォルトは HDD です。手動での設定が必要です。

## 新しい BEs を追加した後、タブレットが自動的に再バランスされました。古い BEs をすぐに退役させることはできますか？

はい。再バランスが完了するまで待つ必要はありません。最大 2 ノードを一度に退役させることができます。

## 新しい BEs を追加し、古いものを削除するとパフォーマンスに影響しますか？

再バランスは自動的に行われ、通常の操作には影響しないはずです。ノードを一度に 1 つずつ削除することをお勧めします。

## 6 つの BE ノードを 6 つの新しいノードに置き換えるにはどうすればよいですか？

6 つの新しい BE ノードを追加し、古いノードを 1 つずつ退役させます。

## ノードを退役させた後も不健康なタブレットの数が減らないのはなぜですか？

単一レプリカのテーブルを確認してください。修復が継続的に再試行されると、他の修復がブロックされる可能性があります。

## この BE ログは何を意味しますか？"tcmalloc: large alloc xxxxxxxx bytes"

大きなメモリ割り当て要求が発生しました。これは通常、大きなクエリによって引き起こされます。対応する `query_id` を `be.INFO` で確認して SQL を特定してください。

## ノードを追加した後のタブレット移行はディスク I/O の変動を引き起こしますか？

はい、バランス調整中に一時的な I/O の変動が予想されます。

## クラウドでの展開における推奨されるデータ移行方法は何ですか？

- 単一のテーブルを移行するには、次の方法があります。
  - StarRocks 外部テーブルを作成し、INSERT INTO SELECT を使用してデータをロードします。
  - Spark-connector ベースのプログラムを使用してソース BE からデータを読み取り、カプセル化された STREAM LOAD を使用してターゲット BE にデータをロードします。
- 複数のテーブルを移行するには、バックアップとリストアを使用できます。まず、ソースクラスターからリモートストレージにデータをバックアップします。その後、リモートストレージからターゲットクラスターにデータをリストアします。
- クラスターがリモートストレージとして HDFS を使用している場合、最初に `distcp` を使用してデータファイルを移行し、その後 Broker Load を使用してターゲットクラスターにデータをロードします。

## "failed to create task: disk ... exceed limit usage" エラーを解決するにはどうすればよいですか？

ディスクがいっぱいです。ストレージを拡張するか、ゴミ箱をクリーンアップしてください。

## FE ログに "tablet migrate failed" と表示されます。これを解決するにはどうすればよいですか？

`storage_root_path` が HDD であり、テーブルプロパティ `storage_medium` が `SSD` に設定されている可能性があります。テーブルプロパティを `HDD` に設定できます。

```SQL
ALTER TABLE db.table MODIFY PARTITION (*) SET("storage_medium"="HDD");
```

## メタデータのみをコピーして FE を別のマシンに移行できますか？

いいえ。推奨される方法を使用してください。新しいノードを追加し、古いノードを削除します。

## 1 つの BE のディスク使用量が不均一です（500 GB 99%、2 TB 20%）。なぜバランスが取れていないのですか？

バランスは同じサイズのディスクを前提としています。同じサイズのディスクを使用して均等な分散を確保してください。

## `max_backend_down_time_second` が `3600` に設定されている場合、BE の障害を 1 時間以内に復旧しなければならないという意味ですか？

BE のダウンタイムがこの設定を超えると、FE は他の BE にレプリカを補充します。障害が発生した BE がクラスターに再追加されると、大きな再バランスコストが発生する可能性があります。

## テーブル構造とビューを本番環境からエクスポートするための IDE ツールはありますか？

はい。 [olapdb-tool](https://github.com/Astralidea/olapdb-tool) を参照してください。

## 複数のシステム変数を 1 つの SQL で設定できますか（例えば、タイムアウトと並行性）？

はい。

例:

```SQL
SELECT /*+ SET_VAR(query_timeout=1, is_report_success=true, parallel_fragment_exec_instance_num=2) */ COUNT(1)
FROM table;
```

## ソートキーの VARCHAR 列は 36 バイトのプレフィックス制限に従いますか？

VARCHAR 列は実際の長さに基づいて切り捨てられます。最初の列のみがショートキーインデックスを取得します。可能であれば、VARCHAR ソートキーを 3 番目の位置に配置してください。

## BE が "while lock file" エラーで起動しません。どう対処すればよいですか？

BE プロセスがまだ実行中です。デーモンプロセスを終了して再起動してください。

## クライアントは読み取り専用クエリのために observer FEs に明示的に接続する必要がありますか？

いいえ。書き込み要求は自動的に Leader FE にルーティングされ、observer は読み取り専用クエリを処理します。

## FE が `LOG_FILE_NOT_FOUND` で終了し、ファイルが多すぎることが原因です。これを解決するにはどうすればよいですか？

OS のファイルディスクリプタ制限を確認し、`cat /proc/$pid/limits` を実行し、`lsof -n -p $fe_pid>/tmp/fe_fd.txt` を実行して使用中のファイルディスクリプタを確認してください。

## パーティションとバケットの数に制限はありますか？

- パーティションのデフォルト制限は `4096` です（FE 設定 `max_partitions_in_one_batch` で設定可能）。
- バケットには制限がありません。各バケットの推奨サイズは 1 GB です。

## FE リーダーを手動で切り替えるにはどうすればよいですか？

現在のリーダーを停止すると、新しいリーダーが自動的に選出されます。

## 3 つの FE ノード（1 Leader と 2 Follower）を新しいマシンに置き換えるにはどうすればよいですか？

1. クラスターに 2 つの新しい Follower を追加します。
2. 古い Follower を 1 つ削除します。
3. 最後の新しい Follower を追加します。
4. 残りの古い Follower を削除します。
5. 古い Leader を削除します。

## 動的パーティションの作成が期待通りに動作しません。なぜですか？

動的パーティションのチェックは 10 分ごとに実行されます。この動作は FE 設定 `dynamic_partition_check_interval_seconds` によって制御されます。

## FE ログに頻繁に "connect processor exception because，java.io.IOException: Connection reset by peer" と表示されます。なぜですか？

クライアント側の切断、接続プールのドロップ、またはネットワークの問題が原因である可能性があります。OS のバックログメトリクスとネットワークの安定性を確認してください。

次のメトリクスが変更されたかどうかを確認してください。

```Bash
netstat -s | grep -i LISTEN
netstat -s | grep TCPBacklogDrop
cat /proc/sys/net/core/somaxconn
```

## TRUNCATE ステートメントを実行した後、ストレージスペースはいつ解放されますか？

即座に。

## バケットの分布が均等かどうかを確認するにはどうすればよいですか？

次のコマンドを実行します。

```SQL
SHOW TABLET FROM db.table PARTITION (<partition_name>);
```

## "Fail to get master client from cache" エラーを解決するにはどうすればよいですか？

これは FE–BE 間の通信障害です。IP とポートの接続性を確認してください。

## IP が変更された場合、StarRocks をどのように移行しますか？

FQDN モードでのデプロイメントを推奨します。

## 非パーティションテーブルをパーティションテーブルに変換できますか？

いいえ。新しいパーティションテーブルを作成し、INSERT INTO SELECT を使用してデータを移行できます。

## 過去の SQL 実行をクエリできますか？監査ログはありますか？

はい。`fe.audit.log` を参照してください。

## 非パーティションテーブルを SSD ストレージに変換するにはどうすればよいですか？

次の SQL を実行します。

```SQL
ALTER TABLE db.tbl MODIFY PARTITION (*) SET ("storage_medium"="SSD");
```

## ALTER TABLE ADD COLUMN を実行した後、`information_schema.COLUMNS` に対するクエリが遅延を示します。これは正常ですか？

はい。ALTER 操作は非同期です。`SHOW ALTER COLUMN` で進捗を確認してください。

## 動的パーティションテーブルの保持期間を 366 日から 732 日に変更した場合、過去のパーティションは自動的に作成されますか？

次の手順に従ってください。

1. 動的パーティションを無効にします。

   ```SQL
   ALTER TABLE db.tbl SET  ("dynamic_partition.enable" = "false");
   ```

2. 手動でパーティションを追加します。

   ```SQL
   ALTER TABLE db.tbl ADD PARTITIONS START ("2019-01-01") END ("2019-12-31") EVERY (interval 1 day);
   ```

3. 動的パーティションを再度有効にします。

   ```SQL
   ALTER TABLE db.tbl SET  ("dynamic_partition.enable" = "true"); 
   ```

## Routine Load タスクが Running から Paused に切り替わったときに監視とアラートを行うことはできますか？

はい。StarRocks は Routine Load タスクの監視メトリクスをサポートしており、アラートシステムに接続できます。

## 不健康なレプリカを診断するにはどうすればよいですか？

次のステートメントを実行して UnhealthyTablets を特定します。

```SQL
SHOW PROC '/statistic'
SHOW PROC '/statistic/<db_id>'
```

その後、`SHOW TABLET tablet_id` を使用して UnhealthyTablets を分析します。

結果が 2 つのレプリカがデータが一致しているが、1 つのレプリカがデータが一致していないことを示している場合、つまり 3 つのレプリカのうち 2 つが書き込みに成功した場合、これは成功した書き込みと見なされます。その後、UnhealthyTablets のタブレットが修正されているかどうかを確認できます。修正されている場合、問題があることを示しています。ステータスが変化している場合、対応するテーブルのロード頻度を調整できます。

## エラー: "SyntaxErrorException: Reach limit of connections". どのようにトラブルシューティングしますか？

次のコマンドを実行して、ユーザーごとの制限を増やします。

```SQL
ALTER USER 'jack' SET PROPERTIES ('max_user_connections'='1000');
```

また、ロードバランサーとアイドル接続の蓄積（`wait_timeout`）を確認してください。

## XFS と ext4 は QPS にどのように影響しますか？

StarRocks は通常、XFS でより良いパフォーマンスを発揮します。

## BE がダウンと見なされ、タブレットの移行が始まるまでどのくらいかかりますか？

1. ハートビート（デフォルト: 5 秒ごと）が 3 回失敗した場合、BE は生存していないとマークされます。
2. その後、クローンを実行する前に 60 秒の意図的な遅延があります。
3. その後、レプリカのクローンが開始されます。

BE が後で復旧した場合、そのレプリカは削除されます。

## 新しいノードでのタブレットスケジューリングが遅い（1 回に 100 個のみ）。どのように調整しますか？

次の FE 設定を調整します。

```SQL
ADMIN SET FRONTEND CONFIG ("schedule_slot_num_per_path"="8");
ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets"="1000");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets"="1000");
```

## BACKUP 操作は直列ですか？HDFS ディレクトリが 1 つだけ変更されているように見えます

BACKUP 操作は並行して行われますが、HDFS へのアップロードは単一のワーカーを使用します。これは BE 設定 `upload_worker_count` によって制御されます。ディスク I/O とネットワーク I/O に影響を与える可能性があるため、注意して調整してください。

## FE の OOM エラー "OutOfMemoryError: GC overhead limit exceeded" を解決するにはどうすればよいですか？

FE JVM メモリを増やします。

## "Cannot truncate a file by broker" エラーを解決するにはどうすればよいですか？

1. ブローカーログでエラーメッセージを確認します。
2. BE 警告ログでエラー "remote file checksum is invalid. remote:**** local: *****" を確認します。
3. ブローカー `apache_hdfs_broker.log` でエラー "receive a check path request, request detail" を検索し、重複するファイルを特定します。
4. 問題のあるリモートファイルを削除またはリネームして再試行します。

## ディスクの削除が完了したことを確認するにはどうすればよいですか？

`SHOW PROC '/statistic'` を実行し、`UnhealthyTablet` の数がゼロであることを確認します。

## 過去のパーティションのレプリカ数を変更するにはどうすればよいですか？

次のコマンドを実行します。

```SQL
ALTER TABLE db.tbl MODIFY PARTITION (*) SET("replication_num"="3");
```

## 3 レプリカのテーブルで、BE ノードのディスクが損傷した場合、レプリカは自動的に回復して 3 つのコピーを維持しますか？

はい、十分な BE ノードが利用可能であれば。

## ユーザー制限を増やした後も "reach limit connections" エラーが続く場合、どのようにトラブルシューティングしますか？

ロードバランサー（ProxySQL、F5）、アイドル接続の蓄積を確認し、`wait_timeout` を 2～4 時間に減らします。

## FE メタデータが失われた場合、すべてのクラスター メタデータが失われますか？

メタデータは FE に存在します。FE が 1 つしかない場合、復旧は不可能です。複数の FE がある場合、失敗したノードを再追加するとメタデータが複製されます。

## "INTERNAL_ERROR, FE leader shows NullPointerException" ロードエラーを解決するにはどうすればよいですか？

JVM オプション `-XX:-OmitStackTraceInFastThrow` を追加し、FE を再起動して完全なスタックトレースを取得します。

## 主キーテーブルのパーティション列を設定する際に "The partition column could not be aggregated column" エラーを解決するにはどうすればよいですか？

パーティション列はキー列でなければなりません。
