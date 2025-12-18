---
displayed_sidebar: docs
---

# SQL クエリ

このトピックでは、SQL に関するよくある質問への回答を提供します。

## マテリアライズドビューを作成するときに "fail to allocate memory." というエラーが発生する

この問題を解決するには、**be.conf** ファイルの `memory_limitation_per_thread_for_schema_change` パラメータの値を増やしてください。このパラメータは、スキームを変更するために単一のタスクに割り当てられる最大ストレージを指します。最大ストレージのデフォルト値は 2 GB です。

## StarRocks はクエリ結果のキャッシュをサポートしていますか？

StarRocks は最終的なクエリ結果を直接キャッシュしません。v2.5 以降、StarRocks は Query Cache 機能を使用して、キャッシュ内の最初の段階の集約の中間結果を保存します。以前のクエリと意味的に同等の新しいクエリは、キャッシュされた計算結果を再利用して計算を高速化できます。Query Cache は BE メモリを使用します。詳細は [Query cache](../using_starrocks/caching/query_cache.md) を参照してください。

## 計算に `Null` が含まれる場合、ISNULL() 関数を除いて関数の計算結果は false になります

標準 SQL では、`NULL` 値を持つオペランドを含むすべての計算は `NULL` を返します。

## StarRocks は DECODE 関数をサポートしていますか？

StarRocks は Oracle データベースの DECODE 関数をサポートしていません。StarRocks は MySQL と互換性があるため、CASE WHEN ステートメントを使用できます。

## StarRocks の主キーテーブルにデータがロードされた直後に最新のデータをクエリできますか？

はい。StarRocks は Google Mesa を参考にしてデータをマージします。StarRocks では、BE がデータマージをトリガーし、データをマージするための 2 種類の Compaction を持っています。データマージが完了していない場合は、クエリ中に完了します。したがって、データロード後に最新のデータを読み取ることができます。

## StarRocks に保存された utf8mb4 文字が切り捨てられたり文字化けしたりしますか？

いいえ。

## `alter table` コマンドを実行すると "table's state is not normal" というエラーが発生する

このエラーは、前回の変更が完了していないために発生します。以下のコードを実行して、前回の変更の状態を確認できます。

```SQL
show tablet from lineitem where State="ALTER"; 
```

変更操作にかかる時間はデータ量に関連しています。一般的に、変更は数分で完了します。テーブルを変更している間は、データロードが変更の完了速度を低下させるため、StarRocks へのデータロードを停止することをお勧めします。

## Apache Hive の外部テーブルをクエリすると "get partition detail failed: org.apache.doris.common.DdlException: get hive partition meta data failed: java.net.UnknownHostException:hadooptest" というエラーが発生する

このエラーは、Apache Hive パーティションのメタデータを取得できない場合に発生します。この問題を解決するには、**core-sit.xml** と **hdfs-site.xml** を **fe.conf** ファイルと **be.conf** ファイルにコピーしてください。

## データをクエリすると "planner use long time 3000 remaining task num 1" というエラーが発生する

このエラーは通常、完全なガベジコレクション (full GC) によって発生し、バックエンドのモニタリングと **fe.gc** ログを使用して確認できます。この問題を解決するには、以下の操作のいずれかを実行します。

- SQL のクライアントが複数のフロントエンド (FEs) に同時にアクセスできるようにして、負荷を分散します。
- Java Virtual Machine (JVM) のヒープサイズを **fe.conf** ファイルで 8 GB から 16 GB に変更して、メモリを増やし、full GC の影響を軽減します。

## 列 A のカーディナリティが小さい場合、`select B from tbl order by A limit 10` のクエリ結果が毎回異なる

SQL は列 A の順序を保証することしかできず、列 B の順序が各クエリで同じであることを保証できません。MySQL はスタンドアロンデータベースであるため、列 A と列 B の順序を保証できます。

StarRocks は分散型データベースであり、基礎となるテーブルに保存されたデータはシャーディングパターンになっています。列 A のデータは複数のマシンに分散されているため、複数のマシンから返される列 B の順序は各クエリで異なる場合があり、結果として B の順序が毎回一致しません。この問題を解決するには、`select B from tbl order by A limit 10` を `select B from tbl order by A,B limit 10` に変更します。

## SELECT * と SELECT の間に列効率の大きなギャップがあるのはなぜですか？

この問題を解決するには、プロファイルを確認し、MERGE の詳細を確認してください。

- ストレージ層での集約に時間がかかりすぎていないか確認します。

- インジケータ列が多すぎるか確認します。もしそうなら、何百万行もの列を集約します。

```plaintext
MERGE:

    - aggr: 26s270ms

    - sort: 15s551ms
```

## DELETE はネストされた関数をサポートしていますか？

ネストされた関数はサポートされていません。例えば、`DELETE from test_new WHERE to_days(now())-to_days(publish_time) >7;` の `to_days(now())` などです。

## データベースに数百のテーブルがある場合、データベースの使用効率を向上させるにはどうすればよいですか？

効率を向上させるために、MySQL のクライアントサーバーに接続する際に `-A` パラメータを追加します: `mysql -uroot -h127.0.0.1 -P8867 -A`。MySQL のクライアントサーバーはデータベース情報を事前に読み込みません。

## BE ログと FE ログが占めるディスクスペースを削減するにはどうすればよいですか？

ログレベルと対応するパラメータを調整します。詳細は [Parameter Configuration](../administration/management/BE_configuration.md) を参照してください。

## レプリケーション数を変更しようとすると "table *** is colocate table, cannot change replicationNum" というエラーが発生する

コロケートテーブルを作成する際には、`group` プロパティを設定する必要があります。そのため、単一のテーブルのレプリケーション数を変更することはできません。グループ内のすべてのテーブルのレプリケーション数を変更するには、以下の手順を実行します。

1. グループ内のすべてのテーブルに対して `group_with` を `empty` に設定します。
2. グループ内のすべてのテーブルに対して適切な `replication_num` を設定します。
3. `group_with` を元の値に戻します。

## VARCHAR を最大値に設定するとストレージに影響がありますか？

VARCHAR は可変長データ型であり、実際のデータ長に基づいて変更できる指定された長さを持っています。テーブルを作成する際に異なる varchar 長を指定しても、同じデータに対するクエリパフォーマンスにはほとんど影響がありません。

## テーブルを切り捨てると "create partititon timeout" というエラーが発生する

テーブルを切り捨てるには、対応するパーティションを作成してからそれらをスワップする必要があります。作成する必要があるパーティションの数が多い場合、このエラーが発生します。さらに、多くのデータロードタスクがある場合、Compaction プロセス中にロックが長時間保持されます。そのため、テーブルを作成する際にロックを取得できません。データロードタスクが多すぎる場合は、**be.conf** ファイルで `tablet_map_shard_size` を `512` に設定してロック競合を減らします。

## Apache Hive の外部テーブルにアクセスすると "Failed to specify server's Kerberos principal name" というエラーが発生する

**fe.conf** ファイルと **be.conf** ファイルの **hdfs-site.xml** に次の情報を追加します。

```HTML
<property>

<name>dfs.namenode.kerberos.principal.pattern</name>

<value>*</value>

</property>
```

## "2021-10" は StarRocks の日付形式ですか？

いいえ。

## "2021-10" をパーティションフィールドとして使用できますか？

いいえ、関数を使用して "2021-10" を "2021-10-01" に変更し、"2021-10-01" をパーティションフィールドとして使用してください。

## StarRocks データベースまたはテーブルのサイズをどこでクエリできますか？

[SHOW DATA](../sql-reference/sql-statements/Database/SHOW_DATA.md) コマンドを使用できます。

`SHOW DATA;` は、現在のデータベース内のすべてのテーブルのデータサイズとレプリカを表示します。

`SHOW DATA FROM <db_name>.<table_name>;` は、指定されたデータベースの指定されたテーブルのデータサイズ、レプリカの数、および行数を表示します。

## StarRocks on ES で Elasticsearch 外部テーブルを作成する際、関連する文字列の長さが 256 を超えると、select ステートメントを使用してその列をクエリできなくなる

動的マッピングでは、Elasticsearch のデータ型は次のようになります。

```json
          "k4": {
                "type": "text",
                "fields": {
                   "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                   }
                }
             }
```

StarRocks はキーワードデータ型を使用してクエリステートメントを変換します。列のキーワード長が 256 を超えるため、その列をクエリできません。

解決策: フィールドマッピングを削除して、テキスト型を使用します。

```json
            "fields": {
                   "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                   }
                }
```

## StarRocks データベースとテーブルのサイズ、およびそれらが占有するディスクリソースを迅速にカウントする方法は？

[SHOW DATA](../sql-reference/sql-statements/Database/SHOW_DATA.md) コマンドを使用して、データベースとテーブルのストレージサイズを表示できます。

`SHOW DATA;` は、現在のデータベース内のすべてのテーブルのデータ量とレプリカ数を表示します。

`SHOW DATA FROM <db_name>.<table_name>;` は、指定されたデータベースの特定のテーブルのデータ量、レプリカ数、および行数を表示します。

## パーティションキーに関数を使用するとクエリが遅くなるのはなぜですか？

パーティションキーに関数を使用すると、パーティションプルーニングが不正確になり、クエリパフォーマンスが低下する可能性があります。

## DELETE ステートメントがネストされた関数をサポートしていないのはなぜですか？

```SQL
mysql > DELETE FROM starrocks.ods_sale_branch WHERE create_time >= concat(substr(202201,1,4),'01') and create_time <= concat(substr(202301,1,4),'12');

SQL Error [1064][42000]: Right expr of binary predicate should be value
```

BINARY プレディケートは `column op literal` タイプでなければならず、式を使用することはできません。現在、比較値として式をサポートする計画はありません。

## 予約キーワードで列を命名する方法は？

予約キーワード（例: `rank`）はエスケープする必要があります。例えば、`` `rank` `` を使用します。

## 実行中の SQL を停止する方法は？

`show processlist;` を使用して実行中の SQL を表示し、`kill <id>;` を使用して対応する SQL を終了できます。また、`SHOW PROC '/current_queries';` を通じて表示および管理することもできます。

## アイドル接続をクリーンアップする方法は？

セッション変数 `wait_timeout`（単位: 秒）を使用してアイドル接続のタイムアウトを制御できます。MySQL はデフォルトで約 8 時間後にアイドル接続を自動的にクリーンアップします。

## UNION ALL 内の複数の SQL セグメントは並行して実行されますか？

はい、並行して実行されます。

## SQL が BE をクラッシュさせた場合はどうすればよいですか？

1. `be.out` エラースタックに基づいて、クラッシュを引き起こした `query_id` を見つけます。
2. `query_id` を使用して `fe.audit.log` で対応する SQL を見つけます。

次の情報をサポートチームに収集して送信してください。

- `be.out` ログ
- `pstack $be_pid > pstack.log` を実行して SQL を実行します。
- コアダンプファイル

コアファイルを収集する手順:

1. 対応する BE プロセスを取得します。

   ```Bash
   ps aux| grep be
   ```

2. コアファイルサイズの制限を無制限に設定します。

   ```Bash
   prlimit -p $bePID --core=unlimited:unlimited
   ```

   サイズ制限が無制限であるかどうかを確認します。

   ```Bash
   cat /proc/$bePID/limits
   ```

`0` でない場合、プロセスがクラッシュすると、BE デプロイメントのルートディレクトリにコアファイルが生成されます。

## Hints を使用してテーブルジョインオプティマイザの動作を制御する方法は？

`broadcast` と `shuffle` Hints をサポートしています。例えば:

- `select * from a join [broadcast] b on a.id = b.id;`
- `select * from a join [shuffle] b on a.id = b.id;`

## SQL クエリの同時実行性を向上させる方法は？

セッション変数 `pipeline_dop` を調整することで実現できます。

## DDL の実行進捗を確認する方法は？

- デフォルトデータベース内のすべての列変更タスクを表示します。

   ```SQL
   SHOW ALTER TABLE COLUMN;
   ```

- 特定のテーブルの最新の列変更タスクを表示します。

   ```SQL
   SHOW ALTER TABLE COLUMN WHERE TableName="table1" ORDER BY CreateTime DESC LIMIT 1;
   ```

## 浮動小数点数を比較するとクエリ結果が不一致になるのはなぜですか？

浮動小数点数を直接 `=` で比較すると、誤差のために不安定になる可能性があります。範囲チェックを使用することをお勧めします。

## 浮動小数点計算で誤差が生じるのはなぜですか？

FLOAT/DOUBLE 型は `avg`、`sum` などの計算で精度誤差が生じ、クエリ結果が不一致になる可能性があります。高精度が必要な場合は DECIMAL 型を使用してください。ただし、パフォーマンスは 2～3 倍低下します。

## サブクエリ内の ORDER BY が効果を発揮しないのはなぜですか？

分散実行では、サブクエリの外層で ORDER BY が指定されていない場合、グローバルな順序付けを保証できません。これは期待される動作です。

## row_number() の結果が複数回の実行で不一致になるのはなぜですか？

ORDER BY フィールドに重複がある場合（例: 複数の行が同じ `createTime` を持つ）、SQL 標準は安定したソートを保証しません。安定性を確保するために、ユニークなフィールド（例: `employee_id`）を ORDER BY に含めることをお勧めします。

## SQL の最適化やトラブルシューティングに必要な情報は何ですか？

- `EXPLAIN COSTS <SQL>`（統計情報を含む）
- `EXPLAIN VERBOSE <SQL>`（データ型、nullable、最適化戦略を含む）
- Query Profile（FE Web インターフェースの `http://<fe_ip>:<fe_http_port>` で Queries タブに移動して表示可能）
- Query Dump（HTTP API を介して取得可能）

  ```Bash
  wget --user=${username} --password=${password} --post-file ${query_file} http://${fe_host}:${fe_http_port}/api/query_dump?db=${database} -O ${dump_file}
  ```

Query Dump には次の情報が含まれます：

- クエリステートメント
- クエリで参照されるテーブルスキーマ
- セッション変数
- BE の数
- 統計情報（最小値、最大値）
- 例外情報（例外スタック）

## データスキューを確認する方法は？

`ADMIN SHOW REPLICA DISTRIBUTION FROM <table>` を使用して、タブレットの分布を確認します。

## メモリ関連のエラーをトラブルシューティングする方法は？

一般的なシナリオは 3 つあります：

- **単一クエリのメモリ制限を超えた場合：**
  - エラー: `Mem usage has exceed the limit of single query, You can change the limit by set session variable exec_mem_limit.`
  - 解決策: `exec_mem_limit` を調整します。
- **クエリプールのメモリ制限を超えた場合：**
  - エラー: `Mem usage has exceed the limit of query pool`
  - 解決策: SQL を最適化します。
- **BE の総メモリ制限を超えた場合：**
  - エラー: `Mem usage has exceed the limit of BE`
  - 解決策: メモリ使用量を分析します。

メモリ分析方法：

```Bash
curl -XGET -s http://BE_IP:BE_HTTP_PORT/metrics | grep "^starrocks_be_.*_mem_bytes\|^starrocks_be_tcmalloc_bytes_in_use"
curl -XGET -s http://BE_IP:BE_HTTP_PORT/mem_tracker
```

---

## `StarRocks planner use long time xxx ms in logical phase` エラーが発生した場合の対処法は？

1. `fe.gc.log` を分析して、Full GC の発生を確認します。
2. SQL 実行プランが複雑な場合、`new_planner_optimize_timeout`（単位: ms）を増やします：

   ```SQL
   set global new_planner_optimize_timeout = 6000;
   ```

## Unknown Error をトラブルシューティングする方法は？

次のパラメータを一つずつ調整してから SQL を再実行します：

```SQL
set disable_join_reorder = true;
set enable_global_runtime_filter = false;
set enable_query_cache = false;
set cbo_enable_low_cardinality_optimize = false;
```

その後、EXPLAIN COSTS、EXPLAIN VERBOSE、PROFILE、および Query Dump を収集し、サポートチームに提供してください。

## `select now()` はどのタイムゾーンを返しますか？

`time_zone` システム変数で指定されたタイムゾーンを返します。FE/BE ログはマシンのローカルタイムゾーンを使用します。

## リソースが正常でも高い同時実行性下で SQL が遅くなるのはなぜですか？

原因は高いネットワークまたは RPC レイテンシです。BE パラメータ `brpc_connection_type` を `pooled` に調整し、BE を再起動してください。

## 統計収集を無効にする方法は？

- 自動収集を無効にする：

  ```SQL
  enable_statistic_collect = false;
  ```

- インポートトリガーの収集を無効にする：

  ```SQL
  enable_statistic_collect_on_first_load = false;
  ```

- v3.3 以上にアップグレードしたバージョンの場合、手動で設定：

  ```SQL
  set global analyze_mv = "";
  ```