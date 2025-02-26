---
displayed_sidebar: docs
---

# StarRocks バージョン 3.1

## 3.1.17

リリース日: 2025年1月3日

### バグ修正

以下の問題を修正しました:

- クロスクラスタデータ移行ツールが、ターゲットクラスタでのパーティション削除を考慮していないため、データ同期とコミット中に Follower FE がクラッシュする問題を修正しました。[#54061](https://github.com/StarRocks/starrocks/pull/54061)
- クロスクラスタデータ移行ツールを使用して DELETE 操作を行うテーブルを同期する際に、ターゲットクラスタの BE がクラッシュする可能性がある問題を修正しました。[#54081](https://github.com/StarRocks/starrocks/pull/54081)
- BDBJE ハンドシェイクメカニズムのバグにより、Leader FE が Follower FE からの再接続試行を拒否し、Follower FE ノードが終了する問題を修正しました。[#50412](https://github.com/StarRocks/starrocks/pull/50412)
- FE のメモリ統計が重複しているため、過剰なメモリ使用量が発生する問題を修正しました。[#53055](https://github.com/StarRocks/starrocks/pull/53055)
- 非同期マテリアライズドビューのリフレッシュタスクのステータスが複数の FE ノード間で不一致であり、クエリ中にマテリアライズドビューの状態が不正確になる問題を修正しました。[#54236](https://github.com/StarRocks/starrocks/pull/54236)

## 3.1.16

リリース日: 2024年12月16日

### 改善点

- テーブル関連の統計を最適化しました。[#50316](https://github.com/StarRocks/starrocks/pull/50316)

### バグ修正

以下の問題を修正しました:

- ディスクフルシナリオでのエラーコード処理の粒度が不十分で、BE がディスクエラーを誤って識別し、データを削除する問題を修正しました。[#51411](https://github.com/StarRocks/starrocks/pull/51411)
- HTTP 1.0 を使用して送信された場合の Stream Load の失敗を修正しました。[#53010](https://github.com/StarRocks/starrocks/pull/53010) [#53008](https://github.com/StarRocks/starrocks/pull/53008)
- トランザクションが期限切れになった場合に Routine Load タスクがキャンセルされる問題を修正しました（現在は、データベースまたはテーブルが存在しない場合にのみタスクがキャンセルされ、トランザクションが期限切れになった場合は一時停止されます）。[#50334](https://github.com/StarRocks/starrocks/pull/50334)
- Broker を使用して `file://` にデータをアンロードする際に、ファイルのリネームエラーが発生し、エクスポートが失敗する問題を修正しました。[#52544](https://github.com/StarRocks/starrocks/pull/52544)
- 等値ジョインの結合条件が低基数の列に基づく式である場合、システムが誤って Runtime Filter の述語をプッシュダウンし、BE がクラッシュする可能性がある問題を修正しました。[#50690](https://github.com/StarRocks/starrocks/pull/50690)

## 3.1.15

リリース日: 2024年9月4日

### バグ修正

以下の問題を修正しました:

- 非同期マテリアライズドビューを使用したクエリの書き換え中に、特定のテーブルに対する `count(*)` が NULL を返す問題を修正しました。[#49288](https://github.com/StarRocks/starrocks/pull/49288)
- `partition_linve_nubmer` が効果を発揮しない問題を修正しました。[#49213](https://github.com/StarRocks/starrocks/pull/49213)
- FE がタブレット例外をスローする問題: BE ディスクがオフラインで、タブレットを移行できない。[#47833](https://github.com/StarRocks/starrocks/pull/47833)

## 3.1.14

リリース日: 2024年7月29日

### 改善点

- Stream Load が行と列の区切り文字として `\t` と `\n` を使用することをサポートしました。ユーザーはこれらを 16 進 ASCII コードに変換する必要がありません。[#47302](https://github.com/StarRocks/starrocks/pull/47302)

### バグ修正

以下の問題を修正しました:

- 主キーテーブルに対する頻繁な INSERT および UPDATE 操作が、データベース内の書き込みおよびクエリの遅延を引き起こす可能性がある問題を修正しました。[#47838](https://github.com/StarRocks/starrocks/pull/47838)
- 主キーテーブルがデータ永続化の失敗に直面した場合、永続性インデックスがエラーをキャプチャできず、データ損失と「Insert found duplicate key」エラーを報告する問題を修正しました。[#48045](https://github.com/StarRocks/starrocks/pull/48045)
- マテリアライズドビューのリフレッシュ時に権限が不足していると報告される問題を修正しました。[#47561](https://github.com/StarRocks/starrocks/pull/47561)
- マテリアライズドビューのリフレッシュ時に「For input string」エラーが報告される問題を修正しました。[#46131](https://github.com/StarRocks/starrocks/pull/46131)
- マテリアライズドビューのリフレッシュ中にロックが過度に長く保持され、デッドロック検出スクリプトによって Leader FE が再起動される問題を修正しました。[#48256](https://github.com/StarRocks/starrocks/pull/48256)
- 定義に IN 句を含むビューに対するクエリが不正確な結果を返す問題を修正しました。[#47484](https://github.com/StarRocks/starrocks/pull/47484)
- グローバル Runtime Filter が不正確な結果を引き起こす問題を修正しました。[#48496](https://github.com/StarRocks/starrocks/pull/48496)
- MySQL プロトコル `COM_CHANGE_USER` が `conn_attr` をサポートしていない問題を修正しました。[#47796](https://github.com/StarRocks/starrocks/pull/47796)

### 動作の変更

- ユーザーがバケット数を指定せずに非パーティション化テーブルを作成する場合、システムがテーブルに設定する最小バケット数は `16` です（以前は `2*BE または CN カウント` に基づいて `2` でした）。小さなテーブルを作成する際により小さなバケット数を設定したい場合は、明示的に設定する必要があります。[#47005](https://github.com/StarRocks/starrocks/pull/47005)

## 3.1.13

リリース日: 2024年6月26日

### 改善点

- Broker プロセスが Tencent Cloud COS Posix バケットへのアクセスをサポートしました。ユーザーは Broker Load を使用して COS Posix バケットからデータをロードしたり、SELECT INTO OUTFILE ステートメントを使用して COS Posix バケットにデータをアンロードしたりできます。[#46597](https://github.com/StarRocks/starrocks/pull/46597)
- SHOW CREATE TABLE を使用して Hive Catalogs 内の Hive テーブルのコメントを表示することをサポートしました。[#37686](https://github.com/StarRocks/starrocks/pull/37686)
- WHERE 句内の Conjunct の評価時間を最適化しました。同じ列に対する複数の LIKE 句や CASE WHEN 式など。[#46914](https://github.com/StarRocks/starrocks/pull/46914)

### バグ修正

以下の問題を修正しました:

- 共有データクラスタで削除するパーティションの数が多すぎる場合、DELETE ステートメントが失敗する問題を修正しました。[#46229](https://github.com/StarRocks/starrocks/pull/46229)

## 3.1.12

リリース日: 2024年5月30日

### 新機能

- Flink コネクタが StarRocks から複雑なデータ型 ARRAY、MAP、および STRUCT を読み取ることをサポートしました。[#42932](https://github.com/StarRocks/starrocks/pull/42932) [#347](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/347)

### 改善点

- 以前は、BE が RPC を介して FE と通信できなかった場合、FE は一般的なエラーメッセージを返していました: `call frontend service failed reason=xxx`。具体的な問題が不明確でした。エラーメッセージは、タイムアウトやサーバーのビジー状態などの具体的な理由を含むように最適化されました。[#44153](https://github.com/StarRocks/starrocks/pull/44153)
- データロード中の具体的な問題を示すエラーメッセージを改善しました。たとえば、エラーデータ行の数が制限を超えた場合、列数が一致しない場合、無効な列名がある場合、または任意のパーティションにデータがない場合など。

### セキュリティ

- CVE-2023-25194 セキュリティ問題を修正するために Kafka クライアント依存関係を v3.4.0 にアップグレードしました。[#45382](https://github.com/StarRocks/starrocks/pull/45382)

### バグ修正

以下の問題を修正しました:

- マテリアライズドビューの定義に同じテーブルの複数の自己結合が含まれており、そのテーブルに基づいてパーティションごとにインクリメンタルリフレッシュが行われる場合、誤ったパーティション選択により不正確な結果が発生する問題を修正しました。[#45936](https://github.com/StarRocks/starrocks/pull/45936)
- 共有データクラスタでマテリアライズドビューにビットマップインデックスを作成すると FEs がクラッシュする問題を修正しました。[#45665](https://github.com/StarRocks/starrocks/pull/45665)
- ODBC 経由で FE フォロワーに接続し、CREATE TABLE を実行すると、ヌルポインタの問題により BEs がクラッシュする問題を修正しました。[#45043](https://github.com/StarRocks/starrocks/pull/45043)
- 多くの非同期タスクが存在する場合、`information_schema.task_runs` をクエリすると頻繁に失敗する問題を修正しました。[#45520](https://github.com/StarRocks/starrocks/pull/45520)
- SQL ステートメントに複数の COUNT DISTINCT が含まれ、LIMIT が含まれている場合、LIMIT が誤って処理され、ステートメントが実行されるたびに返されるデータが不一致になる問題を修正しました。[#44749](https://github.com/StarRocks/starrocks/pull/44749)
- Duplicate Key テーブルおよび集計テーブルに対する ORDER BY LIMIT 句を含むクエリが不正確な結果を生成する問題を修正しました。[#45037](https://github.com/StarRocks/starrocks/pull/45037)

## 3.1.11

リリース日: 2024年4月28日

### 動作の変更

- ユーザーは DROP TABLE を使用してシステムデータベース `information_schema` 内のビューを削除することはできません。[#43556](https://github.com/StarRocks/starrocks/pull/43556)
- Primary Key テーブルを作成する際に ORDER BY 句で重複したキーを指定することはできません。[#43374](https://github.com/StarRocks/starrocks/pull/43374)

### 改善点

- Parquet 形式の Iceberg v2 テーブルに対するクエリが等価削除をサポートします。

### バグ修正

以下の問題を修正しました:

- 外部カタログ内の外部テーブルからデータをクエリする際、ユーザーがそのテーブルに対して SELECT 権限を持っている場合でもアクセスが拒否される問題を修正しました。SHOW GRANTS もユーザーがこの権限を持っていることを示します。[#44061](https://github.com/StarRocks/starrocks/pull/44061)
- `str_to_map` が BEs をクラッシュさせる可能性がある問題を修正しました。[#43930](https://github.com/StarRocks/starrocks/pull/43930)
- Routine Load ジョブが進行中のときに `show proc '/routine_loads'` を実行するとデッドロックが発生し、スタックする問題を修正しました。[#44249](https://github.com/StarRocks/starrocks/pull/44249)
- 主キーテーブルの永続性インデックスが同時実行制御の問題により BEs をクラッシュさせる可能性がある問題を修正しました。[#43720](https://github.com/StarRocks/starrocks/pull/43720)
- `leaderFE_IP:8030` のページに表示される `pending_task_run_count` が不正確である問題を修正しました。表示される数値は、保留中のタスクと実行中のタスクの合計であり、保留中のタスクではありません。さらに、`followerFE_IP:8030` を使用して `refresh_pending` メトリックの情報を表示できません。[#43052](https://github.com/StarRocks/starrocks/pull/43052)
- `information_schema.task_runs` をクエリすると頻繁に失敗する問題を修正しました。[#43052](https://github.com/StarRocks/starrocks/pull/43052)
- CTE を含む一部の SQL クエリが `Invalid plan: PhysicalTopNOperator` エラーに遭遇する問題を修正しました。[#44185](https://github.com/StarRocks/starrocks/pull/44185)

## 3.1.10 (取り下げ)

:::tip

このバージョンは、Hive や Iceberg などの外部カタログ内の外部テーブルのクエリにおける権限の問題のためにオフラインにされました。

- **問題**: 外部カタログ内の外部テーブルからデータをクエリする際、ユーザーがそのテーブルに対して SELECT 権限を持っている場合でもアクセスが拒否されます。SHOW GRANTS もユーザーがこの権限を持っていることを示します。

- **影響範囲**: この問題は、外部カタログ内の外部テーブルに対するクエリにのみ影響します。他のクエリには影響しません。

- **一時的な回避策**: このテーブルに対して再度 SELECT 権限を付与するとクエリが成功します。ただし、`SHOW GRANTS` は重複した権限エントリを返します。v3.1.11 にアップグレードした後、ユーザーは `REVOKE` を実行して権限エントリの一つを削除できます。

:::

リリース日: 2024年3月29日

### 新機能

- 主キーテーブルがサイズ階層型 Compaction をサポートしました。[#42474](https://github.com/StarRocks/starrocks/pull/42474)
- パターンマッチング関数 `regexp_extract_all` を追加しました。[#42178](https://github.com/StarRocks/starrocks/pull/42178)

### 動作の変更

- JSON データ内の null 値が `IS NULL` 演算子に基づいて評価される場合、それらは SQL 言語に従って NULL 値と見なされます。たとえば、`SELECT parse_json('{"a": null}') -> 'a' IS NULL` に対して `true` が返されます（この動作変更前は `false` が返されていました）。[#42815](https://github.com/StarRocks/starrocks/pull/42815)

### 改善点

- Broker Load を使用して TIMESTAMP 型データを含む ORC ファイルからデータをロードする場合、StarRocks はタイムスタンプを DATETIME データ型に変換する際にマイクロ秒を保持することをサポートしました。[#42348](https://github.com/StarRocks/starrocks/pull/42348)

### バグ修正

以下の問題を修正しました:

- 共有データモードでは、主キーテーブルに作成された永続性インデックスを処理するためのガーベジコレクションとスレッドエビクションメカニズムが CN ノードで効果を発揮しない問題を修正しました。その結果、古いデータが削除されません。[#42241](https://github.com/StarRocks/starrocks/pull/42241)
- Hive カタログを使用して ORC ファイルをクエリする場合、StarRocks が位置によるマッピングに基づいて ORC ファイルを読み取るため、クエリ結果が不正確になる可能性がある問題を修正しました。この問題を解決するために、ユーザーはセッション変数 `orc_use_column_names` を `true` に設定し、列名によるマッピングに基づいて Hive から ORC ファイルを読み取るように指定できます。[#42905](https://github.com/StarRocks/starrocks/pull/42905)
- AD システムのために LDAP 認証が採用されている場合、パスワードなしでのログインが許可される問題を修正しました。[#42476](https://github.com/StarRocks/starrocks/pull/42476)
- ディスクデバイス名が数字で終わる場合、これらの数字が削除された後にディスクデバイス名が無効になる可能性があるため、モニタリングメトリックの値が 0 のままになる問題を修正しました。[#42741](https://github.com/StarRocks/starrocks/pull/42741)

## 3.1.9

リリース日: 2024年3月8日

### 新機能

- 共有データクラスタ内のクラウドネイティブ主キーテーブルが、サイズ階層型 Compaction をサポートし、多数の小サイズファイルのロードにおける書き込み I/O 増幅を削減します。[#41610](https://github.com/StarRocks/starrocks/pull/41610)
- パーティションの詳細なメタデータを記録するビュー `information_schema.partitions_meta` を追加しました。[#41101](https://github.com/StarRocks/starrocks/pull/41101)
- StarRocks のメモリ使用量を記録するビュー `sys.fe_memory_usage` を追加しました。[#41083](https://github.com/StarRocks/starrocks/pull/41083)

### 動作の変更

- 動的パーティション化のロジックが変更されました。現在、DATE 型のパーティション列は時間レベルのデータをサポートしていません。DATETIME 型のパーティション列は引き続き時間レベルのデータをサポートしています。[#40328](https://github.com/StarRocks/starrocks/pull/40328)
- マテリアライズドビューをリフレッシュできるユーザーが `root` ユーザーからマテリアライズドビューを作成したユーザーに変更されました。この変更は既存のマテリアライズドビューには影響しません。[#40698](https://github.com/StarRocks/starrocks/pull/40698)
- デフォルトでは、定数型と文字列型の列を比較する際、StarRocks はそれらを文字列として比較します。ユーザーはセッション変数 `cbo_eq_base_type` を使用して、比較に使用されるデフォルトのルールを調整できます。たとえば、`cbo_eq_base_type` を `decimal` に設定すると、StarRocks は列を数値として比較します。[#41712](https://github.com/StarRocks/starrocks/pull/41712)

### 改善点

- StarRocks は、AWS SDK を介してアクセスできる S3 互換オブジェクトストレージを指定するためのパラメータ `s3_compatible_fs_list` を使用し、HDFS スキーマを介してアクセスが必要な非 S3 互換オブジェクトストレージを指定するためのパラメータ `fallback_to_hadoop_fs_list` をサポートします（この方法ではベンダー提供の JAR パッケージの使用が必要です）。[#41612](https://github.com/StarRocks/starrocks/pull/41612)
- Trino の SQL ステートメント構文との互換性が最適化され、Trino の次の関数の変換をサポートします: `current_catalog`, `current_schema`, `to_char`, `from_hex`, `to_date`, `to_timestamp`, および `index`。[#41505](https://github.com/StarRocks/starrocks/pull/41505) [#41270](https://github.com/StarRocks/starrocks/pull/41270) [#40838](https://github.com/StarRocks/starrocks/pull/40838)
- クエリ計画中に許可される候補マテリアライズドビューの最大数を制御するために、新しいセッション変数 `cbo_materialized_view_rewrite_related_mvs_limit` が追加されました。このセッション変数のデフォルト値は `64` です。このセッション変数は、クエリ計画中にクエリのための候補マテリアライズドビューの数が多すぎることによる過剰なリソース消費を軽減するのに役立ちます。[#39829](https://github.com/StarRocks/starrocks/pull/39829)
- 集計テーブルの BITMAP 型列の `agg_type` を `replace_if_not_null` に設定して、テーブルの一部の列のみを更新することをサポートします。[#42102](https://github.com/StarRocks/starrocks/pull/42102)
- セッション変数 `cbo_eq_base_type` が最適化され、文字列型と数値型のデータを含むデータの比較に適用される暗黙の変換ルールを指定することをサポートします。デフォルトでは、そのようなデータは文字列として比較されます。[#40619](https://github.com/StarRocks/starrocks/pull/41712)
- Iceberg テーブルのパーティション式をよりよくサポートするために、より多くの DATE 型データ（たとえば、"%Y-%m-%e %H:%i"）を認識できるようになりました。[#40474](https://github.com/StarRocks/starrocks/pull/40474)
- JDBC コネクタが TIME データ型をサポートします。[#31940](https://github.com/StarRocks/starrocks/pull/31940)
- ファイル外部テーブルを作成するための SQL ステートメントの `path` パラメータがワイルドカード（`*`）をサポートします。ただし、Broker Load ジョブを作成するための SQL ステートメントの `DATA INFILE` パラメータと同様に、`path` パラメータはワイルドカード（`*`）を使用してディレクトリまたはファイルの最大 1 レベルを一致させることをサポートします。[#40844](https://github.com/StarRocks/starrocks/pull/40844)
- 統計とマテリアライズドビューに関連するログデータを記録するための新しい内部 SQL ログファイルが追加されました。[#40682](https://github.com/StarRocks/starrocks/pull/40682)

### バグ修正

以下の問題を修正しました:

- Hive ビューの作成にクエリされたテーブルまたはビューの名前またはエイリアスに一貫性のない大文字小文字が割り当てられている場合、「Analyze Error」がスローされる問題を修正しました。[#40921](https://github.com/StarRocks/starrocks/pull/40921)
- 主キーテーブルに永続性インデックスが作成されている場合、I/O 使用量が上限に達する問題を修正しました。[#39959](https://github.com/StarRocks/starrocks/pull/39959)
- 共有データクラスタでは、主キーインデックスディレクトリが 5 時間ごとに削除される問題を修正しました。[#40745](https://github.com/StarRocks/starrocks/pull/40745)
- リストパーティション化が有効になっているテーブルが切り捨てられたり、そのパーティションが切り捨てられたりした後、そのテーブルのパーティションキーに基づくクエリがデータを返さない問題を修正しました。[#40495](https://github.com/StarRocks/starrocks/pull/40495)
- ユーザーが手動で ALTER TABLE COMPACT を実行した後、Compaction 操作のメモリ使用量統計が異常になる問題を修正しました。[#41150](https://github.com/StarRocks/starrocks/pull/41150)
- クラスタ間のデータ移行中に、列モードで一部の列のみが更新される場合、宛先クラスタがクラッシュする可能性がある問題を修正しました。[#40692](https://github.com/StarRocks/starrocks/pull/40692)
- 提出された SQL ステートメントに複数のスペースまたは改行文字が含まれている場合、SQL ブラックリストが効果を発揮しない可能性がある問題を修正しました。[#40457](https://github.com/StarRocks/starrocks/pull/40457)

## 3.1.8

リリース日: 2024年2月5日

### 新機能

- StarRocks コミュニティは、共有なしクラスタから別の共有なしクラスタまたは共有データクラスタへのデータ移行をサポートする StarRocks クロスクラスタデータ移行ツールを提供します。
- WHERE 句を指定して同期マテリアライズドビューを作成することをサポートします。
- データキャッシュのメモリ使用量を MemTracker に表示するメトリックを追加しました。[#39600](https://github.com/StarRocks/starrocks/pull/39600)

### パラメータ変更

- 共有データ StarRocks クラスタ内の主キーテーブルの Compaction タスクで許可される最大入力行セット数を制御する BE 構成項目 `lake_pk_compaction_max_input_rowsets` を追加しました。これにより、Compaction タスクのリソース消費が最適化されます。[#39611](https://github.com/StarRocks/starrocks/pull/39611)

### 改善点

- CTAS ステートメントで ORDER BY および INDEX 句をサポートします。[#38886](https://github.com/StarRocks/starrocks/pull/38886)
- ORC 形式の Iceberg v2 テーブルで等価削除をサポートします。[#37419](https://github.com/StarRocks/starrocks/pull/37419)
- リストパーティション化戦略で作成されたクラウドネイティブテーブルに対して `datacache.partition_duration` プロパティを設定することをサポートします。このプロパティはデータキャッシュの有効期間を制御し、動的に構成できます。[#35681](https://github.com/StarRocks/starrocks/pull/35681) [#38509](https://github.com/StarRocks/starrocks/pull/38509)
- BE 構成項目 `update_compaction_per_tablet_min_interval_seconds` を最適化しました。このパラメータは元々、主キーテーブルの Compaction タスクの頻度を制御するためにのみ使用されていました。最適化後、主キーテーブルインデックスの主要な Compaction タスクの頻度を制御するためにも使用できます。[#39640](https://github.com/StarRocks/starrocks/pull/39640)
- Parquet Reader は、Parquet 形式のデータ内の INT32 型データを DATETIME 型データに変換し、結果のデータを StarRocks に保存することをサポートします。[#39808](https://github.com/StarRocks/starrocks/pull/39808)

### バグ修正

以下の問題を修正しました:

- ORDER BY 列として NaN（Not a Number）列を使用すると、BEs がクラッシュする可能性がある問題を修正しました。[#30759](https://github.com/StarRocks/starrocks/pull/30759)
- 主キーインデックスの更新に失敗すると、「get_applied_rowsets failed」というエラーが発生する可能性がある問題を修正しました。[#27488](https://github.com/StarRocks/starrocks/pull/27488)
- Compaction タスクの失敗後に compaction_state_cache に占有されたリソースが再利用されない問題を修正しました。[#38499](https://github.com/StarRocks/starrocks/pull/38499)
- 外部テーブルのパーティション列に null 値が含まれている場合、それらのテーブルに対するクエリが BEs をクラッシュさせる問題を修正しました。[#38888](https://github.com/StarRocks/starrocks/pull/38888)
- テーブルが削除され、その後同じテーブル名で再作成された後、そのテーブルに作成された非同期マテリアライズドビューのリフレッシュが失敗する問題を修正しました。[#38008](https://github.com/StarRocks/starrocks/pull/38008)
- 空の Iceberg テーブルに作成された非同期マテリアライズドビューのリフレッシュが失敗する問題を修正しました。[#24068](https://starrocks.atlassian.net/browse/SR-24068)

## 3.1.7

リリース日: 2024年1月12日

### 新機能

- 新しい関数 `unnest_bitmap` を追加しました。[#38136](https://github.com/StarRocks/starrocks/pull/38136)
- [Broker Load](https://docs.starrocks.io/docs/3.1/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/#opt_properties) の条件付き更新をサポートします。[#37400](https://github.com/StarRocks/starrocks/pull/37400)

### 動作の変更

- マテリアライズドビューが INSERT INTO SELECT ステートメント内のクエリを書き換えるかどうかを制御するセッション変数 `enable_materialized_view_for_insert` を追加しました。デフォルト値は `false` です。[#37505](https://github.com/StarRocks/starrocks/pull/37505)
- FE 動的パラメータ `enable_new_publish_mechanism` が静的パラメータに変更されました。パラメータ設定を変更した後、FE を再起動する必要があります。[#35338](https://github.com/StarRocks/starrocks/pull/35338)
- セッション変数 `enable_strict_order_by` を追加しました。この変数がデフォルト値 `TRUE` に設定されている場合、クエリパターンに対してエラーが報告されます: クエリの異なる式で重複したエイリアスが使用され、このエイリアスが ORDER BY のソートフィールドでもある場合、たとえば `select distinct t1.* from tbl1 t1 order by t1.k1;`。ロジックは v2.3 およびそれ以前と同じです。この変数が `FALSE` に設定されている場合、緩やかな重複排除メカニズムが使用され、そのようなクエリを有効な SQL クエリとして処理します。[#37910](https://github.com/StarRocks/starrocks/pull/37910)

### パラメータ変更

- FE 構成項目 `routine_load_unstable_threshold_second` を追加しました。[#36222](https://github.com/StarRocks/starrocks/pull/36222)
- HTTP リクエストを処理するための HTTP サーバーのスレッド数を指定する FE 構成項目 `http_worker_threads_num` を追加しました。デフォルト値は `0` です。このパラメータの値が負の値または `0` に設定されている場合、実際のスレッド数は CPU コア数の 2 倍です。[#37530](https://github.com/StarRocks/starrocks/pull/37530)
- ディスク上の Compaction の最大同時実行数を構成するための BE 構成項目 `pindex_major_compaction_limit_per_disk` を追加しました。これにより、Compaction によるディスク間の I/O の不均一性の問題が解決されます。この問題は、特定のディスクの I/O が非常に高くなる原因となります。デフォルト値は `1` です。[#36681](https://github.com/StarRocks/starrocks/pull/36681)
- トランザクションアクセスモードを指定するセッション変数 `transaction_read_only` と `tx_read_only` を追加しました。これらは MySQL バージョン 5.7.20 以降と互換性があります。[#37249](https://github.com/StarRocks/starrocks/pull/37249)
- マテリアライズドビューが作成された後にすぐにリフレッシュするかどうかを指定する FE 構成項目 `default_mv_refresh_immediate` を追加しました。デフォルト値は `true` です。[#37093](https://github.com/StarRocks/starrocks/pull/37093)
- 共有データクラスタで Compaction タスクがローカルディスクにデータをキャッシュすることを許可するかどうかを指定する新しい BE 構成項目 `lake_enable_vertical_compaction_fill_data_cache` を追加しました。デフォルト値は `false` です。[#37296](https://github.com/StarRocks/starrocks/pull/37296)

### 改善点

- INSERT INTO FILE() SELECT FROM がテーブルから BINARY 型データを読み取り、リモートストレージに Parquet 形式のファイルとしてデータをエクスポートすることをサポートします。[#36797](https://github.com/StarRocks/starrocks/pull/36797)
- 非同期マテリアライズドビューが、データキャッシュ内のホットデータの有効期間を制御する `datacache.partition_duration` プロパティを動的に設定することをサポートします。[#35681](https://github.com/StarRocks/starrocks/pull/35681)
- JDK を使用する場合、デフォルトの GC アルゴリズムは G1 です。[#37386](https://github.com/StarRocks/starrocks/pull/37386)
- LIKE 演算子の右側の文字列が `%` または `_` を含まない場合、LIKE 演算子は `=` 演算子に変換されます。[#37515](https://github.com/StarRocks/starrocks/pull/37515)
- Kafka トピックの各パーティション内の最新メッセージの位置を記録するために、[SHOW ROUTINE LOAD](https://docs.starrocks.io/zh/docs/3.1/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) の戻り結果に新しいフィールド `LatestSourcePosition` が追加されました。これにより、データロードの遅延を確認できます。[#38298](https://github.com/StarRocks/starrocks/pull/38298)
- システム変数 `spill_mode` が `auto` に設定されている場合、リソースグループが中間結果のスピリングをトリガーするメモリ使用量のしきい値（パーセンテージ）を制御する新しいリソースグループプロパティ `spill_mem_limit_threshold` が追加されました。有効な範囲は (0, 1) です。デフォルト値は `1` で、しきい値は効果を発揮しません。[#37707](https://github.com/StarRocks/starrocks/pull/37707)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/3.1/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) ステートメントの戻り結果に、各パーティションからの消費メッセージのタイムスタンプが含まれるようになりました。[#36222](https://github.com/StarRocks/starrocks/pull/36222)
- Routine Load のスケジューリングポリシーが最適化され、遅いタスクが他の通常のタスクの実行を妨げないようになりました。[#37638](https://github.com/StarRocks/starrocks/pull/37638)

### バグ修正

以下の問題を修正しました:

- [ANALYZE TABLE](https://docs.starrocks.io/docs/3.1/sql-reference/sql-statements/data-definition/ANALYZE_TABLE/) の実行が時々スタックする問題を修正しました。[#36836](https://github.com/StarRocks/starrocks/pull/36836)
- PageCache によるメモリ消費が、BE 動的パラメータ `storage_page_cache_limit` で指定されたしきい値を超える場合がある問題を修正しました。[#37740](https://github.com/StarRocks/starrocks/pull/37740)
- [Hive catalogs](https://docs.starrocks.io/docs/3.1/data_source/catalog/hive_catalog/) 内の Hive メタデータが、Hive テーブルに新しいフィールドが追加されたときに自動的に更新されない問題を修正しました。[#37668](https://github.com/StarRocks/starrocks/pull/37668)
- 場合によっては、`bitmap_to_string` がデータ型のオーバーフローにより不正確な結果を返す可能性がある問題を修正しました。[#37405](https://github.com/StarRocks/starrocks/pull/37405)
- 空のテーブルに対して DELETE ステートメントを実行すると、「ERROR 1064 (HY000): Index: 0, Size: 0」が返される問題を修正しました。[#37461](https://github.com/StarRocks/starrocks/pull/37461)
- FE 動的パラメータ `enable_sync_publish` が `TRUE` に設定されている場合、BEs がクラッシュして再起動した後に書き込まれたデータに対するクエリが失敗する可能性がある問題を修正しました。[#37398](https://github.com/StarRocks/starrocks/pull/37398)
- StarRocks Information Schema の `views` の `TABLE_CATALOG` フィールドの値が `null` である問題を修正しました。[#37570](https://github.com/StarRocks/starrocks/pull/37570)
- `SELECT ... FROM ... INTO OUTFILE` を実行して CSV ファイルにデータをエクスポートすると、FROM 句に複数の定数が含まれている場合、「Unmatched number of columns」というエラーが報告される問題を修正しました。[#38045](https://github.com/StarRocks/starrocks/pull/38045)

## 3.1.6

リリース日: 2023年12月18日

### 新機能

- 指定された小数秒の精度（マイクロ秒までの精度）で現在の日付と時刻を返す [now(p)](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/now/) 関数を追加しました。`p` が指定されていない場合、この関数は秒までの精度で日付と時刻のみを返します。[#36676](https://github.com/StarRocks/starrocks/pull/36676)
- 許可される最大行セット数を設定するための新しいメトリック `max_tablet_rowset_num` を追加しました。このメトリックは、Compaction の問題を検出し、「バージョンが多すぎる」というエラーの発生を減らすのに役立ちます。[#36539](https://github.com/StarRocks/starrocks/pull/36539)
- コマンドラインツールを使用してヒーププロファイルを取得することをサポートし、トラブルシューティングを容易にします。[#35322](https://github.com/StarRocks/starrocks/pull/35322)
- 一般的なテーブル式（CTE）を使用して非同期マテリアライズドビューを作成することをサポートします。[#36142](https://github.com/StarRocks/starrocks/pull/36142)
- 次のビットマップ関数を追加しました: [subdivide_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/), [bitmap_from_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_from_binary/), および [bitmap_to_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_to_binary/)。[#35817](https://github.com/StarRocks/starrocks/pull/35817) [#35621](https://github.com/StarRocks/starrocks/pull/35621)
- 主キーテーブルの Compaction スコアを計算するためのロジックを最適化し、他の 3 つのテーブルタイプとより一貫性のある範囲内で主キーテーブルの Compaction スコアを整合させました。[#36534](https://github.com/StarRocks/starrocks/pull/36534)

### パラメータ変更

- ゴミ箱ファイルのデフォルトの保持期間が元の 3 日から 1 日に変更されました。[#37113](https://github.com/StarRocks/starrocks/pull/37113)
- 新しい BE 構成項目 `enable_stream_load_verbose_log` が追加されました。デフォルト値は `false` です。このパラメータを `true` に設定すると、StarRocks は Stream Load ジョブの HTTP リクエストとレスポンスを記録し、トラブルシューティングを容易にします。[#36113](https://github.com/StarRocks/starrocks/pull/36113)
- 新しい BE 構成項目 `enable_lazy_delta_column_compaction` が追加されました。デフォルト値は `true` で、StarRocks がデルタ列に対して頻繁な Compaction 操作を行わないことを示します。[#36654](https://github.com/StarRocks/starrocks/pull/36654)
- ベーステーブル（ビュー）がスキーマ変更を受けたり、削除されて再作成されたために非アクティブに設定された非同期マテリアライズドビューをシステムが自動的にチェックして再アクティブ化するかどうかを制御するための新しい FE 構成項目 `enable_mv_automatic_active_check` が追加されました。デフォルト値は `true` です。[#36463](https://github.com/StarRocks/starrocks/pull/36463)

### 改善点

- セッション変数 [sql_mode](https://docs.starrocks.io/docs/reference/System_variable/#sql_mode) に新しい値オプション `GROUP_CONCAT_LEGACY` が追加され、v2.5 より前のバージョンでの [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) 関数の実装ロジックとの互換性が提供されます。[#36150](https://github.com/StarRocks/starrocks/pull/36150)
- [SHOW DATA](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_DATA/) ステートメントによって返される主キーテーブルのサイズには、部分列更新および生成列に関連する **.cols** ファイルと永続性インデックスファイルのサイズが含まれます。[#34898](https://github.com/StarRocks/starrocks/pull/34898)
- MySQL 外部テーブルおよび JDBC カタログ内の外部テーブルに対するクエリが WHERE 句にキーワードを含めることをサポートします。[#35917](https://github.com/StarRocks/starrocks/pull/35917)
- プラグインのロード失敗がエラーを引き起こしたり、FE の起動失敗を引き起こしたりすることはなくなります。代わりに、FE は適切に起動し、プラグインのエラーステータスを [SHOW PLUGINS](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/SHOW_PLUGINS/) を使用してクエリできます。[#36566](https://github.com/StarRocks/starrocks/pull/36566)
- 動的パーティション化がランダム分布をサポートします。[#35513](https://github.com/StarRocks/starrocks/pull/35513)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) ステートメントによって返される結果に、新しいフィールド `OtherMsg` が追加され、最後に失敗したタスクに関する情報が表示されます。[#35806](https://github.com/StarRocks/starrocks/pull/35806)
- Broker Load ジョブでの AWS S3 の認証情報 `aws.s3.access_key` および `aws.s3.access_secret` が監査ログで非表示になります。[#36571](https://github.com/StarRocks/starrocks/pull/36571)
- `information_schema` データベースの `be_tablets` ビューに新しいフィールド `INDEX_DISK` が追加され、永続性インデックスのディスク使用量（バイト単位）が記録されます。[#35615](https://github.com/StarRocks/starrocks/pull/35615)

### バグ修正

以下の問題を修正しました:

- データが破損している場合に永続性インデックスを作成すると BEs がクラッシュする問題を修正しました。[#30841](https://github.com/StarRocks/starrocks/pull/30841)
- ネストされたクエリを含む非同期マテリアライズドビューを作成すると、「resolve partition column failed」というエラーが報告される問題を修正しました。[#26078](https://github.com/StarRocks/starrocks/issues/26078)
- データが破損しているベーステーブルに非同期マテリアライズドビューを作成すると、「Unexpected exception: null」というエラーが報告される問題を修正しました。[#30038](https://github.com/StarRocks/starrocks/pull/30038)
- ウィンドウ関数を含むクエリを実行すると、SQL エラー「[1064] [42000]: Row count of const column reach limit: 4294967296」が報告される問題を修正しました。[#33561](https://github.com/StarRocks/starrocks/pull/33561)
- FE 構成項目 `enable_collect_query_detail_info` を `true` に設定すると、FE のパフォーマンスが低下する問題を修正しました。[#35945](https://github.com/StarRocks/starrocks/pull/35945)
- StarRocks 共有データモードでは、オブジェクトストレージからファイルを削除しようとすると「Reduce your request rate」というエラーが報告される可能性がある問題を修正しました。[#35566](https://github.com/StarRocks/starrocks/pull/35566)
- マテリアライズドビューをリフレッシュするとデッドロックが発生する可能性がある問題を修正しました。[#35736](https://github.com/StarRocks/starrocks/pull/35736)
- DISTINCT ウィンドウオペレータープッシュダウン機能が有効になっている場合、ウィンドウ関数によって計算された列の複雑な式に対して SELECT DISTINCT 操作を実行するとエラーが報告される問題を修正しました。[#36357](https://github.com/StarRocks/starrocks/pull/36357)
- ソースデータファイルが ORC 形式でネストされた配列を含む場合、BEs がクラッシュする問題を修正しました。[#36127](https://github.com/StarRocks/starrocks/pull/36127)
- 一部の S3 互換オブジェクトストレージが重複したファイルを返し、BEs がクラッシュする問題を修正しました。[#36103](https://github.com/StarRocks/starrocks/pull/36103)
- [array_distinct](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_distinct/) 関数が時々 BEs をクラッシュさせる問題を修正しました。[#36377](https://github.com/StarRocks/starrocks/pull/36377)
- グローバル Runtime Filter が特定のシナリオで BEs をクラッシュさせる可能性がある問題を修正しました。[#35776](https://github.com/StarRocks/starrocks/pull/35776)

## 3.1.5

リリース日: 2023年11月28日

### 新機能

- StarRocks 共有データクラスタの CN ノードがデータエクスポートをサポートするようになりました。[#34018](https://github.com/StarRocks/starrocks/pull/34018)

### 改善点

- システムデータベース `INFORMATION_SCHEMA` の [`COLUMNS`](https://docs.starrocks.io/docs/reference/information_schema/columns/) ビューが ARRAY、MAP、および STRUCT 列を表示できるようになりました。[#33431](https://github.com/StarRocks/starrocks/pull/33431)
- [Hive](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/) に保存されている LZO で圧縮された Parquet、ORC、および CSV 形式のファイルに対するクエリをサポートします。[#30923](https://github.com/StarRocks/starrocks/pull/30923)  [#30721](https://github.com/StarRocks/starrocks/pull/30721)
- 自動パーティション化されたテーブルの指定されたパーティションへの更新をサポートします。指定されたパーティションが存在しない場合、エラーが返されます。[#34777](https://github.com/StarRocks/starrocks/pull/34777)
- テーブルおよびビュー（これらのビューに関連付けられた他のテーブルおよびマテリアライズドビューを含む）に対して Swap、Drop、または Schema Change 操作が実行されたときにマテリアライズドビューの自動リフレッシュをサポートします。[#32829](https://github.com/StarRocks/starrocks/pull/32829)
- 一部のビットマップ関連操作のパフォーマンスを最適化しました。これには次のものが含まれます:
  - ネストされたループジョインを最適化しました。[#340804](https://github.com/StarRocks/starrocks/pull/34804)  [#35003](https://github.com/StarRocks/starrocks/pull/35003)
  - `bitmap_xor` 関数を最適化しました。[#34069](https://github.com/StarRocks/starrocks/pull/34069)
  - ビットマップパフォーマンスを最適化し、メモリ消費を削減するために Copy on Write をサポートします。[#34047](https://github.com/StarRocks/starrocks/pull/34047)

### バグ修正

以下の問題を修正しました:

- Broker Load ジョブでフィルタリング条件が指定されている場合、特定の状況でデータロード中に BEs がクラッシュする可能性がある問題を修正しました。[#29832](https://github.com/StarRocks/starrocks/pull/29832)
- SHOW GRANTS を実行すると不明なエラーが報告される問題を修正しました。[#30100](https://github.com/StarRocks/starrocks/pull/30100)
- 式に基づく自動パーティション化を使用するテーブルにデータがロードされると、「Error: The row create partition failed since Runtime error: failed to analyse partition value」というエラーがスローされる可能性がある問題を修正しました。[#33513](https://github.com/StarRocks/starrocks/pull/33513)
- クエリに対して「get_applied_rowsets failed, tablet updates is in error state: tablet:18849 actual row size changed after compaction」というエラーが返される問題を修正しました。[#33246](https://github.com/StarRocks/starrocks/pull/33246)
- StarRocks 共有なしクラスタで、Iceberg または Hive テーブルに対するクエリが BEs をクラッシュさせる可能性がある問題を修正しました。[#34682](https://github.com/StarRocks/starrocks/pull/34682)
- StarRocks 共有なしクラスタで、データロード中に複数のパーティションが自動的に作成される場合、ロードされたデータが時々一致しないパーティションに書き込まれる可能性がある問題を修正しました。[#34731](https://github.com/StarRocks/starrocks/pull/34731)
- 永続性インデックスが有効になっている主キーテーブルに対する長時間の頻繁なデータロードが BEs をクラッシュさせる可能性がある問題を修正しました。[#33220](https://github.com/StarRocks/starrocks/pull/33220)
- クエリに対して「Exception: java.lang.IllegalStateException: null」というエラーが返される問題を修正しました。[#33535](https://github.com/StarRocks/starrocks/pull/33535)
- `show proc '/current_queries';` が実行されている間にクエリが実行され始めると、BEs がクラッシュする可能性がある問題を修正しました。[#34316](https://github.com/StarRocks/starrocks/pull/34316)
- 永続性インデックスが有効になっている主キーテーブルに大量のデータをロードするとエラーがスローされる可能性がある問題を修正しました。[#34352](https://github.com/StarRocks/starrocks/pull/34352)
- StarRocks が v2.4 以前から後のバージョンにアップグレードされた後、Compaction スコアが予期せず上昇する問題を修正しました。[#34618](https://github.com/StarRocks/starrocks/pull/34618)
- MariaDB ODBC データベースドライバを使用して `INFORMATION_SCHEMA` をクエリすると、`schemata` ビューで返される `CATALOG_NAME` 列が `null` 値のみを保持する問題を修正しました。[#34627](https://github.com/StarRocks/starrocks/pull/34627)
- 異常なデータがロードされると FEs がクラッシュし、再起動できない問題を修正しました。[#34590](https://github.com/StarRocks/starrocks/pull/34590)
- Stream Load ジョブが **PREPARED** 状態にある間にスキーマ変更が実行されると、ジョブによってロードされるソースデータの一部が失われる問題を修正しました。[#34381](https://github.com/StarRocks/starrocks/pull/34381)
- HDFS ストレージパスの末尾にスラッシュ（`/`）が 2 つ以上含まれていると、HDFS からのデータのバックアップと復元が失敗する問題を修正しました。[#34601](https://github.com/StarRocks/starrocks/pull/34601)
- セッション変数 `enable_load_profile` を `true` に設定すると、Stream Load ジョブが失敗しやすくなる問題を修正しました。[#34544](https://github.com/StarRocks/starrocks/pull/34544)
- 列モードで主キーテーブルに部分更新を実行すると、テーブルの一部のタブレットがそのレプリカ間でデータの不一致を示す問題を修正しました。[#34555](https://github.com/StarRocks/starrocks/pull/34555)
- ALTER TABLE ステートメントを使用して追加された `partition_live_number` プロパティが効果を発揮しない問題を修正しました。[#34842](https://github.com/StarRocks/starrocks/pull/34842)
- FEs が起動に失敗し、「failed to load journal type 118」というエラーを報告する問題を修正しました。[#34590](https://github.com/StarRocks/starrocks/pull/34590)
- FE パラメータ `recover_with_empty_tablet` を `true` に設定すると FEs がクラッシュする可能性がある問題を修正しました。[#33071](https://github.com/StarRocks/starrocks/pull/33071)
- レプリカ操作の再生に失敗すると FEs がクラッシュする可能性がある問題を修正しました。[#32295](https://github.com/StarRocks/starrocks/pull/32295)

### パラメータ変更

#### FE/BE パラメータ

- 統計クエリのプロファイルを生成するかどうかを制御する FE 構成項目 [`enable_statistics_collect_profile`](https://docs.starrocks.io/docs/administration/FE_configuration#enable_statistics_collect_profile) を追加しました。デフォルト値は `false` です。[#33815](https://github.com/StarRocks/starrocks/pull/33815)
- FE 構成項目 [`mysql_server_version`](https://docs.starrocks.io/docs/administration/FE_configuration#mysql_server_version) は現在可変です。新しい設定は FE の再起動を必要とせずに現在のセッションに対して有効になります。[#34033](https://github.com/StarRocks/starrocks/pull/34033)
- StarRocks 共有データクラスタ内の主キーテーブルに対する Compaction がマージできるデータの最大割合を制御する BE/CN 構成項目 [`update_compaction_ratio_threshold`](https://docs.starrocks.io/docs/administration/BE_configuration#update_compaction_ratio_threshold) を追加しました。デフォルト値は `0.5` です。単一のタブレットが過度に大きくなる場合、この値を縮小することをお勧めします。StarRocks 共有なしクラスタの場合、主キーテーブルに対する Compaction がマージできるデータの割合は引き続き自動的に調整されます。[#35129](https://github.com/StarRocks/starrocks/pull/35129)

#### システム変数

- CBO が DECIMAL 型データから STRING 型データへの変換をどのように行うかを制御するセッション変数 `cbo_decimal_cast_string_strict` を追加しました。この変数が `true` に設定されている場合、v2.5.x およびそれ以降のバージョンで組み込まれたロジックが優先され、システムは厳密な変換を実装します（つまり、生成された文字列を切り捨て、スケール長に基づいて 0 を埋めます）。この変数が `false` に設定されている場合、v2.5.x より前のバージョンで組み込まれたロジックが優先され、システムはすべての有効な数字を処理して文字列を生成します。デフォルト値は `true` です。[#34208](https://github.com/StarRocks/starrocks/pull/34208)
- DECIMAL 型データと STRING 型データの比較に使用されるデータ型を指定するセッション変数 `cbo_eq_base_type` を追加しました。デフォルト値は `VARCHAR` で、`DECIMAL` も有効な値です。[#34208](https://github.com/StarRocks/starrocks/pull/34208)
- セッション変数 [`enable_profile`](https://docs.starrocks.io/docs/reference/System_variable#enable_profile) が `false` に設定されており、クエリにかかる時間が `big_query_profile_second_threshold` 変数で指定されたしきい値を超える場合、そのクエリのプロファイルが生成されるようにするセッション変数 `big_query_profile_second_threshold` を追加しました。[#33825](https://github.com/StarRocks/starrocks/pull/33825)

## 3.1.4

リリース日: 2023年11月2日

### 新機能

- 共有データ StarRocks クラスタで作成された主キーテーブルに対するソートキーをサポートします。
- 非同期マテリアライズドビューのパーティション式を指定するために str2date 関数を使用することをサポートします。これにより、外部カタログに存在し、STRING 型データをパーティション式として使用するテーブルに作成された非同期マテリアライズドビューのインクリメンタル更新とクエリの書き換えが容易になります。[#29923](https://github.com/StarRocks/starrocks/pull/29923) [#31964](https://github.com/StarRocks/starrocks/pull/31964)
- 同じタブレットに対する複数のクエリを固定レプリカに向けるかどうかを制御するセッション変数 `enable_query_tablet_affinity` を追加しました。このセッション変数はデフォルトで `false` に設定されています。[#33049](https://github.com/StarRocks/starrocks/pull/33049)
- 指定されたロールが現在のセッションでアクティブ化されているかどうかを確認するためのユーティリティ関数 `is_role_in_session` を追加しました。ユーザーに付与されたネストされたロールの確認をサポートします。[#32984](https://github.com/StarRocks/starrocks/pull/32984)
- グローバル変数 `enable_group_level_query_queue`（デフォルト値: `false`）によって制御されるリソースグループレベルのクエリキューを設定することをサポートします。グローバルレベルまたはリソースグループレベルのリソース消費が事前定義されたしきい値に達した場合、新しいクエリはキューに配置され、グローバルレベルのリソース消費とリソースグループレベルのリソース消費の両方がしきい値を下回ったときに実行されます。
  - 各リソースグループに対して `concurrency_limit` を設定し、BE ごとに許可される最大同時クエリ数を制限できます。
  - 各リソースグループに対して `max_cpu_cores` を設定し、BE ごとに許可される最大 CPU 消費量を制限できます。
- リソースグループのクラシファイアに対して 2 つのパラメータ、`plan_cpu_cost_range` と `plan_mem_cost_range` を追加しました。
  - `plan_cpu_cost_range`: システムによって推定される CPU 消費範囲。デフォルト値 `NULL` は制限がないことを示します。
  - `plan_mem_cost_range`: システムによって推定されるメモリ消費範囲。デフォルト値 `NULL` は制限がないことを示します。

### 改善点

- ウィンドウ関数 COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、および STDDEV_SAMP が ORDER BY 句とウィンドウ句をサポートするようになりました。[#30786](https://github.com/StarRocks/starrocks/pull/30786)
- DECIMAL 型データのクエリ中に小数点オーバーフローが発生した場合、NULL ではなくエラーが返されるようになりました。[#30419](https://github.com/StarRocks/starrocks/pull/30419)
- クエリキューで許可される同時クエリ数は現在 Leader FE によって管理されています。各 Follower FE はクエリの開始と終了時に Leader FE に通知します。同時クエリ数がグローバルレベルまたはリソースグループレベルの `concurrency_limit` に達した場合、新しいクエリは拒否されるか、キューに配置されます。

### バグ修正

以下の問題を修正しました:

- Spark または Flink が不正確なメモリ使用量統計のためにデータ読み取りエラーを報告する可能性がある問題を修正しました。[#30702](https://github.com/StarRocks/starrocks/pull/30702)  [#30751](https://github.com/StarRocks/starrocks/pull/30751)
- メタデータキャッシュのメモリ使用量統計が不正確である問題を修正しました。[#31978](https://github.com/StarRocks/starrocks/pull/31978)
- libcurl が呼び出されると BEs がクラッシュする問題を修正しました。[#31667](https://github.com/StarRocks/starrocks/pull/31667)
- Hive ビューに作成された StarRocks マテリアライズドビューがリフレッシュされると、「java.lang.ClassCastException: com.starrocks.catalog.HiveView cannot be cast to com.starrocks.catalog.HiveMetaStoreTable」というエラーが返される問題を修正しました。[#31004](https://github.com/StarRocks/starrocks/pull/31004)
- ORDER BY 句に集計関数が含まれている場合、「java.lang.IllegalStateException: null」というエラーが返される問題を修正しました。[#30108](https://github.com/StarRocks/starrocks/pull/30108)
- 共有データ StarRocks クラスタでは、`information_schema.COLUMNS` にテーブルキーの情報が記録されていないため、Flink Connector を使用してデータをロードする際に DELETE 操作を実行できない問題を修正しました。[#31458](https://github.com/StarRocks/starrocks/pull/31458)
- Flink Connector を使用してデータをロードする際に、高度に同時実行されるロードジョブがあり、HTTP スレッド数とスキャンスレッド数が上限に達している場合、ロードジョブが予期せず中断される問題を修正しました。[#32251](https://github.com/StarRocks/starrocks/pull/32251)
- フィールドが数バイトしか追加されていない場合、データ変更が完了する前に SELECT COUNT(*) を実行すると「error: invalid field name」というエラーが返される問題を修正しました。[#33243](https://github.com/StarRocks/starrocks/pull/33243)
- クエリキャッシュが有効になっている場合、クエリ結果が不正確になる問題を修正しました。[#32781](https://github.com/StarRocks/starrocks/pull/32781)
- ハッシュジョイン中にクエリが失敗し、BEs がクラッシュする問題を修正しました。[#32219](https://github.com/StarRocks/starrocks/pull/32219)
- BINARY または VARBINARY データ型の `DATA_TYPE` および `COLUMN_TYPE` が `information_schema.columns` ビューで `unknown` と表示される問題を修正しました。[#32678](https://github.com/StarRocks/starrocks/pull/32678)

### 動作の変更

- v3.1.4 以降、新しい StarRocks クラスタで作成された主キーテーブルに対して永続性インデックスがデフォルトで有効になります（これは、以前のバージョンから v3.1.4 にアップグレードされた既存の StarRocks クラスタには適用されません）。[#33374](https://github.com/StarRocks/starrocks/pull/33374)
- データを主キーテーブルにロードする Publish フェーズの後に Apply タスクが終了した後にのみ実行結果を返す新しい FE パラメータ `enable_sync_publish` が追加されました。このパラメータが `true` に設定されている場合、ロードジョブが成功メッセージを返した後にすぐにロードされたデータをクエリできます。ただし、このパラメータを `true` に設定すると、主キーテーブルへのデータロードに時間がかかる可能性があります。（このパラメータが追加される前は、Apply タスクは Publish フェーズと非同期でした。）[#27055](https://github.com/StarRocks/starrocks/pull/27055)

## 3.1.3

リリース日: 2023年9月25日

### 新機能

- 共有データ StarRocks クラスタで作成された主キーテーブルが、共有なし StarRocks クラスタと同様にローカルディスクへのインデックス永続化をサポートします。
- 集計関数 [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) が DISTINCT キーワードと ORDER BY 句をサポートします。[#28778](https://github.com/StarRocks/starrocks/pull/28778)
- [Stream Load](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD/)、[Broker Load](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/)、[Kafka Connector](https://docs.starrocks.io/docs/loading/Kafka-connector-starrocks/)、[Flink Connector](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/)、および [Spark Connector](https://docs.starrocks.io/docs/loading/Spark-connector-starrocks/) が、主キーテーブルに対する列モードでの部分更新をサポートします。[#28288](https://github.com/StarRocks/starrocks/pull/28288)
- パーティション内のデータを時間の経過とともに自動的にクールダウンすることができます。（この機能は [リストパーティション化](https://docs.starrocks.io/docs/table_design/list_partitioning/) には対応していません。）[#29335](https://github.com/StarRocks/starrocks/pull/29335) [#29393](https://github.com/StarRocks/starrocks/pull/29393)

### 改善点

無効なコメントを含む SQL コマンドの実行が MySQL と一致する結果を返すようになりました。[#30210](https://github.com/StarRocks/starrocks/pull/30210)

### バグ修正

以下の問題を修正しました:

- [DELETE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/DELETE/) ステートメントの WHERE 句に [BITMAP](https://docs.starrocks.io/docs/sql-reference/data-types/other-data-types/BITMAP/) または [HLL](https://docs.starrocks.io/docs/sql-reference/data-types/other-data-types/HLL/) データ型が指定されている場合、ステートメントが適切に実行されない問題を修正しました。[#28592](https://github.com/StarRocks/starrocks/pull/28592)
- Follower FE が再起動された後、CpuCores 統計が最新でないため、クエリパフォーマンスが低下する問題を修正しました。[#28472](https://github.com/StarRocks/starrocks/pull/28472) [#30434](https://github.com/StarRocks/starrocks/pull/30434)
- [to_bitmap()](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/to_bitmap/) 関数の実行コストが誤って計算される問題を修正しました。その結果、マテリアライズドビューが書き換えられた後、関数に対して不適切な実行プランが選択されます。[#29961](https://github.com/StarRocks/starrocks/pull/29961)
- 共有データアーキテクチャの特定の使用例では、Follower FE が再起動された後、Follower FE に送信されたクエリが「Backend node not found. Check if any backend node is down」というエラーを返す問題を修正しました。[#28615](https://github.com/StarRocks/starrocks/pull/28615)
- [ALTER TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_TABLE/) ステートメントを使用してテーブルが変更されている間にデータが継続的にロードされる場合、「Tablet is in error state」というエラーがスローされる可能性がある問題を修正しました。[#29364](https://github.com/StarRocks/starrocks/pull/29364)
- `ADMIN SET FRONTEND CONFIG` コマンドを使用して FE 動的パラメータ `max_broker_load_job_concurrency` を変更しても効果がない問題を修正しました。[#29964](https://github.com/StarRocks/starrocks/pull/29964) [#29720](https://github.com/StarRocks/starrocks/pull/29720)
- [date_diff()](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/date_diff/) 関数の時間単位が定数であるが、日付が定数でない場合、BEs がクラッシュする問題を修正しました。[#29937](https://github.com/StarRocks/starrocks/issues/29937)
- 共有データアーキテクチャでは、非同期ロードが有効になった後、自動パーティション化が効果を発揮しない問題を修正しました。[#29986](https://github.com/StarRocks/starrocks/issues/29986)
- [CREATE TABLE LIKE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE_LIKE/) ステートメントを使用して主キーテーブルを作成すると、「Unexpected exception: Unknown properties: `{persistent_index_type=LOCAL}`」というエラーがスローされる問題を修正しました。[#30255](https://github.com/StarRocks/starrocks/pull/30255)
- 主キーテーブルを復元すると、BEs が再起動された後にメタデータの不整合が発生する問題を修正しました。[#30135](https://github.com/StarRocks/starrocks/pull/30135)
- 主キーテーブルにデータをロードし、同時にトランケート操作とクエリが実行される場合、「java.lang.NullPointerException」というエラーが特定のケースでスローされる問題を修正しました。[#30573](https://github.com/StarRocks/starrocks/pull/30573)
- マテリアライズドビュー作成ステートメントに述語式が指定されている場合、それらのマテリアライズドビューのリフレッシュ結果が不正確になる問題を修正しました。[#29904](https://github.com/StarRocks/starrocks/pull/29904)
- ユーザーが StarRocks クラスタを v3.1.2 にアップグレードした後、アップグレード前に作成されたテーブルのストレージボリュームプロパティが `null` にリセットされる問題を修正しました。[#30647](https://github.com/StarRocks/starrocks/pull/30647)
- タブレットメタデータのチェックポイントと復元が同時に実行される場合、一部のタブレットレプリカが失われ、取得できなくなる問題を修正しました。[#30603](https://github.com/StarRocks/starrocks/pull/30603)
- CloudCanal を使用して `NOT NULL` に設定されているがデフォルト値が指定されていないテーブル列にデータをロードすると、「Unsupported dataFormat value is : \N」というエラーがスローされる問題を修正しました。[#30799](https://github.com/StarRocks/starrocks/pull/30799)

### 動作の変更

- [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) 関数を使用する場合、ユーザーはセパレータを宣言するために SEPARATOR キーワードを使用する必要があります。
- [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) 関数によって返される文字列のデフォルトの最大長を制御するセッション変数 [`group_concat_max_len`](https://docs.starrocks.io/docs/reference/System_variable#group_concat_max_len) のデフォルト値が無制限から `1024` に変更されました。

## 3.1.2

リリース日: 2023年8月25日

### バグ修正

以下の問題を修正しました:

- ユーザーがデフォルトで接続されるデータベースを指定し、ユーザーがそのデータベース内のテーブルに対する権限を持っているが、データベースに対する権限を持っていない場合、ユーザーがデータベースに対する権限を持っていないというエラーがスローされる問題を修正しました。[#29767](https://github.com/StarRocks/starrocks/pull/29767)
- クラウドネイティブテーブルに対する RESTful API アクション `show_data` によって返される値が不正確である問題を修正しました。[#29473](https://github.com/StarRocks/starrocks/pull/29473)
- [array_agg()](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_agg/) 関数が実行されている間にクエリがキャンセルされると BEs がクラッシュする問題を修正しました。[#29400](https://github.com/StarRocks/starrocks/issues/29400)
- [SHOW FULL COLUMNS](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_FULL_COLUMNS/) ステートメントによって返される [BITMAP](https://docs.starrocks.io/docs/sql-reference/data-types/other-data-types/BITMAP/) または [HLL](https://docs.starrocks.io/docs/sql-reference/data-types/other-data-types/HLL/) データ型の列の `Default` フィールド値が不正確である問題を修正しました。[#29510](https://github.com/StarRocks/starrocks/pull/29510)
- クエリに [array_map()](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_map/) 関数が含まれ、複数のテーブルが関与する場合、プッシュダウン戦略の問題によりクエリが失敗する問題を修正しました。[#29504](https://github.com/StarRocks/starrocks/pull/29504)
- ORC 形式のファイルに対するクエリが、Apache ORC のバグ修正 ORC-1304 ([apache/orc#1299](https://github.com/apache/orc/pull/1299)) がマージされていないために失敗する問題を修正しました。[#29804](https://github.com/StarRocks/starrocks/pull/29804)

### 動作の変更

新しくデプロイされた StarRocks v3.1 クラスタでは、[SET CATALOG](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/SET_CATALOG/) を実行して宛先外部カタログに切り替える場合、そのカタログに対する USAGE 権限を持っている必要があります。[GRANT](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/GRANT/) を使用して必要な権限を付与できます。

以前のバージョンからアップグレードされた v3.1 クラスタでは、継承された権限で SET CATALOG を実行できます。

## 3.1.1

リリース日: 2023年8月18日

### 新機能

- [共有データクラスタ](https://docs.starrocks.io/docs/deployment/shared_data/s3/) に対する Azure Blob Storage のサポートを追加しました。
- [共有データクラスタ](https://docs.starrocks.io/docs/deployment/shared_data/s3/) に対するリストパーティション化のサポートを追加しました。
- 集計関数 [COVAR_SAMP](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/covar_samp/)、[COVAR_POP](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/covar_pop/)、および [CORR](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/corr/) のサポートを追加しました。
- 次の [ウィンドウ関数](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/) のサポートを追加しました: COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、および STDDEV_SAMP。

### 改善点

すべての複合述語および WHERE 句内のすべての式に対する暗黙の変換をサポートします。セッション変数 [enable_strict_type](https://docs.starrocks.io/docs/reference/System_variable/) を使用して暗黙の変換を有効または無効にできます。このセッション変数のデフォルト値は `false` です。

### バグ修正

以下の問題を修正しました:

- 複数のレプリカを持つテーブルにデータがロードされると、一部のパーティションが空の場合に大量の無効なログレコードが書き込まれる問題を修正しました。[#28824](https://github.com/StarRocks/starrocks/issues/28824)
- 平均行サイズの不正確な推定により、主キーテーブルに対する列モードでの部分更新が過度に大きなメモリを占有する問題を修正しました。[#27485](https://github.com/StarRocks/starrocks/pull/27485)
- エラー状態のタブレットに対してクローン操作がトリガーされると、ディスク使用量が増加する問題を修正しました。[#28488](https://github.com/StarRocks/starrocks/pull/28488)
- Compaction がコールドデータをローカルキャッシュに書き込む問題を修正しました。[#28831](https://github.com/StarRocks/starrocks/pull/28831)

## 3.1.0

リリース日: 2023年8月7日

### 新機能

#### 共有データクラスタ

- 永続性インデックスを有効にできない主キーテーブルのサポートを追加しました。
- 各データ行に対してグローバルに一意の ID を有効にし、データ管理を簡素化する [AUTO_INCREMENT](https://docs.starrocks.io/docs/sql-reference/sql-statements/auto_increment/) 列属性をサポートします。
- [ロード中にパーティションを自動的に作成し、パーティション式を使用してパーティションルールを定義することをサポートします](https://docs.starrocks.io/docs/table_design/expression_partitioning/)。これにより、パーティション作成がより簡単に使用でき、柔軟になります。
- 共有データ StarRocks クラスタで、ユーザーがストレージの場所と認証情報を構成できる [ストレージボリュームの抽象化](https://docs.starrocks.io/docs/deployment/shared_data/s3#use-your-shared-data-starrocks-cluster) をサポートします。ユーザーはデータベースまたはテーブルを作成する際に既存のストレージボリュームを直接参照でき、認証構成が容易になります。

#### データレイク分析

- [Hive カタログ](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/) 内のテーブルに作成されたビューへのアクセスをサポートします。
- Parquet 形式の Iceberg v2 テーブルへのアクセスをサポートします。
- [Parquet 形式の Iceberg テーブルへのデータのシンク](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog#sink-data-to-an-iceberg-table) をサポートします。
- [プレビュー] [Elasticsearch カタログ](https://docs.starrocks.io/docs/data_source/catalog/elasticsearch_catalog/) を使用して Elasticsearch に保存されたデータへのアクセスをサポートします。これにより、Elasticsearch 外部テーブルの作成が簡素化されます。
- [プレビュー] [Paimon カタログ](https://docs.starrocks.io/docs/data_source/catalog/paimon_catalog/) を使用して Apache Paimon に保存されたストリーミングデータに対する分析をサポートします。

#### ストレージエンジン、データ取り込み、およびクエリ

- 自動パーティション化を [式に基づくパーティション化](https://docs.starrocks.io/docs/table_design/expression_partitioning/) にアップグレードしました。ユーザーは、簡単なパーティション式（時間関数式または列式のいずれか）を使用してテーブル作成時にパーティション化の手法を指定するだけで、StarRocks はデータの特性とパーティション式で定義されたルールに基づいてデータロード中に自動的にパーティションを作成します。このパーティション作成の方法は、ほとんどのシナリオに適しており、より柔軟でユーザーフレンドリーです。
- [リストパーティション化](https://docs.starrocks.io/docs/table_design/list_partitioning/) をサポートします。データは特定の列に対して事前定義された値のリストに基づいてパーティション化され、クエリを高速化し、明確に分類されたデータをより効率的に管理できます。
- `Information_schema` データベースに新しいテーブル `loads` を追加しました。ユーザーは `loads` テーブルから [Broker Load](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/) および [Insert](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/INSERT/) ジョブの結果をクエリできます。
- [Stream Load](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD/)、[Broker Load](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/)、および [Spark Load](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SPARK_LOAD/) ジョブによってフィルタリングされた不適格なデータ行をログに記録することをサポートします。ユーザーはロードジョブで `log_rejected_record_num` パラメータを使用してログに記録できるデータ行の最大数を指定できます。
- [ランダムバケット法](https://docs.starrocks.io/docs/table_design/Data_distribution#how-to-choose-the-bucketing-columns) をサポートします。この機能を使用すると、ユーザーはテーブル作成時にバケット列を構成する必要がなくなり、StarRocks はロードされたデータをランダムにバケットに分配します。この機能を v2.5.7 以降で提供されているバケット数（`BUCKETS`）を自動的に設定する機能と組み合わせて使用すると、ユーザーはバケット構成を考慮する必要がなくなり、テーブル作成ステートメントが大幅に簡素化されます。ただし、大規模データおよび高性能を要求するシナリオでは、ユーザーが引き続きハッシュバケット法を使用することをお勧めします。これにより、バケットプルーニングを使用してクエリを高速化できます。
- [INSERT INTO](https://docs.starrocks.io/docs/loading/InsertInto/) でテーブル関数 FILES() を使用して、AWS S3 に保存された Parquet または ORC 形式のデータファイルのデータを直接ロードすることをサポートします。FILES() 関数はテーブルスキーマを自動的に推論できるため、データロード前に外部カタログやファイル外部テーブルを作成する必要がなくなり、データロードプロセスが大幅に簡素化されます。
- [生成列](https://docs.starrocks.io/docs/sql-reference/sql-statements/generated_columns/) をサポートします。生成列機能を使用すると、StarRocks は列式の値を自動的に生成して保存し、クエリを自動的に書き換えてクエリパフォーマンスを向上させます。
- [Spark connector](https://docs.starrocks.io/docs/loading/Spark-connector-starrocks/) を使用して Spark から StarRocks にデータをロードすることをサポートします。[Spark Load](https://docs.starrocks.io/docs/loading/SparkLoad/) と比較して、Spark connector はより包括的な機能を提供します。ユーザーは Spark ジョブを定義してデータに対して ETL 操作を実行でき、Spark connector は Spark ジョブのシンクとして機能します。
- [MAP](https://docs.starrocks.io/docs/sql-reference/data-types/semi_structured/Map/) および [STRUCT](https://docs.starrocks.io/docs/sql-reference/data-types/semi_structured/STRUCT/) データ型の列にデータをロードすることをサポートし、ARRAY、MAP、および STRUCT に Fast Decimal 値をネストすることをサポートします。

#### SQL リファレンス

- 次のストレージボリューム関連のステートメントを追加しました: [CREATE STORAGE VOLUME](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/CREATE_STORAGE_VOLUME/)、[ALTER STORAGE VOLUME](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/ALTER_STORAGE_VOLUME/)、[DROP STORAGE VOLUME](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/DROP_STORAGE_VOLUME/)、[SET DEFAULT STORAGE VOLUME](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/SET_DEFAULT_STORAGE_VOLUME/)、[DESC STORAGE VOLUME](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/DESC_STORAGE_VOLUME/)、[SHOW STORAGE VOLUMES](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/SHOW_STORAGE_VOLUMES/)。

- [ALTER TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_TABLE/) を使用してテーブルコメントを変更することをサポートします。[#21035](https://github.com/StarRocks/starrocks/pull/21035)

- 次の関数を追加しました:

  - 構造体関数: [struct (row)](https://docs.starrocks.io/docs/sql-reference/sql-functions/struct-functions/row/)、[named_struct](https://docs.starrocks.io/docs/sql-reference/sql-functions/struct-functions/named_struct/)
  - マップ関数: [str_to_map](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/str_to_map/)、[map_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_concat/)、[map_from_arrays](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_from_arrays/)、[element_at](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/element_at/)、[distinct_map_keys](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/distinct_map_keys/)、[cardinality](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/cardinality/)
  - 高階マップ関数: [map_filter](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_filter/)、[map_apply](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_apply/)、[transform_keys](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/transform_keys/)、[transform_values](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/transform_values/)
  - 配列関数: [array_agg](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_agg/) が `ORDER BY` をサポート、[array_generate](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_generate/)、[element_at](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/element_at/)、[cardinality](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/cardinality/)
  - 高階配列関数: [all_match](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/all_match/)、[any_match](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/any_match/)
  - 集計関数: [min_by](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/min_by/)、[percentile_disc](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/percentile_disc/)
  - テーブル関数: [FILES](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/)、[generate_series](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/generate_series/)
  - 日付関数: [next_day](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/next_day/)、[previous_day](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/previous_day/)、[last_day](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/last_day/)、[makedate](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/makedate/)、[date_diff](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/date_diff/)
  - ビットマップ関数: [bitmap_subset_limit](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_subset_limit/)、[bitmap_subset_in_range](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_subset_in_range/)
  - 数学関数: [cosine_similarity](https://docs.starrocks.io/docs/sql-reference/sql-functions/math-functions/cos_similarity/)、[cosine_similarity_norm](https://docs.starrocks.io/docs/sql-reference/sql-functions/math-functions/cos_similarity_norm/)

#### 権限とセキュリティ

ストレージボリュームに関連する[権限項目](https://docs.starrocks.io/docs/administration/privilege_item#storage-volume)および外部カタログに関連する[権限項目](https://docs.starrocks.io/docs/administration/privilege_item#catalog)を追加し、[GRANT](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/GRANT/)および[REVOKE](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/REVOKE/)を使用してこれらの権限を付与および取り消すことをサポートします。

### 改善点

#### 共有データクラスタ

共有データ StarRocks クラスタのデータキャッシュを最適化しました。最適化されたデータキャッシュは、ホットデータの範囲を指定でき、コールドデータに対するクエリがローカルディスクキャッシュを占有するのを防ぎ、ホットデータに対するクエリのパフォーマンスを確保します。

#### マテリアライズドビュー

- 非同期マテリアライズドビューの作成を最適化しました:
  - ランダムバケット法をサポートします。ユーザーがバケット列を指定しない場合、StarRocks はデフォルトでランダムバケット法を採用します。
  - `ORDER BY` を使用してソートキーを指定することをサポートします。
  - `colocate_group`、`storage_medium`、および `storage_cooldown_time` などの属性を指定することをサポートします。
  - セッション変数を使用することをサポートします。ユーザーは `properties("session.<variable_name>" = "<value>")` 構文を使用してこれらの変数を構成し、ビューのリフレッシュ戦略を柔軟に調整できます。
  - すべての非同期マテリアライズドビューに対してスピル機能を有効にし、デフォルトで 1 時間のクエリタイムアウト期間を実装します。
  - ビューに基づいてマテリアライズドビューを作成することをサポートします。これにより、データモデリングシナリオでマテリアライズドビューがより使いやすくなり、ユーザーは異なるニーズに基づいてビューとマテリアライズドビューを柔軟に使用して階層化モデリングを実装できます。
- 非同期マテリアライズドビューを使用したクエリの書き換えを最適化しました:
  - Stale Rewrite をサポートします。指定された時間間隔内にリフレッシュされていないマテリアライズドビューを、マテリアライズドビューのベーステーブルが更新されているかどうかに関係なくクエリの書き換えに使用できるようにします。ユーザーはマテリアライズドビュー作成時に `mv_rewrite_staleness_second` プロパティを使用して時間間隔を指定できます。
  - Hive カタログテーブルに作成されたマテリアライズドビューに対する View Delta Join クエリの書き換えをサポートします（主キーと外部キーを定義する必要があります）。
  - クエリの書き換えメカニズムを最適化し、結合や COUNT DISTINCT、time_slice などの関数を含むクエリの書き換えをサポートします。
- 非同期マテリアライズドビューのリフレッシュを最適化しました:
  - Hive カタログテーブルに作成されたマテリアライズドビューのリフレッシュメカニズムを最適化しました。StarRocks は現在、パーティションレベルのデータ変更を認識し、各自動リフレッシュ中にデータ変更があるパーティションのみをリフレッシュします。
  - `REFRESH MATERIALIZED VIEW WITH SYNC MODE` 構文を使用してマテリアライズドビューのリフレッシュタスクを同期的に呼び出すことをサポートします。
- 非同期マテリアライズドビューの使用を強化しました:
  - `ALTER MATERIALIZED VIEW {ACTIVE | INACTIVE}` を使用してマテリアライズドビューを有効または無効にすることをサポートします。無効にされた（`INACTIVE` 状態の）マテリアライズドビューはリフレッシュやクエリの書き換えに使用できませんが、直接クエリすることができます。
  - `ALTER MATERIALIZED VIEW SWAP WITH` を使用して 2 つのマテリアライズドビューをスワップすることをサポートします。ユーザーは新しいマテリアライズドビューを作成し、既存のマテリアライズドビューと原子スワップを実行して既存のマテリアライズドビューのスキーマ変更を実装できます。
- 同期マテリアライズドビューを最適化しました:
  - SQL ヒント `[_SYNC_MV_]` を使用して同期マテリアライズドビューに対して直接クエリを実行することをサポートし、まれにクエリが適切に書き換えられない問題を回避できます。
  - `CASE-WHEN`、`CAST`、および数学的操作などのより多くの式をサポートし、マテリアライズドビューをより多くのビジネスシナリオに適用できるようにします。

#### データレイク分析

- Iceberg のメタデータキャッシュとアクセスを最適化し、Iceberg データクエリのパフォーマンスを向上させました。
- データキャッシュを最適化し、データレイク分析のパフォーマンスをさらに向上させました。

#### ストレージエンジン、データ取り込み、およびクエリ

- [スピル](https://docs.starrocks.io/docs/3.1/administration/management/resource_management/spill_to_disk/) 機能の一般提供を発表しました。この機能は、一部のブロッキングオペレータの中間計算結果をディスクにスピルすることをサポートします。スピル機能が有効になっている場合、クエリに集計、ソート、またはジョインオペレータが含まれている場合、StarRocks はオペレータの中間計算結果をディスクにキャッシュしてメモリ消費を削減し、メモリ制限によるクエリの失敗を最小限に抑えます。
- 基数保持ジョインでのプルーニングをサポートします。ユーザーが多数のテーブルを保持し、それらがスタースキーマ（たとえば SSB）またはスノーフレークスキーマ（たとえば TCP-H）で編成されているが、少数のテーブルのみをクエリする場合、この機能は不要なテーブルをプルーニングしてジョインのパフォーマンスを向上させます。
- 列モードでの部分更新をサポートします。ユーザーは [UPDATE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/UPDATE/) ステートメントを使用して主キーテーブルに部分更新を実行する際に列モードを有効にできます。列モードは少数の列を更新するが多数の行を更新するのに適しており、更新パフォーマンスを最大 10 倍向上させることができます。
- CBO の統計収集を最適化しました。これにより、統計収集がデータ取り込みに与える影響が軽減され、統計収集のパフォーマンスが向上します。
- マージアルゴリズムを最適化し、順列シナリオでの全体的なパフォーマンスを最大 2 倍向上させました。
- クエリロジックを最適化し、データベースロックへの依存を減らしました。
- 動的パーティション化がパーティション単位として年をサポートするようになりました。[#28386](https://github.com/StarRocks/starrocks/pull/28386)

#### SQL リファレンス

- 条件付き関数 case、coalesce、if、ifnull、および nullif が ARRAY、MAP、STRUCT、および JSON データ型をサポートします。
- 次の配列関数がネストされた型 MAP、STRUCT、および ARRAY をサポートします:
  - array_agg
  - array_contains、array_contains_all、array_contains_any
  - array_slice、array_concat
  - array_length、array_append、array_remove、array_position
  - reverse、array_distinct、array_intersect、arrays_overlap
  - array_sortby
- 次の配列関数が Fast Decimal データ型をサポートします:
  - array_agg
  - array_append、array_remove、array_position、array_contains
  - array_length
  - array_max、array_min、array_sum、array_avg
  - arrays_overlap、array_difference
  - array_slice、array_distinct、array_sort、reverse、array_intersect、array_concat
  - array_sortby、array_contains_all、array_contains_any

### バグ修正

以下の問題を修正しました:

- Routine Load ジョブのために Kafka への再接続要求が適切に処理されない問題を修正しました。[#23477](https://github.com/StarRocks/starrocks/issues/23477)
- 複数のテーブルを含み、`WHERE` 句を含む SQL クエリに対して、これらの SQL クエリが同じセマンティクスを持っているが、各 SQL クエリ内のテーブルの順序が異なる場合、これらの SQL クエリの一部が関連するマテリアライズドビューを利用するために書き換えられない問題を修正しました。[#22875](https://github.com/StarRocks/starrocks/issues/22875)
- `GROUP BY` 句を含むクエリに対して重複したレコードが返される問題を修正しました。[#19640](https://github.com/StarRocks/starrocks/issues/19640)
- lead() または lag() 関数を呼び出すと BE がクラッシュする可能性がある問題を修正しました。[#22945](https://github.com/StarRocks/starrocks/issues/22945)
- 外部カタログテーブルに基づいて作成されたマテリアライズドビューに基づく部分パーティションクエリの書き換えが失敗する問題を修正しました。[#19011](https://github.com/StarRocks/starrocks/issues/19011)
- バックスラッシュ（`\`）とセミコロン（`;`）の両方を含む SQL ステートメントが適切に解析されない問題を修正しました。[#16552](https://github.com/StarRocks/starrocks/issues/16552)
- テーブルに作成されたマテリアライズドビューが削除された場合、そのテーブルをトランケートできない問題を修正しました。[#19802](https://github.com/StarRocks/starrocks/issues/19802)

### 動作の変更

- 共有データ StarRocks クラスタで使用されるテーブル作成構文から `storage_cache_ttl` パラメータが削除されました。現在、ローカルキャッシュ内のデータは LRU アルゴリズムに基づいて削除されます。
- BE 構成項目 `disable_storage_page_cache` および `alter_tablet_worker_count`、および FE 構成項目 `lake_compaction_max_tasks` が不変パラメータから可変パラメータに変更されました。
- BE 構成項目 `block_cache_checksum_enable` のデフォルト値が `true` から `false` に変更されました。
- BE 構成項目 `enable_new_load_on_memory_limit_exceeded` のデフォルト値が `false` から `true` に変更されました。
- FE 構成項目 `max_running_txn_num_per_db` のデフォルト値が `100` から `1000` に変更されました。
- FE 構成項目 `http_max_header_size` のデフォルト値が `8192` から `32768` に変更されました。
- FE 構成項目 `tablet_create_timeout_second` のデフォルト値が `1` から `10` に変更されました。
- FE 構成項目 `max_routine_load_task_num_per_be` のデフォルト値が `5` から `16` に変更され、多数の Routine Load タスクが作成されるとエラー情報が返されます。
- FE 構成項目 `quorom_publish_wait_time_ms` が `quorum_publish_wait_time_ms` に名前が変更され、FE 構成項目 `async_load_task_pool_size` が `max_broker_load_job_concurrency` に名前が変更されました。
- BE 構成項目 `routine_load_thread_pool_size` は廃止されました。現在、BE ノードごとの Routine Load スレッドプールサイズは FE 構成項目 `max_routine_load_task_num_per_be` のみで制御されます。
- BE 構成項目 `txn_commit_rpc_timeout_ms` およびシステム変数 `tx_visible_wait_timeout` は廃止されました。
- FE 構成項目 `max_broker_concurrency` および `load_parallel_instance_num` は廃止されました。
- FE 構成項目 `max_routine_load_job_num` は廃止されました。現在、StarRocks は `max_routine_load_task_num_per_be` パラメータに基づいて各個別の BE ノードがサポートする Routine Load タスクの最大数を動的に推測し、タスクの失敗に関する提案を提供します。
- CN 構成項目 `thrift_port` が `be_port` に名前が変更されました。
- Routine Load ジョブ内の個々のロードタスクの最大消費時間とタイムアウト期間を制御するために、2 つの新しい Routine Load ジョブプロパティ `task_consume_second` および `task_timeout_second` が追加され、ジョブの調整がより柔軟になります。ユーザーが Routine Load ジョブでこれらの 2 つのプロパティを指定しない場合、FE 構成項目 `routine_load_task_consume_second` および `routine_load_task_timeout_second` が優先されます。
- セッション変数 `enable_resource_group` は廃止されました。v3.1.0 以降、[リソースグループ](https://docs.starrocks.io/docs/administration/resource_group/) 機能はデフォルトで有効になっています。
- 2 つの新しい予約キーワード、COMPACTION および TEXT が追加されました。