---
displayed_sidebar: docs
---

# StarRocks version 3.4

## 3.4.7

リリース日：2025年9月1日

### バグ修正

以下の問題を修正しました：

- Routine Load ジョブが `max_filter_ratio` をシリアライズしていませんでした。 [#61755](https://github.com/StarRocks/starrocks/pull/61755)
- Stream Load の `now(precision)` 関数で精度パラメータが失われる問題。 [#61721](https://github.com/StarRocks/starrocks/pull/61721)
- Audit Log において、`INSERT INTO SELECT` 文の Scan Rows 結果が正確ではありませんでした。 [#61381](https://github.com/StarRocks/starrocks/pull/61381)
- クラスターを v3.4.5 にアップグレードした後、`fslib read iops` 指標がアップグレード前より高くなる問題。 [#61724](https://github.com/StarRocks/starrocks/pull/61724)
- JDBC Catalog を使用して SQLServer にクエリすると、クエリが頻繁にハングする問題。 [#61719](https://github.com/StarRocks/starrocks/pull/61719)

## 3.4.6

リリース日：2025年8月7日

### 改善点

- `INSERT INTO FILES` でデータを Parquet ファイルにエクスポートする際に、[`parquet.version`](https://docs.starrocks.io/docs/ja/sql-reference/sql-functions/table-functions/files.md#parquetversion) を指定してエクスポートする Parquet ファイルのバージョンを選べるようになりました。他のツールでエクスポートされた Parquet ファイルを読み取る際の互換性が向上します。 [#60843](https://github.com/StarRocks/starrocks/pull/60843)

### バグ修正

以下の問題を修正しました：

- `TableMetricsManager` におけるロックの粒度が大きすぎて、インポートジョブが失敗する問題。 [#58911](https://github.com/StarRocks/starrocks/pull/58911)
- `FILES()` を使用して Parquet データをインポートする際、列名が大文字小文字を区別していた問題。 [#61059](https://github.com/StarRocks/starrocks/pull/61059)
- ストレージとコンピュートが分離されたクラスタを v3.3 から v3.4 以降にアップグレードした後、キャッシュが有効にならない問題。 [#60973](https://github.com/StarRocks/starrocks/pull/60973)
- パーティション ID が null の場合にゼロ除算エラーが発生し、BE がクラッシュする問題。 [#60842](https://github.com/StarRocks/starrocks/pull/60842)
- BE 拡張中に Broker Load ジョブがエラーになる問題。 [#60224](https://github.com/StarRocks/starrocks/pull/60224)

### 動作の変更

- `information_schema.keywords` ビュー内の `keyword` 列は `word` にリネームされ、MySQL の定義と互換性を持たせました。 [#60863](https://github.com/StarRocks/starrocks/pull/60863)

## 3.4.5

リリース日: 2025年7月10日

### 改善点

- ロードジョブの実行状況の可観測性を向上: ロードタスクの実行情報を `information_schema.loads` ビューに統一しました。このビューでは、すべての INSERT、Broker Load、Stream Load、Routine Load のサブタスクの実行情報を確認できます。また、より多くのカラムを追加し、ロードタスクの実行状況や親ジョブ（PIPES、Routine Load Job）との関連情報を明確に把握できるようにしました。
- `ALTER ROUTINE LOAD` ステートメントで `kafka_broker_list` を変更することをサポート。

### バグ修正

以下の問題を修正しました：

- 高頻度ロード時に Compaction が遅延する可能性がある問題。 [#59998](https://github.com/StarRocks/starrocks/pull/59998)
- Unified Catalog 経由で Iceberg 外部テーブルをクエリすると、`not support getting unified metadata table factory` エラーが発生する問題。 [#59412](https://github.com/StarRocks/starrocks/pull/59412)
- `DESC FILES()` でリモートストレージ上のCSVファイルを確認する際、システムが `xinf` をFLOAT型と誤認したため、結果が誤って返される問題。 [#59574](https://github.com/StarRocks/starrocks/pull/59574)
- 空のパーティションに対して `INSERT INTO` を行うと BE がクラッシュする問題。 [#59553](https://github.com/StarRocks/starrocks/pull/59553)
- Iceberg の Equality Delete ファイルを読み込む際、Icebergテーブルで既に削除されたデータが StarRocks で読み取れてしまう問題。 [#59709](https://github.com/StarRocks/starrocks/pull/59709)
- カラム名変更後にクエリが失敗する問題。 [#59178](https://github.com/StarRocks/starrocks/pull/59178)

### 動作の変更

- BE 構成パラメータ `skip_pk_preload` のデフォルト値が `false` から `true` に変更されました。これにより、プライマリキーのインデックスプリロードをスキップし、`Reached Timeout` エラーの発生を低減します。この変更により、プライマリキーインデックスの読み込みが必要なクエリの応答時間が増加する可能性があります。

## 3.4.4

リリース日：2025 年 6 月 10 日

### 改善点

- Storage Volume が Managed Identity 認証を用いた ADLS2 をサポートしました。[#58454](https://github.com/StarRocks/starrocks/pull/58454)
- [混合式に基づくパーティション](https://docs.starrocks.io/ja/docs/table_design/data_distribution/expression_partitioning/#混合式に基づくパーティション化v34以降) において、大部分の DATETIME 関連関数で効果的なパーティションプルーニングが可能になりました。
- `FILES` テーブル関数を使用して、Azure から Avro データファイルのインポートをサポートしました。 [#58131](https://github.com/StarRocks/starrocks/pull/58131)
- Routine Load で不正な JSON 形式のデータを読み込んだ際、現在処理中のパーティションおよび Offset 情報をエラーログに出力するように改善しました。問題の特定が容易になります。 [#55772](https://github.com/StarRocks/starrocks/pull/55772)

### バグ修正

以下の問題を修正しました：

- パーティションテーブルの同一パーティションへの同時クエリが Hive Metastore をハングさせる問題。 [#58089](https://github.com/StarRocks/starrocks/pull/58089)
- `INSERT` タスクが異常終了した場合、対応するジョブが `QUEUEING` 状態のままになる問題。 [#58603](https://github.com/StarRocks/starrocks/pull/58603)
- v3.4.0 から v3.4.2 にアップグレードした後、多数のタブレットレプリカが異常状態になる問題。 [#58518](https://github.com/StarRocks/starrocks/pull/58518)
- 不正な `UNION` 実行プランが FE のメモリ不足 (OOM) を引き起こす問題。 [#59040](https://github.com/StarRocks/starrocks/pull/59040)
- パーティション回収時に無効なデータベース ID があると FE の起動に失敗する問題。 [#59666](https://github.com/StarRocks/starrocks/pull/59666)
- CheckPoint 処理の失敗後に FE が正常に終了できず、処理がブロックされる問題。 [#58602](https://github.com/StarRocks/starrocks/pull/58602)

## 3.4.3

リリース日： 2025 年 4 月 30 日

### 改善点

- Routine Load および Stream Load は、`columns` パラメータで Lambda 式を使用して、複雑な列データの抽出をサポートします。ユーザーは、`array_filter` / `map_filter` を使用して ARRAY / MAP データをフィルタリングおよび抽出できます。`cast` 関数を組み合わせて JSON Array / JSON Object を ARRAY および MAP 型に変換することで、JSON データの複雑なフィルタリングと抽出が可能になります。例えば、`COLUMNS (js, col=array_filter(i -> json_query(i, '$.type')=='t1', cast(js as Array<JSON>))[1])` を使用すると、`js` という JSON Array から `type` が `t1` の最初の JSON Object を抽出できます。 [#58149](https://github.com/StarRocks/starrocks/pull/58149)
- `cast` 関数を使用して JSON Object を MAP 型に変換し、`map_filter` と組み合わせて、特定の条件を満たす JSON Object 内の項目を抽出することができます。例えば、`map_filter((k, v) -> json_query(v, '$.type') == 't1', cast(js AS MAP<String, JSON>))` を使用すると、`js` という JSON Object から `type` が `t1` の JSON Object を抽出できます。 [#58045](https://github.com/StarRocks/starrocks/pull/58045)
- `information_schema.task_runs` ビューのクエリ時に LIMIT がサポートされるようになりました。 [#57404](https://github.com/StarRocks/starrocks/pull/57404)

### バグ修正

以下の問題を修正しました：

- ORC フォーマットの Hive テーブルをクエリする際に、エラー `OrcChunkReader::lazy_seek_to failed. reason = bad read in RleDecoderV2: :readByte` が発生する問題。 [#57454](https://github.com/StarRocks/starrocks/pull/57454)
- Equality Delete ファイルを含む Iceberg テーブルをクエリする際に、上位の RuntimeFilter がプッシュダウンできない問題。 [#57651](https://github.com/StarRocks/starrocks/pull/57651)
- ディスクスピルの事前集計戦略を有効にすると、クエリがクラッシュする問題。 [#58022](https://github.com/StarRocks/starrocks/pull/58022)
- クエリがエラー `ConstantRef-cmp-ConstantRef not supported here, null != 111 should be eliminated earlier` で返される。 [#57735](https://github.com/StarRocks/starrocks/pull/57735)
- クエリキュー機能が有効でない状態で、`query_queue_pending_timeout_second` パラメータでタイムアウトが発生する問題。 [#57719](https://github.com/StarRocks/starrocks/pull/57719)

## 3.4.2

リリース日： 2025 年 4 月 10 日

### 改善点

- FEはシステムの可用性向上のため、優雅なシャットダウンをサポートします。`./stop_fe.sh -g`でFEをシャットダウンする際、FEはまず`/api/health`APIを通じてフロントエンドのロードバランサーに500エラーを返し、シャットダウン準備中であることを通知します。これにより、ロードバランサーは他の利用可能なFEノードに切り替えることができます。その間、実行中のクエリは終了するか、タイムアウト（デフォルト60秒）するまで実行され続けます。[#56823](https://github.com/StarRocks/starrocks/pull/56823)

### バグ修正

以下の問題を修正しました：

- パーティション列が生成列である場合、パーティションプルーニングが無効になる可能性がある問題を修正。[#54543](https://github.com/StarRocks/starrocks/pull/54543)
- `concat` 関数の引数処理に問題があり、クエリ実行中に BE がクラッシュする可能性がある問題を修正。[#57522](https://github.com/StarRocks/starrocks/pull/57522)
- Broker Load を使用してデータをインポートする際に、`ssl_enable` プロパティが有効にならない問題を修正。[#57229](https://github.com/StarRocks/starrocks/pull/57229)
- NULL データが存在する場合に、STRUCT 型列のサブフィールドをクエリすると BE がクラッシュする問題を修正。[#56496](https://github.com/StarRocks/starrocks/pull/56496)
- `ALTER TABLE {table} PARTITIONS (p1, p1) DISTRIBUTED BY ...` 文でパーティション名を重複指定すると、内部で生成された一時パーティションが削除できなくなる問題を修正。[#57005](https://github.com/StarRocks/starrocks/pull/57005)
- 共有データクラスタで `SHOW PROC '/current_queries'` を実行すると、"Error 1064 (HY000): Sending collect query statistics request fails" エラーが発生する問題を修正。[#56597](https://github.com/StarRocks/starrocks/pull/56597)
- `INSERT OVERWRITE` インポートタスクを並列実行した場合に、"ConcurrentModificationException: null" エラーが発生し、インポートが失敗する問題を修正。[#56557](https://github.com/StarRocks/starrocks/pull/56557)
- v2.5.21 から v3.1.17 にアップグレードした後、複数の Broker Load タスクを並列実行すると異常が発生する可能性がある問題を修正。[#56512](https://github.com/StarRocks/starrocks/pull/56512)

### 動作の変更

- BEの設定項目 `avro_ignore_union_type_tag` のデフォルト値が `true` に変更され、`["NULL", "STRING"]` を STRING 型データとして直接解析できるようになり、一般的なユーザーの利用要件により適合するようになりました。[#57553](https://github.com/StarRocks/starrocks/pull/57553)
- セッション変数 `big_query_profile_threshold` のデフォルト値が 0 から 30（秒）に変更されました。[#57177](https://github.com/StarRocks/starrocks/pull/57177)
- 新しい FE 設定項目 `enable_mv_refresh_collect_profile` が追加され、マテリアライズドビューのリフレッシュ中に Profile 情報を収集するかどうかを制御できるようになりました。デフォルト値は `false`（以前はシステムで Profile がデフォルトで収集されていました）。[#56971](https://github.com/StarRocks/starrocks/pull/56971)

## 3.4.1（廃止予定）

リリース日: 2025年3月12日

:::tip


このバージョンは、共有データクラスタにおけるメタデータ損失の問題によりオフラインになりました。

- **問題**：共有データクラスタ内のリーダー FE ノードのシフト中に、まだ Publish されていないコミット済みコンパクショントランザクションがある場合、シフト後にメタデータ損失が発生することがある。

- **影響範囲**：この問題は共有データクラスタにのみ影響します。共有なしクラスタは影響を受けません。

- **一時的な回避策**：Publish タスクがエラーで返された場合、`SHOW PROC 'compactions'` を実行して、空の `FinishTime` を持つ 2 つのコンパクショントランザクションを持つパーティションがあるかどうかを確認できます。`ALTER TABLE DROP PARTITION FORCE`を実行してパーティションを削除すると、Publish タスクがハングアップするのを防ぐことができます。

:::

### 新機能と改善点

- Data Lake 分析で Delta Lake の Deletion Vector をサポートしました。
- セキュアビューをサポートしました。セキュアビューを作成することで、基となるテーブルの SELECT 権限を持たないユーザーが、ビューをクエリできないようにできます（そのユーザーがビュー自体の SELECT 権限を持っていたとしても）。
- Sketch HLL ([`ds_hll_count_distinct`](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/ds_hll_count_distinct/)) をサポートしました。`approx_count_distinct`と比較して、より高精度な近似重複排除が可能になります。
- 共有データクラスタで、クラスターリカバリのための自動 Snapshot 作成をサポートしました。
- 共有データクラスタの Storage Volume が Azure Data Lake Storage Gen2 をサポートしました。
- MySQL プロトコルを使用した StarRocks への接続で SSL 認証をサポートしました。これにより、クライアントと StarRocks クラスター間のデータ通信が不正アクセスから保護されます。

### バグ修正

以下の問題を修正しました：

- OLAP ビューがマテリアライズドビューの処理ロジックに影響を与える問題を修正しました。[#52989](https://github.com/StarRocks/starrocks/pull/52989)
- 1 つのレプリカが見つからない場合、他のレプリカがいくつ成功してもトランザクションが失敗する問題を修正しました。（修正後は、大多数のレプリカが成功すればトランザクションが完了するようになりました。）[#55212](https://github.com/StarRocks/starrocks/pull/55212)
- Alive 状態が false のノードに Stream Load がスケジュールされるとインポートが失敗する問題を修正しました。[#55371](https://github.com/StarRocks/starrocks/pull/55371)
- クラスター Snapshot 内のファイルが誤って削除される問題を修正しました。[#56338](https://github.com/StarRocks/starrocks/pull/56338)

### 動作の変更

- グレースフルシャットダウンのデフォルト設定を「無効」から「有効」に変更しました。関連するBE/CNパラメータ `loop_count_wait_fragments_finish` のデフォルト値が `2` に変更され、システムは実行中のクエリが完了するまで最大20秒待機するようになりました。[#56002](https://github.com/StarRocks/starrocks/pull/56002)

## 3.4.0

リリース日: 2025年1月24日

### データレイク分析

- Iceberg V2のクエリパフォーマンスを最適化し、delete-filesの繰り返し読み込みを減らすことでメモリ使用量を削減しました。
- Delta Lakeテーブルのカラムマッピングをサポートし、Delta Schema Evolution後のデータに対するクエリを可能にしました。詳細は[Delta Lake catalog - Feature support](https://docs.starrocks.io/docs/data_source/catalog/deltalake_catalog/#feature-support)をご覧ください。
- Data Cacheに関連する改善:
  - Segmented LRU (SLRU) キャッシュ削除戦略を導入し、偶発的な大規模クエリによるキャッシュ汚染を大幅に防ぎ、キャッシュヒット率を向上させ、クエリパフォーマンスの変動を減少させます。大規模クエリのシミュレーションテストケースでは、SLRUベースのクエリパフォーマンスが70%以上向上することがあります。詳細は[Data Cache - Cache replacement policies](https://docs.starrocks.io/docs/data_source/data_cache/#cache-replacement-policies)をご覧ください。
  - 共有データアーキテクチャとデータレイククエリシナリオの両方で使用されるData Cacheインスタンスを統一し、設定を簡素化し、リソース利用を向上させました。詳細は[Data Cache](https://docs.starrocks.io/docs/using_starrocks/caching/block_cache/)をご覧ください。
  - Data Cacheの適応型I/O戦略最適化を提供し、キャッシュディスクの負荷とパフォーマンスに基づいて一部のクエリ要求をリモートストレージに柔軟にルーティングし、全体的なアクセススループットを向上させます。
  - データレイククエリシナリオでのData Cache内のデータの永続化をサポートします。以前にキャッシュされたデータは、BE再起動後に再利用でき、クエリパフォーマンスの変動を減少させます。
- クエリによってトリガーされる自動ANALYZEタスクを通じて、外部テーブル統計の自動収集をサポートします。これにより、メタデータファイルと比較してより正確なNDV情報を提供し、クエリプランを最適化し、クエリパフォーマンスを向上させます。詳細は[Query-triggered collection](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/#query-triggered-collection)をご覧ください。
- IcebergのTime Travelクエリ機能を提供し、TIMESTAMPまたはVERSIONを指定することで、指定されたBRANCHまたはTAGからデータを読み取ることができます。
- データレイククエリのクエリフラグメントの非同期配信をサポートします。これにより、FEがクエリを実行する前にすべてのファイルを取得する必要があるという制限を回避し、FEがクエリファイルを取得し、BEがクエリを並行して実行できるようになり、キャッシュにない多数のファイルを含むデータレイククエリの全体的な遅延を削減します。同時に、ファイルリストをキャッシュすることによるFEのメモリ負荷を軽減し、クエリの安定性を向上させます。（現在、HudiとDelta Lakeの最適化が実装されており、Icebergの最適化はまだ開発中です。）

### パフォーマンスの向上とクエリの最適化

- [実験的] 遅いクエリの自動最適化のための初期的なQuery Feedback機能を提供します。システムは遅いクエリの実行詳細を収集し、潜在的な最適化の機会を自動的に分析し、クエリに対するカスタマイズされた最適化ガイドを生成します。CBOが後続の同一クエリに対して同じ悪いプランを生成した場合、システムはこのクエリプランをガイドに基づいてローカルに最適化します。詳細は[Query Feedback](https://docs.starrocks.io/docs/using_starrocks/query_feedback/)をご覧ください。
- [実験的] Python UDFをサポートし、Java UDFと比較してより便利な関数カスタマイズを提供します。詳細は[Python UDF](https://docs.starrocks.io/docs/sql-reference/sql-functions/Python_UDF/)をご覧ください。
- 複数カラムのOR述語のプッシュダウンを可能にし、複数カラムのOR条件（例: `a = xxx OR b = yyy`）を持つクエリが特定のカラムインデックスを利用できるようにし、データの読み取り量を削減し、クエリパフォーマンスを向上させます。
- TPC-DS 1TB IcebergデータセットでTPC-DSクエリパフォーマンスを約20%最適化しました。最適化方法には、テーブルプルーニングと主キーおよび外部キーを使用した集約カラムプルーニング、集約プッシュダウンが含まれます。

### 共有データの強化

- Query Cacheをサポートし、共有なしアーキテクチャと整合性を持たせました。
- 同期マテリアライズドビューをサポートし、共有なしアーキテクチャと整合性を持たせました。

### ストレージエンジン

- すべてのパーティション化の手法を式に基づくパーティション化に統一し、各レベルが任意の式であるマルチレベルパーティション化をサポートしました。詳細は[Expression Partitioning](https://docs.starrocks.io/docs/table_design/data_distribution/expression_partitioning/)をご覧ください。
- [プレビュー] 集計テーブルでのすべてのネイティブ集計関数をサポートします。汎用集計関数状態ストレージフレームワークを導入することで、StarRocksがサポートするすべてのネイティブ集計関数を使用して集計テーブルを定義できます。
- ベクトルインデックスをサポートし、大規模で高次元のベクトルの高速な近似最近傍検索（ANNS）を可能にします。これは、ディープラーニングや機械学習のシナリオで一般的に必要とされます。現在、StarRocksは2種類のベクトルインデックスをサポートしています: IVFPQとHNSW。

### ロード

- INSERT OVERWRITEは新しいセマンティック - Dynamic Overwriteをサポートします。このセマンティックが有効な場合、取り込まれたデータは新しいパーティションを作成するか、または新しいデータレコードに対応する既存のパーティションを上書きします。関与しないパーティションは切り捨てられたり削除されたりしません。このセマンティックは、ユーザーが特定のパーティションのデータを復元したい場合に特に便利です。詳細は[Dynamic Overwrite](https://docs.starrocks.io/docs/loading/InsertInto/#dynamic-overwrite)をご覧ください。
- INSERT from FILESを使用したデータ取り込みを最適化し、Broker Loadを優先するロード方法として置き換えました:
  - FILESはリモートストレージ内のファイルのリスト化をサポートし、ファイルの基本統計を提供します。詳細は[FILES - list_files_only](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#list_files_only)をご覧ください。
  - INSERTはカラム名による一致をサポートし、特に同一の名前を持つ多数のカラムからデータをロードする際に便利です。（デフォルトの動作はカラムの位置による一致です。）詳細は[Match column by name](https://docs.starrocks.io/docs/loading/InsertInto/#match-column-by-name)をご覧ください。
  - INSERTは他のロード方法と整合性を持たせてPROPERTIESを指定することをサポートします。ユーザーはINSERT操作のために`strict_mode`、`max_filter_ratio`、および`timeout`を指定してデータ取り込みの動作と品質を制御できます。詳細は[INSERT - PROPERTIES](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT/#properties)をご覧ください。
  - INSERT from FILESは、より正確なソースデータスキーマを推測するために、FILESのスキャン段階にターゲットテーブルスキーマチェックをプッシュダウンすることをサポートします。詳細は[Push down target table schema check](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#push-down-target-table-schema-check)をご覧ください。
  - FILESは異なるスキーマを持つファイルの統合をサポートします。ParquetとORCファイルのスキーマはカラム名に基づいて統合され、CSVファイルのスキーマはカラムの位置（順序）に基づいて統合されます。カラムが一致しない場合、ユーザーはプロパティ`fill_mismatch_column_with`を指定してカラムをNULLで埋めるかエラーを返すかを選択できます。詳細は[Union files with different schema](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#union-files-with-different-schema)をご覧ください。
  - FILESはParquetファイルからSTRUCT型データを推測することをサポートします。（以前のバージョンでは、STRUCTデータはSTRING型として推測されていました。）詳細は[Infer STRUCT type from Parquet](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#infer-struct-type-from-parquet)をご覧ください。
- 複数の同時Stream Loadリクエストを単一のトランザクションにマージし、データをバッチでコミットすることをサポートし、リアルタイムデータ取り込みのスループットを向上させます。これは、高い同時実行性、小規模バッチ（KBから数十MB）のリアルタイムロードシナリオ向けに設計されています。頻繁なロード操作によって引き起こされる過剰なデータバージョン、Compaction中のリソース消費、および過剰な小ファイルによるIOPSとI/O遅延を削減できます。

### その他

- BEとCNの優雅な終了プロセスを最適化し、優雅な終了中のBEまたはCNノードのステータスを正確に`SHUTDOWN`として表示します。
- ログの印刷を最適化し、過剰なディスクスペースの占有を回避します。
- 共有なしクラスタは、ビュー、external catalogメタデータ、および式に基づくパーティション化とリストパーティション化戦略で作成されたパーティションなど、より多くのオブジェクトのバックアップと復元をサポートします。
- [プレビュー] CheckPointをFollower FEでサポートし、CheckPoint中のLeader FEの過剰なメモリを回避し、Leader FEの安定性を向上させます。

### 動作の変更

共有データアーキテクチャとデータレイククエリシナリオの両方で使用される Data Cache インスタンスが統一されたため、v3.4.0 へのアップグレード後に以下の動作が変更されます：

- BE の設定項目 `datacache_disk_path` は廃止された。データは `${storage_root_path}/datacache` ディレクトリ以下にキャッシュされる。Data Cache 専用のディスクを割り当てたい場合は、シンボリックリンクを使用して手動で上記のディレクトリを指定してください。
- 共有データクラスタのキャッシュデータは自動的に `${storage_root_path}/datacache` に移行され、アップグレード後も再利用できる。
- `datacache_disk_size` の動作が変わる：

  - `datacache_disk_size` が `0` (デフォルト) の場合、キャッシュ容量の自動調整が有効になる (アップグレード前の動作と同じ)。
  - `datacache_disk_size` が `0` より大きい値に設定されている場合、システムは `datacache_disk_size` と `starlet_star_cache_disk_size_percent` の間の大きい値をキャッシュ容量として選択する。

### ダウングレードノート

- クラスタはv3.4.0からv3.3.9以降にのみダウングレードできます。
