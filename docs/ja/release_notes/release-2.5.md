---
displayed_sidebar: docs
---

# StarRocks バージョン 2.5

## 2.5.20

リリース日: 2024年3月22日

### 改善点

- `replace_if_not_null` が集計テーブルの BITMAP カラムをサポートしました。ユーザーは集計テーブルの BITMAP カラムに対して集計関数として `replace_if_not_null` を指定できます。 [#42104](https://github.com/StarRocks/starrocks/pull/42104)
- JDK 9 以降ではデフォルトで G1 ガベージコレクタが使用されます。 [#41374](https://github.com/StarRocks/starrocks/pull/41374)

### パラメータの変更

- BE パラメータ `update_compaction_size_threshold` のデフォルト値が 256 MB から 64 MB に変更され、コンパクションが加速されます。 [#42776](https://github.com/StarRocks/starrocks/pull/42776)

### バグ修正

以下の問題を修正しました:

- StarRocks 外部テーブルを使用してデータを同期すると、"commit and publish txn failed" というエラーが発生します。再試行後に同期は成功しますが、同じデータが2回ロードされます。 [#25165](https://github.com/StarRocks/starrocks/pull/25165)
- GC の問題により、RPC 送信リソースが一時的に利用できなくなります。 [#41636](https://github.com/StarRocks/starrocks/pull/41636)
- v2.5 の array_agg() が NULL を v2.3 とは異なる方法で処理します。その結果、v2.3 から v2.5 にアップグレードした後、クエリ結果が不正確になります。 [#42639](https://github.com/StarRocks/starrocks/pull/42639)
- クエリ内の Sink Operator が予期せず終了し、BE がクラッシュします。 [#38662](https://github.com/StarRocks/starrocks/pull/38662)
- 集計テーブルで DELETE コマンドを実行すると、tablet メタデータへのアクセス競争が発生し、BE がクラッシュします。 [#42174](https://github.com/StarRocks/starrocks/pull/42174)
- MemTracker が UDF 呼び出し中に Use-After-Free 問題に遭遇し、BE がクラッシュします。 [#41710](https://github.com/StarRocks/starrocks/pull/41710)
- unnest() 関数がエイリアスをサポートしていません。 [#42138](https://github.com/StarRocks/starrocks/pull/42138)

## 2.5.19

リリース日: 2024年2月8日

### 新機能

- パターンマッチング関数を追加しました: [regexp_extract_all](https://docs.starrocks.io/docs/sql-reference/sql-functions/like-predicate-functions/regexp_extract_all/)。
- Bitmap 値処理関数を追加しました: serialize, deserialize, および serializeToString。 [#40162](https://github.com/StarRocks/starrocks/pull/40162/files)

### 改善点

- 非アクティブなマテリアライズドビューをリフレッシュする際に自動的にアクティブ化をサポートします。 [#38521](https://github.com/StarRocks/starrocks/pull/38521)
- BE ログ出力を最適化し、無関係なログが多すぎるのを防ぎます。 [#22820](https://github.com/StarRocks/starrocks/pull/22820) [#36187](https://github.com/StarRocks/starrocks/pull/36187)
- [Hive UDFs](https://docs.starrocks.io/docs/integrations/hive_bitmap_udf/) を使用して Bitmap データを StarRocks にロードし、StarRocks から Hive にエクスポートすることをサポートします。 [#40165](https://github.com/StarRocks/starrocks/pull/40165) [#40168](https://github.com/StarRocks/starrocks/pull/40168)
- Apache Iceberg テーブルの TIMESTAMP パーティションフィールドをサポートするために、日付フォーマット `yyyy-MM-ddTHH:mm` および `yyyy-MM-dd HH:mm` を追加しました。 [#39986](https://github.com/StarRocks/starrocks/pull/39986)

### バグ修正

以下の問題を修正しました:

- PROPERTIES が指定されていない Spark Load ジョブを実行すると、null ポインタ例外 (NPE) が発生します。 [#38765](https://github.com/StarRocks/starrocks/pull/38765)
- INSERT INTO SELECT が時々 "timeout by txn manager" というエラーに遭遇します。 [#36688](https://github.com/StarRocks/starrocks/pull/36688)
- BE 動的パラメータ `storage_page_cache_limit` によって指定されたしきい値を超えて PageCache のメモリ消費が発生します。 [#37740](https://github.com/StarRocks/starrocks/pull/37740)
- テーブルが削除され、同じテーブル名で再作成された後、そのテーブルに作成された非同期マテリアライズドビューのリフレッシュが失敗します。 [#38008](https://github.com/StarRocks/starrocks/pull/38008) [#38982](https://github.com/StarRocks/starrocks/pull/38982)
- SELECT INTO を使用して S3 バケットにデータを書き込むと、時々 "The tablet write operation update metadata take a long time" というエラーに遭遇します。 [#38443](https://github.com/StarRocks/starrocks/pull/38443)
- データロード中の一部の操作で "reached timeout" に遭遇することがあります。 [#36746](https://github.com/StarRocks/starrocks/pull/36746)
- SHOW CREATE TABLE によって返される DECIMAL 型が CREATE TABLE で指定されたものと一致しません。 [#39297](https://github.com/StarRocks/starrocks/pull/39297)
- 外部テーブルのパーティション列に null 値が含まれている場合、それらのテーブルに対するクエリが BE をクラッシュさせます。 [#38888](https://github.com/StarRocks/starrocks/pull/38888)
- Duplicate Key テーブルからデータを削除する際、DELETE ステートメントの WHERE 句の条件に先行スペースがある場合、削除されたデータを SELECT を使用してクエリできます。 [#39797](https://github.com/StarRocks/starrocks/pull/39797)
- ORC ファイルから StarRocks (`array<json>`) に `array<string>` データをロードすると、BE がクラッシュすることがあります。 [#39233](https://github.com/StarRocks/starrocks/pull/39233)
- Hive カタログのクエリがスタックし、期限切れになることがあります。 [#39863](https://github.com/StarRocks/starrocks/pull/39863)
- PARTITION BY 句で時間レベルのパーティションが指定されている場合、パーティションを動的に作成できません。 [#40256](https://github.com/StarRocks/starrocks/pull/40256)
- Apache Flink からのロード中に "failed to call frontend service" というエラーメッセージが返されます。 [#40710](https://github.com/StarRocks/starrocks/pull/40710)

## 2.5.18

リリース日: 2024年1月10日

### 新機能

- 非同期マテリアライズドビューを [CREATE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/#parameters) または [ALTER](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_MATERIALIZED_VIEW/) する際にセッション変数を設定または変更できます。 [#37401](https://github.com/StarRocks/starrocks/pull/37401)

### 改善点

- JDK を使用する際、デフォルトの GC アルゴリズムは G1 です。 [#37498](https://github.com/StarRocks/starrocks/pull/37498)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) ステートメントによって返される結果に、各パーティションからの消費メッセージのタイムスタンプが含まれるようになりました。 [#36222](https://github.com/StarRocks/starrocks/pull/36222)

### 動作の変更

- INSERT INTO SELECT ステートメントでマテリアライズドビューがクエリを書き換えるかどうかを制御するセッション変数 `enable_materialized_view_for_insert` を追加しました。デフォルト値は `false` です。 [#37505](https://github.com/StarRocks/starrocks/pull/37505)
- セッション変数 `enable_strict_order_by` を追加しました。この変数がデフォルト値 `TRUE` に設定されている場合、クエリパターンに対してエラーが報告されます: クエリの異なる式で重複したエイリアスが使用され、このエイリアスが ORDER BY のソートフィールドでもある場合、例えば `select distinct t1.* from tbl1 t1 order by t1.k1;`。このロジックは v2.3 およびそれ以前と同じです。この変数が `FALSE` に設定されている場合、緩やかな重複排除メカニズムが使用され、そのようなクエリを有効な SQL クエリとして処理します。 [#37910](https://github.com/StarRocks/starrocks/pull/37910)

### パラメータの変更

- トランザクションアクセスモードを指定するためのセッション変数 `transaction_read_only` および `tx_read_only` を追加しました。これらは MySQL バージョン 5.7.20 以降と互換性があります。 [#37249](https://github.com/StarRocks/starrocks/pull/37249)
- FE の設定項目 `routine_load_unstable_threshold_second` を追加しました。 [#36222](https://github.com/StarRocks/starrocks/pull/36222)
- HTTP リクエストを処理するための HTTP サーバーのスレッド数を指定する FE の設定項目 `http_worker_threads_num` を追加しました。デフォルト値は `0` です。このパラメータの値が負の値または 0 に設定されている場合、実際のスレッド数は CPU コア数の 2 倍になります。 [#37530](https://github.com/StarRocks/starrocks/pull/37530)
- ディスク上のコンパクションの最大同時実行数を設定する BE の設定項目 `pindex_major_compaction_limit_per_disk` を追加しました。これは、コンパクションによるディスク間の I/O の不均一性の問題に対処します。この問題は、特定のディスクの I/O が非常に高くなる原因となります。デフォルト値は `1` です。 [#37695](https://github.com/StarRocks/starrocks/pull/37695)

### バグ修正

以下の問題を修正しました:

- ORDER BY カラムとして NaN (Not a Number) カラムを使用すると、BE がクラッシュすることがあります。 [#30759](https://github.com/StarRocks/starrocks/pull/30759)
- 主キーインデックスの更新に失敗すると、"get_applied_rowsets failed" というエラーが発生することがあります。 [#27488](https://github.com/StarRocks/starrocks/pull/27488)
- [Hive カタログ](https://docs.starrocks.io/docs/2.5/data_source/catalog/hive_catalog/) の Hive メタデータが、Hive テーブルに新しいフィールドが追加されたときに自動的に更新されません。 [#37668](https://github.com/StarRocks/starrocks/pull/37668)
- `SELECT ... FROM ... INTO OUTFILE` を実行してデータを CSV ファイルにエクスポートすると、FROM 句に複数の定数が含まれている場合、"Unmatched number of columns" というエラーが報告されます。 [#38045](https://github.com/StarRocks/starrocks/pull/38045)
- 一部のケースで、`bitmap_to_string` がデータ型のオーバーフローにより不正確な結果を返すことがあります。 [#37405](https://github.com/StarRocks/starrocks/pull/37405)

## 2.5.17

リリース日: 2023年12月19日

### 新機能

- 設定可能な最大行セット数を設定するための新しいメトリック `max_tablet_rowset_num` を追加しました。このメトリックは、コンパクションの問題を検出し、"too many versions" エラーの発生を減少させます。 [#36539](https://github.com/StarRocks/starrocks/pull/36539)
- [subdivide_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/) 関数を追加しました。 [#35817](https://github.com/StarRocks/starrocks/pull/35817)

### 改善点

- [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) ステートメントによって返される結果に、新しいフィールド `OtherMsg` が追加され、最後に失敗したタスクに関する情報が表示されます。 [#35806](https://github.com/StarRocks/starrocks/pull/35806)
- ゴミファイルのデフォルトの保持期間が元の3日から1日に変更されました。 [#37113](https://github.com/StarRocks/starrocks/pull/37113)
- 主キーテーブルのすべての行セットでコンパクションが行われる場合、永続性インデックスの更新のパフォーマンスを最適化し、ディスクの読み取り I/O を削減しました。 [#36819](https://github.com/StarRocks/starrocks/pull/36819)
- 主キーテーブルのコンパクションスコアを計算するロジックを最適化し、他の3つのテーブルタイプとより一貫した範囲内で主キーテーブルのコンパクションスコアを整合させました。 [#36534](https://github.com/StarRocks/starrocks/pull/36534)
- MySQL 外部テーブルおよび JDBC カタログ内の外部テーブルに対するクエリで WHERE 句にキーワードを含めることをサポートします。 [#35917](https://github.com/StarRocks/starrocks/pull/35917)
- Spark Load に bitmap_from_binary 関数を追加し、バイナリデータのロードをサポートします。 [#36050](https://github.com/StarRocks/starrocks/pull/36050)
- bRPC の有効期限が1時間からセッション変数 [`query_timeout`](https://docs.starrocks.io/zh/docs/3.2/reference/System_variable/#query_timeout) で指定された期間に短縮されました。これにより、RPC リクエストの期限切れによるクエリの失敗を防ぎます。 [#36778](https://github.com/StarRocks/starrocks/pull/36778)

### パラメータの変更

- BE の設定項目 `enable_stream_load_verbose_log` が追加されました。デフォルト値は `false` です。このパラメータを `true` に設定すると、StarRocks は Stream Load ジョブの HTTP リクエストとレスポンスを記録し、トラブルシューティングを容易にします。 [#36113](https://github.com/StarRocks/starrocks/pull/36113)
- BE の静的パラメータ `update_compaction_per_tablet_min_interval_seconds` が可変になりました。 [#36819](https://github.com/StarRocks/starrocks/pull/36819)

### バグ修正

以下の問題を修正しました:

- ハッシュジョイン中にクエリが失敗し、BE がクラッシュします。 [#32219](https://github.com/StarRocks/starrocks/pull/32219)
- FE の設定項目 `enable_collect_query_detail_info` を `true` に設定すると、FE のパフォーマンスが急落します。 [#35945](https://github.com/StarRocks/starrocks/pull/35945)
- 大量のデータを永続性インデックスが有効な主キーテーブルにロードすると、エラーが発生することがあります。 [#34352](https://github.com/StarRocks/starrocks/pull/34352)
- `./agentctl.sh stop be` を使用して BE を停止すると、starrocks_be プロセスが予期せず終了することがあります。 [#35108](https://github.com/StarRocks/starrocks/pull/35108)
- [array_distinct](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_distinct/) 関数が時々 BE をクラッシュさせることがあります。 [#36377](https://github.com/StarRocks/starrocks/pull/36377)
- ユーザーがマテリアライズドビューをリフレッシュすると、デッドロックが発生することがあります。 [#35736](https://github.com/StarRocks/starrocks/pull/35736)
- 一部のシナリオで、動的パーティション化がエラーに遭遇し、FE の起動に失敗することがあります。 [#36846](https://github.com/StarRocks/starrocks/pull/36846)

## 2.5.16

リリース日: 2023年12月1日

### バグ修正

以下の問題を修正しました:

- 特定のシナリオでグローバルランタイムフィルタが BE をクラッシュさせることがあります。 [#35776](https://github.com/StarRocks/starrocks/pull/35776)

## 2.5.15

リリース日: 2023年11月29日

### 改善点

- 遅いリクエストを追跡するための遅いリクエストログを追加しました。 [#33908](https://github.com/StarRocks/starrocks/pull/33908)
- ファイル数が多い場合に Spark Load を使用して Parquet および ORC ファイルを読み取るパフォーマンスを最適化しました。 [#34787](https://github.com/StarRocks/starrocks/pull/34787)
- Bitmap に関連するいくつかの操作のパフォーマンスを最適化しました。これには以下が含まれます:
  - ネストループジョインを最適化しました。 [#340804](https://github.com/StarRocks/starrocks/pull/34804) [#35003](https://github.com/StarRocks/starrocks/pull/35003)
  - `bitmap_xor` 関数を最適化しました。 [#34069](https://github.com/StarRocks/starrocks/pull/34069)
  - Bitmap パフォーマンスを最適化し、メモリ消費を削減するために Copy on Write をサポートします。 [#34047](https://github.com/StarRocks/starrocks/pull/34047)

### パラメータの変更

- FE の動的パラメータ `enable_new_publish_mechanism` が静的パラメータに変更されました。パラメータ設定を変更した後、FE を再起動する必要があります。 [#35338](https://github.com/StarRocks/starrocks/pull/35338)

### バグ修正

以下の問題を修正しました:

- Broker Load ジョブでフィルタリング条件が指定されている場合、特定の状況でデータロード中に BE がクラッシュすることがあります。 [#29832](https://github.com/StarRocks/starrocks/pull/29832)
- レプリカ操作の再生に失敗すると FE がクラッシュすることがあります。 [#32295](https://github.com/StarRocks/starrocks/pull/32295)
- FE パラメータ `recover_with_empty_tablet` を `true` に設定すると FE がクラッシュすることがあります。 [#33071](https://github.com/StarRocks/starrocks/pull/33071)
- クエリに対して "get_applied_rowsets failed, tablet updates is in error state: tablet:18849 actual row size changed after compaction" というエラーが返されます。 [#33246](https://github.com/StarRocks/starrocks/pull/33246)
- ウィンドウ関数を含むクエリが BE をクラッシュさせることがあります。 [#33671](https://github.com/StarRocks/starrocks/pull/33671)
- `show proc '/statistic'` を実行するとデッドロックが発生することがあります。 [#34237](https://github.com/StarRocks/starrocks/pull/34237/files)
- 大量のデータを永続性インデックスが有効な主キーテーブルにロードすると、エラーが発生することがあります。 [#34566](https://github.com/StarRocks/starrocks/pull/34566)
- StarRocks を v2.4 以前から後のバージョンにアップグレードすると、コンパクションスコアが予期せず上昇することがあります。 [#34618](https://github.com/StarRocks/starrocks/pull/34618)
- MariaDB ODBC データベースドライバを使用して `INFORMATION_SCHEMA` をクエリすると、`schemata` ビューで返される `CATALOG_NAME` カラムがすべて `null` 値を持ちます。 [#34627](https://github.com/StarRocks/starrocks/pull/34627)
- スキーマ変更が実行されている間に Stream Load ジョブが **PREPARED** 状態にある場合、ジョブによってロードされるソースデータの一部が失われます。 [#34381](https://github.com/StarRocks/starrocks/pull/34381)
- HDFS ストレージパスの末尾に2つ以上のスラッシュ (`/`) を含めると、HDFS からのデータのバックアップと復元が失敗します。 [#34601](https://github.com/StarRocks/starrocks/pull/34601)
- ロードタスクまたはクエリを実行すると FE がハングすることがあります。 [#34569](https://github.com/StarRocks/starrocks/pull/34569)

## 2.5.14

リリース日: 2023年11月14日

### 改善点

- システムデータベース `INFORMATION_SCHEMA` の `COLUMNS` テーブルが ARRAY、MAP、および STRUCT カラムを表示できるようになりました。 [#33431](https://github.com/StarRocks/starrocks/pull/33431)

### パラメータの変更

#### システム変数

- DECIMAL 型データを STRING 型データに変換する方法を制御するセッション変数 `cbo_decimal_cast_string_strict` を追加しました。この変数が `true` に設定されている場合、v2.5.x 以降のバージョンで組み込まれたロジックが優先され、システムは厳密な変換を実行します（つまり、生成された文字列を切り捨て、スケール長に基づいて0を埋めます）。この変数が `false` に設定されている場合、v2.5.x より前のバージョンで組み込まれたロジックが優先され、システムはすべての有効な桁を処理して文字列を生成します。デフォルト値は `true` です。 [#34208](https://github.com/StarRocks/starrocks/pull/34208)
- DECIMAL 型データと STRING 型データの比較に使用されるデータ型を指定するセッション変数 `cbo_eq_base_type` を追加しました。デフォルト値は `VARCHAR` で、DECIMAL も有効な値です。 [#34208](https://github.com/StarRocks/starrocks/pull/34208)

### バグ修正

以下の問題を修正しました:

- ON 条件がサブクエリとネストされている場合、`java.lang.IllegalStateException: null` エラーが報告されます。 [#30876](https://github.com/StarRocks/starrocks/pull/30876)
- `INSERT INTO SELECT ... LIMIT` が正常に実行された直後に COUNT(*) を実行すると、レプリカ間で COUNT(*) の結果が一致しません。 [#24435](https://github.com/StarRocks/starrocks/pull/24435)
- cast() 関数で指定されたターゲットデータ型が元のデータ型と同じ場合、特定のデータ型に対して BE がクラッシュすることがあります。 [#31465](https://github.com/StarRocks/starrocks/pull/31465)
- Broker Load を介してデータをロードする際に特定のパス形式が使用されると、エラーが報告されます: `msg:Fail to parse columnsFromPath, expected: [rec_dt]`。 [#32721](https://github.com/StarRocks/starrocks/issues/32721)
- 3.x にアップグレードする際に、いくつかのカラムタイプもアップグレードされる場合（例えば、Decimal が Decimal v3 にアップグレードされる場合）、特定の特性を持つテーブルでコンパクションが実行されると BE がクラッシュします。 [#31626](https://github.com/StarRocks/starrocks/pull/31626)
- Flink Connector を使用してデータをロードする際、高度に同時実行されるロードジョブがあり、HTTP およびスキャンスレッドの数が上限に達している場合、ロードジョブが予期せず中断されます。 [#32251](https://github.com/StarRocks/starrocks/pull/32251)
- libcurl が呼び出されると BE がクラッシュします。 [#31667](https://github.com/StarRocks/starrocks/pull/31667)
- Primary Key テーブルに BITMAP カラムを追加すると、次のエラーが発生します: `Analyze columnDef error: No aggregate function specified for 'userid'`。 [#31763](https://github.com/StarRocks/starrocks/pull/31763)
- 永続性インデックスが有効な主キーテーブルに長時間、頻繁にデータをロードすると、BE がクラッシュすることがあります。 [#33220](https://github.com/StarRocks/starrocks/pull/33220)
- Query Cache が有効な場合、クエリ結果が不正確です。 [#32778](https://github.com/StarRocks/starrocks/pull/32778)
- 主キーテーブルを作成する際に nullable なソートキーを指定すると、コンパクションが失敗します。 [#29225](https://github.com/StarRocks/starrocks/pull/29225)
- 複雑なジョインクエリに対して "StarRocks planner use long time 10000 ms in logical phase" というエラーが時々発生します。 [#34177](https://github.com/StarRocks/starrocks/pull/34177)

## 2.5.13

リリース日: 2023年9月28日

### 改善点

- ウィンドウ関数 COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、および STDDEV_SAMP が ORDER BY 句およびウィンドウ句をサポートするようになりました。 [#30786](https://github.com/StarRocks/starrocks/pull/30786)
- DECIMAL 型データのクエリ中にオーバーフローが発生した場合、NULL の代わりにエラーが返されます。 [#30419](https://github.com/StarRocks/starrocks/pull/30419)
- 無効なコメントを含む SQL コマンドの実行結果が MySQL と一致するようになりました。 [#30210](https://github.com/StarRocks/starrocks/pull/30210)
- 削除された tablet に対応する rowset がクリーンアップされ、BE の起動時のメモリ使用量が削減されます。 [#30625](https://github.com/StarRocks/starrocks/pull/30625)

### バグ修正

以下の問題を修正しました:

- Spark Connector または Flink Connector を使用して StarRocks からデータを読み取ると、"Set cancelled by MemoryScratchSinkOperator" エラーが発生します。 [#30702](https://github.com/StarRocks/starrocks/pull/30702) [#30751](https://github.com/StarRocks/starrocks/pull/30751)
- 集計関数を含む ORDER BY 句を持つクエリ中に "java.lang.IllegalStateException: null" エラーが発生します。 [#30108](https://github.com/StarRocks/starrocks/pull/30108)
- 非アクティブなマテリアライズドビューが存在する場合、FE の再起動に失敗します。 [#30015](https://github.com/StarRocks/starrocks/pull/30015)
- 重複したパーティションに対して INSERT OVERWRITE 操作を実行すると、メタデータが破損し、FE の再起動に失敗します。 [#27545](https://github.com/StarRocks/starrocks/pull/27545)
- 主キーテーブルに存在しないカラムを変更しようとすると、"java.lang.NullPointerException: null" エラーが発生します。 [#30366](https://github.com/StarRocks/starrocks/pull/30366)
- パーティション化された StarRocks 外部テーブルにデータをロードする際に "get TableMeta failed from TNetworkAddress" エラーが発生します。 [#30124](https://github.com/StarRocks/starrocks/pull/30124)
- CloudCanal を介してデータをロードする際に特定のシナリオでエラーが発生します。 [#30799](https://github.com/StarRocks/starrocks/pull/30799)
- Flink Connector を介してデータをロードするか、DELETE および INSERT 操作を実行する際に "current running txns on db xxx is 200, larger than limit 200" エラーが発生します。 [#18393](https://github.com/StarRocks/starrocks/pull/18393)
- 集計関数を含む HAVING 句を使用する非同期マテリアライズドビューがクエリを書き換えることができません。 [#29976](https://github.com/StarRocks/starrocks/pull/29976)

## 2.5.12

リリース日: 2023年9月4日

### 改善点

- SQL のコメントが監査ログに保持されます。 [#29747](https://github.com/StarRocks/starrocks/pull/29747)
- INSERT INTO SELECT の CPU およびメモリ統計が監査ログに追加されました。 [#29901](https://github.com/StarRocks/starrocks/pull/29901)

### バグ修正

以下の問題を修正しました:

- Broker Load を使用してデータをロードする際、一部のフィールドの NOT NULL 属性が BE をクラッシュさせたり、"msg:mismatched row count" エラーを引き起こすことがあります。 [#29832](https://github.com/StarRocks/starrocks/pull/29832)
- ORC 形式のファイルに対するクエリが失敗するのは、Apache ORC のバグ修正 ORC-1304 ([apache/orc#1299](https://github.com/apache/orc/pull/1299)) がマージされていないためです。 [#29804](https://github.com/StarRocks/starrocks/pull/29804)
- 主キーテーブルを復元すると、BE が再起動された後にメタデータの不整合が発生します。 [#30135](https://github.com/StarRocks/starrocks/pull/30135)

## 2.5.11

リリース日: 2023年8月28日

### 改善点

- すべての複合述語および WHERE 句内のすべての式に対する暗黙の型変換をサポートします。暗黙の型変換は、[セッション変数](https://docs.starrocks.io/docs/sql-reference/System_variable/#enable_strict_type) `enable_strict_type` を使用して有効または無効にできます。デフォルト値は `false` です。 [#21870](https://github.com/StarRocks/starrocks/pull/21870)
- Iceberg カタログを作成する際に `hive.metastore.uri` を指定しない場合に返されるプロンプトを最適化しました。エラープロンプトがより正確になりました。 [#16543](https://github.com/StarRocks/starrocks/issues/16543)
- エラーメッセージ `xxx too many versions xxx` により多くのプロンプトを追加しました。 [#28397](https://github.com/StarRocks/starrocks/pull/28397)
- 動的パーティション化は、パーティション単位として `year` をさらにサポートします。 [#28386](https://github.com/StarRocks/starrocks/pull/28386)

### バグ修正

以下の問題を修正しました:

- 複数のレプリカを持つテーブルにデータをロードする際、テーブルの一部のパーティションが空の場合、大量の無効なログレコードが書き込まれます。 [#28824](https://github.com/StarRocks/starrocks/issues/28824)
- WHERE 条件のフィールドが BITMAP または HLL フィールドである場合、DELETE 操作が失敗します。 [#28592](https://github.com/StarRocks/starrocks/pull/28592)
- 非同期呼び出し (SYNC MODE) を介して非同期マテリアライズドビューを手動でリフレッシュすると、`information_schema.task_runs` テーブルに複数の INSERT OVERWRITE レコードが生成されます。 [#28060](https://github.com/StarRocks/starrocks/pull/28060)
- エラー状態の tablet に対して CLONE 操作がトリガーされると、ディスク使用量が増加します。 [#28488](https://github.com/StarRocks/starrocks/pull/28488)
- Join Reorder が有効な場合、クエリするカラムが定数であるとクエリ結果が不正確になります。 [#29239](https://github.com/StarRocks/starrocks/pull/29239)
- SSD と HDD の間で tablet の移行が行われる際、FE が BE に過剰な移行タスクを送信すると、BE が OOM 問題に遭遇します。 [#29055](https://github.com/StarRocks/starrocks/pull/29055)
- `/apache_hdfs_broker/lib/log4j-1.2.17.jar` のセキュリティ脆弱性。 [#28866](https://github.com/StarRocks/starrocks/pull/28866)
- Hive カタログを介してデータをクエリする際、WHERE 句にパーティション列と OR 演算子が使用されている場合、クエリ結果が不正確になります。 [#28876](https://github.com/StarRocks/starrocks/pull/28876)
- データクエリ中に "java.util.ConcurrentModificationException: null" エラーが時々発生します。 [#29296](https://github.com/StarRocks/starrocks/pull/29296)
- 非同期マテリアライズドビューのベーステーブルが削除されると、FE を再起動できません。 [#29318](https://github.com/StarRocks/starrocks/pull/29318)
- データがこのマテリアライズドビューのベーステーブルに書き込まれているときに、データベースをまたいで作成された非同期マテリアライズドビューで Leader FE が時々デッドロックに遭遇します。 [#29432](https://github.com/StarRocks/starrocks/pull/29432)

## 2.5.10

リリース日: 2023年8月7日

### 新機能

- 集計関数 [COVAR_SAMP](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/covar_samp/)、[COVAR_POP](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/covar_pop/)、および [CORR](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/corr/) をサポートします。
- 次の [ウィンドウ関数](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/) をサポートします: COVAR_SAMP、COVAR_POP、CORR、VARIANCE、VAR_SAMP、STD、および STDDEV_SAMP。

### 改善点

- 修復されない tablet を繰り返しスケジュールしないように、TabletChecker のスケジューリングロジックを最適化しました。 [#27648](https://github.com/StarRocks/starrocks/pull/27648)
- スキーマ変更と Routine Load が同時に発生する場合、スキーマ変更が先に完了すると Routine Load ジョブが失敗することがあります。この状況で報告されるエラーメッセージが最適化されました。 [#28425](https://github.com/StarRocks/starrocks/pull/28425)
- 外部テーブルを作成する際に NOT NULL カラムを定義することが禁止されました（NOT NULL カラムが定義されている場合、アップグレード後にエラーが発生し、テーブルを再作成する必要があります）。外部カタログは v2.3.0 以降で外部テーブルの代わりとして推奨されます。 [#25485](https://github.com/StarRocks/starrocks/pull/25441)
- Broker Load の再試行時にエラーメッセージが追加されました。これにより、データロード中のトラブルシューティングとデバッグが容易になります。 [#21982](https://github.com/StarRocks/starrocks/pull/21982)
- ロードジョブが UPSERT および DELETE 操作の両方を含む場合、大規模なデータ書き込みをサポートします。 [#17264](https://github.com/StarRocks/starrocks/pull/17264)
- マテリアライズドビューを使用したクエリの書き換えを最適化しました。 [#27934](https://github.com/StarRocks/starrocks/pull/27934) [#25542](https://github.com/StarRocks/starrocks/pull/25542) [#22300](https://github.com/StarRocks/starrocks/pull/22300) [#27557](https://github.com/StarRocks/starrocks/pull/27557) [#22300](https://github.com/StarRocks/starrocks/pull/22300) [#26957](https://github.com/StarRocks/starrocks/pull/26957) [#27728](https://github.com/StarRocks/starrocks/pull/27728) [#27900](https://github.com/StarRocks/starrocks/pull/27900)

### バグ修正

以下の問題を修正しました:

- 文字列を配列に変換するために CAST が使用される場合、入力に定数が含まれていると結果が不正確になることがあります。 [#19793](https://github.com/StarRocks/starrocks/pull/19793)
- SHOW TABLET が ORDER BY および LIMIT を含む場合、結果が不正確になります。 [#23375](https://github.com/StarRocks/starrocks/pull/23375)
- マテリアライズドビューの外部結合およびアンチ結合の書き換えエラー。 [#28028](https://github.com/StarRocks/starrocks/pull/28028)
- FE のテーブルレベルのスキャン統計が不正確であるため、テーブルクエリおよびロードのメトリックが不正確になります。 [#27779](https://github.com/StarRocks/starrocks/pull/27779)
- HMS にイベントリスナーが設定されていて Hive メタデータをインクリメンタルに更新する場合、FE ログに "An exception occurred when using the current long link to access metastore. msg: Failed to get next notification based on last event id: 707602" が報告されます。 [#21056](https://github.com/StarRocks/starrocks/pull/21056)
- パーティション化されたテーブルのソートキーが変更されると、クエリ結果が安定しません。 [#27850](https://github.com/StarRocks/starrocks/pull/27850)
- Spark Load を使用してロードされたデータが、バケット列が DATE、DATETIME、または DECIMAL 列である場合、誤ったバケットに分配されることがあります。 [#27005](https://github.com/StarRocks/starrocks/pull/27005)
- regex_replace 関数が一部のシナリオで BE をクラッシュさせることがあります。 [#27117](https://github.com/StarRocks/starrocks/pull/27117)
- sub_bitmap 関数の入力が BITMAP 値でない場合、BE がクラッシュします。 [#27982](https://github.com/StarRocks/starrocks/pull/27982)
- Join Reorder が有効な場合、クエリに対して "Unknown error" が返されます。 [#27472](https://github.com/StarRocks/starrocks/pull/27472)
- 平均行サイズの不正確な推定により、主キーの部分更新が過剰に大きなメモリを占有します。 [#27485](https://github.com/StarRocks/starrocks/pull/27485)
- 低基数最適化が有効な場合、一部の INSERT ジョブが `[42000][1064] Dict Decode failed, Dict can't take cover all key :0` を返します。 [#26463](https://github.com/StarRocks/starrocks/pull/26463)
- ユーザーが HDFS からデータをロードするために作成した Broker Load ジョブで `"hadoop.security.authentication" = "simple"` を指定すると、ジョブが失敗します。 [#27774](https://github.com/StarRocks/starrocks/pull/27774)
- マテリアライズドビューのリフレッシュモードを変更すると、リーダー FE とフォロワー FE の間でメタデータが不一致になります。 [#28082](https://github.com/StarRocks/starrocks/pull/28082) [#28097](https://github.com/StarRocks/starrocks/pull/28097)
- SHOW CREATE CATALOG および SHOW RESOURCES を使用して特定の情報をクエリする際にパスワードが隠されません。 [#28059](https://github.com/StarRocks/starrocks/pull/28059)
- LabelCleaner スレッドがブロックされることによる FE のメモリリーク。 [#28311](https://github.com/StarRocks/starrocks/pull/28311)

## 2.5.9

リリース日: 2023年7月19日

### 新機能

- マテリアライズドビューとは異なるタイプのジョインを含むクエリを書き換えることができます。 [#25099](https://github.com/StarRocks/starrocks/pull/25099)

### 改善点

- 宛先クラスターが現在の StarRocks クラスターである StarRocks 外部テーブルを作成できません。 [#25441](https://github.com/StarRocks/starrocks/pull/25441)
- クエリされたフィールドがマテリアライズドビューの出力カラムに含まれていないが、マテリアライズドビューの述語に含まれている場合でも、クエリを書き換えることができます。 [#23028](https://github.com/StarRocks/starrocks/issues/23028)
- データベース `Information_schema` のテーブル `tables_config` に新しいフィールド `table_id` を追加しました。`tables_config` を `be_tablets` と `table_id` で結合して、tablet が属するデータベースおよびテーブルの名前をクエリできます。 [#24061](https://github.com/StarRocks/starrocks/pull/24061)

### バグ修正

以下の問題を修正しました:

- Duplicate Key テーブルの Count Distinct 結果が不正確です。 [#24222](https://github.com/StarRocks/starrocks/pull/24222)
- ジョインキーが大きな BINARY カラムである場合、BE がクラッシュすることがあります。 [#25084](https://github.com/StarRocks/starrocks/pull/25084)
- STRUCT に挿入される CHAR データの長さが STRUCT カラムで定義された最大 CHAR 長を超える場合、INSERT 操作がハングします。 [#25942](https://github.com/StarRocks/starrocks/pull/25942)
- coalesce() の結果が不正確です。 [#26250](https://github.com/StarRocks/starrocks/pull/26250)
- データが復元された後、tablet のバージョン番号が BE と FE で一致しません。 [#26518](https://github.com/StarRocks/starrocks/pull/26518/files)
- 復元されたテーブルに対してパーティションを自動的に作成できません。 [#26813](https://github.com/StarRocks/starrocks/pull/26813)

## 2.5.8

リリース日: 2023年6月30日

### 改善点

- 非パーティションテーブルにパーティションを追加する際に報告されるエラーメッセージを最適化しました。 [#25266](https://github.com/StarRocks/starrocks/pull/25266)
- テーブルの [自動 tablet 配布ポリシー](https://docs.starrocks.io/docs/2.5/table_design/Data_distribution/#determine-the-number-of-tablets) を最適化しました。 [#24543](https://github.com/StarRocks/starrocks/pull/24543)
- CREATE TABLE ステートメントのデフォルトコメントを最適化しました。 [#24803](https://github.com/StarRocks/starrocks/pull/24803)
- 非同期マテリアライズドビューの手動リフレッシュを最適化しました。REFRESH MATERIALIZED VIEW WITH SYNC MODE 構文を使用してマテリアライズドビューのリフレッシュタスクを同期的に呼び出すことをサポートします。 [#25910](https://github.com/StarRocks/starrocks/pull/25910)

### バグ修正

以下の問題を修正しました:

- マテリアライズドビューが Union 結果に基づいて構築されている場合、非同期マテリアライズドビューの COUNT 結果が不正確になることがあります。 [#24460](https://github.com/StarRocks/starrocks/issues/24460)
- ユーザーがルートパスワードを強制的にリセットしようとすると "Unknown error" が報告されます。 [#25492](https://github.com/StarRocks/starrocks/pull/25492)
- クラスターに3つ未満の生存 BE がある場合に INSERT OVERWRITE が実行されると、不正確なエラーメッセージが表示されます。 [#25314](https://github.com/StarRocks/starrocks/pull/25314)

## 2.5.7

リリース日: 2023年6月14日

### 新機能

- 非アクティブなマテリアライズドビューを `ALTER MATERIALIZED VIEW <mv_name> ACTIVE` を使用して手動でアクティブ化できます。この SQL コマンドを使用して、ベーステーブルが削除されて再作成されたマテリアライズドビューをアクティブ化できます。詳細については、[ALTER MATERIALIZED VIEW](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_MATERIALIZED_VIEW/) を参照してください。 [#24001](https://github.com/StarRocks/starrocks/pull/24001)
- テーブルを作成する際やパーティションを追加する際に、適切な数の tablet を自動的に設定できるようになり、手動操作が不要になります。詳細については、[Determine the number of tablets](https://docs.starrocks.io/docs/2.5/table_design/Data_distribution/#determine-the-number-of-tablets) を参照してください。 [#10614](https://github.com/StarRocks/starrocks/pull/10614)

### 改善点

- 外部テーブルクエリで使用されるスキャンノードの I/O 同時実行性を最適化し、メモリ使用量を削減し、外部テーブルからのデータロードの安定性を向上させました。 [#23617](https://github.com/StarRocks/starrocks/pull/23617) [#23624](https://github.com/StarRocks/starrocks/pull/23624) [#23626](https://github.com/StarRocks/starrocks/pull/23626)
- Broker Load ジョブのエラーメッセージを最適化しました。エラーメッセージには再試行情報とエラーのあるファイル名が含まれます。 [#18038](https://github.com/StarRocks/starrocks/pull/18038) [#21982](https://github.com/StarRocks/starrocks/pull/21982)
- CREATE TABLE のタイムアウト時に返されるエラーメッセージを最適化し、パラメータ調整のヒントを追加しました。 [#24510](https://github.com/StarRocks/starrocks/pull/24510)
- テーブルのステータスが Normal でないために ALTER TABLE が失敗した場合に返されるエラーメッセージを最適化しました。 [#24381](https://github.com/StarRocks/starrocks/pull/24381)
- CREATE TABLE ステートメントで全角スペースを無視します。 [#23885](https://github.com/StarRocks/starrocks/pull/23885)
- Broker Load ジョブの成功率を高めるために Broker アクセスタイムアウトを最適化しました。 [#22699](https://github.com/StarRocks/starrocks/pull/22699)
- 主キーテーブルの場合、SHOW TABLET によって返される `VersionCount` フィールドには Pending 状態の Rowset が含まれます。 [#23847](https://github.com/StarRocks/starrocks/pull/23847)
- 永続性インデックスポリシーを最適化しました。 [#22140](https://github.com/StarRocks/starrocks/pull/22140)

### バグ修正

以下の問題を修正しました:

- Parquet データを StarRocks にロードする際、型変換中に DATETIME 値がオーバーフローし、データエラーが発生します。 [#22356](https://github.com/StarRocks/starrocks/pull/22356)
- 動的パーティション化が無効化された後、バケット情報が失われます。 [#22595](https://github.com/StarRocks/starrocks/pull/22595)
- CREATE TABLE ステートメントでサポートされていないプロパティを使用すると、null ポインタ例外 (NPE) が発生します。 [#23859](https://github.com/StarRocks/starrocks/pull/23859)
- `information_schema` でのテーブル権限フィルタリングが無効になります。その結果、ユーザーは権限のないテーブルを表示できます。 [#23804](https://github.com/StarRocks/starrocks/pull/23804)
- SHOW TABLE STATUS によって返される情報が不完全です。 [#24279](https://github.com/StarRocks/starrocks/issues/24279)
- スキーマ変更中にデータロードが同時に発生すると、スキーマ変更がハングすることがあります。 [#23456](https://github.com/StarRocks/starrocks/pull/23456)
- RocksDB WAL フラッシュが brpc ワーカーをブロックし、bthreads を処理できなくなり、主キーテーブルへの高頻度データロードが中断されます。 [#22489](https://github.com/StarRocks/starrocks/pull/22489)
- StarRocks でサポートされていない TIME 型カラムが正常に作成されることがあります。 [#23474](https://github.com/StarRocks/starrocks/pull/23474)
- マテリアライズドビューの Union 書き換えが失敗します。 [#22922](https://github.com/StarRocks/starrocks/pull/22922)

## 2.5.6

リリース日: 2023年5月19日

### 改善点

- `thrift_server_max_worker_thread` 値が小さいために INSERT INTO ... SELECT が期限切れになる場合に報告されるエラーメッセージを最適化しました。 [#21964](https://github.com/StarRocks/starrocks/pull/21964)
- CTAS を使用して作成されたテーブルはデフォルトで3つのレプリカを持ち、通常のテーブルのデフォルトレプリカ数と一致します。 [#22854](https://github.com/StarRocks/starrocks/pull/22854)

### バグ修正

以下の問題を修正しました:

- パーティション名に対して TRUNCATE 操作が大文字小文字を区別するため、パーティションの切り捨てが失敗します。 [#21809](https://github.com/StarRocks/starrocks/pull/21809)
- マテリアライズドビューの一時パーティションの作成に失敗すると、BE の退役が失敗します。 [#22745](https://github.com/StarRocks/starrocks/pull/22745)
- 配列値を必要とする動的 FE パラメータを空の配列に設定できません。 [#22225](https://github.com/StarRocks/starrocks/pull/22225)
- `partition_refresh_number` プロパティが指定されたマテリアライズドビューが完全にリフレッシュされないことがあります。 [#21619](https://github.com/StarRocks/starrocks/pull/21619)
- SHOW CREATE TABLE がクラウド資格情報情報をマスクし、メモリ内の資格情報情報が不正確になります。 [#21311](https://github.com/StarRocks/starrocks/pull/21311)
- 外部テーブルを介してクエリされた一部の ORC ファイルで述語が効果を発揮しません。 [#21901](https://github.com/StarRocks/starrocks/pull/21901)
- min-max フィルタがカラム名の大文字小文字を正しく処理できません。 [#22626](https://github.com/StarRocks/starrocks/pull/22626)
- 後期実体化が複雑なデータ型 (STRUCT または MAP) のクエリでエラーを引き起こします。 [#22862](https://github.com/StarRocks/starrocks/pull/22862)
- 主キーテーブルを復元する際に発生する問題。 [#23384](https://github.com/StarRocks/starrocks/pull/23384)

## 2.5.5

リリース日: 2023年4月28日

### 新機能

主キーテーブルの tablet ステータスを監視するためのメトリックを追加しました:

- FE メトリック `err_state_metric` を追加しました。
- **err_state** tablet の数を表示するために `SHOW PROC '/statistic/'` の出力に `ErrorStateTabletNum` カラムを追加しました。
- **err_state** tablet の ID を表示するために `SHOW PROC '/statistic/<db_id>/'` の出力に `ErrorStateTablets` カラムを追加しました。

詳細については、[SHOW PROC](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/SHOW_PROC/) を参照してください。

### 改善点

- 複数の BE が追加された場合のディスクバランス速度を最適化しました。 [# 19418](https://github.com/StarRocks/starrocks/pull/19418)
- `storage_medium` の推論を最適化しました。BE が SSD と HDD の両方をストレージデバイスとして使用する場合、プロパティ `storage_cooldown_time` が指定されている場合、StarRocks は `storage_medium` を `SSD` に設定します。それ以外の場合、StarRocks は `storage_medium` を `HDD` に設定します。 [#18649](https://github.com/StarRocks/starrocks/pull/18649)
- 値カラムから統計を収集することを禁止することで、ユニークキーテーブルのパフォーマンスを最適化しました。 [#19563](https://github.com/StarRocks/starrocks/pull/19563)

### バグ修正

以下の問題を修正しました:

- コロケーションテーブルの場合、`ADMIN SET REPLICA STATUS PROPERTIES ("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");` のようなステートメントを使用してレプリカステータスを手動で `bad` に指定できます。BE の数がレプリカの数以下の場合、破損したレプリカは修復できません。 [# 17876](https://github.com/StarRocks/starrocks/issues/17876)
- BE が起動された後、そのプロセスは存在しますが、BE ポートを有効にできません。 [# 19347](https://github.com/StarRocks/starrocks/pull/19347)
- ウィンドウ関数がネストされた集計クエリに対して誤った結果が返されます。 [# 19725](https://github.com/StarRocks/starrocks/issues/19725)
- マテリアライズドビュー (MV) が初めてリフレッシュされるときに `auto_refresh_partitions_limit` が効果を発揮しません。その結果、すべてのパーティションがリフレッシュされます。 [# 19759](https://github.com/StarRocks/starrocks/issues/19759)
- 配列データが MAP や STRUCT などの複雑なデータとネストされている CSV Hive 外部テーブルをクエリするとエラーが発生します。 [# 20233](https://github.com/StarRocks/starrocks/pull/20233)
- Spark コネクタを使用するクエリがタイムアウトします。 [# 20264](https://github.com/StarRocks/starrocks/pull/20264)
- 2つのレプリカを持つテーブルの1つのレプリカが破損している場合、テーブルは回復できません。 [# 20681](https://github.com/StarRocks/starrocks/pull/20681)
- MV クエリの書き換えの失敗によるクエリの失敗。 [# 19549](https://github.com/StarRocks/starrocks/issues/19549)
- データベースロックによるメトリックインターフェースの期限切れ。 [# 20790](https://github.com/StarRocks/starrocks/pull/20790)
- Broadcast Join に対して誤った結果が返されます。 [# 20952](https://github.com/StarRocks/starrocks/issues/20952)
- CREATE TABLE でサポートされていないデータ型を使用すると NPE が返されます。 [# 20999](https://github.com/StarRocks/starrocks/issues/20999)
- Query Cache 機能を使用した window_funnel() によって引き起こされる問題。 [# 21474](https://github.com/StarRocks/starrocks/issues/21474)
- CTE が書き換えられた後、最適化プランの選択に予期せぬ長い時間がかかります。 [# 16515](https://github.com/StarRocks/starrocks/pull/16515)

## 2.5.4

リリース日: 2023年4月4日

### 改善点

- クエリプランニング中のマテリアライズドビューに対するクエリの書き換えパフォーマンスを最適化しました。クエリプランニングにかかる時間が約70%削減されました。 [#19579](https://github.com/StarRocks/starrocks/pull/19579)
- 型推論ロジックを最適化しました。`SELECT sum(CASE WHEN XXX);` のようなクエリに定数 `0` が含まれている場合、例えば `SELECT sum(CASE WHEN k1 = 1 THEN v1 ELSE 0 END) FROM test;` のように、事前集計が自動的に有効になり、クエリが加速されます。 [#19474](https://github.com/StarRocks/starrocks/pull/19474)
- マテリアライズドビューの作成ステートメントを表示するために `SHOW CREATE VIEW` をサポートします。 [#19999](https://github.com/StarRocks/starrocks/pull/19999)
- BE ノード間の単一の bRPC リクエストで 2 GB 以上のサイズのパケットを送信することをサポートします。 [#20283](https://github.com/StarRocks/starrocks/pull/20283) [#20230](https://github.com/StarRocks/starrocks/pull/20230)
- 外部カタログの作成ステートメントをクエリするために [SHOW CREATE CATALOG](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_CREATE_CATALOG/) をサポートします。

### バグ修正

以下のバグを修正しました:

- マテリアライズドビューに対するクエリが書き換えられた後、低基数最適化のためのグローバル辞書が効果を発揮しません。 [#19615](https://github.com/StarRocks/starrocks/pull/19615)
- マテリアライズドビューに対するクエリが書き換えに失敗した場合、クエリが失敗します。 [#19774](https://github.com/StarRocks/starrocks/pull/19774)
- 主キーまたはユニークキーテーブルに基づいてマテリアライズドビューが作成された場合、そのマテリアライズドビューに対するクエリは書き換えられません。 [#19600](https://github.com/StarRocks/starrocks/pull/19600)
- マテリアライズドビューのカラム名が大文字小文字を区別します。ただし、テーブルを作成する際に、テーブル作成ステートメントの `PROPERTIES` でカラム名が不正確であってもエラーメッセージが表示されず、さらにそのテーブルに作成されたマテリアライズドビューに対するクエリの書き換えが失敗します。 [#19780](https://github.com/StarRocks/starrocks/pull/19780)
- マテリアライズドビューに対するクエリが書き換えられた後、クエリプランにパーティションカラムに基づく無効な述語が含まれることがあり、クエリパフォーマンスに影響を与えます。 [#19784](https://github.com/StarRocks/starrocks/pull/19784)
- 新しく作成されたパーティションにデータがロードされると、マテリアライズドビューに対するクエリが書き換えに失敗することがあります。 [#20323](https://github.com/StarRocks/starrocks/pull/20323)
- マテリアライズドビューの作成時に `"storage_medium" = "SSD"` を設定すると、マテリアライズドビューのリフレッシュが失敗します。 [#19539](https://github.com/StarRocks/starrocks/pull/19539) [#19626](https://github.com/StarRocks/starrocks/pull/19626)
- 主キーテーブルで同時コンパクションが発生することがあります。 [#19692](https://github.com/StarRocks/starrocks/pull/19692)
- 大量の DELETE 操作の後にコンパクションがすぐに発生しません。 [#19623](https://github.com/StarRocks/starrocks/pull/19623)
- ステートメントの式に複数の低基数カラムが含まれている場合、式が正しく書き換えられないことがあります。その結果、低基数最適化のためのグローバル辞書が効果を発揮しません。 [#20161](https://github.com/StarRocks/starrocks/pull/20161)

## 2.5.3

リリース日: 2023年3月10日

### 改善点

- マテリアライズドビュー (MV) のクエリ書き換えを最適化しました。
  - 外部結合およびクロス結合を含むクエリの書き換えをサポートします。 [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - MV のデータスキャンロジックを最適化し、書き換えられたクエリをさらに加速します。 [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - 単一テーブル集計クエリの書き換え機能を強化しました。 [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - View Delta シナリオでの書き換え機能を強化しました。これは、クエリされたテーブルが MV のベーステーブルのサブセットである場合です。 [#18800](https://github.com/StarRocks/starrocks/pull/18800)
- ウィンドウ関数 RANK() がフィルタまたはソートキーとして使用される場合のパフォーマンスとメモリ使用量を最適化しました。 [#17553](https://github.com/StarRocks/starrocks/issues/17553)

### バグ修正

以下のバグを修正しました:

- ARRAY データの null リテラル `[]` によって引き起こされるエラー。 [#18563](https://github.com/StarRocks/starrocks/pull/18563)
- 一部の複雑なクエリシナリオでの低基数最適化辞書の誤用。辞書を適用する前に辞書マッピングチェックを追加しました。 [#17318](https://github.com/StarRocks/starrocks/pull/17318)
- 単一の BE 環境で、ローカルシャッフルが GROUP BY に重複した結果を生成します。 [#17845](https://github.com/StarRocks/starrocks/pull/17845)
- 非パーティション MV に対するパーティション関連の PROPERTIES の誤用が MV リフレッシュの失敗を引き起こすことがあります。MV を作成する際にパーティション PROPERTIES チェックが行われるようになりました。 [#18741](https://github.com/StarRocks/starrocks/pull/18741)
- Parquet Repetition カラムの解析エラー。 [#17626](https://github.com/StarRocks/starrocks/pull/17626) [#17788](https://github.com/StarRocks/starrocks/pull/17788) [#18051](https://github.com/StarRocks/starrocks/pull/18051)
- 取得されたカラムの nullable 情報が不正確です。解決策: CTAS を使用して主キーテーブルを作成する場合、主キーのカラムのみが非 nullable であり、非主キーのカラムは nullable です。 [#16431](https://github.com/StarRocks/starrocks/pull/16431)
- 主キーテーブルからデータを削除することによって引き起こされるいくつかの問題。 [#18768](https://github.com/StarRocks/starrocks/pull/18768)

## 2.5.2

リリース日: 2023年2月21日

### 新機能

- AWS S3 および AWS Glue にアクセスするためのインスタンスプロファイルおよびアサムドロールベースの資格情報メソッドをサポートします。 [#15958](https://github.com/StarRocks/starrocks/pull/15958)
- 次のビット関数をサポートします: bit_shift_left, bit_shift_right, および bit_shift_right_logical。 [#14151](https://github.com/StarRocks/starrocks/pull/14151)

### 改善点

- メモリ解放ロジックを最適化し、大量の集計クエリを含むクエリのピークメモリ使用量を大幅に削減しました。 [#16913](https://github.com/StarRocks/starrocks/pull/16913)
- ソートのメモリ使用量を削減しました。ウィンドウ関数またはソートを含むクエリの場合、メモリ消費が半分になります。 [#16937](https://github.com/StarRocks/starrocks/pull/16937) [#17362](https://github.com/StarRocks/starrocks/pull/17362) [#17408](https://github.com/StarRocks/starrocks/pull/17408)

### バグ修正

以下のバグを修正しました:

- MAP および ARRAY データを含む Apache Hive 外部テーブルがリフレッシュできません。 [#17548](https://github.com/StarRocks/starrocks/pull/17548)
- Superset がマテリアライズドビューのカラムタイプを識別できません。 [#17686](https://github.com/StarRocks/starrocks/pull/17686)
- SET GLOBAL/SESSION TRANSACTION を解析できないため、BI 接続が失敗します。 [#17295](https://github.com/StarRocks/starrocks/pull/17295)
- Colocate Group の動的パーティション化されたテーブルのバケット数を変更できず、エラーメッセージが返されます。 [#17418](https://github.com/StarRocks/starrocks/pull/17418/)
- Prepare ステージの失敗によって引き起こされる潜在的な問題。 [#17323](https://github.com/StarRocks/starrocks/pull/17323)

### 動作の変更

- `enable_experimental_mv` のデフォルト値を `false` から `true` に変更しました。これにより、非同期マテリアライズドビューがデフォルトで有効になります。
- 予約キーワードリストに CHARACTER を追加しました。 [#17488](https://github.com/StarRocks/starrocks/pull/17488)

## 2.5.1

リリース日: 2023年2月5日

### 改善点

- 外部カタログに基づいて作成された非同期マテリアライズドビューがクエリの書き換えをサポートします。 [#11116](https://github.com/StarRocks/starrocks/issues/11116) [#15791](https://github.com/StarRocks/starrocks/issues/15791)
- 自動 CBO 統計収集の収集期間を指定できるようになり、自動フルコレクションによるクラスターのパフォーマンスのジッターを防ぎます。 [#14996](https://github.com/StarRocks/starrocks/pull/14996)
- Thrift サーバーキューを追加しました。INSERT INTO SELECT の際にすぐに処理できないリクエストは Thrift サーバーキューで保留され、リクエストが拒否されるのを防ぎます。 [#14571](https://github.com/StarRocks/starrocks/pull/14571)
- FE パラメータ `default_storage_medium` を非推奨にしました。ユーザーがテーブルを作成する際に `storage_medium` を明示的に指定しない場合、システムは BE ディスクタイプに基づいてテーブルのストレージメディアを自動的に推論します。詳細については、[CREATE TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_VIEW/) の `storage_medium` の説明を参照してください。 [#14394](https://github.com/StarRocks/starrocks/pull/14394)

### バグ修正

以下のバグを修正しました:

- SET PASSWORD によって引き起こされるヌルポインタ例外 (NPE)。 [#15247](https://github.com/StarRocks/starrocks/pull/15247)
- 空のキーを持つ JSON データを解析できません。 [#16852](https://github.com/StarRocks/starrocks/pull/16852)
- 無効な型のデータが ARRAY データに正常に変換されることがあります。 [#16866](https://github.com/StarRocks/starrocks/pull/16866)
- 例外が発生した場合にネストループジョインが中断されません。 [#16875](https://github.com/StarRocks/starrocks/pull/16875)

### 動作の変更

- FE パラメータ `default_storage_medium` を非推奨にしました。テーブルのストレージメディアはシステムによって自動的に推論されます。 [#14394](https://github.com/StarRocks/starrocks/pull/14394)

## 2.5.0

リリース日: 2023年1月22日

### 新機能

- [Hudi カタログ](https://docs.starrocks.io/docs/data_source/catalog/hudi_catalog/) および [Hudi 外部テーブル](https://docs.starrocks.io/docs/data_source/External_table#deprecated-hudi-external-table) を使用して Merge On Read テーブルをクエリすることをサポートします。 [#6780](https://github.com/StarRocks/starrocks/pull/6780)
- [Hive カタログ](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/)、Hudi カタログ、および [Iceberg カタログ](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/) を使用して STRUCT および MAP データをクエリすることをサポートします。 [#10677](https://github.com/StarRocks/starrocks/issues/10677)
- 外部ストレージシステム（HDFS など）に保存されているホットデータのアクセスパフォーマンスを向上させるために [Data Cache](https://docs.starrocks.io/docs/data_source/data_cache/) を提供します。 [#11597](https://github.com/StarRocks/starrocks/pull/11579)
- [Delta Lake カタログ](https://docs.starrocks.io/docs/data_source/catalog/deltalake_catalog/) を作成することをサポートし、Delta Lake からのデータに直接クエリを実行できます。 [#11972](https://github.com/StarRocks/starrocks/issues/11972)
- Hive、Hudi、および Iceberg カタログが AWS Glue と互換性があります。 [#12249](https://github.com/StarRocks/starrocks/issues/12249)
- [ファイル外部テーブル](https://docs.starrocks.io/docs/data_source/file_external_table/) を作成することをサポートし、HDFS およびオブジェクトストアから Parquet および ORC ファイルに直接クエリを実行できます。 [#13064](https://github.com/StarRocks/starrocks/pull/13064)
- Hive、Hudi、Iceberg カタログ、およびマテリアライズドビューに基づいてマテリアライズドビューを作成することをサポートします。詳細については、[マテリアライズドビュー](https://docs.starrocks.io/docs/using_starrocks/Materialized_view/) を参照してください。 [#11116](https://github.com/StarRocks/starrocks/issues/11116) [#11873](https://github.com/StarRocks/starrocks/pull/11873)
- 主キーテーブルを使用するテーブルに対する条件付き更新をサポートします。詳細については、[ロードによるデータの変更](https://docs.starrocks.io/docs/loading/Load_to_Primary_Key_tables/) を参照してください。 [#12159](https://github.com/StarRocks/starrocks/pull/12159)
- [Query Cache](https://docs.starrocks.io/docs/using_starrocks/query_cache/) をサポートし、クエリの中間計算結果を保存し、高度に同時実行される単純なクエリの QPS を向上させ、平均レイテンシーを削減します。 [#9194](https://github.com/StarRocks/starrocks/pull/9194)
- Broker Load ジョブの優先度を指定することをサポートします。詳細については、[BROKER LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/) を参照してください。 [#11029](https://github.com/StarRocks/starrocks/pull/11029)
- StarRocks 内部テーブルのデータロードに対するレプリカ数を指定することをサポートします。詳細については、[CREATE TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/) を参照してください。 [#11253](https://github.com/StarRocks/starrocks/pull/11253)
- [クエリキュー](https://docs.starrocks.io/docs/administration/query_queues/) をサポートします。 [#12594](https://github.com/StarRocks/starrocks/pull/12594)
- データロードによって占有される計算リソースを分離し、データロードタスクのリソース消費を制限することをサポートします。詳細については、[リソースグループ](https://docs.starrocks.io/docs/administration/resource_group/) を参照してください。 [#12606](https://github.com/StarRocks/starrocks/pull/12606)
- StarRocks 内部テーブルに対して次のデータ圧縮アルゴリズムを指定することをサポートします: LZ4、Zstd、Snappy、および Zlib。詳細については、[データ圧縮](https://docs.starrocks.io/docs/table_design/data_compression/) を参照してください。 [#10097](https://github.com/StarRocks/starrocks/pull/10097) [#12020](https://github.com/StarRocks/starrocks/pull/12020)
- [ユーザー定義変数](https://docs.starrocks.io/docs/reference/user_defined_variables/) をサポートします。 [#10011](https://github.com/StarRocks/starrocks/pull/10011)
- [ラムダ式](https://docs.starrocks.io/docs/sql-reference/sql-functions/Lambda_expression/) および次の高階関数をサポートします: [array_map](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_map/)、[array_sum](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_sum/)、および [array_sortby](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_sortby/)。 [#9461](https://github.com/StarRocks/starrocks/pull/9461) [#9806](https://github.com/StarRocks/starrocks/pull/9806) [#10323](https://github.com/StarRocks/starrocks/pull/10323) [#14034](https://github.com/StarRocks/starrocks/pull/14034)
- ウィンドウ関数の結果をフィルタリングするための QUALIFY 句を提供します。 [#13239](https://github.com/StarRocks/starrocks/pull/13239)
- テーブルを作成する際に uuid() および uuid_numeric() 関数によって返される結果をカラムのデフォルト値として使用することをサポートします。詳細については、[CREATE TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/) を参照してください。 [#11155](https://github.com/StarRocks/starrocks/pull/11155)
- 次の関数をサポートします: [map_size](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_size/)、[map_keys](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_keys/)、[map_values](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_values/)、[max_by](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/max_by/)、[sub_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/sub_bitmap/)、[bitmap_to_base64](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_to_base64/)、[host_name](https://docs.starrocks.io/docs/sql-reference/sql-functions/utility-functions/host_name/)、および [date_slice](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/date_slice/)。 [#11299](https://github.com/StarRocks/starrocks/pull/11299) [#11323](https://github.com/StarRocks/starrocks/pull/11323) [#12243](https://github.com/StarRocks/starrocks/pull/12243) [#11776](https://github.com/StarRocks/starrocks/pull/11776) [#12634](https://github.com/StarRocks/starrocks/pull/12634) [#14225](https://github.com/StarRocks/starrocks/pull/14225)

### 改善点

- [Hive カタログ](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/)、[Hudi カタログ](https://docs.starrocks.io/docs/data_source/catalog/hudi_catalog/)、および [Iceberg カタログ](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/) を使用して外部データをクエリする際のメタデータアクセスパフォーマンスを最適化しました。 [#11349](https://github.com/StarRocks/starrocks/issues/11349)
- [Elasticsearch 外部テーブル](https://docs.starrocks.io/docs/data_source/External_table#deprecated-elasticsearch-external-table) を使用して ARRAY データをクエリすることをサポートします。 [#9693](https://github.com/StarRocks/starrocks/pull/9693)
- マテリアライズドビューの次の側面を最適化しました:
  - 非同期マテリアライズドビューは、SPJG 型のマテリアライズドビューに基づいて自動的かつ透過的にクエリを書き換えることをサポートします。詳細については、[マテリアライズドビュー](https://docs.starrocks.io/docs/using_starrocks/Materialized_view#rewrite-and-accelerate-queries-with-the-asynchronous-materialized-view) を参照してください。 [#13193](https://github.com/StarRocks/starrocks/issues/13193)
  - 非同期マテリアライズドビューは、複数の非同期リフレッシュメカニズムをサポートします。詳細については、[マテリアライズドビュー](https://docs.starrocks.io/docs/using_starrocks/Materialized_view#manually-refresh-an-asynchronous-materialized-view) を参照してください。 [#12712](https://github.com/StarRocks/starrocks/pull/12712) [#13171](https://github.com/StarRocks/starrocks/pull/13171) [#13229](https://github.com/StarRocks/starrocks/pull/13229) [#12926](https://github.com/StarRocks/starrocks/pull/12926)
  - マテリアライズドビューのリフレッシュ効率が向上しました。 [#13167](https://github.com/StarRocks/starrocks/issues/13167)
- データロードの次の側面を最適化しました:
  - マルチレプリカシナリオでのロードパフォーマンスを最適化し、「シングルリーダーレプリケーション」モードをサポートします。データロードは1倍のパフォーマンス向上を達成します。「シングルリーダーレプリケーション」についての詳細は、[CREATE TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/) の `replicated_storage` を参照してください。 [#10138](https://github.com/StarRocks/starrocks/pull/10138)
  - Broker Load および Spark Load は、1つの HDFS クラスターまたは1つの Kerberos ユーザーが構成されている場合、データロードにブローカーを依存する必要がなくなりました。ただし、複数の HDFS クラスターまたは複数の Kerberos ユーザーがある場合は、ブローカーをデプロイする必要があります。詳細については、[HDFS またはクラウドストレージからデータをロードする](https://docs.starrocks.io/docs/loading/BrokerLoad/) および [Apache Spark™ を使用してバルクロードする](https://docs.starrocks.io/docs/loading/SparkLoad/) を参照してください。 [#9049](https://github.com/starrocks/starrocks/pull/9049) [#9228](https://github.com/StarRocks/starrocks/pull/9228)
  - 小さな ORC ファイルが大量にロードされる場合の Broker Load のパフォーマンスを最適化しました。 [#11380](https://github.com/StarRocks/starrocks/pull/11380)
  - 主キーテーブルにデータをロードする際のメモリ使用量を削減しました。
- `information_schema` データベースおよびその中の `tables` および `columns` テーブルを最適化しました。新しいテーブル `table_config` を追加しました。詳細については、[Information Schema](https://docs.starrocks.io/docs/reference/overview-pages/information_schema/) を参照してください。 [#10033](https://github.com/StarRocks/starrocks/pull/10033)
- データのバックアップと復元を最適化しました:
  - データベース内の複数のテーブルから一度にデータをバックアップおよび復元することをサポートします。詳細については、[データのバックアップと復元](https://docs.starrocks.io/docs/administration/Backup_and_restore/) を参照してください。 [#11619](https://github.com/StarRocks/starrocks/issues/11619)
  - 主キーテーブルからのデータのバックアップと復元をサポートします。詳細については、バックアップと復元を参照してください。 [#11885](https://github.com/StarRocks/starrocks/pull/11885)
- 次の関数を最適化しました:
  - [time_slice](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/time_slice/) 関数にオプションのパラメータを追加しました。このパラメータは、時間間隔の開始または終了を返すかどうかを決定するために使用されます。 [#11216](https://github.com/StarRocks/starrocks/pull/11216)
  - [window_funnel](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/window_funnel/) 関数に新しいモード `INCREASE` を追加し、重複したタイムスタンプの計算を回避します。 [#10134](https://github.com/StarRocks/starrocks/pull/10134)
  - [unnest](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/unnest/) 関数で複数の引数を指定することをサポートします。 [#12484](https://github.com/StarRocks/starrocks/pull/12484)
  - lead() および lag() 関数は HLL および BITMAP データのクエリをサポートします。詳細については、[ウィンドウ関数](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/) を参照してください。 [#12108](https://github.com/StarRocks/starrocks/pull/12108)
  - 次の ARRAY 関数は JSON データのクエリをサポートします: [array_agg](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_agg/)、[array_sort](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_sort/)、[array_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_concat/)、[array_slice](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_slice/)、および [reverse](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/reverse/)。 [#13155](https://github.com/StarRocks/starrocks/pull/13155)
  - 一部の関数の使用を最適化しました。`current_date`、`current_timestamp`、`current_time`、`localtimestamp`、および `localtime` 関数は `()` を使用せずに実行できます。たとえば、`select current_date;` を直接実行できます。 [# 14319](https://github.com/StarRocks/starrocks/pull/14319)
- FE ログから一部の冗長な情報を削除しました。 [# 15374](https://github.com/StarRocks/starrocks/pull/15374)

### バグ修正

以下のバグを修正しました:

- append_trailing_char_if_absent() 関数が最初の引数が空の場合に不正確な結果を返すことがあります。 [#13762](https://github.com/StarRocks/starrocks/pull/13762)
- RECOVER ステートメントを使用してテーブルを復元した後、テーブルが存在しません。 [#13921](https://github.com/StarRocks/starrocks/pull/13921)
- SHOW CREATE MATERIALIZED VIEW ステートメントによって返される結果に、マテリアライズドビューが作成されたときにクエリステートメントで指定されたデータベースおよびカタログが含まれていません。 [#12833](https://github.com/StarRocks/starrocks/pull/12833)
- `waiting_stable` 状態のスキーマ変更ジョブをキャンセルできません。 [#12530](https://github.com/StarRocks/starrocks/pull/12530)
- Leader FE と非 Leader FE で `SHOW PROC '/statistic';` コマンドを実行すると異なる結果が返されます。 [#12491](https://github.com/StarRocks/starrocks/issues/12491)
- SHOW CREATE TABLE によって返される ORDER BY 句の位置が不正確です。 [# 13809](https://github.com/StarRocks/starrocks/pull/13809)
- ユーザーが Hive カタログを使用して Hive データをクエリする際、FE によって生成された実行プランにパーティション ID が含まれていない場合、BE は Hive パーティションデータをクエリできません。 [# 15486](https://github.com/StarRocks/starrocks/pull/15486).

### 動作の変更

- `AWS_EC2_METADATA_DISABLED` パラメータのデフォルト値を `False` に変更しました。これにより、AWS リソースにアクセスするために Amazon EC2 のメタデータが取得されます。
- セッション変数 `is_report_success` を `enable_profile` に名前を変更し、SHOW VARIABLES ステートメントを使用してクエリできます。
- 4つの予約キーワードを追加しました: `CURRENT_DATE`、`CURRENT_TIME`、`LOCALTIME`、および `LOCALTIMESTAMP`。 [# 14319](https://github.com/StarRocks/starrocks/pull/14319)
- テーブルおよびデータベース名の最大長は最大 1023 文字です。 [# 14929](https://github.com/StarRocks/starrocks/pull/14929) [# 15020](https://github.com/StarRocks/starrocks/pull/15020)
- BE の設定項目 `enable_event_based_compaction_framework` および `enable_size_tiered_compaction_strategy` はデフォルトで `true` に設定されており、大量の tablet がある場合や単一の tablet に大きなデータ量がある場合のコンパクションオーバーヘッドを大幅に削減します。

### アップグレードノート

- クラスターを 2.5.0 にアップグレードするには、2.0.x、2.1.x、2.2.x、2.3.x、または 2.4.x からアップグレードできます。ただし、ロールバックを行う必要がある場合は、2.4.x のみにロールバックすることをお勧めします。