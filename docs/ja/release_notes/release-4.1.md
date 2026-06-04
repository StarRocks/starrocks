---
displayed_sidebar: docs
---

# StarRocks version 4.1

:::danger

**コンテナイメージの問題 (v4.1.0)**

v4.1.0 のコンテナイメージにはロード順序が不安定な問題があり、コンテナ環境で BE プロセスが正常に起動しないことがあります。**コンテナ環境のユーザーは v4.1.0 にアップグレードしないでください。** 修正が含まれる v4.1.1 のリリースをお待ちください（[#71825](https://github.com/StarRocks/starrocks/pull/71825)）。

:::

:::warning

**ダウングレードに関する注意事項**

- StarRocks を v4.1 にアップグレードした後、v4.0.6 未満の v4.0 バージョンにはダウングレードしないでください。

  v4.1 で導入されたデータレイアウトの内部変更（タブレットの分割および分散メカニズムに関連）により、v4.1 にアップグレードされたクラスターは、以前のバージョンと完全に互換性のないメタデータおよびストレージ構造を生成する可能性があります。その結果、v4.1 からのダウングレードは v4.0.6 以降のみサポートされます。v4.0.6 より前のバージョンへのダウングレードはサポートされていません。この制限は、以前のバージョンがタブレットのレイアウトと分散メタデータを解釈する方法における後方互換性の制約によるものです。

:::

## 4.1.1

リリース日: 2026年5月29日

### 動作の変更

- Hive Connector は、従来の JNI Avro Scanner に代わり、デフォルトでネイティブ C++ Avro Scanner を使用するようになりました。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- INCREMENTAL/AUTO マテリアライズドビューに対するクエリリライトが無効化され、INCREMENTAL/AUTO マテリアライズドビューに対する FORCE リフレッシュおよびパーティションリフレッシュは拒否されるようになりました。[#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### 改善点

- Java UDF/UDAF/UDTF がより多くの型をサポートするようになりました。UDAF/UDTF は STRUCT 型の引数および戻り値をサポートし、ネストされた ARRAY/MAP 型、DATE/DATETIME、DECIMAL、および可変長引数（varargs）をサポートします。[#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- スカラー UDF が STRUCT 型の引数をサポートするようになりました。[#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDF がネストされた ARRAY/MAP 型をサポートするようになりました。[#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAF はロードと初期化が一度のみ行われ、複数のクエリ間で再利用されるようになり、クエリごとのオーバーヘッドが削減されました。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- Hive Connector において JNI Avro Scanner をネイティブ C++ Scanner に置き換え、直接バイナリデコードに対応し、`avro.schema.literal` および `avro.schema.url` をサポートしました。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- CTAS ステートメントにおける Trino の `WITH` 句をサポートしました。[#71960](https://github.com/StarRocks/starrocks/pull/71960)
- Sink パスにおける Iceberg `timestamptz` パーティション変換のサポートを完成させました。[#73397](https://github.com/StarRocks/starrocks/pull/73397)
- Iceberg テーブルの集約に対する TopN ランタイムフィルタのプッシュダウンを有効化しました。[#72332](https://github.com/StarRocks/starrocks/pull/72332)
- Iceberg の datetime min/max 最適化をサポートしました。[#71870](https://github.com/StarRocks/starrocks/pull/71870)
- 複数の HDFS クラスターへのアクセスを可能にするため、Catalog および BE での HDFS HA 設定のパススルーを許可しました。[#71521](https://github.com/StarRocks/starrocks/pull/71521)
- 外部テーブルクエリにパーティションスキャン数の上限を追加しました。[#68480](https://github.com/StarRocks/starrocks/pull/68480)
- サポートされていない Iceberg V3 機能に対して fail-fast を行うようにしました。[#70242](https://github.com/StarRocks/starrocks/pull/70242)
- INSERT INTO FILES による CSV エクスポートで `csv.enclose` および `csv.escape` をサポートしました。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- 完全なスキーマを `files()` にプッシュダウンするための INSERT プロパティ `enable_push_down_schema` を追加しました。[#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load ジョブは、リトライ不可能なエラー（主キーサイズ超過など）が発生した場合に一時停止するようになりました。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- 2 つの子ノードからの複雑な式に対する Join Reorder をサポートしました。[#71615](https://github.com/StarRocks/starrocks/pull/71615)
- `date_trunc`、`array_map`、CASE WHEN、IS NULL、UNION、定数に対する MCV/NULL 率の伝播を含む、CBO 統計推定を改善しました。[#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- スキュー Join の検出を改善しました。すべての Join Key がスキューしている場合のみスキューと判定し、スキュールールを強制適用するための `force_group_by_skew_eliminate_when_skewed` スイッチを追加しました。[#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- FE 側で `regexp_replace` の定数畳み込みをサポートしました。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 定数パーティション値を持つ日付パーティション列に対する MIN/MAX を最適化しました。[#69880](https://github.com/StarRocks/starrocks/pull/69880)
- マテリアライズドビューのリフレッシュにおいて、`ASYNC` の同義語として `SCHEDULE` キーワードを導入しました。[#72329](https://github.com/StarRocks/starrocks/pull/72329)
- 共有データモードにおける Lake テーブルのタブレット作成リトライをサポートしました。[#71068](https://github.com/StarRocks/starrocks/pull/71068)
- Lake の列モード部分更新における条件付き更新をサポートしました。[#71961](https://github.com/StarRocks/starrocks/pull/71961)
- 部分更新の Publish、永続インデックスの初期化、SSTable のオープンを並列化し、取り込みスループットを向上させました。[#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- 共有なしから共有データへのレプリケーション中の DCG ファイル同期をサポートしました。[#69339](https://github.com/StarRocks/starrocks/pull/69339)
- Key 列および非 Key 列の VARCHAR 長拡張に対するスキーマ進化をサポートしました。[#70747](https://github.com/StarRocks/starrocks/pull/70747)
- クラスタースナップショットの整合性チェック用に `snapshot_meta.json` マーカーを追加しました。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- DN パターンによる LDAP ダイレクトバインド認証をサポートしました。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- クエリのトラブルシューティングを容易にする `get_query_dump_from_query_id` メタ関数を追加しました。[#72875](https://github.com/StarRocks/starrocks/pull/72875)
- 監査ログでクエリ対象のリレーションを監査できるようにしました。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- MySQL バイナリ結果エンコーディング用のセッション変数を追加しました。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 共有データクラスターの `tablet_num`、`MemtableIOSpeed`、`staros_shard_count`、Iceberg メタデータテーブルのクエリメトリクスなど、可観測性向上のためのメトリクスを追加しました。[#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- FE 設定項目 `deploy_serialization_min_thread_pool_size` を追加しました。[#72274](https://github.com/StarRocks/starrocks/pull/72274)
- MergeTabletJob の作成を無効化するための設定項目 `tablet_reshard_enable_tablet_merge` を追加しました。[#70906](https://github.com/StarRocks/starrocks/pull/70906)
- `SO_REUSEPORT` により HTTP サーバーの accept における thundering-herd を解消しました。[#72956](https://github.com/StarRocks/starrocks/pull/72956)

### セキュリティ

- [CVE] Netty を 4.1.133.Final にアップグレードしました。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] pgjdbc を 42.7.11 に（SCRAM PBKDF2 反復回数の上限欠如によるクライアント側 DoS を修正）、BouncyCastle を 1.84 に（FrodoKEM の秘密鍵漏洩を修正）アップグレードしました。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- [CVE-2026-32280] [CVE-2026-32282] Golang の CVE を解消するため、go1.25.9 で pprof をビルドしました。[#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- jetty-http を 9.4.58.v20250814 にアップグレードしました。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- Broker 依存関係の CVE をクリーンアップし、`wildfly-openssl` を削除しました。[#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- INSERT INTO FILES のエラーメッセージ内の認証情報をマスクしました。[#71245](https://github.com/StarRocks/starrocks/pull/71245)

### バグ修正

以下の問題を修正しました。

- `hash_util` の静的初期化順序に起因する CN 起動時のセグフォルトを修正しました。[#71825](https://github.com/StarRocks/starrocks/pull/71825)
- 物理分割が有効な状態で空のタブレットをスキャンした際の CN クラッシュを修正しました。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- `information_schema.warehouse_queries` のクエリ時の BE クラッシュを修正しました。[#72019](https://github.com/StarRocks/starrocks/pull/72019)
- Lake Compaction で rowset の `num_rows` が 0 の場合の SIGFPE を修正しました。[#71742](https://github.com/StarRocks/starrocks/pull/71742)
- ExecutionDAG フラグメント接続におけるゼロ除算を修正しました。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- SinkBuffer におけるグレースフルイグジット時のクラッシュを修正しました。[#73202](https://github.com/StarRocks/starrocks/pull/73202)
- スピル可能な Hash Join Probe のクラッシュを修正しました。[#72397](https://github.com/StarRocks/starrocks/pull/72397)
- 一時的な `std::string` へのフォーマット時のスタックバッファオーバーフローを修正しました。[#72728](https://github.com/StarRocks/starrocks/pull/72728)
- `reverse(DecimalV3)` のクラッシュを修正しました。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- 一時的な `shared_ptr` の破棄に起因する `LoadChannel::get_load_replica_status` の Use-after-free を修正しました。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- スレッド作成失敗時の `ThreadPool::do_submit` の Use-after-free を修正しました。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- フラグメント破棄時の Hive パーティション記述子の Use-after-free を修正しました。[#73176](https://github.com/StarRocks/starrocks/pull/73176)
- Information Schema Sink の Use-after-free を修正しました。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- HttpClient インスタンスを再利用することで FE のファイルディスクリプタリークを修正しました。[#73239](https://github.com/StarRocks/starrocks/pull/73239)
- `JDBCScanner::_init_jdbc_scanner` の JNI ローカル参照リークを修正しました。[#72913](https://github.com/StarRocks/starrocks/pull/72913)
- MV プランコンテキストのキャッシュ時のメモリリークを修正しました。[#72300](https://github.com/StarRocks/starrocks/pull/72300)
- Local Exchange における想定外のメモリ過剰使用を修正しました。[#72262](https://github.com/StarRocks/starrocks/pull/72262)
- Lake `publish_version` の `response->tablet_metas` における競合を修正しました。[#73274](https://github.com/StarRocks/starrocks/pull/73274)
- `DeltaWriter::commit()` における並行 `SegmentFlushTask` の競合を修正しました。[#73371](https://github.com/StarRocks/starrocks/pull/73371)
- シリアライズ中の `RuntimeProfile` min/max の競合を修正しました。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- クエリコンテキスト破棄中の `PipelineTimerTask` の競合を修正しました。[#73082](https://github.com/StarRocks/starrocks/pull/73082)
- `_all_global_rf_ready_or_timeout` における競合を修正しました。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- `map_apply` および `array_length` における共有 `NullColumn` の問題を修正しました。[#71258](https://github.com/StarRocks/starrocks/pull/71258)
- パーティションバージョンのギャップに起因するバッチ Publish のデッドロックを修正しました。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- 共有なしモードで rowset メタデータの LRU キャッシュをウォームアップする際のデッドロックを修正しました。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `Locker` のロールバックを例外安全にし、アンロック順序を修正しました。[#72789](https://github.com/StarRocks/starrocks/pull/72789)
- DDL や StarOS RPC とのロック競合を軽減するため、複数の読み取り専用パスおよびメタデータパスの DB ロックを緩和しました。[#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- Project ノードの欠落に起因する誤ったシャッフル分散を修正しました。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- AGG TopN ランタイムフィルタの `exprOrder` 不一致に起因するクラッシュおよび誤った結果を修正しました。[#71479](https://github.com/StarRocks/starrocks/pull/71479)
- dict-merge GROUP BY の誤った結果を修正しました。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- Query Cache と Local Shuffle 集約の競合を修正しました。[#73194](https://github.com/StarRocks/starrocks/pull/73194)
- Flat JSON におけるグローバル辞書生成の不整合を修正しました。[#72953](https://github.com/StarRocks/starrocks/pull/72953)
- Flat JSON Merge の空値不整合を修正しました。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- Key/Value 型を明示的に宣言した場合の Map リテラルの型不一致を修正しました。[#71316](https://github.com/StarRocks/starrocks/pull/71316)
- JOIN USING トランスフォーマで COALESCE の子を共通型にキャストするようにしました。[#72338](https://github.com/StarRocks/starrocks/pull/72338)
- グローバル変数を伴う reduce-cast 後の VARCHAR 長が保持されない問題を修正しました。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- MySQL 結果セットのネスト型内における VARBINARY エンコーディングを修正しました。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- 小さい LIMIT で集約スピルを無効化する際の HAVING 句チェックの問題を修正しました。[#72705](https://github.com/StarRocks/starrocks/pull/72705)
- 日付解析前に引用符を除去し、PostgreSQL の日付/時刻の問題を修正しました。[#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)
- Range 分散タブレットのデータファイル削除をスキップすることで、タブレット分割後のデータ損失を修正しました。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- データファイルの共有フラグの損失により、兄弟分割タブレットから参照されているファイルが Vacuum で削除される問題を修正しました。[#71585](https://github.com/StarRocks/starrocks/pull/71585)
- split→compaction→merge のシーケンスにおけるタブレットマージの正確性を修正しました。[#72350](https://github.com/StarRocks/starrocks/pull/72350)
- タブレット分割中の cross-published txn log の num_rows/data_size の膨張を修正しました。[#71144](https://github.com/StarRocks/starrocks/pull/71144)
- 同一 Publish バッチ内の write-before-compaction に起因する delvec の孤立エントリを修正しました。[#71001](https://github.com/StarRocks/starrocks/pull/71001)
- StarMgr ジャーナルのリプレイを同期することで、Follower FE における "no queryable replica" の問題を修正しました。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 通常の rowset commit を適用する際に `merge_condition` を保持するようにしました。[#72542](https://github.com/StarRocks/starrocks/pull/72542)
- Iceberg DELETE の競合検出が誤ったスナップショット ID およびフィルタを使用する問題を修正しました。[#73354](https://github.com/StarRocks/starrocks/pull/73354)
- 無効な Iceberg Transform 引数に起因する NPE を修正しました。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- Planner によって挿入された追加列のために Iceberg min/max 最適化がスキップされる問題を修正しました。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- Iceberg ベーステーブルに対する aggregate-join-pushdown の MV リライトを修正しました。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- INSERT OVERWRITE commit の前に、欠落している Hive パーティションディレクトリを作成するようにしました。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- JNI Scanner に AWS assume-role が適用されない問題を修正しました。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- 剪定された子およびネストされた nullable スキーマに対する Avro 複合型のデコードを修正しました。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- Parquet Broker Load のエラーにファイル/列/行のコンテキスト情報を追加しました。[#73236](https://github.com/StarRocks/starrocks/pull/73236)
- Parquet Scanner が Arrow Dictionary 値をサポートするようにしました。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- SHOW CREATE および DESC ステートメントで Paimon テーブルの主キーを表示するようにしました。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- PostgreSQL/Oracle JDBC の型互換性、および末尾スラッシュを伴う JDBC URL の構築を修正しました。[#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)
- JDBC Catalog 内の SQL Server テーブルに対する MV リフレッシュを修正しました。[#72962](https://github.com/StarRocks/starrocks/pull/72962)
- Outer Join 上の MV における遅延マテリアライゼーションのスロット nullability を修正しました。[#72621](https://github.com/StarRocks/starrocks/pull/72621)
- AUTO および INCREMENTAL MV のパーティションリフレッシュを拒否するようにしました。[#71355](https://github.com/StarRocks/starrocks/pull/71355)
- マテリアライズドビューが inactive になった後、MV スケジューラを停止するようにしました。[#71265](https://github.com/StarRocks/starrocks/pull/71265)
- MySQL クライアント互換性のため `SHOW GRANTS FOR CURRENT_USER()` をサポートしました。[#71959](https://github.com/StarRocks/starrocks/pull/71959)
- 明示的なトランザクション内での SHOW ステートメントの実行を許可しました。[#72954](https://github.com/StarRocks/starrocks/pull/72954)
- 空の結果セットの場合に Arrow Flight が列名 `r` を返す問題を修正しました。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Java UDF コードに JNI 例外処理チェックを追加しました。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `ai_query` 関数の登録を修正しました。[#72103](https://github.com/StarRocks/starrocks/pull/72103)
- `enable_load_profile` 使用時の Stream Load プロファイル収集を修正しました。[#71952](https://github.com/StarRocks/starrocks/pull/71952)
- プロファイルの START_TIME/END_TIME をセッションのタイムゾーンで表示するようにしました。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `star_mgr_meta_sync_interval_sec` を実行時に変更可能にしました。[#71675](https://github.com/StarRocks/starrocks/pull/71675)
- `information_schema.tables` が等値述語で特殊文字をエスケープしない問題を修正しました。[#71273](https://github.com/StarRocks/starrocks/pull/71273)

## 4.1.0

リリース日: 2026年4月13日

### 共有データアーキテクチャ

- **新しいマルチテナントデータ管理**

  共有データクラスターは、範囲ベースのデータ分散とタブレットの自動分割およびマージをサポートするようになりました。タブレットは、スキーマ変更、SQL変更、またはデータ再取り込みを必要とせずに、サイズが大きくなりすぎたりホットスポットになったりした場合に自動的に分割できます。この機能は、マルチテナントワークロードにおけるデータスキューとホットスポットの問題に直接対処することで、使いやすさを大幅に向上させることができます。[#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量タブレットサポート（フェーズ1）**

  これにより、共有データクラスタは1つのタブレットあたり大幅に多くのデータを格納できるようになり、長期的な目標として1タブレットあたり100 GBを目指しています。フェーズ1では、データ取り込み、主キーの更新、およびコンパクションのパイプライン全体にわたるタブレット内並列処理を導入するため、単一のLakeタブレットがサイズ拡大に伴いシングルスレッドのボトルネックになることはなくなります。改善点としては、単一タブレット内での並列コンパクション（セグメントレベルの分割を含む）、Lakeロード（ロード・スピル・パスを含む）のための並列MemTableのファイナライズ、フラッシュ、マージ、プライマリキーテーブルに対するタブレット内部の並列パブリッシュおよび並列コンディション更新、そしてリモートストレージマッパーファイルのサポートを備えたクラウドネイティブのプライマリキーインデックス向けの範囲分割／並列／サイズ階層型コンパクションなどが挙げられます。これらの変更により、大規模タブレットワークロードにおけるデータ取り込み時のメモリオーバーヘッド、コンパクションの増幅効果、およびFEメタデータへの負荷が大幅に軽減されます。[#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **高速スキーマ進化 V2**

  共有データクラスターは、スキーマ操作の秒単位のDDL実行を可能にし、マテリアライズドビューにもサポートを拡張する高速スキーマ進化 V2 をサポートするようになりました。[#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **[ベータ版] 共有データ上の転置インデックス**

  共有データクラスター向けに組み込みの転置インデックスを有効にし、テキストフィルタリングと全文検索ワークロードを高速化します。[#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **キャッシュの可観測性**

  クエリレベルのキャッシュヒット率が監査ログと監視システムで公開されるようになり、キャッシュの透明性とレイテンシー診断が向上しました。追加のデータキャッシュメトリクスには、メモリとディスクのクォータ使用状況、およびページキャッシュ統計が含まれます。[#63964](https://github.com/StarRocks/starrocks/pull/63964)

- Lakeテーブルにセグメントメタデータフィルターを追加し、スキャン中にソートキー範囲に基づいて関連性のないセグメントをスキップすることで、範囲述語クエリのI/Oを削減します。[#68124](https://github.com/StarRocks/starrocks/pull/68124)
- Lake DeltaWriterの高速キャンセルをサポートし、共有データクラスターにおけるキャンセルされた取り込みジョブのレイテンシーを削減します。[#68877](https://github.com/StarRocks/starrocks/pull/68877)
- 自動クラスター・スナップショットの間隔ベースのスケジューリングをサポートしました。[#67525](https://github.com/StarRocks/starrocks/pull/67525)
- MemTableのフラッシュとマージのパイプライン実行をサポートし、共有データクラスターにおけるクラウドネイティブテーブルの取り込みスループットを向上させます。[#67878](https://github.com/StarRocks/starrocks/pull/67878)
- クラウドネイティブテーブルの修復のための`dry_run`モードをサポートし、ユーザーが実行前に修復アクションをプレビューできるようにします。[#68494](https://github.com/StarRocks/starrocks/pull/68494)
- 共有なしクラスターにおけるパブリッシュトランザクション用のスレッドプールを追加し、パブリッシュスループットを向上させました。[#67797](https://github.com/StarRocks/starrocks/pull/67797)
- クラウドネイティブテーブルの`datacache.enable`プロパティの動的な変更をサポートします。[#69011](https://github.com/StarRocks/starrocks/pull/69011)

### データレイク分析

- **Iceberg DELETEのサポート**

  Icebergテーブルの位置削除ファイルの書き込みをサポートし、StarRocksから直接Icebergテーブルに対するDELETE操作を可能にします。このサポートは、Plan、Sink、Commit、Auditの完全なパイプラインをカバーします。[#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **HiveおよびIcebergテーブルのTRUNCATE**

  外部HiveおよびIcebergテーブルに対するTRUNCATE TABLEをサポートします。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Iceberg上の増分マテリアライズドビュー**

  増分マテリアライズドビューのリフレッシュのサポートをIcebergの追記専用テーブルに拡張し、テーブル全体の再読み込みなしでクエリの高速化を可能にします。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **Icebergにおける半構造化データのためのVARIANT型**

  IcebergカタログにおけるVARIANTデータ型をサポートし、半構造化データの柔軟なスキーマオンリードストレージとクエリを可能にします。読み取り、書き込み、型キャスト、Parquet統合をサポートします。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3のサポート**

  Iceberg v3のデフォルト値機能と行リネージのサポートを追加しました。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Icebergテーブルのメンテナンス手順**

  `rewrite_manifests`プロシージャのサポートを追加し、`expire_snapshots`および`remove_orphan_files`プロシージャを、よりきめ細やかなテーブルメンテナンスのための追加引数で拡張しました。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- **Iceberg `$properties` メタデータテーブル**

  `$properties` メタデータテーブルを介した Iceberg テーブルプロパティのクエリのサポートを追加しました。[#68504](https://github.com/StarRocks/starrocks/pull/68504)

- Iceberg テーブルからのファイルパスと行位置メタデータ列の読み取りをサポートします。[#67003](https://github.com/StarRocks/starrocks/pull/67003)
- Iceberg v3 テーブルからの `_row_id` の読み取りをサポートし、Iceberg v3 のグローバル遅延具現化をサポートします。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)
- カスタムプロパティを持つ Iceberg ビューの作成をサポートし、SHOW CREATE VIEW 出力にプロパティを表示します。[#65938](https://github.com/StarRocks/starrocks/pull/65938)
- 特定のブランチ、タグ、バージョン、またはタイムスタンプを持つ Paimon テーブルのクエリをサポートします。[#63316](https://github.com/StarRocks/starrocks/pull/63316)
- Paimon テーブルの複合型 (ARRAY, MAP, STRUCT) をサポートします。[#66784](https://github.com/StarRocks/starrocks/pull/66784)
- Paimon ビューをサポートします。[#56058](https://github.com/StarRocks/starrocks/pull/56058)
- Paimon テーブルの TRUNCATE をサポートします。[#67559](https://github.com/StarRocks/starrocks/pull/67559)
- Iceberg テーブル作成時の括弧構文によるパーティション変換をサポートします。[#68945](https://github.com/StarRocks/starrocks/pull/68945)
- Iceberg テーブルの ALTER TABLE REPLACE PARTITION COLUMN をサポートします。[#70508](https://github.com/StarRocks/starrocks/pull/70508)
- データ編成を改善するために、Transform Partition に基づく Iceberg グローバルシャッフルをサポートします。[#70009](https://github.com/StarRocks/starrocks/pull/70009)
- Iceberg テーブルシンクのグローバルシャッフルを動的に有効にすることをサポートします。[#67442](https://github.com/StarRocks/starrocks/pull/67442)
- 同時コミットの競合を避けるために、Iceberg テーブルシンクにコミットキューを導入しました。[#68084](https://github.com/StarRocks/starrocks/pull/68084)
- データ編成と読み取りパフォーマンスを向上させるために、Iceberg テーブルシンクにホストレベルのソートを追加しました。[#68121](https://github.com/StarRocks/starrocks/pull/68121)
- ETL 実行モードで追加の最適化をデフォルトで有効にし、明示的な設定なしで INSERT INTO SELECT、CREATE TABLE AS SELECT、および同様のバッチ操作のパフォーマンスを向上させました。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- Iceberg テーブルでの INSERT および DELETE 操作のコミット監査情報を追加しました。[#69198](https://github.com/StarRocks/starrocks/pull/69198)
- Iceberg REST Catalog でのビューエンドポイント操作の有効化または無効化をサポートします。[#66083](https://github.com/StarRocks/starrocks/pull/66083)
- CachingIcebergCatalog でのキャッシュルックアップ効率を最適化しました。[#66388](https://github.com/StarRocks/starrocks/pull/66388)
- さまざまな Iceberg カタログタイプでの EXPLAIN をサポートします。[#66563](https://github.com/StarRocks/starrocks/pull/66563)
- AWS Glue Catalog テーブル内のテーブルのパーティションプロジェクションをサポートします。[#67601](https://github.com/StarRocks/starrocks/pull/67601)
- AWS Glue `GetDatabases` API のリソース共有タイプサポートを追加しました。[#69056](https://github.com/StarRocks/starrocks/pull/69056)
- エンドポイントインジェクション (`azblob`/`adls2`) を使用した Azure ABFS/WASB パスマッピングをサポートします。[#67847](https://github.com/StarRocks/starrocks/pull/67847)
- リモートRPCのオーバーヘッドと外部システム障害の影響を軽減するため、JDBCカタログにデータベースメタデータキャッシュを追加しました。[#68256](https://github.com/StarRocks/starrocks/pull/68256)
- カスタムスキーマ解決をサポートするため、JDBCカタログに `schema_resolver` プロパティを追加しました。[#68682](https://github.com/StarRocks/starrocks/pull/68682)
- `information_schema` のPostgreSQLテーブルの列コメントをサポートします。[#70520](https://github.com/StarRocks/starrocks/pull/70520)
- OracleおよびPostgreSQLのJDBC型マッピングを改善しました。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### クエリエンジン

- **再帰的CTE**

  階層トラバーサル、グラフクエリ、反復SQL計算のための再帰的共通テーブル式 (CTE) をサポートします。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 統計ベースのスキュー検出、ヒストグラムサポート、NULLスキュー認識により、Skew Join v2のリライトを改善しました。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)
- ウィンドウに対するCOUNT DISTINCTを改善し、結合された複数DISTINCT集計のサポートを追加しました。[#67453](https://github.com/StarRocks/starrocks/pull/67453)
- ウィンドウ関数に対する明示的なスキューヒントをサポートし、スキューしたパーティションキーを持つウィンドウ関数をUNIONに分割することで自動最適化します。[#68739](https://github.com/StarRocks/starrocks/pull/68739) [#67944](https://github.com/StarRocks/starrocks/pull/67944)
- CTEの具現化ヒントをサポートします。[#70802](https://github.com/StarRocks/starrocks/pull/70802)
- グローバル遅延具現化をデフォルトで有効にし、必要な時まで列の読み取りを延期することでクエリパフォーマンスを向上させました。[#70412](https://github.com/StarRocks/starrocks/pull/70412)
- TrinoパーサーにおけるINSERTステートメントのEXPLAINおよびEXPLAIN ANALYZEをサポートします。[#70174](https://github.com/StarRocks/starrocks/pull/70174)
- クエリキューの可視性のためのEXPLAINをサポートします。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 関数とSQL構文

- 以下の関数を追加しました:
  - `array_top_n`: 値でランク付けされた配列の上位N要素を返します。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`: 複数の配列を要素ごとに結合し、構造体の配列にします。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`: JSON文字列をインデントでフォーマットします。 [#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`: JSON文字列内の指定されたパスに値を設定します。 [#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`: 各単語の最初の文字を大文字に変換します。 [#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`: 同じキーを持つ行のMAP値を合計します。 [#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`: 現在のセッションのタイムゾーンを返します。 [#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`: 現在のウェアハウスの名前を返します。 [#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`: 秒数をTIME値に変換します。 [#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`: 推論ワークロードのためにSQLから外部AIモデルを呼び出します。 [#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `min_n` / `max_n`: 上位N個の最小/最大値を返す集計関数。 [#63807](https://github.com/StarRocks/starrocks/pull/63807)
  - `regexp_position`: 文字列内の正規表現の一致の位置を返します。 [#67252](https://github.com/StarRocks/starrocks/pull/67252)
  - `is_json_scalar`: JSON値がスカラーであるかどうかを返します。 [#66050](https://github.com/StarRocks/starrocks/pull/66050)
  - `get_json_scalar`: JSON文字列からスカラー値を抽出します。 [#68815](https://github.com/StarRocks/starrocks/pull/68815)
  - `raise_error`: SQL式でユーザー定義エラーを発生させます。 [#69661](https://github.com/StarRocks/starrocks/pull/69661)
  - `uuid_v7`: 時間順のUUID v7値を生成します。 [#67694](https://github.com/StarRocks/starrocks/pull/67694)
  - `STRING_AGG`: GROUP_CONCATの糖衣構文。 [#64704](https://github.com/StarRocks/starrocks/pull/64704)
- 以下の関数または構文拡張を提供します:
  - `array_sort`でカスタムソート順序のためのラムダコンパレータをサポートします。 [#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - SQL標準セマンティクスを持つFULL OUTER JOINのUSING句をサポートします。 [#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - ORDER BY/PARTITION BYを持つフレーム化されたウィンドウ関数でのDISTINCT集計をサポートします。 [#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - `lead`/`lag`/`first_value`/`last_value`ウィンドウ関数でARRAY型をサポートします。 [#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - count distinctのような集計関数でVARBINARYをサポートします。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - インターバル操作で`MULTIPLY`/`DIVIDE`をサポートします。[#68407](https://github.com/StarRocks/starrocks/pull/68407)
  - IN式での日付型と文字列型のキャストをサポートします。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - BEGIN/START TRANSACTIONでWITH LABEL構文をサポートします。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - SHOWステートメントでのWHERE/ORDER/LIMIT句をサポートします。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - タスク管理のための`ALTER TASK`ステートメントをサポートします。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - `CREATE FUNCTION ... AS <sql_body>`を介したSQL UDFの作成をサポートします。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
  - S3からのUDFのロードをサポートします。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
  - Scala関数での名前付きパラメータをサポートします。[#66344](https://github.com/StarRocks/starrocks/pull/66344)
  - CSVファイルのエクスポートで複数の圧縮形式（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）をサポートします。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 名前ベースの構造体フィールドマッチングのための`STRUCT_CAST_BY_NAME` SQLモードをサポートします。[#69845](https://github.com/StarRocks/starrocks/pull/69845)
  - 簡単なクエリプロファイル分析のために`ANALYZE PROFILE`で`last_query_id()`をサポートします。[#64557](https://github.com/StarRocks/starrocks/pull/64557)

### 管理と可観測性

- マルチウェアハウスのCPUリソース分離を改善するために、リソースグループの`warehouses`、`cpu_weight_percent`、および`exclusive_cpu_weight`属性をサポートします。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- FEスレッドの状態を検査するための`information_schema.fe_threads`システムビューを導入します。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- クラスタレベルで特定のクエリパターンをブロックするためのSQL Digest Blacklistをサポートします。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- ネットワークトポロジの制約によりアクセスできないノードからのArrow Flightデータ取得をサポートします。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 再接続せずにグローバル変数の変更を既存の接続に伝播するREFRESH CONNECTIONSコマンドを導入します。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- クエリプロファイルを分析し、フォーマットされたSQLを表示するための組み込みUI機能を追加し、クエリチューニングをより利用しやすくしました。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 構造化されたクラスタ概要を提供するために`ClusterSummaryActionV2` APIエンドポイントを実装します。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- グローバルな読み取り専用システム変数`@@run_mode`を追加し、現在のクラスター実行モード（shared-dataまたはshared-nothing）を照会できるようにしました。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- クエリキュー管理を改善するため、`query_queue_v2`をデフォルトで有効にしました。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- Stream LoadおよびMerge Commit操作において、ユーザーレベルのデフォルトウェアハウスをサポートしました。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 必要に応じてバックエンドのブラックリスト検証をバイパスするためのセッション変数`skip_black_list`を追加しました。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- メトリクスAPIに`enable_table_metrics_collect`オプションを追加しました。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- クエリ詳細HTTP APIにユーザーの偽装サポートを追加しました。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- `table_query_timeout`をテーブルレベルのプロパティとして追加しました。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- 設定可能なレイテンシしきい値を持つFEプロファイルロギングを追加しました。[#69396](https://github.com/StarRocks/starrocks/pull/69396)
- FEオブザーバーノードの追加をサポートしました。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- ロードジョブの可視性を向上させるため、`information_schema.loads`でのマージコミット情報をサポートしました。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- トラブルシューティングを改善するため、クラウドネイティブテーブルでのタブレットステータス表示をサポートしました。[#69616](https://github.com/StarRocks/starrocks/pull/69616)
- 外部カタログの可観測性を高めるため、カタログタイプごとのクエリメトリクスを追加しました。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- FEおよびBE向けにDebian (.deb) パッケージングサポートを追加しました。[#68821](https://github.com/StarRocks/starrocks/pull/68821)

### セキュリティ

- [CVE-2026-33870] [CVE-2026-33871] AWSバンドルを置き換え、Nettyを4.1.132.Finalに更新しました。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] Hadoopをv3.4.2にアップグレードしました。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] `spark-core_2.12`を3.5.7にアップグレードしました。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### バグ修正

以下の問題が修正されました:

- 範囲分散タブレットのデータファイル削除をスキップすることで、タブレット分割後のデータ損失を修正しました。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 複雑な型における`DefaultValueColumnIterator`のメモリリークを修正しました。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- `BatchUnit`と`FetchTaskContext`間の`shared_ptr`サイクルによって引き起こされるメモリリークを修正しました。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- エラーパスでの並列セグメント/rowsetロードにおけるuse-after-freeを修正しました。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 集計スピル`set_finishing`におけるハッシュテーブルのデータ損失の可能性を修正しました。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- 同時getlineアクセスによるSystemMetricsでのdouble-freeクラッシュを修正しました。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- eager mergeがすべてのブロックを消費した際のSpillMemTableSinkでのクラッシュを修正しました。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- 辞書バッキングテーブルが削除された際の`visitDictionaryGetExpr`におけるNPEを修正しました。[#71109](https://github.com/StarRocks/starrocks/pull/71109)
- 参照される列が欠落している場合に、Stream Load/Broker Loadで生成された列を分析する際のNPEを修正しました。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- TTLクリーナーによって自動作成されたパーティションが削除された際のNPEを修正しました。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- スナップショットが期限切れになった際の`IcebergCatalog.getPartitionLastUpdatedTime`におけるNPEを修正しました。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 定数側の列参照を持つ外部結合に対する不正確な述語書き換えを修正しました。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- ディスク再移行 (A→B→A) 中のGC競合によって引き起こされるPKタブレットrowsetメタ損失を修正しました。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- SharedDataStorageVolumeMgrにおけるDB読み取りロックリークを修正しました。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- 共有データでCHAR列の長さを変更した後の誤ったクエリ結果を修正しました。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 複数のテーブルの場合のMVリフレッシュバグを修正しました。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 強制リフレッシュされた場合のMVリサイクル時間の誤りを修正しました。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 同期MVにおけるすべてのnull値処理バグを修正しました。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 高速スキーマ変更ADD COLUMN後にMVをクエリする際の重複列IDエラーを修正しました。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- IVMリフレッシュが不完全なPCTパーティションメタデータを記録する問題を修正しました。[#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 共有DecodeInfoによって引き起こされる低カーディナリティ書き換えNPEを修正しました。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 低カーディナリティ結合述語の型不一致を修正しました。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- Parquetページインデックスフィルターで`null_counts`が空の場合のセグメンテーション違反を修正しました。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- JSONのフラット化における、同一パスでの配列とオブジェクトの競合を修正しました。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- Icebergキャッシュの重み付けの不正確さを修正しました。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- Icebergテーブルキャッシュのメモリ制限を修正しました。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- Icebergの削除列のNULL許容性の問題を修正しました。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- Azure ABFS/WASB FileSystemのキャッシュキーにコンテナを含めるように修正しました。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- HMS接続プールが満杯の際のデッドロックを修正しました。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- PaimonカタログにおけるVARCHARフィールドタイプの誤った長さを修正しました。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- Paimonカタログのリフレッシュ時にObjectTableでClassCastExceptionが発生してクラッシュする問題を修正しました。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- PaimonViewがPaimonカタログではなくdefault_catalogに対してテーブル参照を解決する問題を修正しました。[#70217](https://github.com/StarRocks/starrocks/pull/70217)
- 定数サブクエリを使用するFULL OUTER JOIN USINGを修正しました。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- CTEスコープでの結合ON句のバグを修正しました。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- ショートサーキットポイントルックアップにおけるパーティション述語の欠落を修正しました。[#71124](https://github.com/StarRocks/starrocks/pull/71124)
- bindScope()パターンを使用することでConnectContextのメモリリークを修正しました。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- 共有なしクラスターにおける`CatalogRecycleBin.asyncDeleteForTables`のメモリリークを修正しました。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- Thriftのacceptスレッドが例外に遭遇した際に終了する問題を修正しました。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- ルーチンロード列マッピングにおけるUDF解決を修正しました。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- `DROP FUNCTION IF EXISTS`が`ifExists`フラグを無視する問題を修正しました。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- 辞書ページが大きすぎる場合のスキャン結果エラーを修正しました。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- 範囲パーティションの重複を修正しました。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- クエリキューの割り当て時間と保留中のタイムアウトを修正しました。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- nullリテラル配列の処理時に発生する`array_map`クラッシュを修正しました。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- `to_base64`のスタックオーバーフローを修正しました。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- オプティマイザのタイムアウトの問題を修正しました。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- LDAP認証における大文字小文字を区別しないユーザー名の正規化を修正しました。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- API `proc_file`に対するSSRFリスクを軽減しました。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 監査およびSQLリダクションでユーザー認証文字列をマスクしました。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 動作の変更

- ETL実行モードの最適化がデフォルトで有効になりました。これにより、明示的な設定変更なしで、INSERT INTO SELECT、CREATE TABLE AS SELECT、および同様のバッチワークロードにメリットがあります。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead`ウィンドウ関数の3番目の引数が、定数値に加えて列参照をサポートするようになりました。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USINGはSQL標準のセマンティクスに従うようになり、USING列は出力に2回ではなく1回表示されます。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- グローバル遅延具現化がデフォルトで有効になりました。[#70412](https://github.com/StarRocks/starrocks/pull/70412)
- `query_queue_v2`がデフォルトで有効になりました。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQLトランザクションは、デフォルトでセッション変数`enable_sql_transaction`によってゲートされます。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
