---
displayed_sidebar: docs
---

# StarRocks version 4.0

:::warning

**ダウングレードに関する注意事項**

- StarRocks を v4.0 にアップグレードした後、v3.5.0 および v3.5.1 にダウングレードしないでください。そうするとメタデータの非互換性が発生し、FE がクラッシュする可能性があります。これらの問題を回避するには、クラスタを v3.5.2 以降にダウングレードする必要があります。

- v4.0.2 から v4.0.1、v4.0.0、および v3.5.2～v3.5.10 へのクラスタのダウングレード前に、次のステートメントを実行してください：

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=false;
  ```

  クラスタをv4.0.2以降にアップグレードした後、次のステートメントを実行してください：

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=true;
  ```

:::

## 4.0.10

リリース日：2026 年 5 月 9 日

### 動作変更

- `INSERT INTO FILES` のエラーメッセージに含まれるクラウドストレージの認証情報がマスクされるようになり、エラーログや `SHOW LOAD` の出力に認証情報が漏洩することを防ぎます。[#71245](https://github.com/StarRocks/starrocks/pull/71245)
- Hive Catalog において、insert-only ACID Hive テーブルへのクエリを許可しなくなりました。以前は INSERT OVERWRITE 操作を認識できないため、クエリ結果が実際の可視行数より多くなる可能性がありました。これらのテーブルへのクエリは明示的なエラーを返すようになり、サイレントなデータ整合性問題を回避します。[#71460](https://github.com/StarRocks/starrocks/pull/71460)

### 改善点

- Iceberg `PartitionData` 構築経路に Avro スキーマキャッシュを追加し、パーティション数の多いテーブルの読み込み時に発生していた重複した Jackson `ObjectMapper` のアロケーションを除去しました。[#72215](https://github.com/StarRocks/starrocks/pull/72215)
- `CatalogRecycleBin.getAdjustedRecycleTimestamp` が呼び出しごとに table-id マップを再構築していたのを最適化し、リサイクルビンクリーンアップおよび Tablet スケジューリングのオーバーヘッドを削減しました。[#72128](https://github.com/StarRocks/starrocks/pull/72128)
- ストレージ・コンピュート分離モードにおいて `OlapTableSink.createLocation` が Tablet ロケーション検索をバッチ化するようになり、Tablet ごとの StarOS RPC によるプランナークリティカルセクションの停滞を解消しました。[#72041](https://github.com/StarRocks/starrocks/pull/72041)
- Java UDAF はクエリごとに 1 度だけロード・初期化され、複数の Pipeline Driver インスタンス間で再利用されるようになりました。これにより高 `pipeline_dop` 時の Driver 準備時間の線形増加を解消します。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- BE メトリクス `starrocks_be_staros_shard_info_fallback_total` および `starrocks_be_staros_shard_info_fallback_failed_total` を追加しました。StarOS Worker のローカルキャッシュがミスして starmgr へフォールバックした回数を追跡できます。[#71620](https://github.com/StarRocks/starrocks/pull/71620)
- File Bundle 書き込みが Tablet ローカルの集約ノードを優先するようになり、束ねられたメタデータパスでクロスノードの Shard 情報検索が不要になりました。[#71613](https://github.com/StarRocks/starrocks/pull/71613)
- 監査ログのエントリに、各クエリで参照されたテーブルとビューが含まれるようになりました。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- `INSERT INTO FILES` の CSV エクスポートで `csv.enclose` および `csv.escape` プロパティをサポートし、フィールドの引用とエスケープを制御できるようになりました。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- DN パターンによる LDAP ダイレクトバインド認証をサポートしました。シングルテナント LDAP 環境で管理者検索アカウントの構成が不要になります。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- ストレージ・コンピュート分離クラスタ向けに `starrocks_fe_tablet_num` メトリクスを追加し、ストレージ・コンピュート一体型クラスタとメトリクスセットを揃えました。[#71444](https://github.com/StarRocks/starrocks/pull/71444)
- `star_mgr_meta_sync_interval_sec` が `ADMIN SET FRONTEND CONFIG` で動的に変更可能になりました。新しい値は次の同期サイクルから FE 再起動なしに有効になります。[#71675](https://github.com/StarRocks/starrocks/pull/71675)

### バグ修正

以下の問題を修正しました：

- ストレージ・コンピュート分離 Combined Txn Log モードで、パーティション単位コーディネーターディスパッチの INSERT におけるクロスセンダー競合により、正当な txn log がオーファン扱いで破棄され、トランザクションが VISIBLE 状態に到達しなくなる問題を修正しました。[#72237](https://github.com/StarRocks/starrocks/pull/72237)
- ストレージ・コンピュート分離 Combined Txn Log モードで、ランタイムに `_incremental_open_node_channel` で開いた増分チャネルが、旧来の "sender_id == 0 がすべてのログを収集する" ルールに従い txn log を黙って取りこぼす問題を修正しました。[#71992](https://github.com/StarRocks/starrocks/pull/71992)
- `RuntimeProfile::to_thrift()` でプロファイルシリアライズ中に他スレッドが Counter の min/max をリセットすると `std::bad_optional_access` で BE がクラッシュする問題を修正しました。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- フラット JSON マージで一方の入力が空の場合に結果が不整合になる問題を修正しました。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- ユーザーが `CREATE TABLE ... PROPERTIES (...)` で `format-version` を明示的に指定した場合に Iceberg テーブル作成が "Multiple entries with same key: format-version" で失敗する問題を修正しました。[#72828](https://github.com/StarRocks/starrocks/pull/72828)
- `CompactionScheduler.startCompaction` が単一テーブルのクリティカルセクション全体で DB 全体の READ ロックを保持し、同一データベース内の他テーブルの DDL を阻害していた問題を修正しました。DB に対しては IS、対象テーブルにのみ READ を取得するように変更しました。[#72178](https://github.com/StarRocks/starrocks/pull/72178)
- `StarMgrMetaSyncer.syncTableMetaInternal` および `syncTableColocationInfo` が外部 StarOS RPC を行う間に DB の READ/WRITE ロックを保持し、同一データベース内のすべてのテーブルの CREATE/DROP/ALTER/RENAME を凍結する問題を修正しました。[#72108](https://github.com/StarRocks/starrocks/pull/72108)
- `StarMgrMetaSyncer.getAllPartitionShardGroupId` がすべてのクラウドネイティブテーブルおよび物理パーティションのイテレーション中に DB READ ロックを保持し続け、大規模カタログで DB 書き込みロックを待つ FE スレッドを停滞させる問題を修正しました。[#71614](https://github.com/StarRocks/starrocks/pull/71614)
- `getTableNamesViewWithLock` における冗長な DB READ ロックを削除しました。基底の `nameToTable` は `ConcurrentHashMap` のため、外側のロックは正しさには寄与せず競合のみを増やしていました。[#72042](https://github.com/StarRocks/starrocks/pull/72042)
- 読み取り専用の `/api/{db}/{table}/_count` REST エンドポイントが `proximateRowCount()` 計算のために不必要に DB WRITE ロックを取得していた問題を修正しました。[#72053](https://github.com/StarRocks/starrocks/pull/72053)
- Tablet Split、Schema Change、ALTER などの操作が `nextVersion` のみを進めて publish なしにバージョンギャップを生じさせ、バッチ publish のデッドロックを引き起こす問題を修正しました。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- ストレージ・コンピュート一体型モードで Rowset メタデータの LRU キャッシュが満杯のときにウォームアップを行うとデッドロックする問題を修正しました。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `PipelineTimerTask` がコンシューマー登録と finished 通知の順序の不正により `waitUtilFinished` でスタックする問題を修正しました。[#72058](https://github.com/StarRocks/starrocks/pull/72058)
- `ConnectorSinkPassthroughExchanger::accept` で `_writer_count` 上の条件競合が原因でベクター範囲外アクセスが発生し BE が SIGSEGV でクラッシュする問題を修正しました。[#71848](https://github.com/StarRocks/starrocks/pull/71848)
- `LoadChannel::get_load_replica_status` で一時的な `shared_ptr` の破棄により発生する use-after-free を修正しました。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- Information Schema Sink で非同期 RPC クロージャ処理時の参照カウント不足による use-after-free を修正しました。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- `reverse(DecimalV3)` で Decimal 値の幅処理が不適切なため BE がクラッシュする問題を修正しました。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- `UNNEST` で生成された列の define 式が ARRAY 型として誤って設定され、下流のグローバル辞書生成で BE クラッシュを引き起こす問題を修正しました。[#72027](https://github.com/StarRocks/starrocks/pull/72027)
- Iceberg 外部テーブル作成時に Transform 引数の順序が不正な場合（`bucket(4, region)` 等）、FE が NPE をスローしていた問題を修正しました。現在は通常のアナライザーエラーを返します。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- テーブルへの最初のクエリが列統計情報を要求しない場合（`SELECT *` 等）に、Iceberg Manifest Data File キャッシュエントリに列統計情報が欠落する問題を修正しました。[#71913](https://github.com/StarRocks/starrocks/pull/71913)
- Iceberg テーブルが `bucket(col, N)` でパーティショニングされている場合に、`PruneHDFSScanColumnRule` がプレースホルダのマテリアライズドカラムを注入することで min/max 最適化が静かにスキップされ全ファイルスキャンへフォールバックする問題を修正しました。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- `AggregateJoinPushDownRule` が外部テーブル（IcebergTable など）の同一性を `Table.getId()` で比較しており、コネクタテーブル ID がプラン再構築で変わるため Iceberg ベーステーブル上のマテリアライズドビュー書き換えが失敗する問題を修正しました。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- Hive 動的パーティションへの INSERT OVERWRITE で、メタストアにパーティションが残っているがファイルシステム上のロケーションが存在しない場合にコミットが失敗する問題を修正しました。コミット前に欠落しているパーティションディレクトリを作成するようになりました。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- Arrow が辞書型カラム（ARRAY、STRUCT、MAP 内にネストされた辞書を含む）を返す場合に、Parquet スキャナが `Illegal converting from arrow type(dictionary) ...` で失敗する問題を修正しました。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- `ColocatedBackendSelector.Assignment` で増分バッチ処理時に前バッチのスキャンレンジが残存し、ファイルが再デプロイ・再スキャンされる問題を修正しました。[#71789](https://github.com/StarRocks/starrocks/pull/71789)
- `PruneShuffleColumnRule` が Exchange Shuffle カラムの剪定後に Join の `outputProperty` を更新せず、下流の分散情報が誤りになる問題を修正しました。[#72003](https://github.com/StarRocks/starrocks/pull/72003)
- 多段マテリアライズドビュー書き換えの第一段階で `JoinPredicatePushDown` が無効化されている場合に `PushDownJoinOnExpressionToChildProject` が機能せず、後続段階で Project ノードが欠落して Shuffle 分散が誤りになる問題を修正しました。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- 述語の正規化により同一スカラサブクエリのプレースホルダが複数回出現する場合に、`ReplaceSubqueryRewriteRule` が `Apply` ノードを重複してアタッチしてしまう問題を修正しました。[#71155](https://github.com/StarRocks/starrocks/pull/71155)
- `EventScheduler` で Join Probe が完了しているにもかかわらずパイプライン全体が完了状態に遷移しない short-circuit 問題を修正しました。[#71740](https://github.com/StarRocks/starrocks/pull/71740)
- `aws.s3.iam_role_arn` で構成された AWS Assume Role が JNI スキャナ（RCFile/Avro/SequenceFile/Hudi）に適用されず S3 403 エラーになる問題を修正しました。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- Oracle JDBC の述語プッシュダウンで日付リテラルが Oracle NLS フォーマットに合致せず SQL エラーになる問題を修正しました。リテラルは `date '...'` の形式で発行されるようになりました。[#71412](https://github.com/StarRocks/starrocks/pull/71412)
- ストレージ・コンピュート分離モードで、Follower FE が Leader へ DDL を転送した後 FE Journal の再生のみ待機し StarMgr Journal を待たないため、テーブル作成直後のクエリで "no queryable replica" が発生する問題を修正しました。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 主キーテーブルの `get_tablet_stats` が各セグメントについて `get_del_vec_in_meta()` を経由して完全な `TabletMetadata` を繰り返しロードする問題を修正しました。[#71672](https://github.com/StarRocks/starrocks/pull/71672)
- Arrow Flight で空の結果セットの場合にカラム名が `r`（プレースホルダ名）として返り、実際のスキーマが返らない問題を修正しました。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- `parallel_clone_task_per_path` を更新する際に CLONE スレッドプールサイズの計算で Store Path 数が考慮されない問題を修正しました。[#71484](https://github.com/StarRocks/starrocks/pull/71484)
- リソースグループのユーザー分類器が `CREATE USER` で許可される数字始まりのユーザー名を拒否する問題を修正しました。分類器は `CREATE USER` と同じ検証ルールを使用するようになりました。[#71470](https://github.com/StarRocks/starrocks/pull/71470)
- `HttpServerHandler.channelInactive` が `isRegistered()` が false のときに `unregisterConnection` をスキップし、早期失敗リクエストでコネクションマップエントリがリークする問題を修正しました。[#72006](https://github.com/StarRocks/starrocks/pull/72006)
- Java UDF の JNI 呼び出し（`NewObject`、`NewArray`、`NewStringUTF` 等）で例外チェックや null 戻り値チェックが欠落しており、サイレント失敗や未定義動作の原因となる問題を修正しました。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `be_tablets.DATA_SIZE` が `total_disk_size`（Rowset 内蔵インデックスや Lake PK の永続化インデックスを含む）を報告していたのを、Rowset 列データバイト数の報告に統一しました。[#70735](https://github.com/StarRocks/starrocks/pull/70735)
- `StarMgrMetaSyncer` が削除対象の Shard が無いにもかかわらず "Failed to batch drop tablets" 警告ログを出力する問題を修正しました。[#72209](https://github.com/StarRocks/starrocks/pull/72209)
- CVE-2026-42198（pgjdbc）および CVE-2026-5598（BouncyCastle）対応：`org.postgresql:postgresql` を 42.7.11 に、BouncyCastle を 1.84 に更新しました。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- Netty CVE 対応：Netty を 4.1.133.Final に更新しました。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- Broker の netty / jetty / awssdk / jackson 依存関係を更新し、既知の CVE に対処しました。[#72184](https://github.com/StarRocks/starrocks/pull/72184)
- jetty-http を 9.4.58.v20250814 に更新し、旧バージョンの既知 CVE に対処しました。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- jetty 9.x が EOL であり上流からの修正版が公開されないため、ビルドのアンブロックのために CVE-2026-2332 を一時的にマスクしました。[#71914](https://github.com/StarRocks/starrocks/pull/71914)

## 4.0.9

リリース日：2026 年 4 月 16 日

### 動作変更

- VARBINARY 列が複合型（ARRAY、MAP、STRUCT）の内部に含まれる場合、StarRocks は MySQL 結果セットでその値を正しいバイナリ形式でエンコードするようになりました。以前は生のバイトが直接出力されていたため、ヌルバイトや非表示文字が含まれる場合にテキストプロトコルの解析が壊れることがありました。この変更は、複合型内の VARBINARY データを処理するダウンストリームクライアントやツールに影響する場合があります。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- Routine Load ジョブは、主キーサイズ制限超過など、再試行不可能なエラーが発生した場合に自動的に一時停止するようになりました。以前はこのようなエラーが再試行不可能として認識されず、ジョブが無限に再試行し続けていました。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- `SHOW CREATE TABLE` および `DESC` ステートメントが Paimon 外部テーブルの主キー列を表示するようになりました。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- クラウドネイティブ Tablet メタデータ取得操作（`get_tablet_stats`、`get_tablet_metadatas` など）に専用スレッドプールが導入されました。これにより、メタデータ取得が `UPDATE_TABLET_META_INFO` 共有プールの他タスクと競合しなくなります。新しい BE 設定パラメータでスレッドプールサイズを調整できます。[#70492](https://github.com/StarRocks/starrocks/pull/70492)

### 改善点

- MySQL プロトコルレスポンスにおける VARBINARY 値のエンコード動作を制御するセッション変数を追加し、接続ごとのバイナリ結果エンコードをきめ細かく制御できるようになりました。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- クラスタースナップショットに `snapshot_meta.json` マーカーファイルを追加し、スナップショット復元前の完全性検証をサポートしました。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- `WarehouseManager` でサイレントに握り潰された例外に対する警告ログを追加し、障害の可観測性を向上させました。[#71215](https://github.com/StarRocks/starrocks/pull/71215)
- Iceberg メタデータテーブルクエリのメトリクスを追加し、パフォーマンスの監視と診断をサポートしました。[#70825](https://github.com/StarRocks/starrocks/pull/70825)
- `regexp_replace()` 関数が FE クエリプランニング段階での定数畳み込みをサポートするようになり、引数が定数文字列の場合のプランニングオーバーヘッドが削減されました。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- Iceberg タイムトラベルクエリのカテゴリ別メトリクスを追加し、監視とパフォーマンス分析を強化しました。[#70788](https://github.com/StarRocks/starrocks/pull/70788)
- Update Compaction が一時停止された際のログ出力を追加し、Compaction ライフサイクルの可視性を向上させました。[#70538](https://github.com/StarRocks/starrocks/pull/70538)
- `SHOW COLUMNS` が PostgreSQL 外部テーブルの列コメントを返すようになりました。[#70520](https://github.com/StarRocks/starrocks/pull/70520)
- クエリで例外が発生した際にクエリ実行プランをダンプする機能を追加し、実行時障害の診断性を向上させました。[#70387](https://github.com/StarRocks/starrocks/pull/70387)
- DDL 操作中の Tablet 削除をバッチ処理するようになり、Tablet メタデータの書き込みロック競合が軽減されました。[#70052](https://github.com/StarRocks/starrocks/pull/70052)
- エラー状態に陥って通常の手段では削除できない同期マテリアライズドビューに対する Force Drop 回復メカニズムを追加しました。[#70029](https://github.com/StarRocks/starrocks/pull/70029)

### バグ修正

以下の問題を修正しました：

- プロファイルの `START_TIME` と `END_TIME` がセッションのタイムゾーンで表示されなかった問題を修正しました。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `PushDownAggregateRewriter` が CASE-WHEN/IF 式を処理する際に共有オブジェクトが変更される問題を修正しました。この問題によりクエリ結果が不正になることがありました。[#71309](https://github.com/StarRocks/starrocks/pull/71309)
- スレッド作成に失敗した際に `ThreadPool::do_submit` で発生するuse-after-freeのバグを修正しました。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- `information_schema.tables` が等値述語内の特殊文字を適切にエスケープせず、誤った結果が返される問題を修正しました。[#71273](https://github.com/StarRocks/starrocks/pull/71273)
- マテリアライズドビューが非アクティブになった後も、そのスケジューラーが実行し続けていた問題を修正しました。[#71265](https://github.com/StarRocks/starrocks/pull/71265)
- 同時実行の ALTER ジョブ間で `UpdateTabletSchemaTask` のシグネチャ衝突が発生し、スキーマ更新タスクがスキップされることがあった問題を修正しました。[#71242](https://github.com/StarRocks/starrocks/pull/71242)
- MCV（最頻値）エントリのみを含むヒストグラムで行数推定が NaN になる問題を修正しました。[#71241](https://github.com/StarRocks/starrocks/pull/71241)
- AWS SDK 統合において AWS S3 Transfer Manager への依存が欠落していた問題を修正しました。[#71230](https://github.com/StarRocks/starrocks/pull/71230)
- `TaskManager` スケジューラーコールバックが現在のノードがリーダーかどうかを確認していなかった問題を修正しました。この問題によりフォロワーノードでタスクが重複実行されることがありました。[#71156](https://github.com/StarRocks/starrocks/pull/71156)
- リーダー転送リクエスト完了後に `ConnectContext` のスレッドローカル情報がクリアされず、後続リクエストへのコンテキスト汚染が発生する問題を修正しました。[#71141](https://github.com/StarRocks/starrocks/pull/71141)
- ショートサーキットポイントルックアップでパーティション述語が欠落し、クエリ結果が不正になる問題を修正しました。[#71124](https://github.com/StarRocks/starrocks/pull/71124)
- Stream Load または Broker Load で生成列を解析する際、参照される列がロードスキーマに存在しない場合に NullPointerException が発生する問題を修正しました。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- 並列セグメント/行セット読み込みのエラー処理パスにおける use-after-free のバグを修正しました。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 同一 Publish バッチ内で書き込み操作が Compaction より前に実行された場合に delvec の孤立エントリが残る問題を修正しました。[#71049](https://github.com/StarRocks/starrocks/pull/71049)
- 内部的にクエリ進捗を確認する HTTP ループバックを通じたクエリが `current_queries` に表示されていた問題を修正しました。[#71032](https://github.com/StarRocks/starrocks/pull/71032)
- CVE-2026-33870 および CVE-2026-33871 に対処するため、AWS SDK バンドルおよび Netty を 4.1.132.Final にアップグレードしました。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- `SharedDataStorageVolumeMgr` の読み取りロックリークを修正しました。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- `locate()` 関数の入力列と結果列が BinaryColumns 内で同じ NullColumn 参照を共有し、誤った結果が生じる問題を修正しました。[#70957](https://github.com/StarRocks/starrocks/pull/70957)
- Share-Nothing モードの ALTER 操作で安全削除チェックが誤って適用されていた問題を修正しました。[#70934](https://github.com/StarRocks/starrocks/pull/70934)
- `_all_global_rf_ready_or_timeout` の競合状態を修正しました。この問題によりグローバル Runtime Filter が正しく適用されないことがありました。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- メトリクスマクロ `ACCUMULATED` の int32 オーバーフローによりメトリクス値がサイレントオーバーフローする問題を修正しました。[#70889](https://github.com/StarRocks/starrocks/pull/70889)
- 辞書エンコードされたマージ GROUP BY クエリで誤った集計結果が返される問題を修正しました。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- CVE-2025-54920 に対処するため、`spark-core_2.12` をバージョン 3.5.7 にアップグレードしました。[#70862](https://github.com/StarRocks/starrocks/pull/70862)
- `set_finishing` 中のハッシュテーブル状態処理が不正なことによる集計スピルのデータ損失の可能性を修正しました。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- `proxy_pass_request_body` が無効な場合に `content-length` ヘッダーがリセットされない問題を修正しました。[#70821](https://github.com/StarRocks/starrocks/pull/70821)
- ロード操作のスピルディレクトリがオブジェクトのデストラクタでクリーンアップされており、`DeltaWriter::close()` 内で行われていないためスピルデータが早期削除される可能性があった問題を修正しました。[#70778](https://github.com/StarRocks/starrocks/pull/70778)
- `INSERT INTO ... BY NAME` で `FILES()` から部分的な列セットをインポートする際のスキーマプッシュダウンが正しく処理されない問題を修正しました。[#70774](https://github.com/StarRocks/starrocks/pull/70774)
- コネクタスキャンノードがクエリ再試行時にスキャン範囲ソースをリセットせず、再試行後に誤った結果が返される問題を修正しました。[#70762](https://github.com/StarRocks/starrocks/pull/70762)
- ディスク再マイグレーション（A→B→A）中の GC 競合により Primary Key モデルの Tablet で行セットメタデータが失われる可能性があった問題を修正しました。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- クエリスコープの Warehouse ヒントにより `ComputeResource` オブジェクトが `ConnectContext` にリークし、同一接続の後続クエリに影響する問題を修正しました。[#70706](https://github.com/StarRocks/starrocks/pull/70706)
- `MySqlScanNode` および `JDBCScanNode` の冗長な Conjunct が `VectorizedInPredicate` 型不一致に関する BE エラーを引き起こす問題を修正しました。[#70694](https://github.com/StarRocks/starrocks/pull/70694)
- Ubuntu ランタイム環境に `libssl-dev` 依存関係が欠落していた問題を修正しました。[#70688](https://github.com/StarRocks/starrocks/pull/70688)
- Iceberg マニフェストキャッシュの読み取り時に完全性が検証されず、キャッシュが部分的にのみ書き込まれていた場合に誤ったスキャン結果が返される問題を修正しました。[#70675](https://github.com/StarRocks/starrocks/pull/70675)
- `_tablet_multi_get_rpc` でクロージャへの参照が重複しており、use-after-free が発生する可能性があった問題を修正しました。[#70657](https://github.com/StarRocks/starrocks/pull/70657)
- Iceberg `ManifestReader` でマニフェストキャッシュの書き込みが不完全になり、キャッシュエントリが不完全になる問題を修正しました。[#70652](https://github.com/StarRocks/starrocks/pull/70652)
- null リテラル要素を含む配列を処理する際に `array_map()` がクラッシュする問題を修正しました。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- 大きな入力を処理する際に `to_base64()` 関数でスタックオーバーフローが発生する問題を修正しました。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- `INSERT INTO ... BY NAME` で `FILES()` からインポートする際に名前ベースのマッピングではなく位置ベースのマッピングが使用され、データが誤った列に書き込まれる問題を修正しました。[#70622](https://github.com/StarRocks/starrocks/pull/70622)
- `NOT NULL` 制約が `FILES()` のスキーマ推論に誤ってプッシュダウンされ、null 許容列のロードが失敗する問題を修正しました。[#70621](https://github.com/StarRocks/starrocks/pull/70621)
- Iceberg ライクなコネクタで、精密な外部マテリアライズドビューのリフレッシュが正しくフォールバックしない問題を修正しました。[#70589](https://github.com/StarRocks/starrocks/pull/70589)
- 部分的な Tablet スキーマを構築する際の `num_short_key_columns` 不一致によりデータ読み取りエラーが発生する問題を修正しました。[#70586](https://github.com/StarRocks/starrocks/pull/70586)
- `MaskMergeIterator` で子イテレーターが枯渇した際に BE がクラッシュする問題を修正しました。[#70539](https://github.com/StarRocks/starrocks/pull/70539)
- 対応する Iceberg スナップショットが期限切れになったパーティションに対してマテリアライズドビューのリフレッシュジョブが繰り返しリフレッシュを行う問題を修正しました。[#70523](https://github.com/StarRocks/starrocks/pull/70523)
- starlet の設定パラメータを設定できなかった問題を修正しました。[#70482](https://github.com/StarRocks/starrocks/pull/70482)
- ロックフリーのマテリアライズドビュー書き換えパスが誤ってライブメタデータにフォールバックし、書き換えの動作が不一致になる問題を修正しました。[#70475](https://github.com/StarRocks/starrocks/pull/70475)
- `JoinHashTable::merge_ht` で式ベースの結合キー列のダミー行がスキップされず、結合結果が誤りになる問題を修正しました。[#70465](https://github.com/StarRocks/starrocks/pull/70465)
- `InformationFunction` の等値比較ロジックが誤っており、特定のクエリで誤った結果が返される問題を修正しました。[#70464](https://github.com/StarRocks/starrocks/pull/70464)
- 内部関数 `__iceberg_transform_bucket` の列型不一致を修正しました。[#70443](https://github.com/StarRocks/starrocks/pull/70443)
- Iceberg スナップショットのタイムスタンプが単調でない場合に Iceberg マテリアライズドビューのリフレッシュが失敗する問題を修正しました。[#70382](https://github.com/StarRocks/starrocks/pull/70382)
- ユーザー認証情報が監査ログおよび SQL 難読化出力に公開されていた問題を修正しました。[#70360](https://github.com/StarRocks/starrocks/pull/70360)
- Physical Split が有効な場合に空の Tablet をスキャンすると CN がクラッシュする問題を修正しました。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- クエリ最適化中に冗長な CAST が除去された後に VARCHAR 列の長さが保持されない問題を修正しました。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- brpc 接続再試行ロジックがラップされた `NoSuchElementException` を正しく処理せず、再試行後に接続が失敗する問題を修正しました。[#70203](https://github.com/StarRocks/starrocks/pull/70203)
- 統計推定中に外部結合列の null fraction が保持されず、クエリプランが最適でなくなる問題を修正しました。[#70144](https://github.com/StarRocks/starrocks/pull/70144)
- ポーラースレッドで実行されるコネクタシンク操作のメモリトラッカーリークを修正しました。[#70121](https://github.com/StarRocks/starrocks/pull/70121)

## 4.0.8

リリース日：2026 年 3 月 25 日

### 動作変更

- `sql_mode` の処理を改善しました：`DIVISION_BY_ZERO` または `FAIL_PARSE_DATE` モードが設定されている場合、`str_to_date`/`str2date` 関数でのゼロ除算および日付パース失敗が暗黙的に無視されるのではなく、エラーを返すようになりました。[#70004](https://github.com/StarRocks/starrocks/pull/70004)
- `FORBID_INVALID_DATE` sql_mode が有効な場合、`INSERT VALUES` 句の無効な日付がバイパスされずに正しく拒否されるようになりました。[#69803](https://github.com/StarRocks/starrocks/pull/69803)
- 式パーティションの生成列が `DESC` および `SHOW CREATE TABLE` の出力に表示されなくなりました。[#69793](https://github.com/StarRocks/starrocks/pull/69793)
- 監査ログにクライアント ID が含まれなくなりました。[#69383](https://github.com/StarRocks/starrocks/pull/69383)

### 改善点

- ローカルエクスチェンジバッファサイズを `dop × local_exchange_buffer_mem_limit_per_driver` に制限する設定項目 `local_exchange_buffer_mem_limit_per_driver` を追加しました。[#70393](https://github.com/StarRocks/starrocks/pull/70393)
- `check_missing_files` において、バージョン間のファイル存在チェック結果をキャッシュし、冗長なストレージ I/O を削減しました。[#70364](https://github.com/StarRocks/starrocks/pull/70364)
- `desc_hint_split_range` が ≤ 0 に設定された場合、降順 TopN ランタイムフィルタの範囲分割とリバーススキャン最適化を無効化できるようにしました。[#70307](https://github.com/StarRocks/starrocks/pull/70307)
- Trino 方言の `INSERT` ステートメントに `EXPLAIN` および `EXPLAIN ANALYZE` のサポートを追加しました。[#70174](https://github.com/StarRocks/starrocks/pull/70174)
- ポジションデリートが存在する場合の Iceberg 読み取り性能を最適化しました。[#69717](https://github.com/StarRocks/starrocks/pull/69717)
- 分散キーに基づくマテリアライズドビュー最適選択戦略を改善し、マテリアライズドビュー選択の精度を向上させました。[#69679](https://github.com/StarRocks/starrocks/pull/69679)

### バグ修正

以下の問題を修正しました：

- JDBC MySQL プッシュダウンでサポートされていないキャスト操作が失敗する問題。[#70415](https://github.com/StarRocks/starrocks/pull/70415)
- マテリアライズドビューリフレッシュ時のパーティションタイプ不一致を解消するため、`mv_refresh_force_partition_type` 設定項目を追加しました。[#70381](https://github.com/StarRocks/starrocks/pull/70381)
- バックアップから復元する際に `dataVersion` が正しく設定されない問題。[#70373](https://github.com/StarRocks/starrocks/pull/70373)
- マテリアライズドビューリフレッシュタスクでパーティション名が重複する問題。[#70354](https://github.com/StarRocks/starrocks/pull/70354)
- SLF4J のパラメータ化ログでプレースホルダーの代わりに文字列結合が使用される問題。[#70330](https://github.com/StarRocks/starrocks/pull/70330)
- Hive テーブル作成時にコメントが設定されない問題。[#70318](https://github.com/StarRocks/starrocks/pull/70318)
- HDFS のクローズが遅い場合に `FileSystemExpirationChecker` がブロックされる問題。[#70311](https://github.com/StarRocks/starrocks/pull/70311)
- `OlapTableSink` において異なるパーティション間で分散列の検証が行われない問題。[#70310](https://github.com/StarRocks/starrocks/pull/70310)
- 定数畳み込みで倍精度浮点数の加算がオーバーフローした際にエラーではなく INF が返される問題。[#70309](https://github.com/StarRocks/starrocks/pull/70309)
- Iceberg テーブル作成時のフィールド名のタイポ：`common` の代わりに `comment` が使用される問題。[#70267](https://github.com/StarRocks/starrocks/pull/70267)
- 一部のシナリオで root ユーザーが全 Ranger 権限チェックをバイパスできない問題。[#70254](https://github.com/StarRocks/starrocks/pull/70254)
- データ取り込み中に `query_pool` メモリトラッカーが負の値になる問題。[#70228](https://github.com/StarRocks/starrocks/pull/70228)
- `AuditEventProcessor` スレッドが `OutOfMemoryException` により終了する問題。[#70206](https://github.com/StarRocks/starrocks/pull/70206)
- `SplitTopNRule` がパーティションプルーニングを正しく適用しない問題。[#70154](https://github.com/StarRocks/starrocks/pull/70154)
- スキーマ変更のパブリッシュ時に `cal_new_base_version` で範囲外アクセスが発生する問題。[#70132](https://github.com/StarRocks/starrocks/pull/70132)
- マテリアライズドビュー書き換え時にベーステーブルから削除されたパーティションが無視される問題。[#70130](https://github.com/StarRocks/starrocks/pull/70130)
- 境界比較における型不一致によりパーティション述語が意図せず削除される問題。[#70097](https://github.com/StarRocks/starrocks/pull/70097)
- `str_to_date` が BE ランタイムでマイクロ秒精度を失う問題。[#70068](https://github.com/StarRocks/starrocks/pull/70068)
- Join スピルプロセスが `set_callback_function` でクラッシュする問題。[#70030](https://github.com/StarRocks/starrocks/pull/70030)
- `gcs-connector` をバージョン 3.0.13 にアップグレード後、Broker Load の GCS 認証が失敗する問題。[#70012](https://github.com/StarRocks/starrocks/pull/70012)
- `DeltaWriter::close()` が bthread コンテキストから呼び出された際に DCHECK が失敗する問題。[#69960](https://github.com/StarRocks/starrocks/pull/69960)
- `AsyncDeltaWriter` のクローズ/完了ライフサイクルにおける use-after-free 競合状態。[#69940](https://github.com/StarRocks/starrocks/pull/69940)
- 競合状態により書き込みトランザクションの EditLog エントリが欠落する問題。[#69899](https://github.com/StarRocks/starrocks/pull/69899)
- 既知の CVE 脆弱性。[#69863](https://github.com/StarRocks/starrocks/pull/69863)
- フォロワー FE が `changeCatalogDb` でジャーナルのリプレイを待機しない問題。[#69834](https://github.com/StarRocks/starrocks/pull/69834)
- バックスラッシュエスケープシーケンスを含む `LIKE` パターンマッチングの結果が不正確な問題。[#69775](https://github.com/StarRocks/starrocks/pull/69775)
- パーティション列のリネーム後に式分析が失敗する問題。[#69771](https://github.com/StarRocks/starrocks/pull/69771)
- `AsyncDeltaWriter::close` における use-after-free クラッシュ。[#69770](https://github.com/StarRocks/starrocks/pull/69770)
- ローカルパーティション TopN 実行時のクラッシュ。[#69752](https://github.com/StarRocks/starrocks/pull/69752)
- `Partition.hasStorageData` に起因する `PartitionColumnMinMaxRewriteRule` の不正な動作。[#69751](https://github.com/StarRocks/starrocks/pull/69751)
- ファイルシンクの出力ファイル名に CSV 圧縮サフィックスが重複する問題。[#69749](https://github.com/StarRocks/starrocks/pull/69749)
- `lake_capture_tablet_and_rowsets` 操作が実験的な設定フラグで制御されていない問題。[#69748](https://github.com/StarRocks/starrocks/pull/69748)
- シャドウパーティションが存在する場合のパーティション最小値プルーニングが不正確な問題。[#69641](https://github.com/StarRocks/starrocks/pull/69641)
- Java UDTF/UDAF でメソッドパラメータにジェネリック型を使用するとクラッシュする問題。[#69197](https://github.com/StarRocks/starrocks/pull/69197)
- クエリ計画完了後にクエリレベルのメタデータが解放されず、同時クエリ実行時に FE の OOM が発生する問題。[#68444](https://github.com/StarRocks/starrocks/pull/68444)
- クエリスコープの Warehouse ヒントにより `ConnectContext` 内の `ComputeResource` がリークする問題。[#70706](https://github.com/StarRocks/starrocks/pull/70706)
- ロックフリーのマテリアライズドビュー書き換えがライブメタデータに誤ってフォールバックする問題。[#70475](https://github.com/StarRocks/starrocks/pull/70475)
- `_tablet_multi_get_rpc` にクロージャの重複参照が存在する問題。[#70657](https://github.com/StarRocks/starrocks/pull/70657)
- `ReplaceColumnRefRewriter` で無限再帰が発生する問題。[#66974](https://github.com/StarRocks/starrocks/pull/66974)
- `NOT NULL` 制約が `FILES()` テーブル関数のスキーマに誤ってプッシュダウンされる問題。[#70621](https://github.com/StarRocks/starrocks/pull/70621)
- 部分タブレットスキーマで `num_short_key_columns` が一致しない問題。[#70586](https://github.com/StarRocks/starrocks/pull/70586)
- 共有データクラスタでの `COLUMN_UPSERT_MODE` チェックサムエラー。[#65320](https://github.com/StarRocks/starrocks/pull/65320)
- `__iceberg_transform_bucket` の列型不一致。[#70443](https://github.com/StarRocks/starrocks/pull/70443)
- Starlet の設定項目が反映されない問題。[#70482](https://github.com/StarRocks/starrocks/pull/70482)
- 部分更新で列モードから行モードに切り替えた際に DCG データが正しく読み取られない問題。[#61529](https://github.com/StarRocks/starrocks/pull/61529)

## 4.0.7

リリース日：2026年3月12日

### 動作変更

- Iceberg ビューに基づいてマテリアライズドビューを作成することを禁止しました。 [#69471](https://github.com/StarRocks/starrocks/pull/69471)
- 複数ステートメントの Stream Load トランザクションにおける動作の不整合を修正しました。 [#68542](https://github.com/StarRocks/starrocks/pull/68542)

### 改善点

- Publish フェーズにおいて `LakePersistentIndex` の詳細なトレースカウンターを追加しました。 [#69640](https://github.com/StarRocks/starrocks/pull/69640)
- 再構築された行数がしきい値を超えた場合に、`LakePersistentIndex` で早期 flush をトリガーするようにしました。 [#69698](https://github.com/StarRocks/starrocks/pull/69698)
- `meta_tool` に `dump_lake_persistent_index_sst` 操作を追加しました。 [#69682](https://github.com/StarRocks/starrocks/pull/69682)
- `REPAIR TABLE` 機能および `SHOW TABLET` のステータス表示を改善しました。 [#69656](https://github.com/StarRocks/starrocks/pull/69656)
- クラウドネイティブテーブルで `ADMIN SHOW TABLET STATUS` をサポートしました。 [#69616](https://github.com/StarRocks/starrocks/pull/69616)
- `hadoop-client` を 3.4.2 から 3.4.3 にアップグレードしました。 [#69503](https://github.com/StarRocks/starrocks/pull/69503)
- デシリアライズの不一致が発生した場合のクラッシュを防止しました。 [#69481](https://github.com/StarRocks/starrocks/pull/69481)
- `information_schema.loads` をクエリする際、述語を FE にプッシュダウンするようにしました。 [#69472](https://github.com/StarRocks/starrocks/pull/69472)
- マテリアライズドビューのリフレッシュ TaskRun に表示される SQL を最適化しました。 [#69437](https://github.com/StarRocks/starrocks/pull/69437)
- vended credentials が有効な場合、`CachingIcebergCatalog` のキャッシュをバイパスするようにしました。 [#69434](https://github.com/StarRocks/starrocks/pull/69434)
- ロック競合を減らすため、`canTxnFinished` でタイムアウト付き `tryLock` を使用するようにしました。 [#69427](https://github.com/StarRocks/starrocks/pull/69427)
- グローバル読み取り専用変数 `@@run_mode` を追加しました。 [#69247](https://github.com/StarRocks/starrocks/pull/69247)
- `DeltaLakeMetastore` でキャッシュエントリの重みを推定するため Estimator を使用するようにしました。 [#69244](https://github.com/StarRocks/starrocks/pull/69244)
- AWS Glue `GetDatabases` API に resource share タイプのサポートを追加しました。 [#69056](https://github.com/StarRocks/starrocks/pull/69056)
- `convert_tz` を含むスカラサブクエリから範囲述語を抽出するようにしました。 [#69055](https://github.com/StarRocks/starrocks/pull/69055)
- ByteBuffer Estimator を追加しました。 [#69042](https://github.com/StarRocks/starrocks/pull/69042)
- 共有データクラスタで Lake DeltaWriter の高速キャンセルをサポートしました。 [#68877](https://github.com/StarRocks/starrocks/pull/68877)
- ランダム分散テーブルに物理パーティションを追加するためのインターフェースをサポートしました。 [#68503](https://github.com/StarRocks/starrocks/pull/68503)
- セッション変数 `enable_sql_transaction` により SQL トランザクションを制御できるようにしました（デフォルト: true）。 [#63535](https://github.com/StarRocks/starrocks/pull/63535)
- 外部テーブルをクエリする際のパーティションスキャン数の制限を追加しました。 [#68480](https://github.com/StarRocks/starrocks/pull/68480)

### バグ修正

以下の問題を修正しました。

- リソースがビジーな場合に、メトリック `g_publish_version_failed_tasks` の値が正しくない問題。 [#69526](https://github.com/StarRocks/starrocks/pull/69526)
- スナップショットの有効期限が切れた場合に `IcebergCatalog.getPartitionLastUpdatedTime` で NPE が発生する問題。 [#68925](https://github.com/StarRocks/starrocks/pull/68925)
- bthread コンテキストから `DeltaWriter::close()` を呼び出した際に DCHECK 失敗が発生する問題。 [#70057](https://github.com/StarRocks/starrocks/pull/70057)
- 複数の use-after-free 問題。 [#69968](https://github.com/StarRocks/starrocks/pull/69968)
- AsyncDeltaWriter の close/finish ライフサイクルにおける use-after-free レース問題。 [#69961](https://github.com/StarRocks/starrocks/pull/69961)
- PK SST テーブルの破損したキャッシュがクリアされない問題。 [#69693](https://github.com/StarRocks/starrocks/pull/69693)
- `AsyncFlushOutputStream` の use-after-free 問題。 [#69688](https://github.com/StarRocks/starrocks/pull/69688)
- `disableRecoverPartitionWithSameName` における保持時間クロックのリセットおよび不完全なスキャンの問題。 [#69677](https://github.com/StarRocks/starrocks/pull/69677)
- デシリアライズ後に `StreamLoadMultiStmtTask.cancelAfterRestart` で NPE が発生する問題。 [#69662](https://github.com/StarRocks/starrocks/pull/69662)
- `SchemaBeTabletsScanner` のロジック誤りにより不要な RPC とメタデータクエリが発生する問題。 [#69645](https://github.com/StarRocks/starrocks/pull/69645)
- Graceful exit により異なるトランザクションが同じバージョンを publish する問題。 [#69639](https://github.com/StarRocks/starrocks/pull/69639)
- Primary Key Index に compact 済み rowset を指す古いエントリが含まれている場合、`TabletUpdates::get_column_values` が SIGSEGV でクラッシュする問題。 [#69617](https://github.com/StarRocks/starrocks/pull/69617)
- `KILL ANALYZE` が `ANALYZE TABLE` タスクを停止できない問題。 [#69592](https://github.com/StarRocks/starrocks/pull/69592)
- RowGroupWriter のすべての例外が捕捉されないことによる予期しない動作。 [#69568](https://github.com/StarRocks/starrocks/pull/69568)
- マテリアライズドビューの warehouse を変更した後の TaskRun の warehouse 表示の問題。 [#69567](https://github.com/StarRocks/starrocks/pull/69567)
- 集計テーブルおよびユニークテーブルで schema change 後、新しく追加されたキー列がソートキーに含まれない問題。 [#69529](https://github.com/StarRocks/starrocks/pull/69529)
- `isInternalCancelError` が `equals` を使用していることによる問題。 [#69523](https://github.com/StarRocks/starrocks/pull/69523)
- `ALTER MATERIALIZED VIEW` 後の TaskManager スケジューリングの不具合。 [#69504](https://github.com/StarRocks/starrocks/pull/69504)
- `ParquetFileWriter::close` のすべての例外が捕捉されず、Pipeline がブロックまたはクラッシュする問題。 [#69492](https://github.com/StarRocks/starrocks/pull/69492)
- パーティションテーブルのマテリアライズドビュー強制リフレッシュに関する問題。 [#69488](https://github.com/StarRocks/starrocks/pull/69488)
- 一部の writer が flush に失敗した場合に誤ったステータスが返される問題。 [#69473](https://github.com/StarRocks/starrocks/pull/69473)
- Primary Key タブレットが trash に移動された際に rowset ファイルが削除される問題。 [#69438](https://github.com/StarRocks/starrocks/pull/69438)
- 自動パーティションの範囲が既存のマージ済みパーティションに含まれている場合に INSERT が失敗する問題。 [#69429](https://github.com/StarRocks/starrocks/pull/69429)
- マテリアライズドビューの tablet メタデータが FE leader と follower の間で不整合となる問題。 [#69428](https://github.com/StarRocks/starrocks/pull/69428)
- 関数フィールドに関連する並行性の問題。 [#69315](https://github.com/StarRocks/starrocks/pull/69315)
- `computeMinActiveTxnId` が rollup handler のアクティブトランザクション ID を考慮せず、データが早期に削除される問題。 [#69285](https://github.com/StarRocks/starrocks/pull/69285)
- 同時 SWAP 後の名前ベースのテーブル検索により `addPartitions` でロックリークが発生する問題。 [#69284](https://github.com/StarRocks/starrocks/pull/69284)
- `DROP FUNCTION IF EXISTS` が `ifExists` フラグを無視する問題。 [#69216](https://github.com/StarRocks/starrocks/pull/69216)
- `TypeParser` における `CAST(... AS SIGNED)` の動作が MySQL 互換構文と一致しない問題。 [#69181](https://github.com/StarRocks/starrocks/pull/69181)
- クエリテーブルコピーにおけるパーティション検索の大文字小文字非区別に関する問題。 [#69173](https://github.com/StarRocks/starrocks/pull/69173)
- MIN/MAX 統計のリライトが失敗した際に集約関数が欠落する問題。 [#69149](https://github.com/StarRocks/starrocks/pull/69149)
- CVE-2025-67721。 [#69138](https://github.com/StarRocks/starrocks/pull/69138)
- 同期マテリアライズドビューでの all-null 値処理の不具合。 [#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 共有ミュータブル状態によりマテリアライズドビューリライトで projection が失われる問題。 [#69063](https://github.com/StarRocks/starrocks/pull/69063)
- Iceberg キャッシュ Weigher の推定が不正確な問題。 [#69058](https://github.com/StarRocks/starrocks/pull/69058)
- 定数サブクエリにおける `FULL OUTER JOIN USING` の問題。 [#69028](https://github.com/StarRocks/starrocks/pull/69028)
- 重複した定数に対する `DISTINCT ORDER BY` のエイリアス解決の問題。 [#69014](https://github.com/StarRocks/starrocks/pull/69014)
- マテリアライズドビューが reload 時に外部カタログへアクセスする問題。 [#68926](https://github.com/StarRocks/starrocks/pull/68926)
- Iceberg `getPartitions` で NPE が発生する問題。 [#68907](https://github.com/StarRocks/starrocks/pull/68907)
- Azure ABFS/WASB FileSystem のキャッシュキーに container が正しく含まれていない問題。 [#68901](https://github.com/StarRocks/starrocks/pull/68901)
- 共有データクラスタで `CHAR` 列の長さを変更した後に誤ったクエリ結果が返される問題。 [#68808](https://github.com/StarRocks/starrocks/pull/68808)
- LDAP 認証におけるユーザー名の大文字小文字非区別の問題。 [#67966](https://github.com/StarRocks/starrocks/pull/67966)
- テーブルに `storage_cooldown_ttl` を追加した後にパーティションを作成できない問題。 [#60290](https://github.com/StarRocks/starrocks/pull/60290)

## 4.0.6

リリース日：2026 年 2 月 14 日

### 改善点

- Iceberg テーブル作成時に、括弧付きのパーティション変換（例：`PARTITION BY (bucket(k1, 3))`）をサポートしました。[#68945](https://github.com/StarRocks/starrocks/pull/68945)
- Iceberg テーブルにおいて、パーティション列を列リストの末尾に配置する必要があるという制限を削除し、任意の位置に定義できるようになりました。[#68340](https://github.com/StarRocks/starrocks/pull/68340)
- Iceberg テーブルの sink に対してホストレベルのソート機能を導入しました。システム変数 `connector_sink_sort_scope`（デフォルト：FILE）で制御し、データレイアウトを最適化して読み取り性能を向上させます。[#68121](https://github.com/StarRocks/starrocks/pull/68121)
- Iceberg のパーティション変換関数（例：`bucket`、`truncate`）において、引数の数が誤っている場合のエラーメッセージを改善しました。[#68349](https://github.com/StarRocks/starrocks/pull/68349)
- テーブルプロパティ処理をリファクタリングし、Iceberg テーブルにおける異なるファイル形式（ORC/Parquet）および圧縮コーデックのサポートを強化しました。[#68588](https://github.com/StarRocks/starrocks/pull/68588)
- よりきめ細かな制御を可能にするため、テーブルレベルのクエリタイムアウト設定 `table_query_timeout` を追加しました（優先順位：Session &gt; Table &gt; Cluster）。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- `ADMIN SHOW AUTOMATED CLUSTER SNAPSHOT` ステートメントにより、自動スナップショットの状態およびスケジュールを確認できるようになりました。[#68455](https://github.com/StarRocks/starrocks/pull/68455)
- `SHOW CREATE VIEW` で、コメントを含む元のユーザー定義 SQL を表示できるようになりました。[#68040](https://github.com/StarRocks/starrocks/pull/68040)
- `information_schema.loads` において、Merge Commit を有効にした Stream Load タスクを表示し、可観測性を向上させました。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- FE のメモリ使用量を推定するユーティリティ API `/api/memory_usage` を追加しました。[#68287](https://github.com/StarRocks/starrocks/pull/68287)
- パーティションリサイクル時の `CatalogRecycleBin` における不要なログ出力を削減しました。[#68533](https://github.com/StarRocks/starrocks/pull/68533)
- ベーステーブルで Swap/Drop/Replace Partition 操作が実行された際、関連する非同期マテリアライズドビューを自動的にリフレッシュするようにしました。[#68430](https://github.com/StarRocks/starrocks/pull/68430)
- `count distinct` 系の集約関数で `VARBINARY` 型をサポートしました。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
- 式統計情報を強化し、セマンティクス的に安全な式（例：`cast(k as bigint) + 10`）に対してヒストグラムの MCV を伝播することで、データスキュー検出を改善しました。[#68292](https://github.com/StarRocks/starrocks/pull/68292)

### バグ修正

以下の問題を修正しました：

- Skew Join V2 のランタイムフィルタで発生する可能性のあるクラッシュ。[#67611](https://github.com/StarRocks/starrocks/pull/67611)
- 低カーディナリティ書き換えにより発生する Join 述語の型不一致（例：INT = VARCHAR）。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- クエリキューの割り当て時間および待機タイムアウトロジックに関する問題。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- スキーマ変更後の Flat JSON 拡張列における `unique_id` の競合。[#68279](https://github.com/StarRocks/starrocks/pull/68279)
- `OlapTableSink.complete()` におけるパーティションの同時アクセス問題。[#68853](https://github.com/StarRocks/starrocks/pull/68853)
- 手動でダウンロードしたクラスタスナップショットを復元する際のメタデータ追跡不整合。[#68368](https://github.com/StarRocks/starrocks/pull/68368)
- リポジトリパスが `/` で終わる場合に、バックアップパスに二重スラッシュが含まれる問題。[#68764](https://github.com/StarRocks/starrocks/pull/68764)
- `SHOW CREATE CATALOG` の出力において、OBS の AK/SK 認証情報がマスクされていなかった問題。[#65462](https://github.com/StarRocks/starrocks/pull/65462)

## 4.0.5

リリース日：2026年2月3日

### 改善点

- Paimon のバージョンを 1.3.1 に更新しました。[#67098](https://github.com/StarRocks/starrocks/pull/67098)
- DP 統計情報推定における欠落していた最適化を復元し、冗長な計算を削減しました。[#67852](https://github.com/StarRocks/starrocks/pull/67852)
- DP Join 並べ替えにおけるプルーニングを改善し、高コストな候補プランを早期にスキップできるようにしました。[#67828](https://github.com/StarRocks/starrocks/pull/67828)
- JoinReorderDP のパーティション列挙を最適化し、オブジェクト割り当てを削減するとともに、アトム数の上限（≤ 62）を追加しました。[#67643](https://github.com/StarRocks/starrocks/pull/67643)
- DP Join 並べ替えのプルーニングを最適化し、BitSet にチェックを追加してストリーム処理のオーバーヘッドを削減しました。[#67644](https://github.com/StarRocks/starrocks/pull/67644)
- DP 統計情報推定時に述語列の統計情報収集をスキップし、CPU オーバーヘッドを削減しました。[#67663](https://github.com/StarRocks/starrocks/pull/67663)
- 相関 Join の行数推定を最適化し、`Statistics` オブジェクトの再生成を回避しました。[#67773](https://github.com/StarRocks/starrocks/pull/67773)
- `Statistics.getUsedColumns` におけるメモリ割り当てを削減しました。[#67786](https://github.com/StarRocks/starrocks/pull/67786)
- 行数のみを更新する場合に、`Statistics` マップの不要なコピーを回避しました。[#67777](https://github.com/StarRocks/starrocks/pull/67777)
- クエリに集約が存在しない場合、集約プッシュダウン処理をスキップしてオーバーヘッドを削減しました。[#67603](https://github.com/StarRocks/starrocks/pull/67603)
- ウィンドウ関数における COUNT DISTINCT を改善し、複数 DISTINCT 集約の融合に対応するとともに、CTE 生成を最適化しました。[#67453](https://github.com/StarRocks/starrocks/pull/67453)
- Trino 方言で `map_agg` 関数をサポートしました。[#66673](https://github.com/StarRocks/starrocks/pull/66673)
- 物理プランニング時に LakeTablet のロケーション情報をバッチ取得できるようにし、共有データクラスタでの RPC 呼び出しを削減しました。[#67325](https://github.com/StarRocks/starrocks/pull/67325)
- shared-nothing クラスタにおいて Publish Version トランザクション用のスレッドプールを追加し、並行性を向上させました。[#67797](https://github.com/StarRocks/starrocks/pull/67797)
- LocalMetastore のロック粒度を最適化し、データベースレベルのロックをテーブルレベルのロックに置き換えました。[#67658](https://github.com/StarRocks/starrocks/pull/67658)
- MergeCommitTask のライフサイクル管理をリファクタリングし、タスクキャンセルをサポートしました。[#67425](https://github.com/StarRocks/starrocks/pull/67425)
- 自動クラスタスナップショットに対して実行間隔の設定をサポートしました。[#67525](https://github.com/StarRocks/starrocks/pull/67525)
- MemTrackerManager において、未使用の `mem_pool` エントリを自動的にクリーンアップするようにしました。[#67347](https://github.com/StarRocks/starrocks/pull/67347)
- ウェアハウスのアイドルチェック時に `information_schema` クエリを無視するようにしました。[#67958](https://github.com/StarRocks/starrocks/pull/67958)
- データ分布に応じて、Iceberg テーブルの書き込み時にグローバルシャッフルを動的に有効化できるようにしました。[#67442](https://github.com/StarRocks/starrocks/pull/67442)
- Connector Sink モジュール向けに Profile メトリクスを追加しました。[#67761](https://github.com/StarRocks/starrocks/pull/67761)
- Profile におけるロードスピルメトリクスの収集および表示を改善し、ローカル I/O とリモート I/O を区別しました。[#67527](https://github.com/StarRocks/starrocks/pull/67527)
- Async-Profiler のログレベルを Error に変更し、警告ログの繰り返し出力を防止しました。[#67297](https://github.com/StarRocks/starrocks/pull/67297)
- BE シャットダウン時に Starlet へ通知し、StarMgr に SHUTDOWN ステータスを報告するようにしました。[#67461](https://github.com/StarRocks/starrocks/pull/67461)

### バグ修正

以下の問題を修正しました：

- ハイフン（`-`）を含む正当なシンプルパスがサポートされていませんでした。[#67988](https://github.com/StarRocks/starrocks/pull/67988)
- JSON 型を含むグループキーに対して集約プッシュダウンが行われた場合に実行時エラーが発生する問題。[#68142](https://github.com/StarRocks/starrocks/pull/68142)
- JSON パス書き換えルールにより、パーティション述語で参照されているパーティション列が誤ってプルーニングされる問題。[#67986](https://github.com/StarRocks/starrocks/pull/67986)
- 統計情報を用いたシンプル集約の書き換え時に型不一致が発生する問題。[#67829](https://github.com/StarRocks/starrocks/pull/67829)
- パーティション Join におけるヒープバッファオーバーフローの潜在的な問題。[#67435](https://github.com/StarRocks/starrocks/pull/67435)
- 重い式をプッシュダウンする際に `slot_ids` が重複して生成される問題。[#67477](https://github.com/StarRocks/starrocks/pull/67477)
- 前提条件チェック不足により、ExecutionDAG の Fragment 接続でゼロ除算が発生する問題。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- 単一 BE 環境での Fragment 並列準備に起因する潜在的な問題。[#67798](https://github.com/StarRocks/starrocks/pull/67798)
- RawValuesSourceOperator に `set_finished` メソッドが存在せず、オペレーターが正しく終了しない問題。[#67609](https://github.com/StarRocks/starrocks/pull/67609)
- 列アグリゲータで DECIMAL256 型（精度 > 38）がサポートされておらず、BE がクラッシュする問題。[#68134](https://github.com/StarRocks/starrocks/pull/68134)
- DELETE 操作時にリクエストへ `schema_key` を含めていなかったため、共有データクラスタで Fast Schema Evolution v2 がサポートされていなかった問題。[#67456](https://github.com/StarRocks/starrocks/pull/67456)
- 同期マテリアライズドビューおよび従来のスキーマ変更において、共有データクラスタで Fast Schema Evolution v2 がサポートされていなかった問題。[#67443](https://github.com/StarRocks/starrocks/pull/67443)
- FE のダウングレード時にファイルバンドルが無効化されている場合、Vacuum が誤ってファイルを削除する可能性がある問題。[#67849](https://github.com/StarRocks/starrocks/pull/67849)
- MySQLReadListener における正常終了処理が正しくない問題。[#67917](https://github.com/StarRocks/starrocks/pull/67917)

## 4.0.4

リリース日：2026年1月16日

## 改善点

- クエリスケジューリング性能を向上させるため、Operator および Driver の並列 Prepare と、単一ノードでの Fragment 一括デプロイをサポートしました。 [#63956](https://github.com/StarRocks/starrocks/pull/63956)
- 大規模パーティションテーブルに対する `deltaRows` の計算を遅延評価（Lazy Evaluation）方式に最適化しました。 [#66381](https://github.com/StarRocks/starrocks/pull/66381)
- Flat JSON の処理を最適化し、逐次イテレーション方式の採用およびパス導出ロジックを改善しました。 [#66941](https://github.com/StarRocks/starrocks/pull/66941) [#66850](https://github.com/StarRocks/starrocks/pull/66850)
- Group Execution におけるメモリ使用量を削減するため、Spill Operator のメモリを早期に解放できるようにしました。 [#66669](https://github.com/StarRocks/starrocks/pull/66669)
- 文字列比較処理のオーバーヘッドを削減するロジックを最適化しました。 [#66570](https://github.com/StarRocks/starrocks/pull/66570)
- `GroupByCountDistinctDataSkewEliminateRule` および `SkewJoinOptimizeRule` におけるデータスキュー検出を強化し、ヒストグラムおよび NULL ベースの戦略をサポートしました。 [#66640](https://github.com/StarRocks/starrocks/pull/66640) [#67100](https://github.com/StarRocks/starrocks/pull/67100)
- Chunk 内の Column 所有権管理を Move セマンティクスで強化し、Copy-On-Write のオーバーヘッドを削減しました。 [#66805](https://github.com/StarRocks/starrocks/pull/66805)
- Shared-data クラスタ向けに FE の `TableSchemaService` を追加し、`MetaScanNode` を更新して Fast Schema Evolution v2 のスキーマ取得をサポートしました。 [#66142](https://github.com/StarRocks/starrocks/pull/66142) [#66970](https://github.com/StarRocks/starrocks/pull/66970)
- マルチ Warehouse 環境における Backend リソース統計および並列度（DOP）の計算をサポートし、リソース分離を強化しました。 [#66632](https://github.com/StarRocks/starrocks/pull/66632)
- StarRocks セッション変数 `connector_huge_file_size` により Iceberg の Split サイズを設定できるようになりました。 [#67044](https://github.com/StarRocks/starrocks/pull/67044)
- `QueryDumpDeserializer` において、ラベル形式（Label-formatted）の統計情報をサポートしました。 [#66656](https://github.com/StarRocks/starrocks/pull/66656)
- Shared-data クラスタで Full Vacuum を無効化するための FE 設定 `lake_enable_fullvacuum`（デフォルト：`false`）を追加しました。 [#63859](https://github.com/StarRocks/starrocks/pull/63859)
- lz4 依存関係を v1.10.0 にアップグレードしました。 [#67045](https://github.com/StarRocks/starrocks/pull/67045)
- 行数が 0 の場合に、サンプリングベースのカーディナリティ推定に対するフォールバックロジックを追加しました。 [#65599](https://github.com/StarRocks/starrocks/pull/65599)
- `array_sort` における Lambda Comparator の Strict Weak Ordering 特性を検証しました。 [#66951](https://github.com/StarRocks/starrocks/pull/66951)
- 外部テーブル（Delta / Hive / Hudi / Iceberg）のメタデータ取得に失敗した場合のエラーメッセージを改善し、根本原因を表示するようにしました。 [#66916](https://github.com/StarRocks/starrocks/pull/66916)
- クエリタイムアウト時に Pipeline の状態を Dump し、FE 側で `TIMEOUT` 状態としてクエリをキャンセルできるようにしました。 [#66540](https://github.com/StarRocks/starrocks/pull/66540)
- SQL ブラックリストのエラーメッセージに、マッチしたルールのインデックスを表示するようにしました。 [#66618](https://github.com/StarRocks/starrocks/pull/66618)
- `EXPLAIN` 出力に列統計情報のラベルを追加しました。 [#65899](https://github.com/StarRocks/starrocks/pull/65899)
- 正常終了（例：LIMIT 到達）時の「cancel fragment」ログを除外しました。 [#66506](https://github.com/StarRocks/starrocks/pull/66506)
- Warehouse がサスペンドされている場合の Backend ハートビート失敗ログを削減しました。 [#66733](https://github.com/StarRocks/starrocks/pull/66733)
- `ALTER STORAGE VOLUME` 構文で `IF EXISTS` をサポートしました。 [#66691](https://github.com/StarRocks/starrocks/pull/66691)

## バグ修正

以下の問題を修正しました：

- Low Cardinality 最適化下で `withLocalShuffle` が不足していたことにより、`DISTINCT` および `GROUP BY` の結果が不正になる問題を修正しました。 [#66768](https://github.com/StarRocks/starrocks/pull/66768)
- Lambda 式を含む JSON v2 関数におけるリライトエラーを修正しました。 [#66550](https://github.com/StarRocks/starrocks/pull/66550)
- 相関サブクエリ内の Null-aware Left Anti Join において、Partition Join が誤って適用される問題を修正しました。 [#67038](https://github.com/StarRocks/starrocks/pull/67038)
- Meta Scan のリライトルールにおける行数計算の誤りを修正しました。 [#66852](https://github.com/StarRocks/starrocks/pull/66852)
- 統計情報に基づく Meta Scan のリライト時に、Union Node の Nullable 属性が不一致となる問題を修正しました。 [#67051](https://github.com/StarRocks/starrocks/pull/67051)
- `PARTITION BY` および `ORDER BY` が指定されていない Ranking ウィンドウ関数において、最適化ロジックが原因で BE がクラッシュする問題を修正しました。 [#67094](https://github.com/StarRocks/starrocks/pull/67094)
- ウィンドウ関数と組み合わせた Group Execution Join において、誤った結果が返される可能性がある問題を修正しました。 [#66441](https://github.com/StarRocks/starrocks/pull/66441)
- 特定のフィルタ条件下で `PartitionColumnMinMaxRewriteRule` が誤った結果を生成する問題を修正しました。 [#66356](https://github.com/StarRocks/starrocks/pull/66356)
- 集約後の Union 処理において Nullable 属性の推論が誤っていた問題を修正しました。 [#65429](https://github.com/StarRocks/starrocks/pull/65429)
- 圧縮パラメータ処理時に `percentile_approx_weighted` がクラッシュする問題を修正しました。 [#64838](https://github.com/StarRocks/starrocks/pull/64838)
- 大きな文字列エンコーディングを伴う Spill 処理中にクラッシュが発生する問題を修正しました。 [#61495](https://github.com/StarRocks/starrocks/pull/61495)
- ローカル TopN のプッシュダウン時に `set_collector` が複数回呼び出され、クラッシュが発生する問題を修正しました。 [#66199](https://github.com/StarRocks/starrocks/pull/66199)
- LowCardinality リライトロジックにおける依存関係推論エラーを修正しました。 [#66795](https://github.com/StarRocks/starrocks/pull/66795)
- Rowset のコミット失敗時に Rowset ID がリークする問題を修正しました。 [#66301](https://github.com/StarRocks/starrocks/pull/66301)
- Metacache におけるロック競合の問題を修正しました。 [#66637](https://github.com/StarRocks/starrocks/pull/66637)
- 条件付き更新と列モード部分更新を併用した場合に、インジェストが失敗する問題を修正しました。 [#66139](https://github.com/StarRocks/starrocks/pull/66139)
- ALTER 操作中に Tablet が削除されることで、並行インポートが失敗する問題を修正しました。 [#65396](https://github.com/StarRocks/starrocks/pull/65396)
- RocksDB のイテレーションタイムアウトにより Tablet メタデータのロードが失敗する問題を修正しました。 [#65146](https://github.com/StarRocks/starrocks/pull/65146)
- Shared-data クラスタにおいて、テーブル作成および Schema Change 時に圧縮設定が適用されない問題を修正しました。 [#65673](https://github.com/StarRocks/starrocks/pull/65673)
- アップグレード時の Delete Vector における CRC32 互換性問題を修正しました。 [#65442](https://github.com/StarRocks/starrocks/pull/65442)
- Clone タスク失敗後のファイルクリーンアップ処理におけるステータスチェックロジックの誤りを修正しました。 [#65709](https://github.com/StarRocks/starrocks/pull/65709)
- `INSERT OVERWRITE` 実行後の統計情報収集ロジックが異常となる問題を修正しました。 [#65327](https://github.com/StarRocks/starrocks/pull/65327) [#65298](https://github.com/StarRocks/starrocks/pull/65298) [#65225](https://github.com/StarRocks/starrocks/pull/65225)
- FE 再起動後に外部キー制約が失われる問題を修正しました。 [#66474](https://github.com/StarRocks/starrocks/pull/66474)
- Warehouse 削除後にメタデータ取得が失敗する問題を修正しました。 [#66436](https://github.com/StarRocks/starrocks/pull/66436)
- 高い選択度のフィルタ条件下で、監査ログのスキャン統計が不正確になる問題を修正しました。 [#66280](https://github.com/StarRocks/starrocks/pull/66280)
- クエリエラー率メトリクスの計算ロジックが誤っていた問題を修正しました。 [#65891](https://github.com/StarRocks/starrocks/pull/65891)
- タスク終了時に MySQL 接続がリークする可能性がある問題を修正しました。 [#66829](https://github.com/StarRocks/starrocks/pull/66829)
- SIGSEGV クラッシュ発生時に BE ステータスが即時更新されない問題を修正しました。 [#66212](https://github.com/StarRocks/starrocks/pull/66212)
- LDAP ユーザーのログイン処理中に NPE が発生する問題を修正しました。 [#65843](https://github.com/StarRocks/starrocks/pull/65843)
- HTTP SQL リクエストでユーザー切り替えを行った際のエラーログが不正確な問題を修正しました。 [#65371](https://github.com/StarRocks/starrocks/pull/65371)
- TCP 接続再利用時に HTTP コンテキストがリークする問題を修正しました。 [#65203](https://github.com/StarRocks/starrocks/pull/65203)
- Follower から転送されたクエリにおいて、Profile ログに QueryDetail が欠落する問題を修正しました。 [#64395](https://github.com/StarRocks/starrocks/pull/64395)
- 監査ログに Prepare / Execute の詳細が記録されない問題を修正しました。 [#65448](https://github.com/StarRocks/starrocks/pull/65448)
- HyperLogLog のメモリ割り当て失敗によりクラッシュする問題を修正しました。 [#66747](https://github.com/StarRocks/starrocks/pull/66747)
- `trim` 関数のメモリ予約処理に関する問題を修正しました。 [#66477](https://github.com/StarRocks/starrocks/pull/66477) [#66428](https://github.com/StarRocks/starrocks/pull/66428)
- CVE-2025-66566 および CVE-2025-12183 に対応しました。 [#66453](https://github.com/StarRocks/starrocks/pull/66453) [#66362](https://github.com/StarRocks/starrocks/pull/66362) [#67053](https://github.com/StarRocks/starrocks/pull/67053)
- Exec Group Driver のサブミッション処理における競合状態を修正しました。 [#66099](https://github.com/StarRocks/starrocks/pull/66099)
- Pipeline のカウントダウン処理における use-after-free のリスクを修正しました。 [#65940](https://github.com/StarRocks/starrocks/pull/65940)
- キューがクローズされた際に `MemoryScratchSinkOperator` がハングする問題を修正しました。 [#66041](https://github.com/StarRocks/starrocks/pull/66041)
- ファイルシステムキャッシュのキー衝突問題を修正しました。 [#65823](https://github.com/StarRocks/starrocks/pull/65823)
- `SHOW PROC '/compactions'` におけるサブタスク数の表示誤りを修正しました。 [#67209](https://github.com/StarRocks/starrocks/pull/67209)
- Query Profile API が統一された JSON 形式を返さない問題を修正しました。 [#67077](https://github.com/StarRocks/starrocks/pull/67077)
- `getTable` の例外処理が不適切で、マテリアライズドビューのチェックに影響する問題を修正しました。 [#67224](https://github.com/StarRocks/starrocks/pull/67224)
- ネイティブテーブルとクラウドネイティブテーブルで `DESC` 文の `Extra` 列の出力が不一致となる問題を修正しました。 [#67238](https://github.com/StarRocks/starrocks/pull/67238)
- 単一ノード構成における競合状態の問題を修正しました。 [#67215](https://github.com/StarRocks/starrocks/pull/67215)
- サードパーティライブラリからのログ漏洩を修正しました。 [#67129](https://github.com/StarRocks/starrocks/pull/67129)
- REST Catalog の認証ロジック不備により認証に失敗する問題を修正しました。 [#66861](https://github.com/StarRocks/starrocks/pull/66861)

## 4.0.3

リリース日：2025 年 12 月 25 日

### 改善点

- STRUCT データ型に対する `ORDER BY` 句をサポートしました。[#66035](https://github.com/StarRocks/starrocks/pull/66035)
- プロパティ付き Iceberg ビューの作成をサポートし、`SHOW CREATE VIEW` の出力にプロパティを表示できるようになりました。[#65938](https://github.com/StarRocks/starrocks/pull/65938)
- `ALTER TABLE ADD/DROP PARTITION COLUMN` による Iceberg テーブルのパーティション Spec の変更をサポートしました。[#65922](https://github.com/StarRocks/starrocks/pull/65922)
- フレーム付きウィンドウ（例：`ORDER BY` / `PARTITION BY`）上での `COUNT/SUM/AVG(DISTINCT)` 集約をサポートし、最適化オプションを追加しました。[#65815](https://github.com/StarRocks/starrocks/pull/65815)
- 単一文字区切り文字に `memchr` を使用することで、CSV パース性能を最適化しました。[#63715](https://github.com/StarRocks/starrocks/pull/63715)
- ネットワークオーバーヘッド削減のため、Partial TopN を事前集約（Pre-Aggregation）フェーズにプッシュダウンするオプティマイザルールを追加しました。[#61497](https://github.com/StarRocks/starrocks/pull/61497)
- Data Cache の監視機能を強化しました：
  - メモリ／ディスクのクォータおよび使用量に関する新しいメトリクスを追加しました。[#66168](https://github.com/StarRocks/starrocks/pull/66168)
  - `api/datacache/stat` HTTP エンドポイントに Page Cache の統計情報を追加しました。[#66240](https://github.com/StarRocks/starrocks/pull/66240)
  - ネイティブテーブルのヒット率統計を追加しました。[#66198](https://github.com/StarRocks/starrocks/pull/66198)
- OOM 発生時に迅速にメモリを解放できるよう、Sort および Aggregation オペレーターを最適化しました。[#66157](https://github.com/StarRocks/starrocks/pull/66157)
- 共有データクラスターにおいて、CN が必要なスキーマをオンデマンドで取得できるよう、FE に `TableSchemaService` を追加しました。[#66142](https://github.com/StarRocks/starrocks/pull/66142)
- すべての依存する取り込みジョブが完了するまで履歴スキーマを保持するよう、Fast Schema Evolution を最適化しました。[#65799](https://github.com/StarRocks/starrocks/pull/65799)
- `filterPartitionsByTTL` を強化し、NULL パーティション値を正しく処理することで、全パーティションが誤って除外される問題を防止しました。[#65923](https://github.com/StarRocks/starrocks/pull/65923)
- `FusedMultiDistinctState` を最適化し、リセット時に関連する MemPool を解放するようにしました。[#66073](https://github.com/StarRocks/starrocks/pull/66073)
- Iceberg REST Catalog において、`ICEBERG_CATALOG_SECURITY` プロパティのチェックを大文字・小文字を区別しないようにしました。[#66028](https://github.com/StarRocks/starrocks/pull/66028)
- 共有データクラスター向けに、StarOS Service ID を取得する HTTP エンドポイント `GET /service_id` を追加しました。[#65816](https://github.com/StarRocks/starrocks/pull/65816)
- Kafka コンシューマー設定において、非推奨の `metadata.broker.list` を `bootstrap.servers` に置き換えました。[#65437](https://github.com/StarRocks/starrocks/pull/65437)
- Full Vacuum Daemon を無効化できる FE 設定項目 `lake_enable_fullvacuum`（デフォルト：false）を追加しました。[#66685](https://github.com/StarRocks/starrocks/pull/66685)
- lz4 ライブラリを v1.10.0 に更新しました。[#67080](https://github.com/StarRocks/starrocks/pull/67080)

### バグ修正

以下の問題を修正しました：

- `latest_cached_tablet_metadata` により、バッチ Publish 中にバージョンが誤ってスキップされる可能性がありました。[#66558](https://github.com/StarRocks/starrocks/pull/66558)
- 共有なしクラスター実行時に、`CatalogRecycleBin` 内の `ClusterSnapshot` の相対チェックが引き起こす可能性のある問題。[#66501](https://github.com/StarRocks/starrocks/pull/66501)
- Spill 処理中に、複雑なデータ型（ARRAY / MAP / STRUCT）を Iceberg テーブルへ書き込む際に BE がクラッシュする問題。[#66209](https://github.com/StarRocks/starrocks/pull/66209)
- writer の初期化または初回書き込みが失敗した場合に、Connector Chunk Sink がハングする可能性がある問題。[#65951](https://github.com/StarRocks/starrocks/pull/65951)
- Connector Chunk Sink において、`PartitionChunkWriter` の初期化失敗により、クローズ時に Null Pointer 参照が発生する問題。[#66097](https://github.com/StarRocks/starrocks/pull/66097)
- 存在しないシステム変数を設定した際に、エラーが返されず成功してしまう問題。[#66022](https://github.com/StarRocks/starrocks/pull/66022)
- Data Cache が破損している場合に、Bundle メタデータの解析が失敗する問題。[#66021](https://github.com/StarRocks/starrocks/pull/66021)
- 結果が空の場合に、MetaScan が count 列に対して 0 ではなく NULL を返す問題。[#66010](https://github.com/StarRocks/starrocks/pull/66010)
- 旧バージョンで作成されたリソースグループに対し、`SHOW VERBOSE RESOURCE GROUP ALL` が `default_mem_pool` ではなく NULL を表示する問題。[#65982](https://github.com/StarRocks/starrocks/pull/65982)
- `flat_json` テーブル設定を無効化した後、クエリ実行中に `RuntimeException` が発生する問題。[#65921](https://github.com/StarRocks/starrocks/pull/65921)
- 共有データクラスターにおいて、Schema Change 後に `min` / `max` 統計を MetaScan に書き換える際に発生する型不一致の問題。[#65911](https://github.com/StarRocks/starrocks/pull/65911)
- `PARTITION BY` および `ORDER BY` が指定されていない場合に、ランキングウィンドウ最適化によって BE がクラッシュする問題。[#67093](https://github.com/StarRocks/starrocks/pull/67093)
- 実行時フィルター統合時の `can_use_bf` 判定が不正確で、誤った結果やクラッシュを引き起こす可能性がある問題。[#67062](https://github.com/StarRocks/starrocks/pull/67062)
- 実行時 bitset フィルターをネストした OR 述語にプッシュダウンした際に、結果が不正になる問題。[#67061](https://github.com/StarRocks/starrocks/pull/67061)
- DeltaWriter 完了後の書き込みや flush 処理により、データ競合やデータ損失が発生する可能性がある問題。[#66966](https://github.com/StarRocks/starrocks/pull/66966)
- 単純集約を MetaScan に書き換える際、nullable 属性の不一致により実行エラーが発生する問題。[#67068](https://github.com/StarRocks/starrocks/pull/67068)
- MetaScan 書き換えルールにおける行数計算が正しくない問題。[#66967](https://github.com/StarRocks/starrocks/pull/66967)
- Tablet メタデータキャッシュの不整合により、バッチ Publish 中にバージョンが誤ってスキップされる可能性がある問題。[#66575](https://github.com/StarRocks/starrocks/pull/66575)
- HyperLogLog 処理において、メモリ割り当て失敗時のエラーハンドリングが不適切な問題。[#66827](https://github.com/StarRocks/starrocks/pull/66827)

## 4.0.2

リリース日：2025年12月4日

### 新機能

- 新しいリソースグループ属性 `mem_pool` を追加しました。複数のリソースグループが同じメモリプールを共有し、そのプールに対して共同のメモリ上限を適用できます。本機能は後方互換性があります。`mem_pool` が指定されていない場合は `default_mem_pool` が使用されます。[#64112](https://github.com/StarRocks/starrocks/pull/64112)

### 改善点

- File Bundling 有効時の Vacuum におけるリモートストレージアクセスを削減しました。[#65793](https://github.com/StarRocks/starrocks/pull/65793)
- File Bundling 機能が最新のタブレットメタデータをキャッシュするようになりました。[#65640](https://github.com/StarRocks/starrocks/pull/65640)
- 長い文字列を扱うシナリオでの安全性と安定性を向上しました。[#65433](https://github.com/StarRocks/starrocks/pull/65433) [#65148](https://github.com/StarRocks/starrocks/pull/65148)
- `SplitTopNAggregateRule` のロジックを最適化し、性能劣化を回避しました。[#65478](https://github.com/StarRocks/starrocks/pull/65478)
- Iceberg/DeltaLake と同様のテーブル統計収集戦略を他の外部データソースに適用し、単一テーブルの場合に不要な統計収集を行わないよう改善しました。[#65430](https://github.com/StarRocks/starrocks/pull/65430)
- Data Cache HTTP API `api/datacache/app_stat` に Page Cache 指標を追加しました。[#65341](https://github.com/StarRocks/starrocks/pull/65341)
- ORC ファイルの分割をサポートし、大規模 ORC ファイルの並列スキャンが可能になりました。[#65188](https://github.com/StarRocks/starrocks/pull/65188)
- 最適化エンジンに IF 述語の選択率推定を追加しました。[#64962](https://github.com/StarRocks/starrocks/pull/64962)
- FE が `DATE` および `DATETIME` 型に対する `hour`、`minute`、`second` の定数評価をサポートしました。[#64953](https://github.com/StarRocks/starrocks/pull/64953)
- 単純な集計を MetaScan に書き換える機能をデフォルトで有効化しました。[#64698](https://github.com/StarRocks/starrocks/pull/64698)
- 共有データクラスタにおける複数レプリカ割り当ての処理を改善し、信頼性を強化しました。[#64245](https://github.com/StarRocks/starrocks/pull/64245)
- 監査ログおよびメトリクスでキャッシュヒット率を公開しました。[#63964](https://github.com/StarRocks/starrocks/pull/63964)
- HyperLogLog またはサンプリングにより、ヒストグラムのバケットごとの重複除外数（NDV）推定を実施し、述語や JOIN に対してより正確な NDV を提供します。[#58516](https://github.com/StarRocks/starrocks/pull/58516)
- SQL 標準セマンティクスに準拠した FULL OUTER JOIN USING をサポートしました。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- オプティマイザのタイムアウト時にメモリ情報を出力して診断を支援します。[#65206](https://github.com/StarRocks/starrocks/pull/65206)

### バグ修正

以下の問題を修正しました：

- DECIMAL56 の `mod` 演算に関する問題。[#65795](https://github.com/StarRocks/starrocks/pull/65795)
- Iceberg のスキャンレンジ処理に関する問題。[#65658](https://github.com/StarRocks/starrocks/pull/65658)
- 一時パーティションおよびランダム bucket における MetaScan 書き換え問題。[#65617](https://github.com/StarRocks/starrocks/pull/65617)
- 透明なマテリアライズドビュー書き換え後に `JsonPathRewriteRule` が誤ったテーブルを参照する問題。[#65597](https://github.com/StarRocks/starrocks/pull/65597)
- `partition_retention_condition` が生成列を参照している場合のマテリアライズドビューのリフレッシュ失敗。[#65575](https://github.com/StarRocks/starrocks/pull/65575)
- Iceberg の min/max 値の型に関する問題。[#65551](https://github.com/StarRocks/starrocks/pull/65551)
- `enable_evaluate_schema_scan_rule=true` の場合に異なるデータベースを跨いで `information_schema.tables` および `views` をクエリする際の問題。[#65533](https://github.com/StarRocks/starrocks/pull/65533)
- JSON 配列比較における整数オーバーフロー。[#64981](https://github.com/StarRocks/starrocks/pull/64981)
- MySQL Reader が SSL をサポートしていない問題。[#65291](https://github.com/StarRocks/starrocks/pull/65291)
- SVE ビルド非互換性による ARM ビルド問題。[#65268](https://github.com/StarRocks/starrocks/pull/65268)
- bucket-aware 実行に基づくクエリが bucketed Iceberg テーブルでハングする可能性がある問題。[#65261](https://github.com/StarRocks/starrocks/pull/65261)
- OLAP テーブルスキャンでメモリ制限チェックが不足していたことによるエラー伝播およびメモリ安全性の問題。[#65131](https://github.com/StarRocks/starrocks/pull/65131)

### 動作の変更

- マテリアライズドビューが非アクティブ化されると、その依存マテリアライズドビューも再帰的に非アクティブ化されます。[#65317](https://github.com/StarRocks/starrocks/pull/65317)
- SHOW CREATE の生成時に、コメントやフォーマットを含む元のマテリアライズドビュー定義 SQL を使用します。[#64318](https://github.com/StarRocks/starrocks/pull/64318)

## 4.0.1

リリース日：2025年11月17日

### 改善点

- TaskRun セッション変数の処理を最適化し、既知の変数のみを処理するようにしました。 [#64150](https://github.com/StarRocks/starrocks/pull/64150)
- デフォルトで Iceberg および Delta Lake テーブルの統計情報をメタデータから収集できるようになりました。 [#64140](https://github.com/StarRocks/starrocks/pull/64140)
- bucket および truncate パーティション変換を使用する Iceberg テーブルの統計情報収集をサポートしました。 [#64122](https://github.com/StarRocks/starrocks/pull/64122)
- デバッグのために FE `/proc` プロファイルの確認をサポートしました。 [#63954](https://github.com/StarRocks/starrocks/pull/63954)
- Iceberg REST カタログでの OAuth2 および JWT 認証サポートを強化しました。 [#63882](https://github.com/StarRocks/starrocks/pull/63882)
- バンドルタブレットのメタデータ検証およびリカバリ処理を改善しました。 [#63949](https://github.com/StarRocks/starrocks/pull/63949)
- スキャン範囲のメモリ見積もりロジックを改善しました。 [#64158](https://github.com/StarRocks/starrocks/pull/64158)

### バグ修正

以下の問題を修正しました：

- バンドルタブレットを公開する際にトランザクションログが削除される問題を修正しました。 [#64030](https://github.com/StarRocks/starrocks/pull/64030)
- join 後にソートプロパティがリセットされないため、join アルゴリズムがソート特性を保持できない問題を修正しました。 [#64086](https://github.com/StarRocks/starrocks/pull/64086)
- 透過的なマテリアライズドビューのリライトに関連する問題を修正しました。 [#63962](https://github.com/StarRocks/starrocks/pull/63962)

### 動作変更

- Iceberg カタログに `enable_iceberg_table_cache` プロパティを追加し、Iceberg テーブルキャッシュを無効化して常に最新データを読み込むように設定可能にしました。 [#64082](https://github.com/StarRocks/starrocks/pull/64082)
- `INSERT ... SELECT` 実行前に外部テーブルをリフレッシュし、最新のメタデータを読み取るようにしました。 [#64026](https://github.com/StarRocks/starrocks/pull/64026)
- ロックテーブルスロット数を 256 に増加し、スロー・ロックログに `rid` フィールドを追加しました。 [#63945](https://github.com/StarRocks/starrocks/pull/63945)
- イベントベースのスケジューリングとの非互換性により、`shared_scan` を一時的に無効化しました。 [#63543](https://github.com/StarRocks/starrocks/pull/63543)
- Hive カタログのキャッシュ TTL のデフォルト値を 24 時間に変更し、未使用のパラメータを削除しました。 [#63459](https://github.com/StarRocks/starrocks/pull/63459)
- セッション変数および挿入列数に基づいて Partial Update モードを自動判定するようにしました。 [#62091](https://github.com/StarRocks/starrocks/pull/62091)

## 4.0.0

リリース日：2025年10月17日

### データレイク分析

- BE メタデータ用の Page Cache と Data Cache を統合し、スケーリングに適応的な戦略を採用。[#61640](https://github.com/StarRocks/starrocks/issues/61640)
- Iceberg 統計情報のメタデータファイル解析を最適化し、繰り返し解析を回避。[#59955](https://github.com/StarRocks/starrocks/pull/59955)
- Iceberg メタデータに対する COUNT/MIN/MAX クエリを最適化し、データファイルスキャンを効率的にスキップすることで、大規模パーティションテーブルの集約クエリ性能を大幅に向上させ、リソース消費を削減。[#60385](https://github.com/StarRocks/starrocks/pull/60385)
- プロシージャ `rewrite_data_files` により Iceberg テーブルの Compaction をサポート。
- 隠しパーティション（Hidden Partition）を持つ Iceberg テーブルをサポート（作成、書き込み、読み取りを含む）。[#58914](https://github.com/StarRocks/starrocks/issues/58914)
- Iceberg テーブル作成時のソートキー設定をサポートします。
- Iceberg テーブル向けのシンクパフォーマンスを最適化します。
  - Iceberg シンクは、メモリ使用量の最適化と小ファイル問題の解決のため、大規模オペレータのスパイリング、グローバルシャッフル、ローカルソートをサポートします。[#61963](https://github.com/StarRocks/starrocks/pull/61963)
  - Iceberg シンクは、Spill Partition Writer に基づくローカルソートを最適化し、書き込み効率を向上させます。[#62096](https://github.com/StarRocks/starrocks/pull/62096)
  - Iceberg シンクはパーティションのグローバルシャッフルをサポートし、小ファイルをさらに削減します。[#62123](https://github.com/StarRocks/starrocks/pull/62123)
- Iceberg テーブルのバケット対応実行を強化し、バケット化テーブルの並行処理能力と分散処理能力を向上させました。[#61756](https://github.com/StarRocks/starrocks/pull/61756)
- Paimon カタログで TIME データ型をサポート。[#58292](https://github.com/StarRocks/starrocks/pull/58292)
- Iceberg バージョンを 1.10.0 にアップグレード。[#63667](https://github.com/StarRocks/starrocks/pull/63667)

### セキュリティと認証

- JWT 認証と Iceberg REST Catalog を利用するシナリオで、StarRocks は REST Session Catalog を介してユーザーログイン情報を Iceberg に透過し、その後のデータアクセス認証をサポート。[#59611](https://github.com/StarRocks/starrocks/pull/59611) [#58850](https://github.com/StarRocks/starrocks/pull/58850)
- Iceberg カタログ用の Vended Credential をサポート。
- Group Provider 経由で取得した外部グループへの StarRocks 内部ロールの付与をサポート。[#63385](https://github.com/StarRocks/starrocks/pull/63385) [#63258](https://github.com/StarRocks/starrocks/pull/63258)
- 外部テーブルのリフレッシュ権限を制御するため、外部テーブルに REFRESH 権限を追加しました。[#63385](https://github.com/StarRocks/starrocks/pull/62636)

### ストレージ最適化とクラスタ管理

- 共有データクラスタのクラウドネイティブテーブルにファイルバンドル（File Bundling）最適化を導入。ロード、Compaction、Publish 操作によって生成されるデータファイルを自動的にバンドルし、外部ストレージシステムへの高頻度アクセスによる API コストを削減。ファイルバンドリングは、v4.0 以降で作成されたテーブルに対してデフォルトで有効化されています。[#58316](https://github.com/StarRocks/starrocks/issues/58316)
- 複数テーブル間の Write-Write トランザクション（Multi-Table Write-Write Transaction）をサポートし、INSERT、UPDATE、DELETE 操作のアトミックコミットを制御可能。Stream Load および INSERT INTO インターフェイスをサポートし、ETL やリアルタイム書き込みシナリオにおけるクロステーブルの一貫性を保証。[#61362](https://github.com/StarRocks/starrocks/issues/61362)
- Routine Load で Kafka 4.0 をサポート。
- 共有なしクラスタの主キーテーブルに対する全文インバーテッドインデックスをサポート。
- 集約テーブルの集約キーの変更をサポート。[#62253](https://github.com/StarRocks/starrocks/issues/62253)s
- カタログ、データベース、テーブル、ビュー、マテリアライズドビューの名前に対して大文字小文字を区別しない処理を有効化可能。[#61136](https://github.com/StarRocks/starrocks/pull/61136)
- 共有データクラスタにおける Compute  Node のブラックリスト化をサポート。[#60830](https://github.com/StarRocks/starrocks/pull/60830)
- グローバル接続 ID をサポート。[#57256](https://github.com/StarRocks/starrocks/pull/57276)
- 復元可能な削除済みメタデータを表示するため、Information Schema に `recyclebin_catalogs` メタデータビューを追加しました。[#51007](https://github.com/StarRocks/starrocks/pull/51007)

### クエリと性能改善

- DECIMAL256 データ型をサポートし、精度の上限を 38 ビットから 76 ビットに拡張。256 ビットのストレージにより、高精度が求められる金融や科学計算シナリオに柔軟に対応し、大規模集約や高次演算での DECIMAL128 の精度オーバーフロー問題を効果的に緩和。[#59645](https://github.com/StarRocks/starrocks/issues/59645)
- 基本演算子のパフォーマンスを改善しました。[#61691](https://github.com/StarRocks/starrocks/issues/61691) [#61632](https://github.com/StarRocks/starrocks/pull/61632) [#62585](https://github.com/StarRocks/starrocks/pull/62585) [#61405](https://github.com/StarRocks/starrocks/pull/61405)  [#61429](https://github.com/StarRocks/starrocks/pull/61429)
- JOIN および AGG 演算子の性能を最適化。[#61691](https://github.com/StarRocks/starrocks/issues/61691)
- [Preview] SQL Plan Manager を導入し、クエリとクエリプランをバインド可能に。これにより、システム状態（データ更新や統計更新）の変化によるクエリプランの変動を防止し、クエリ性能を安定化。[#56310](https://github.com/StarRocks/starrocks/issues/56310)
- Partition-wise Spillable Aggregate/Distinct 演算子を導入し、従来のソートベース集約による Spill 実装を置き換え。複雑かつ高カーディナリティの GROUP BY シナリオで集約性能を大幅に改善し、読み書き負荷を削減。[#60216](https://github.com/StarRocks/starrocks/pull/60216)
- Flat JSON V2：
  - テーブルレベルで Flat JSON を設定可能。[#57379](https://github.com/StarRocks/starrocks/pull/57379)
  - JSON カラム型ストレージを強化：V1 メカニズムを維持しつつ、Page レベルおよび Segment レベルのインデックス（ZoneMap、ブルームフィルター）、遅延マテリアライゼーションを伴う述語プッシュダウン、辞書エンコーディング、低カーディナリティのグローバル辞書の統合を追加し、実行効率を大幅に向上。[#60953](https://github.com/StarRocks/starrocks/issues/60953)
- STRING データ型向けに適応型 ZoneMap インデックス作成戦略をサポート。[#61960](https://github.com/StarRocks/starrocks/issues/61960)
- クエリ可視性の強化:
  - EXPLAIN ANALYZEの出力を最適化し、実行メトリクスをグループ別および演算子別に表示することで可読性を向上。[#63326](https://github.com/StarRocks/starrocks/pull/63326)
  - `QueryDetailActionV2` および `QueryProfileActionV2` が JSON 形式をサポートし、クロス FE クエリ機能が強化されました。[#63235](https://github.com/StarRocks/starrocks/pull/63235)
  - 全 FE にわたるクエリプロファイル情報の取得をサポート。[#61345](https://github.com/StarRocks/starrocks/pull/61345)
  - SHOW PROCESSLIST 文でカタログ、クエリ ID などの情報を表示。[#62552](https://github.com/StarRocks/starrocks/pull/62552)
  - クエリキューとプロセス監視を強化し、Running/Pending のステータス表示をサポート。[#62261](https://github.com/StarRocks/starrocks/pull/62261)
- マテリアライズドビューの再作成では、元のテーブルの分散キーとソートキーを考慮し、最適なマテリアライズドビューの選択を改善。[#62830](https://github.com/StarRocks/starrocks/pull/62830)

### 関数と SQL 構文

- 以下の関数を追加：
  - `bitmap_hash64` [#56913](https://github.com/StarRocks/starrocks/pull/56913)
  - `bool_or` [#57414](https://github.com/StarRocks/starrocks/pull/57414)
  - `strpos` [#57278](https://github.com/StarRocks/starrocks/pull/57287)
  - `to_datetime` および `to_datetime_ntz` [#60637](https://github.com/StarRocks/starrocks/pull/60637)
  - `regexp_count` [#57182](https://github.com/StarRocks/starrocks/pull/57182)
  - `tokenize` [#58965](https://github.com/StarRocks/starrocks/pull/58965)
  - `format_bytes` [#61535](https://github.com/StarRocks/starrocks/pull/61535)
  - `encode_sort_key` [#61781](https://github.com/StarRocks/starrocks/pull/61781)
  - `column_size` および `column_compressed_size`  [#62481](https://github.com/StarRocks/starrocks/pull/62481)
- 以下の構文拡張を提供：
  - CREATE ANALYZE FULL TABLE で IF NOT EXISTS キーワードをサポート。[#59789](https://github.com/StarRocks/starrocks/pull/59789)
  - SELECT で EXCLUDE 句をサポート。[#57411](https://github.com/StarRocks/starrocks/pull/57411/files)
  - 集約関数で FILTER 句をサポートし、条件付き集約の可読性と実行効率を向上。[#58937](https://github.com/StarRocks/starrocks/pull/58937)

### 動作変更

- マテリアライズドビューのパラメータ `auto_partition_refresh_number` のロジックを調整し、自動リフレッシュか手動リフレッシュかにかかわらず、リフレッシュ対象のパーティション数を制限します。[#62301](https://github.com/StarRocks/starrocks/pull/62301)
- Flat JSON がデフォルトで有効化されました。[#62097](https://github.com/StarRocks/starrocks/pull/62097)
- システム変数 `enable_materialized_view_agg_pushdown_rewrite` のデフォルト値が `true` に設定され、マテリアライズドビューのクエリ書き換えにおける集計プッシュダウンがデフォルトで有効化されました。[#60976](https://github.com/StarRocks/starrocks/pull/60976)
- `information_schema.materialized_views` 内のいくつかの列の型を変更し、対応するデータとの整合性を高めました。[#60054](https://github.com/StarRocks/starrocks/pull/60054)
- `split_part` 関数は区切り文字が一致しない場合に NULL を返すように変更。[#56967](https://github.com/StarRocks/starrocks/pull/56967)
- CTAS/CREATE MATERIALIZED VIEW で固定長 CHAR を STRING に置き換え、誤った列長推論によるマテリアライズドビュー更新失敗を回避。[#63114](https://github.com/StarRocks/starrocks/pull/63114) [#63114](https://github.com/StarRocks/starrocks/pull/63114) [#62476](https://github.com/StarRocks/starrocks/pull/62476)
- データキャッシュ関連の構成が簡素化されました。[#61640](https://github.com/StarRocks/starrocks/issues/61640)
  - `datacache_mem_size` および `datacache_disk_size` が有効になりました。
  - `storage_page_cache_limit`、`block_cache_mem_size`、`block_cache_disk_size` は非推奨となりました。
- Hive および Iceberg のメタデータキャッシュに使用するメモリリソースを制限する新しいカタログプロパティを追加（Hive 用 `remote_file_cache_memory_ratio`、Iceberg 用 `iceberg_data_file_cache_memory_usage_ratio` および `iceberg_delete_file_cache_memory_usage_ratio`）。デフォルト値を `0.1`（10%）に設定。メタデータキャッシュの TTL を24時間に調整。[#63459](https://github.com/StarRocks/starrocks/pull/63459) [#63373](https://github.com/StarRocks/starrocks/pull/63373) [#61966](https://github.com/StarRocks/starrocks/pull/61966) [#62288](https://github.com/StarRocks/starrocks/pull/62288)
- SHOW DATA DISTRIBUTION は、同じバケットシーケンス番号を持つすべてのマテリアライズドインデックスの統計情報を統合しなくなりました。マテリアライズドインデックスレベルでのデータ分散のみを表示します。[#59656](https://github.com/StarRocks/starrocks/pull/59656)
- 自動バケットテーブルのデフォルトバケットサイズを4GBから1GBに変更し、パフォーマンスとリソース利用率を改善しました。[#63168](https://github.com/StarRocks/starrocks/pull/63168)
- システムは対応するセッション変数と INSERT 文の列数に基づいて部分更新モードを決定します。[#62091](https://github.com/StarRocks/starrocks/pull/62091)
- Information Schema 内の `fe_tablet_schedules` ビューを最適化しました。[#62073](https://github.com/StarRocks/starrocks/pull/62073) [#59813](https://github.com/StarRocks/starrocks/pull/59813)
  - `TABLET_STATUS` 列を `SCHEDULE_REASON` に、`CLONE_SRC` 列を `SRC_BE_ID` に、`CLONE_DEST` 列を `DEST_BE_ID` に名称変更しました。
  - `CREATE_TIME`、`SCHEDULE_TIME` および `FINISH_TIME` 列のデータ型を `DOUBLE` から `DATETIME` に変更しました。
- 一部の FE メトリクスに `is_leader` ラベルを追加しました。[#63004](https://github.com/StarRocks/starrocks/pull/63004)
- Microsoft Azure Blob Storage および Data Lake Storage Gen 2 をオブジェクト ストレージとして使用する共有データクラスターは、v4.0 へのアップグレード後にデータキャッシュの障害が発生します。システムは自動的にキャッシュを再読み込みします。
