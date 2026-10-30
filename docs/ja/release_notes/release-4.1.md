---
displayed_sidebar: docs
description: "StarRocks 4.1 リリースノート：マルチテナントのレンジベース tablet 自動分割、大容量 tablet サポート（100 GB 目標）、Fast Schema Evolution V2 など..."
---

# StarRocks バージョン 4.1 {#starrocks-version-41}

:::danger
**コンテナイメージの問題（v4.1.0）**

v4.1.0 コンテナイメージのロード順序が不安定な問題により、コンテナ環境では BE プロセスが確実に起動しない場合があります。**コンテナ環境のユーザーは v4.1.0 へのアップグレードをNOT行ってください。**この修正を含む v4.1.1（[#71825](https://github.com/StarRocks/starrocks/pull/71825)）をお待ちください。
:::

:::warning
**ダウングレードに関する注意事項**

- StarRocks を v4.1 にアップグレードした後、v4.0.6 より前の v4.0 バージョンにダウングレードNOTしてください。

  v4.1 で導入された内部的なデータレイアウトの変更（tablet の分割および分散メカニズムに関連）により、v4.1 にアップグレードされたクラスタでは、以前のバージョンと完全には互換性のないメタデータおよびストレージ構造が生成される場合があります。そのため、v4.1 からのダウングレードは v4.0.6 以降にのみサポートされます。v4.0.6 より前のバージョンへのダウングレードはサポートされていません。これは、以前のバージョンが tablet のレイアウトおよび分散メタデータをどのように解釈するかに関する後方互換性の制約によるものです。
:::

## 4.1.7 {#417}

リリース日：2026年10月26日

### 動作の変更 {#behavior-changes}

- ローカル（共有なし）の主キーのインサートでは、デフォルトでイミュータブルな L1 および L2 の永続性インデックスファイルに対する重複キーのチェックが行われなくなりました。新しい BE 設定項目 `skip_pindex_l1_l2_check_in_insert` のデフォルトは `true` であり、L0（memtable）のチェックは常に実行されます。以前の動作に戻すには `false` に設定してください。[#81304](https://github.com/StarRocks/starrocks/pull/81304)
- BE の JVM メトリクスはデフォルトで有効になりました（`enable_jvm_metrics` のデフォルトが `true` になりました）。これらは JVM が存在する場合にのみ収集されるため、有効にしても、JVM を他に必要としない BE 上で JVM が起動することはなくなりました。[#81113](https://github.com/StarRocks/starrocks/pull/81113) [#81074](https://github.com/StarRocks/starrocks/pull/81074)
- `spill_max_dir_bytes_ratio` のデフォルト値が `0.8` から `0.5` に引き下げられ、スピルは各スピルディレクトリの容量の最大 50% までしか使用しなくなりました。[#80189](https://github.com/StarRocks/starrocks/pull/80189)
- Lake レプリケーションのファイルコピーは、専用の CN 全体のプールで実行されるようになりました。`lake_replication_file_copy_threads` のデフォルトは `0`（4 × CPU コア数を意味していました）ではなく `16` となり、新しい `lake_replication_max_parallel_files_per_tablet`（デフォルト `4`）が、1 つの tablet に対して並行してコピーされるファイル数を制限します。[#78078](https://github.com/StarRocks/starrocks/pull/78078)
- 共有データの主キーテーブルタブレットの削除トリガーによるベースコンパクションは、新しい BE 設定項目 `lake_pk_compaction_base_enable_delete_trigger`（デフォルトは `false`）によりオプトイン方式になりました。[#79926](https://github.com/StarRocks/starrocks/pull/79926)
- `pk_index_parallel_execution_threadpool_max_threads` が `0` の場合、主キーインデックスの並列実行プールのサイズは、CPU コア数の半分ではなく all コア数に設定されます。[#80460](https://github.com/StarRocks/starrocks/pull/80460)
- `CAST(x AS CHAR(N))` は、MySQL と同様に、結果を `N` 文字に切り詰めるようになりました。FE での定数畳み込みと BE での実行の両方にこの切り詰めが適用されます。[#75909](https://github.com/StarRocks/starrocks/pull/75909)
- 設定項目やタスク定義が表示またはログに記録される箇所では、認証情報の値がマスクされます：`ADMIN SHOW FRONTEND CONFIG`、FE の `/variable` ページ、BE の `/varz`、`information_schema.be_configs`、`task_run_history` にアーカイブされるタスク実行定義、および FE と BE の HTTP ログに書き込まれるリクエスト URI とヘッダー。`ADMIN SET FRONTEND CONFIG`、FE の `/api/_set_config` エンドポイント、BE の `/api/update_config` エンドポイントを通じて設定された値も同様にマスクされます。[#80097](https://github.com/StarRocks/starrocks/pull/80097) [#80040](https://github.com/StarRocks/starrocks/pull/80040) [#80492](https://github.com/StarRocks/starrocks/pull/80492) [#80444](https://github.com/StarRocks/starrocks/pull/80444) [#80280](https://github.com/StarRocks/starrocks/pull/80280) [#80447](https://github.com/StarRocks/starrocks/pull/80447) [#80515](https://github.com/StarRocks/starrocks/pull/80515) [#80203](https://github.com/StarRocks/starrocks/pull/80203)
- FE の HTTP エンドポイント `/service_id`、`/dump`、`/dump_starmgr` は管理者ユーザーが必要になりました。[#80749](https://github.com/StarRocks/starrocks/pull/80749) [#80701](https://github.com/StarRocks/starrocks/pull/80701)

### 改善 {#improvements}

- テーブルプロパティ `enable_statistic_auto_collect` を追加しました。`CREATE TABLE` または `ALTER TABLE` を使用して `false` に設定すると、自動統計収集の対象からテーブルを除外できます。データロードによってトリガーされる収集には影響しません。[#81027](https://github.com/StarRocks/starrocks/pull/81027)
- 新しい FE 設定項目 `authentication_ldap_case_insensitive` を `true`（デフォルトは `false`）に設定することで、LDAP および Active Directory のユーザー名とグループ名を大文字小文字を区別せずにマッチングできるようになりました。[#79255](https://github.com/StarRocks/starrocks/pull/79255)
- グループプロバイダーのプロパティをその場で更新するための `ALTER GROUP PROVIDER` を追加しました。[#79166](https://github.com/StarRocks/starrocks/pull/79166)
- Query Queue V2 では、`query_queue_mem_used_pct_limit` がクエリを実行するすべてのノードに適用されます。[#81026](https://github.com/StarRocks/starrocks/pull/81026)
- 並列リバースルックアップとトゥームストーン sstable パスを使用することで、publish 時の大規模な主キーインデックスの削除が高速化されました。[#76785](https://github.com/StarRocks/starrocks/pull/76785)
- 検索対象の `CASE WHEN` は、各分岐がそれに到達した行に対してのみ評価されるようになりました。[#79484](https://github.com/StarRocks/starrocks/pull/79484)
- `convert_tz` は、すべての行に対してタイムゾーンを変換する代わりに、夏時間オフセットのルックアップをキャッシュします。[#78459](https://github.com/StarRocks/starrocks/pull/78459)
- `UNNEST` は、null 許容の配列に対してゼロコピーとなり、`LEFT JOIN` を使用する場合は、入力チャンク全体を具体化するのではなく、一度に 1 チャンクずつ複数の配列を展開するようになりました。また、残りのテーブル関数はメモリ不足に対して保護されています。[#79221](https://github.com/StarRocks/starrocks/pull/79221) [#79222](https://github.com/StarRocks/starrocks/pull/79222)
- フラグメントのプランは、all のインスタンスリクエストに対して一度だけシリアライズされます。[#80716](https://github.com/StarRocks/starrocks/pull/80716)
- Colocate の集計関数とウィンドウ関数を、range-colocate テーブルに対して計画できるようになりました。[#79946](https://github.com/StarRocks/starrocks/pull/79946)
- メインブランチから tablet の事前分割の改善をバックポートしました。これには、サンプルベースの tablet の事前分割がスキャンするデータ量を制限する新しい FE 設定項目（`tablet_pre_split_data_tier_scan_byte_limit`、`tablet_pre_split_data_tier_min_scan_files`、`tablet_pre_split_data_tier_max_scan_files`、`tablet_pre_split_meta_tier_footer_read_parallelism`）が含まれます。[#79949](https://github.com/StarRocks/starrocks/pull/79949)

### セキュリティ {#security}

- pprof を release/20261012 にアップグレードしました。[#80439](https://github.com/StarRocks/starrocks/pull/80439)
- lz4-java を 1.11.4 にアップグレードしました。[#80319](https://github.com/StarRocks/starrocks/pull/80319)
- BouncyCastle を 1.86 に、Jackson を 2.21.7 に、zstd-jni を 1.5.7-15 に、Parquet を 1.18.1 にアップグレードしました。[#80248](https://github.com/StarRocks/starrocks/pull/80248)
- Broker で使用されている Tomcat を 9.0.121 にアップグレードしました。[#79719](https://github.com/StarRocks/starrocks/pull/79719)
- ZooKeeper を 3.8.7 および 3.9.6 にアップグレードしました。[#79707](https://github.com/StarRocks/starrocks/pull/79707)
- `lookup_string` の引数は SQL インジェクションを防ぐためにエスケープされ、その内部クエリは呼び出し元のアイデンティティで実行されます。[#79991](https://github.com/StarRocks/starrocks/pull/79991) [#79959](https://github.com/StarRocks/starrocks/pull/79959)
- FE の web コンソールは、`/system` ページや `/query` ページを含め、管理者やホストが提供した値およびテーブルセルを HTML エンコードするようになり、`/proc_profile` の `node` パラメーターを検証・エンコードするようになりました。[#80656](https://github.com/StarRocks/starrocks/pull/80656) [#80639](https://github.com/StarRocks/starrocks/pull/80639) [#80582](https://github.com/StarRocks/starrocks/pull/80582) [#80565](https://github.com/StarRocks/starrocks/pull/80565) [#80689](https://github.com/StarRocks/starrocks/pull/80689)

### バグ修正 {#bug-fixes}

以下の問題が修正されました：

- Iceberg の equality delete がパーティションの範囲に限定されていませんでした。[#81139](https://github.com/StarRocks/starrocks/pull/81139)
- 空き容量があるにもかかわらず、Datacache のメモリ書き込みが eviction の確率によって拒否されることがありました。[#80542](https://github.com/StarRocks/starrocks/pull/80542)
- パイプラインソースのパーティションタイプにバケットプロパティが引き継がれていませんでした。[#80099](https://github.com/StarRocks/starrocks/pull/80099)
- 送信側ですでに整列済みだった bucket shuffle join の入力が再パーティション化されていました。[#79769](https://github.com/StarRocks/starrocks/pull/79769)
- `EXCEPT`/`INTERSECT` と hash join の入力間でドライバーのパーティション化が整合していませんでした。[#78920](https://github.com/StarRocks/starrocks/pull/78920)
- `yearweek` が不正な `str_to_date` 入力で BE をクラッシュさせていました。[#78358](https://github.com/StarRocks/starrocks/pull/78358)
- spill チャネルがクローズ中であっても、キューに入れられた spill データタスクが実行され続けることがありました。[#80252](https://github.com/StarRocks/starrocks/pull/80252)
- Lake compaction の読み取りバッファがワーカーごとのメモリ予算によって制限されていませんでした。[#80229](https://github.com/StarRocks/starrocks/pull/80229)
- 状態をラップできなかった Java UDAF のマージが、失敗する代わりに `NULL` を返していました。[#80098](https://github.com/StarRocks/starrocks/pull/80098)
- 失敗した spill の追加操作やパーティション分割が失われることがありました。[#79457](https://github.com/StarRocks/starrocks/pull/79457)
- `char` が符号なしであるプラットフォームでは、ORC の `TINYINT` の最小値／最大値統計が誤っていました。[#81086](https://github.com/StarRocks/starrocks/pull/81086)
- flat JSON のパス導出トライが無制限でした。[#80316](https://github.com/StarRocks/starrocks/pull/80316)
- compaction が読み取りレイアウトを導出する際に、スカラーの flat JSON サブカラムが削除されることがありました。[#80295](https://github.com/StarRocks/starrocks/pull/80295)
- スキャン述語ビルダーが唯一の連言を畳み込んで消去した際に、スキャン対象の列が出力されていませんでした。[#80572](https://github.com/StarRocks/starrocks/pull/80572)
- `PSlotDescriptor.global_dict_words` がバイト型として宣言されておらず、UTF-8 の警告が発生していました。[#78657](https://github.com/StarRocks/starrocks/pull/78657)
- `update_memory_limit_percent` によるプライマリインデックスキャッシュのサイズ変更で 100 による除算が行われていませんでした。[#81067](https://github.com/StarRocks/starrocks/pull/81067)
- `TRUE` の透過モードの MV において、ベーステーブルが削除されると MV のスキャンが停止していました。[#81088](https://github.com/StarRocks/starrocks/pull/81088)
- 範囲外のベクトル検索クエリベクトルが、拒否される代わりに BE を異常終了させていました。[#78658](https://github.com/StarRocks/starrocks/pull/78658)
- ANN 距離列が、再利用されたスキャンチャンクを許容していませんでした。[#78686](https://github.com/StarRocks/starrocks/pull/78686)
- flat JSON 列のルートに対する `json_query` がドキュメント全体を再構築していませんでした。[#80772](https://github.com/StarRocks/starrocks/pull/80772)
- `tokenize()` の結果のサイズが、行数ではなくビューアーのデータ列によって決まっていました。[#80337](https://github.com/StarRocks/starrocks/pull/80337)
- MV のリフレッシュがスケジュールしたタスクが、リフレッシュによって MV が非アクティブ化された際に失われていました。これにより、プランナーロックと Hive `INSERT` の変更に関する 2 つの追加修正も行われます。[#81033](https://github.com/StarRocks/starrocks/pull/81033)
- `INSERT` が Hive のソース行を見落とすことがあり、`SELECT *` がリソースマッピングの Hive 列を削除していました。[#81032](https://github.com/StarRocks/starrocks/pull/81032)
- スコープをまたぐ再帰的 CTE 参照が、拒否される代わりにスタックオーバーフローを引き起こしていました。[#78533](https://github.com/StarRocks/starrocks/pull/78533)
- Ranger の監査設定が壊れていると catalog のアクセスが失敗し、また Solr の監査に必要な ZooKeeper クラスが同梱されていませんでした。[#81075](https://github.com/StarRocks/starrocks/pull/81075)
- 非参照ベーステーブルに対するロックタイムアウトで MV のリフレッシュが失敗していませんでした。また `inspect_mv_refresh_info` が、保持しているロックの下でベーステーブルのロックを取得していました。[#81036](https://github.com/StarRocks/starrocks/pull/81036)
- `DATETIME` のドメイン外の統計値の境界が拒否されていませんでした。[#79388](https://github.com/StarRocks/starrocks/pull/79388)
- `INT64_MAX` のしきい値を超える JSON の数値を比較すると例外がスローされていました。[#78969](https://github.com/StarRocks/starrocks/pull/78969)
- 選択範囲が選択元の列より長くないことを確認するアサーションが失敗することがありました。[#80324](https://github.com/StarRocks/starrocks/pull/80324)
- 保持しきれないほど大きい `array_generate` の範囲が、拒否される代わりに BE を異常終了させていました。[#80205](https://github.com/StarRocks/starrocks/pull/80205)
- 定数の JSON/VARIANT から MAP へのキャストが、その入力と同じだけ保持されていませんでした。[#80679](https://github.com/StarRocks/starrocks/pull/80679)
- スカラーシャトルが、複製する必要がある dict オペレーターを登録していませんでした。[#79310](https://github.com/StarRocks/starrocks/pull/79310)
- 高速スキーマ変更後にグローバル辞書が無効化されていませんでした。[#78599](https://github.com/StarRocks/starrocks/pull/78599)
- CTE の consume プロジェクトで、繰り返される列が複製されていませんでした。[#80158](https://github.com/StarRocks/starrocks/pull/80158)
- 緩和された辞書が符号なしバイト順でマージされていませんでした。[#78659](https://github.com/StarRocks/starrocks/pull/78659)
- 集計の構造体フィールドが `GROUP BY` のチェックによって拒否されていました。[#79649](https://github.com/StarRocks/starrocks/pull/79649)
- すべての MV の非アクティブ化とそれに伴うバージョンマップのクリアがジャーナル化されていたわけではありませんでした。[#81030](https://github.com/StarRocks/starrocks/pull/81030)
- 物理パーティション ID のルックアップによって、MV のバージョンマップ修復がスキップされていました。[#78469](https://github.com/StarRocks/starrocks/pull/78469)
- 分析結果のチャンクが公開される前に、ウィンドウ関数のエラーがチェックされていませんでした。[#81022](https://github.com/StarRocks/starrocks/pull/81022)
- LIST パーティション列の MIN/MAX が、データが保持していない可能性のあるパーティション値から読み取られることがありました。[#80558](https://github.com/StarRocks/starrocks/pull/80558)
- 1つのテーブルで失敗した後、analyzeジョブが残りのテーブルの収集を停止していました。[#81028](https://github.com/StarRocks/starrocks/pull/81028)
- ベーステーブルのパーティションの変動とバッチリフレッシュの状況下で、MVのリフレッシュと鮮度が誤っていました。[#81034](https://github.com/StarRocks/starrocks/pull/81034)
- 3件の小規模な既存のFEメタデータ並行処理の問題。[#81037](https://github.com/StarRocks/starrocks/pull/81037)
- skew join v2がシャッフルエクスチェンジを置き換えた際に、グローバル辞書の式が失われていました。[#78672](https://github.com/StarRocks/starrocks/pull/78672)
- 削除されたベースパーティションより長く存在していたMVパーティションがリフレッシュされていませんでした。[#79057](https://github.com/StarRocks/starrocks/pull/79057)
- 整列割り当てに失敗した際に `std::bad_alloc` がスローされていませんでした。[#79054](https://github.com/StarRocks/starrocks/pull/79054)
- ALTER の完了レポートがジョブに一致しておらず、キューに入れられた ALTER タスクが期限切れになっていました。[#81018](https://github.com/StarRocks/starrocks/pull/81018)
- スワップインされた一時パーティションの tablet が強制削除されていました。[#81014](https://github.com/StarRocks/starrocks/pull/81014)
- 残存した `INSERT` ジョブが、そのトランザクションに応じて完了またはキャンセルされていませんでした。[#80711](https://github.com/StarRocks/starrocks/pull/80711)
- インクリメンタルMVの複合型行識別キーが拒否されていませんでした。[#79312](https://github.com/StarRocks/starrocks/pull/79312)
- `greplog` は年が先頭に付いたglogエントリをサポートしていませんでした。[#80990](https://github.com/StarRocks/starrocks/pull/80990)
- アーカイブされたMVタスク実行履歴内のリストパーティション値が縮小されていませんでした。[#80944](https://github.com/StarRocks/starrocks/pull/80944)
- `auto_refresh_partitions_limit` がインクリメンタルに維持されているマテリアライズドビューに対して受理されていました。[#79051](https://github.com/StarRocks/starrocks/pull/79051)
- 読み取りが検証に失敗した際に、バンドルメタデータが再読み込みされていませんでした。[#79928](https://github.com/StarRocks/starrocks/pull/79928)
- コミット時にソースパーティションが存在しなくなっていた場合、`INSERT OVERWRITE` が明確なエラーを報告していませんでした。[#79902](https://github.com/StarRocks/starrocks/pull/79902)
- MVバージョンマップのリセット、`TRUE` の透過的な書き換え、および適時性キャッシュが古い結果を返す可能性がありました。[#80893](https://github.com/StarRocks/starrocks/pull/80893)
- リストパーティションのドリフトが実際のテーブルと比較されていませんでした。[#78470](https://github.com/StarRocks/starrocks/pull/78470)
- 軽量コンパクションがプライマリインデックスのサイズを保持していませんでした。[#80746](https://github.com/StarRocks/starrocks/pull/80746)
- 常時有効な強制設定により、インクリメンタルに維持されているマテリアライズドビューが再マテリアライズされていました。[#79366](https://github.com/StarRocks/starrocks/pull/79366)
- インクリメンタルに維持されているマテリアライズドビューの列 `ALTER` が拒否されていませんでした。[#78726](https://github.com/StarRocks/starrocks/pull/78726)
- MVの `lastRefreshTime` がリプレイ時にログに記録される代わりに再計算されていました。[#78467](https://github.com/StarRocks/starrocks/pull/78467)
- IVMリフレッシュウィンドウが、コミットするトランザクションに記録されていませんでした。[#79315](https://github.com/StarRocks/starrocks/pull/79315)
- V2エンコードされた単一列の固定長キーにおいて、プライマリキーの復元が失敗していました。[#80446](https://github.com/StarRocks/starrocks/pull/80446)
- cloud ネイティブのPrimary Key tablet において、競合するコンパクションの all ファイルが孤立ファイルとして記録されていました。[#80445](https://github.com/StarRocks/starrocks/pull/80445)
- 共有データ型のMVベースパーティションに対して、FEローカルのバージョン時刻が比較されていました。[#79945](https://github.com/StarRocks/starrocks/pull/79945)
- `MultiGet` が早期にリターンした際、インデックスブロックイテレーターが解放されていませんでした。[#79380](https://github.com/StarRocks/starrocks/pull/79380)
- メモテーブルが受信しなかった行が、その行数制限にカウントされていました。[#79705](https://github.com/StarRocks/starrocks/pull/79705)
- 一覧表示中にネストされたIceberg RESTネームスペースが削除されるとエラーが発生していました。[#80846](https://github.com/StarRocks/starrocks/pull/80846)
- プランナーロックの修正によって残されていた、ポリシー検索の繰り返しと無制限の待機。[#80943](https://github.com/StarRocks/starrocks/pull/80943)
- tabletメタデータのダンプがローカルキャッシュに限定されていませんでした。[#78449](https://github.com/StarRocks/starrocks/pull/78449)
- リソースグループの更新時にドライバーキューのカウンターが保持されていませんでした。[#80889](https://github.com/StarRocks/starrocks/pull/80889)
- 不正な形式の数値HTTPパラメーターがBEをクラッシュさせる可能性がありました。[#79022](https://github.com/StarRocks/starrocks/pull/79022)
- `convert_hash_set_to_chunk` における読み取りオーバーフロー。[#80709](https://github.com/StarRocks/starrocks/pull/80709)
- `ALTER VIEW`、`INSERT OVERWRITE`、およびMVリフレッシュにおける既存のメタデータロック競合、ならびに残存していたプランナーとMVメタデータのロックのギャップ。[#80891](https://github.com/StarRocks/starrocks/pull/80891) [#80892](https://github.com/StarRocks/starrocks/pull/80892)
- レイクレプリケーションにおける再利用ファイルの処理。[#79359](https://github.com/StarRocks/starrocks/pull/79359)
- ブルームフィルターの下でサポートされていない列タイプが受理されていました。[#80871](https://github.com/StarRocks/starrocks/pull/80871)
- ロードシンクのクローズがpipelineポーラー上で実行されていました。[#80761](https://github.com/StarRocks/starrocks/pull/80761)
- 単一の収集クエリが失敗した際に、収集済みの外部統計情報が破棄されていました。[#79518](https://github.com/StarRocks/starrocks/pull/79518)
- BEのThrift RPCリトライにジッターが付与されておらず、FEのThrift RPCリトライがジッター付きのバックオフを行っていませんでした。[#80847](https://github.com/StarRocks/starrocks/pull/80847) [#80714](https://github.com/StarRocks/starrocks/pull/80714)
- パイプラインドライバーキューのレベルによって、レベル0が枯渇する可能性がありました。[#80760](https://github.com/StarRocks/starrocks/pull/80760)
- `INSERT ... SELECT` ソースがリフレッシュされた際、リソースマッピングされた外部テーブルが宣言どおりに保持されていませんでした。[#80546](https://github.com/StarRocks/starrocks/pull/80546)
- 互いに依存する重い式がプッシュダウンされていました。[#80710](https://github.com/StarRocks/starrocks/pull/80710)
- brpc の future タイムアウトの契約が破られていました。[#80783](https://github.com/StarRocks/starrocks/pull/80783)
- 解析済みの `INSERT` のソースがプランナーロック取得前にリフレッシュされておらず、ビューのベーステーブルがプランニング時に読み取られる前にロックされていませんでした。[#80786](https://github.com/StarRocks/starrocks/pull/80786) [#80784](https://github.com/StarRocks/starrocks/pull/80784)
- Iceberg REST catalog がコネクタのシャットダウン時にクローズされておらず、Iceberg メタデータキャッシュのバックグラウンドエグゼキュータが catalog のシャットダウン時に停止されていませんでした。[#80712](https://github.com/StarRocks/starrocks/pull/80712) [#80598](https://github.com/StarRocks/starrocks/pull/80598)
- `SHOW PROC` のジョブ詳細は、表示できない ALTER ジョブを拒否する代わりに、キャストに失敗していました。[#80719](https://github.com/StarRocks/starrocks/pull/80719)
- rowset のマイグレーション中にベクトルインデックスファイルがコピーされていませんでした。[#80391](https://github.com/StarRocks/starrocks/pull/80391)
- `ADD ROLLUP` が失敗した際、lake rollup の tablet が転置インデックスから削除されていませんでした。[#80717](https://github.com/StarRocks/starrocks/pull/80717)
- optimize ジョブおよび merge-partition ジョブの書き換えタスクが期限切れになりませんでした。[#80718](https://github.com/StarRocks/starrocks/pull/80718)
- リモート読み取りに失敗した後、共有バッファが再利用される可能性がありました。[#79965](https://github.com/StarRocks/starrocks/pull/79965)
- 列ごとの最小／最大統計情報の収集がスロットリングされていませんでした。[#78544](https://github.com/StarRocks/starrocks/pull/78544)
- アイドル状態のプール済み FE Thrift 接続が、置き換えられずに再利用されていました。[#80715](https://github.com/StarRocks/starrocks/pull/80715)
- 可視の `DELETE` トランザクションが適用された際、`hasDelete` が復元されていませんでした。[#80592](https://github.com/StarRocks/starrocks/pull/80592)
- 小数 `map_agg` キーの精度が失われていました。[#80673](https://github.com/StarRocks/starrocks/pull/80673)
- FE の brpc クライアントが `com.starrocks:jprotobuf-rpc-core` を使用するようになりました。[#80684](https://github.com/StarRocks/starrocks/pull/80684)
- トランザクション Stream Load 上の非ブール値の `partial_update` ヘッダーが拒否されていませんでした。[#79350](https://github.com/StarRocks/starrocks/pull/79350)
- 分割された lake スキャンで物理セグメントおよび rowset の件数が報告されていませんでした。[#80359](https://github.com/StarRocks/starrocks/pull/80359)
- すべての lake rollup ジョブで lake compaction がスキップされていませんでした。[#80655](https://github.com/StarRocks/starrocks/pull/80655)
- Primary Key compaction における行ソースのオーバーフロー。[#80472](https://github.com/StarRocks/starrocks/pull/80472)
- FE Web コンソールの `/system` ページが生の proc パスを検索していませんでした。[#80648](https://github.com/StarRocks/starrocks/pull/80648)
- 統計情報の DML が内部コンテキストをバインドしていませんでした。[#80473](https://github.com/StarRocks/starrocks/pull/80473)
- インスタンス化前にドライバーの容量が予約されていませんでした。[#80495](https://github.com/StarRocks/starrocks/pull/80495)
- ブロックバッファリングが無効な場合、ピアキャッシュデータがコピーされていませんでした。[#80438](https://github.com/StarRocks/starrocks/pull/80438)
- パーティションに複数のインデックスがある場合、すべての create タスクがカウントされていませんでした。[#80640](https://github.com/StarRocks/starrocks/pull/80640)
- Primary Key の並列実行スレッドにおいて、ランタイムの auto へのリセットが機能していませんでした。[#80567](https://github.com/StarRocks/starrocks/pull/80567)
- イベントスケジューラを使用したローカルエクスチェンジで、sink のウェイクアップが失われていました。[#80547](https://github.com/StarRocks/starrocks/pull/80547)
- Tablet のマージおよび tablet の分割結果のうち、範囲が FE と一致しないものが拒否されていませんでした。[#80538](https://github.com/StarRocks/starrocks/pull/80538) [#80418](https://github.com/StarRocks/starrocks/pull/80418)
- パーティションが 32 ビットのバイナリオフセットを超えて増大した後も、ハッシュジョインがパーティション化されたままになっていませんでした。[#80488](https://github.com/StarRocks/starrocks/pull/80488)
- バイパス書き込みトランザクションの開始タイムアウトヘッダーが秒単位として扱われていませんでした。[#80548](https://github.com/StarRocks/starrocks/pull/80548)
- FE のメモリに保持される alter ジョブの履歴に上限がありませんでした。[#80465](https://github.com/StarRocks/starrocks/pull/80465)
- 記録された NULL の拒否に基づいて、NULL 拡張行を拒否していない場合でも外部ジョインが変換されており、また limit、top-N、または window の下にあるジョインの NULL 拒否が外部ジョインを変換していました。[#80510](https://github.com/StarRocks/starrocks/pull/80510) [#79974](https://github.com/StarRocks/starrocks/pull/79974)
- 列を持たない述語や非決定的なジョイン述語が、ジョインの並べ替えによって削除されていました。[#80511](https://github.com/StarRocks/starrocks/pull/80511)
- BE がエンコードできない列タイプに対しても `BITMAP` インデックスが受け入れられていました。[#80459](https://github.com/StarRocks/starrocks/pull/80459)
- キーが重複する短絡 `IN` リストが、行を複数回返していました。[#80318](https://github.com/StarRocks/starrocks/pull/80318)
- `enclose` または `escape` が設定されている場合、CSV ファイルがバイト範囲に分割されていました。[#80386](https://github.com/StarRocks/starrocks/pull/80386)
- 列ごとの ZSTD 圧縮辞書が読み取れませんでした。[#77355](https://github.com/StarRocks/starrocks/pull/77355)
- 複数テーブルトランザクション Stream Load のコミットに失敗した場合のエラーが正しく報告されていませんでした。[#80351](https://github.com/StarRocks/starrocks/pull/80351)
- リクエスト URI が、アクションと同じクエリパーサーでサニタイズされていませんでした。[#80461](https://github.com/StarRocks/starrocks/pull/80461)
- ID を指定しない `CANCEL ALTER TABLE ROLLUP` が all の rollup ジョブをキャンセルしていませんでした。[#80450](https://github.com/StarRocks/starrocks/pull/80450)
- `LIMIT` を伴う `information_schema.columns` が、クエリ対象の catalog から応答されていませんでした。[#80448](https://github.com/StarRocks/starrocks/pull/80448)
- Paimon のブランチ状態がテーブルごとに分離されておらず、削除された Paimon パーティションがメタデータキャッシュから削除されていませんでした。[#79879](https://github.com/StarRocks/starrocks/pull/79879) [#79442](https://github.com/StarRocks/starrocks/pull/79442)
- レンジ分散の tablet が独自の初期メタデータを持たずに作成されていました。[#80421](https://github.com/StarRocks/starrocks/pull/80421)
- `INSERT INTO FILES` を使用して CSV にアンロードする際、重複する出力列名が拒否されていました。[#79897](https://github.com/StarRocks/starrocks/pull/79897)
- `FULL OUTER JOIN ... USING` 列が、結合キー上で独自のタイプを保持していませんでした。[#79655](https://github.com/StarRocks/starrocks/pull/79655)
- コネクタの I/O が FE のメタデータロック下で実行されていました。[#80416](https://github.com/StarRocks/starrocks/pull/80416)
- SQL ダイアレクトが Trino の場合、Iceberg のタイムトラベルが失われていました。[#79171](https://github.com/StarRocks/starrocks/pull/79171)
- Iceberg の decimal マニフェストの境界値が、最小長の 2 の補数でエンコードされていませんでした。[#78456](https://github.com/StarRocks/starrocks/pull/78456)
- Parquet、ORC、CSV の読み取りパスにおけるデータ駆動のアロケーションが、クエリメモリ上限によって制限されていませんでした。[#78573](https://github.com/StarRocks/starrocks/pull/78573)
- アンロードまたは失敗した統計情報が、統計情報なしとしてキャッシュされていました。[#80236](https://github.com/StarRocks/starrocks/pull/80236)
- パーティションまたはソートキー列が生成列であるロードは事前分割されておらず、tablet の事前分割サンプルがロードのタイムゾーンで実行されていませんでした。[#80390](https://github.com/StarRocks/starrocks/pull/80390) [#80362](https://github.com/StarRocks/starrocks/pull/80362)
- 主キーの tablet のクリーンアップがエラー状態としてカウントされていました。[#79558](https://github.com/StarRocks/starrocks/pull/79558)
- FE の再起動後や AD リファラルの際に、LDAP グループプロバイダーが使用不能になっていました。[#80361](https://github.com/StarRocks/starrocks/pull/80361)
- インラインサービスアカウントキーを使用した GCS からの Broker Load が、gcs-connector 3.x で失敗していました。[#78649](https://github.com/StarRocks/starrocks/pull/78649)
- Week 関数が、0 年より前の datetime で異常終了していました。[#80228](https://github.com/StarRocks/starrocks/pull/80228)
- StarOS と starlet を v4.1.5 にアップグレードしました。[#80347](https://github.com/StarRocks/starrocks/pull/80347)
- MemSpaceMonitor スレッドが破棄時に停止されず、BE の起動失敗時に SIGABRT が発生していました。[#79691](https://github.com/StarRocks/starrocks/pull/79691)
- コネクタクエリによってトリガーされた analyze が実行中タスクをリークさせ、デッドスケジューラーを残していました。[#80329](https://github.com/StarRocks/starrocks/pull/80329)
- `createShard` が StarMgr からの応答なしに失敗した場合、一度だけリプレイされるようになっていませんでした。[#80312](https://github.com/StarRocks/starrocks/pull/80312)
- ASOF ジョインの右側のフィルターが、ジョインの上に保持されていませんでした。[#80303](https://github.com/StarRocks/starrocks/pull/80303)
- `NOT LIKE` に対する NGRAMBF プルーニングが正しくありませんでした。[#79243](https://github.com/StarRocks/starrocks/pull/79243)
- ジョインおよびレンジ述語の基数推定が誤っていました。[#80242](https://github.com/StarRocks/starrocks/pull/80242)
- トップ N 集計が分割された際、NULL グループが失われていました。[#80143](https://github.com/StarRocks/starrocks/pull/80143)
- アダプティブな MV パーティションリフレッシュが cloud 内部テーブルをサポートしていませんでした。[#80270](https://github.com/StarRocks/starrocks/pull/80270)
- クリーンアップが必要なテーブルがない場合、`AnalyzeMgr.clearStalePartitionStats` が `IllegalStateException` をスローしていました。[#80265](https://github.com/StarRocks/starrocks/pull/80265)
- `ViewsSystemTable` が、集中的なテーブルロックの代わりにデータベースの読み取りロックを保持していました。[#78344](https://github.com/StarRocks/starrocks/pull/78344)
- `DROP PARTITIONS WHERE` が、その条件に一致しない行を削除していました。[#79958](https://github.com/StarRocks/starrocks/pull/79958)
- Lake の `delete_tablets` のブックキーピングが、tablet 数とバージョン数によって制限されていませんでした。[#80243](https://github.com/StarRocks/starrocks/pull/80243)
- 終了中の `SinkBuffer` から最後の送信者が離脱した際に、シンクオブザーバーに通知されていませんでした。[#80155](https://github.com/StarRocks/starrocks/pull/80155)
- レガシーな MV タスク `postRun` が、リフレッシュ中に無視されていませんでした。[#79943](https://github.com/StarRocks/starrocks/pull/79943)
- グループプロバイダーのファイル I/O が制限されておらず、リプレイされたプロバイダーがウォームになるまでブリッジされていませんでした。[#79896](https://github.com/StarRocks/starrocks/pull/79896)
- Compaction が apply を待っている間に、主キーの tablet `get_compaction_status` で BE がクラッシュしていました。[#80167](https://github.com/StarRocks/starrocks/pull/80167)
- キャッシュのエビクション時にファイルシステムが切断されていました。[#80087](https://github.com/StarRocks/starrocks/pull/80087)
- 0 行のコミットが、テーブル全体をロードされた行としてカウントしていました。[#80182](https://github.com/StarRocks/starrocks/pull/80182)
- バンドル文とスタンドアロン文が混在するマルチステートメントトランザクションが公開されていませんでした。[#79894](https://github.com/StarRocks/starrocks/pull/79894)
- 完了したトランザクションが `finalStatusTransactionStateDeque` に複数回エンキューされることがありました。[#79988](https://github.com/StarRocks/starrocks/pull/79988)
- スキーマの解決、推論、ライター、リーダーにおける Parquet のネスト型のエッジケース。[#80085](https://github.com/StarRocks/starrocks/pull/80085)
- 特殊文字を含む構造体キャストに対する variant パス。[#74168](https://github.com/StarRocks/starrocks/pull/74168)
- NULL を通過するランタイムフィルターが、null 化されたグルーピングキー上で `RepeatNode` の下にプッシュされていました。[#80051](https://github.com/StarRocks/starrocks/pull/80051)
- 主キーの重複排除書き換えが、他の集計を削除していました。[#79895](https://github.com/StarRocks/starrocks/pull/79895)
- Iceberg テーブルのキャッシュリフレッシュが、同時に実行された無効化を上書きすることがありました。[#79968](https://github.com/StarRocks/starrocks/pull/79968)
- `SET_VAR` でヒントされた `custom_query_id` が監査ログに記録されていませんでした。[#79871](https://github.com/StarRocks/starrocks/pull/79871)
- JSON の拡張列が共有 catalog テーブルに登録されていました。[#78611](https://github.com/StarRocks/starrocks/pull/78611)
- `COM_STMT_CLOSE` で準備済みステートメントがリークしていました。[#79860](https://github.com/StarRocks/starrocks/pull/79860)
- レンジコロケートのバケット割り当てが古くなったクエリが再プランされていませんでした。[#79927](https://github.com/StarRocks/starrocks/pull/79927)
- MV のスピル I/O が、マルチキャスト交換が完了するまで維持されていませんでした。[#79899](https://github.com/StarRocks/starrocks/pull/79899)
- 集合演算またはヌルセーフジョインのレンジ子の前段にある交換が削除されていました。[#79901](https://github.com/StarRocks/starrocks/pull/79901)
- イベントスケジューラが無効な場合、BE が `LookUpOperator` でクラッシュしていました。[#79711](https://github.com/StarRocks/starrocks/pull/79711)
- `sys.fe_memory_usage` が収集する代わりに空のマップを読み取っていました。[#79809](https://github.com/StarRocks/starrocks/pull/79809)
- スピルソートの完了が同期されておらず、終了エラーも保持されていませんでした。[#79521](https://github.com/StarRocks/starrocks/pull/79521)
- 圧縮されたグループバイキーのシリアライザが、null のみの定数を受け付けませんでした。[#79499](https://github.com/StarRocks/starrocks/pull/79499)
- マテリアライズドビューの書き換えにより、集計がプロジェクション内に残っていました。[#79611](https://github.com/StarRocks/starrocks/pull/79611)
- ADLS2 アカウントのスコープが保持されておらず、その資格情報も伏字化されていませんでした。[#79526](https://github.com/StarRocks/starrocks/pull/79526)
- `QueryCumulativeCpuTime` と `QuerySpillBytes` が、フラグメントインスタンスごとに1回ずつカウントされていました。[#79774](https://github.com/StarRocks/starrocks/pull/79774)
- `SHOW` 文の `LIMIT` オフセットが末尾を超えている場合、空の結果を返していませんでした。[#79764](https://github.com/StarRocks/starrocks/pull/79764)
- ロードスピルのマージタスクがキャンセルされた際、完了コールバックに応答されていませんでした。[#79360](https://github.com/StarRocks/starrocks/pull/79360)
- `SHOW ALTER TABLE OPTIMIZE` が `ORDER BY` を自身の列に対して解決していませんでした。[#79687](https://github.com/StarRocks/starrocks/pull/79687)
- セキュリティインテグレーション（LDAP）のユーザーが、MySQL 以外のチャネルで認証できませんでした。[#79165](https://github.com/StarRocks/starrocks/pull/79165)
- `EvHttpServer` がシャットダウン時に無関係なファイルディスクリプタを閉じていました。[#79582](https://github.com/StarRocks/starrocks/pull/79582)
- proc ディレクトリの行幅が、その行が応答となる箇所でチェックされていませんでした。[#79619](https://github.com/StarRocks/starrocks/pull/79619)
- 非正の `compact_threads` が拒否されず、CN をクラッシュさせていました。[#79641](https://github.com/StarRocks/starrocks/pull/79641)
- `TableKeeper` が `jdk.jshell` をインポートしていました。[#79520](https://github.com/StarRocks/starrocks/pull/79520)

## 4.1.6 {#416}

リリース日：2026年9月30日

### 動作の変更 {#behavior-changes-1}

- AWS SDK のデフォルト HTTP クライアントが Poco から SDK 自身のクライアントに戻されました：`enable_poco_client_for_aws_sdk` のデフォルトは `false` になります。以前の動作に戻すには `true` に設定してください。[#78375](https://github.com/StarRocks/starrocks/pull/78375)
- オンラインの optimize table は、オプトインになりました：FE 設定 `enable_online_optimize_table` のデフォルトは `false` となり、この設定を有効にしない限り `ALTER TABLE ... DISTRIBUTED BY ...` は再び書き込みをブロックします。[#78206](https://github.com/StarRocks/starrocks/pull/78206)
- jemalloc の専用オーバーサイズアリーナがデフォルトで有効になり、大きなアロケーションが呼び出しスレッドの CPU ごとのアリーナに置かれなくなりました。これにより大きなバッファの再利用が改善され、断片化が減少し、BE と CN のメモリがアリーナ間でどのように分散されるかが変わります。[#78396](https://github.com/StarRocks/starrocks/pull/78396)
- 共有データの tablet メタデータおよびトランザクションログファイルは、デフォルトで Adler-32 チェックサム付きで書き込まれるようになり（`lake_enable_protobuf_file_checksum`）、読み取り時に破損を検出できます。読み取り側はファイルにチェックサムが含まれていれば常にそれを検証していたため、ダウングレード手順は不要です。[#75334](https://github.com/StarRocks/starrocks/pull/75334)
- 同じキーに対して `UPSERT` 行と `DELETE` 行を混在させる共有データの主キーロードは、デフォルトでトランザクション内の順序を保持するようになりました（`lake_enable_pk_preserve_txn_delete_order`）。[#76211](https://github.com/StarRocks/starrocks/pull/76211)
- 複数テーブルトランザクションは、1つずつトランザクションを公開するのではなく、共有データのバッチ公開に参加するようになりました。これにより、CDC のマイクロバッチが多数のテーブルにファンアウトする際に発生していたシリアライゼーションが解消されます。[#77029](https://github.com/StarRocks/starrocks/pull/77029)
- 共有データのパーティションは、バッチ公開の各中間バージョンを経由するのではなく、最終バージョンへ直接進むようになりました。そのため、tablet メタデータオブジェクトを持たない中間バージョンをクエリが要求できなくなりました。これは以前 `The specified key does not exist` として顕在化していました。[#79175](https://github.com/StarRocks/starrocks/pull/79175)
- 失敗し続ける共有データの公開は、試行のたびに `previous publish version task has not finished` メッセージを出力するのではなく、バックオフしてリトライのログ出力を抑制するようになりました。以前はこれが障害発生時に FE リーダーの CPU とログ量を圧迫する可能性がありました。[#79299](https://github.com/StarRocks/starrocks/pull/79299)
- Del-file のトゥームストーン行が `cloud_native_pk_index_rebuild_rows_threshold` にカウントされるようになったため、削除の多い共有データワークロードは、本来発生すべき主キーインデックスの早期フラッシュを正しくトリガーするようになりました。[#76222](https://github.com/StarRocks/starrocks/pull/76222)
- 自身のセグメントを置き換える共有データのカラムアップサート公開は、デリートベクターをマージするようになりました。以前はこれにより、1つの主キーに対して2行がライブのまま残ることがありました。この不具合はかなり後になって、CN の再起動で永続性インデックスが再構築された際に `insert found duplicate key` として表面化していました。[#78797](https://github.com/StarRocks/starrocks/pull/78797)
- 共有データの主キーインデックスコンパクターは、フラッシュ側が sstable を生成し続けている間に静かに停止することがなくなりました。これにより、永続性インデックスが際限なく増大する tablet が発生していました。[#78812](https://github.com/StarRocks/starrocks/pull/78812)
- HDFS ファイルは、クローズされる前に `hsync` されなくなりました。`hsync` は、別の変更ですでに修正されていたクラッシュに対する回避策であり、クローズのたびにすべての DataNode レプリカに対して `fsync()` を強制していました。[#78864](https://github.com/StarRocks/starrocks/pull/78864)
- NULL の数を過少報告する Parquet の `null_count` は、デコードおよびプルーニングの際にそのまま信用されなくなりました。古いバージョンの parquet-mr で書き込まれたファイルは、以前は破損した値を返したり、行を欠落させたりしていました。[#78329](https://github.com/StarRocks/starrocks/pull/78329)
- `!=`、`NOT IN`、`IS NOT NULL` のリストパーティションプルーニングは、値のリストに `NULL` も含まれているという理由だけで、複数値パーティションを破棄することがなくなりました。このようなテーブルに対するクエリ（結合キーに対して `IS NOT NULL` を導出するジョインを含む）は、以前は失われていた非 null の行を返すようになりました。[#78946](https://github.com/StarRocks/starrocks/pull/78946)
- バッチ DDL は、合法な `DATE` 値である `0000` 年から始まる日付レンジパーティションを構築できるようになりました。このような文は以前 `the range of the partition is invalid` で失敗していました。[#78746](https://github.com/StarRocks/starrocks/pull/78746)
- `max_partitions_in_one_batch` とパーティション数の上限は、現在処理中のリクエストがこれから作成しようとしているパーティションをカウントするようになりました。サイズが大きすぎるバッチは、受理された後に次のバッチで失敗するのではなく、事前に拒否されます。[#79379](https://github.com/StarRocks/starrocks/pull/79379)
- `TRUNCATE TABLE <external_catalog>.<db>.<table>` は現在、セッションの現在の catalog ではなく、ステートメント内で指定された catalog に対して `DELETE` 権限をチェックするようになりました。以前は許可されていたユーザーが拒否されるようになったり、その逆が発生したりする可能性があります。[#79047](https://github.com/StarRocks/starrocks/pull/79047)
- ストリーミング集計のチューニングは、`HAVING` 後に残る行数ではなく、group-by のカーディナリティによって駆動されるようになったため、`HAVING` 句がほとんどのグループを除外するクエリでも、ローカルの事前集計が失われなくなりました。[#79064](https://github.com/StarRocks/starrocks/pull/79064)
- 監査ログの行は、外部の Iceberg テーブルまたは Hive テーブル、あるいは `FILES()` テーブル関数を対象とする DML、および非同期マテリアライズドビューのリフレッシュに対して、`scanBytes`、`scanRows`、`returnRows`、`cpuCostNs`、`memCostBytes` を含むようになりました。これらのフィールドは以前は `0` を報告していました。[#74072](https://github.com/StarRocks/starrocks/pull/74072) [#74954](https://github.com/StarRocks/starrocks/pull/74954)
- クエリプロファイルと `EXPLAIN ANALYZE` は、最初の試行時のプランではなく、再試行されたクエリが実際に実行したプランを報告するようになりました。[#79225](https://github.com/StarRocks/starrocks/pull/79225)
- Broker Load は、送信時点でのスナップショットの `enable_tablet_pre_split` を、送信元クライアントのライブセッション変数に書き戻すことがなくなりました。[#79177](https://github.com/StarRocks/starrocks/pull/79177)
- `ALTER MATERIALIZED VIEW ... ACTIVE` が非集計インクリメンタルマテリアライズドビューで再び動作するようになりました。ストレージによって埋められる `__ROW_ID__` 列は、マテリアライズドビューの DDL 列リストから除外されるようになりました。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- `SHOW ALTER TABLE OPTIMIZE` の `Operation` 列は、`com.starrocks.sql.ast.OptimizeClause@b5dc069` のような内部オブジェクトアドレスではなく、最適化操作の読みやすい説明を表示するようになりました。[#75948](https://github.com/StarRocks/starrocks/pull/75948)
- tablet スキーマに存在しない列があっても、述語のプッシュダウン中に BE が異常終了しなくなりました。該当する述語は単にプッシュダウンされません。[#79234](https://github.com/StarRocks/starrocks/pull/79234)
- `OlapTableSink` の初期化が失敗した場合、部分的に初期化されたシンクが完全なクローズ処理を経由しなくなりました。以前はこの処理が、一度も作成されていない状態を参照して BE を異常終了させる可能性がありました。代わりに元の初期化エラーが返されるようになりました。[#79370](https://github.com/StarRocks/starrocks/pull/79370)
- ショートキーインデックスページのジオメトリは、32 ビットオーバーフローなしで検証されるようになったため、破損したページフッターはラップしてヒープの範囲外書き込みを引き起こすのではなく、拒否されるようになりました。[#79048](https://github.com/StarRocks/starrocks/pull/79048)
- TLS ハンドシェイクがタイムアウトした HTTPS リクエストは、接続済みとして扱われ同一ソケットで再試行されることがなくなりました。停滞したリクエストは、`connect_timeout` に加えて 2 回の受信タイムアウトではなく、およそ 1 回のタイムアウト後に失敗するようになりました。[#79369](https://github.com/StarRocks/starrocks/pull/79369)

### 改善 {#improvements-1}

- LDAP および Active Directory のグループが、ユーザーの `memberOf` 属性から解決できるようになったため、新しいディレクトリグループに対してクラスタ上で一致するグループプロバイダー設定が不要になりました。[#79254](https://github.com/StarRocks/starrocks/pull/79254)
- 監査ログには、大まかな `ErrorCode` だけでなく、クエリのエラーメッセージも記録されるようになったため、構文エラー、不明なテーブル、権限エラーを `fe.log` に照合しなくても区別できるようになりました。[#78798](https://github.com/StarRocks/starrocks/pull/78798)
- all のスピル可能なオペレーター全体のスピル活動を集計し、ローカルおよびリモートのストレージバックエンド別に分類するサーバーレベルのスピルメトリクスを追加しました。[#71827](https://github.com/StarRocks/starrocks/pull/71827)
- Hudi メタデータリーダーは、1 回のスキャンのパーティションごとに再構築されるのではなく、再利用されるようになりました。これにより、Hudi メタデータテーブルが有効な場合に、多数のパーティションを持つテーブルのプランニングが短縮されます。[#79367](https://github.com/StarRocks/starrocks/pull/79367)
- タブレットの分割点は、専用のフルソートキーインデックスページではなく、セグメントの既存のインデックスから計算されるようになったため、分割されることのないテーブルはそのページの書き込みと保存のコストを負わなくなりました。[#79050](https://github.com/StarRocks/starrocks/pull/79050)
- `/memz` は jemalloc 自体の統計オプションを受け付けるようになり、これにより稼働中の BE から arena ごとの dirty ページ数や muzzy ページ数、エクステントのサイズクラスの内訳にアクセスできるようになりました。[#79090](https://github.com/StarRocks/starrocks/pull/79090)

### セキュリティ {#security-1}

- [CVE-2026-55971] C++ バインディングの `THeaderTransport::untransform()` におけるヒープベースのバッファオーバーフローを修正するため、Apache Thrift を 0.24.0 にバンプしました。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-42537, CVE-2026-59646] JDBC URL インジェクションによる RCE を修正するため Apache Ranger を 2.9.0 に、DTLS リアセンブラのバッファオーバーランを修正するため Bouncy Castle を 1.85 にバンプしました。[#79331](https://github.com/StarRocks/starrocks/pull/79331)
- [CVE-2026-71290] Apache HttpClient5 を 5.6.4 にバンプしました。[#79538](https://github.com/StarRocks/starrocks/pull/79538)

### バグ修正 {#bug-fixes-1}

以下の問題が修正されました：

- `SHOW PROFILELIST`、`ANALYZE PROFILE`、`get_query_profile()`、`/api/profile` は、他のユーザーの SQL のテキストやプランを含む、キャッシュされたすべてのプロファイルを、認証済みの任意のユーザーに返していました。新しい FE 設定 `authorization_enable_query_profile_access_check` により、キャッシュされたプロファイルはクエリを実行したユーザーおよび `SYSTEM OPERATE` を保持するユーザーに制限されます。[#79375](https://github.com/StarRocks/starrocks/pull/79375)
- 認可チェック、`INSERT` のプランニング、ビューの展開、権限チェック、および `inspect_mv_refresh_info` は、FE メタデータロックを保持したまま外部の catalog とのラウンドトリップを行っていたため、メタストアの応答が遅いと同じロックを待つすべての DDL が停滞していました。これらの呼び出しは、現在はロックの外側で行われるようになりました。[#79319](https://github.com/StarRocks/starrocks/pull/79319) [#79226](https://github.com/StarRocks/starrocks/pull/79226)
- gtid とパーティションバージョンのエポックが非 leader の FE 上で生成される可能性があったため、2 つのノードが同じ瞬間に同じ番号を生成することがありました。[#78799](https://github.com/StarRocks/starrocks/pull/78799)
- `FULL OUTER JOIN ... USING (c1, c2, ...)` は誤った値を返していました。最初の `USING` 列は正しい値でしたが、以降のすべての列は前の列の値を引き継いでいました。同じクエリに `ORDER BY <ordinal>` を追加すると、`IndexOutOfBoundsException` で失敗していました。[#79298](https://github.com/StarRocks/starrocks/pull/79298)
- `GROUPING SETS`、`ROLLUP`、または `CUBE` がユニオンに書き換えられた際に `HAVING` 述語が失われ、クエリが本来フィルタリングされるべき行を返していました。[#79415](https://github.com/StarRocks/starrocks/pull/79415)
- `GROUP BY ROLLUP(a, b, a)`、および `CUBE` または `GROUPING SETS` が 1 つのセット内で同じ列を 2 回指定すると、同じキーを 2 回含むグルーピングセットが生成されていました。[#78261](https://github.com/StarRocks/starrocks/pull/78261)
- 集計キーテーブルの上の集計を削除すると、ストレージ層に到達する前にクエリの `LIMIT` が失われ、BE がテーブル全体を読み込んでいました。[#79414](https://github.com/StarRocks/starrocks/pull/79414)
- `OR` ジョインをユニオンに分割すると、ユニオンの出力列と最初のブランチが列 ID 順に並んでいた一方、他のブランチは投影マップの反復順序に従っていたため、ブランチの整列が一貫していませんでした。[#79473](https://github.com/StarRocks/starrocks/pull/79473)
- スキュージョインの書き換えは、外部ジョインの保持側を複製していたため、1 つの入力行が複数回返されることがあり、結果が実行ごとに変化していました。また、そのソルトテーブルは `generate_series` の引数を誤った順序で受け取っていたため、シリーズが何も出力しませんでした。[#79168](https://github.com/StarRocks/starrocks/pull/79168) [#79268](https://github.com/StarRocks/starrocks/pull/79268)
- `SELECT DISTINCT` は、`DISTINCT` によって削除される列に対する `ORDER BY` を受け付けてしまい、ウィンドウの下での重複排除のプッシュダウンが、削除済みの列を投影内に残していました。[#77217](https://github.com/StarRocks/starrocks/pull/77217) [#77218](https://github.com/StarRocks/starrocks/pull/77218)
- `bitmap_union_count()` のような常に非 null となる集計に対するウィンドウが、宣言された nullability を失っていたため、その上での `DISTINCT` や `GROUP BY` が `error nullablel column` で失敗していました。[#79009](https://github.com/StarRocks/starrocks/pull/79009)
- `percentile_cont` のマージは、値ごとではなく位置ごとに枯渇状態を追跡していたため、BE が異常終了する可能性がありました。[#77261](https://github.com/StarRocks/starrocks/pull/77261)
- `count(DISTINCT percentile_hash(...))`、および `PERCENTILE` 値に対する任意の `DISTINCT` 集計は BE をクラッシュさせていましたが、このような呼び出しは現在拒否されるようになりました。[#78795](https://github.com/StarRocks/starrocks/pull/78795)
- スカラー agg-state コンビネータは、null のメモリプールでクラッシュしたり、ネストした関数コンテキストで競合状態を起こしたりしていました。[#76910](https://github.com/StarRocks/starrocks/pull/76910)
- 集計テーブル上の JSON サブフィールド集計は、合成サブフィールド列に集計タイプがないため、生の `NullPointerException` で失敗していました。[#77952](https://github.com/StarRocks/starrocks/pull/77952)
- `enable_spill = true` 配下でのストリーミング事前集計が、制限されたメモリ予算が一度だけ設定されて再評価されないため、メモリ不足に陥る可能性がありました。[#76702](https://github.com/StarRocks/starrocks/pull/76702)
- ビルドパーティションを復元中にキャンセルされたスピリングハッシュジョインが BE をクラッシュさせていました。これは、プローブオペレーターがスピル I/O タスクの完了を待たずにクローズしていたためです。[#79085](https://github.com/StarRocks/starrocks/pull/79085)
- ローカルパススルー交換は、レシーバーがキャンセルされた後もチャンクの追加を続けており、シンクとレシーバーが所有権を共有していなかったため、そのチャネルが解放後に使用される可能性がありました。また、シンクの準備完了通知がシンクのオブザーバブルを選択していながらソースイベントを発行していたため、`OUTPUT_FULL` でブロックされたドライバがブロックされたままになることがありました。[#76603](https://github.com/StarRocks/starrocks/pull/76603) [#77088](https://github.com/StarRocks/starrocks/pull/77088) [#76782](https://github.com/StarRocks/starrocks/pull/76782)
- クエリキャッシュが不完全なtabletごとの結果を保存し、それを後続のクエリに提供する可能性がありました。また、時間的条件がダイジェストに到達しなかったにもかかわらず、ASOF ジョインを含むフラグメントがキャッシュされていました。[#77066](https://github.com/StarRocks/starrocks/pull/77066) [#77563](https://github.com/StarRocks/starrocks/pull/77563)
- 時間的条件が両辺を比較しない ASOF ジョインが BE をクラッシュさせていました。このような条件は現在拒否されるようになりました。[#79049](https://github.com/StarRocks/starrocks/pull/79049)
- 親タプルを持たないネストされたラムダスロットが、ランタイムフィルターの構築時に TopN ノードのタプルセットによってバインドされていると扱われていました。また、order-by ヒントが、重い式としてスキャンにプッシュされた列のないスロットを参照解決していました。[#78050](https://github.com/StarRocks/starrocks/pull/78050) [#78527](https://github.com/StarRocks/starrocks/pull/78527)
- ネストされた `array_map` および `array_sort` ラムダは、`Input dependency cols check failed` または `Expr evaluate meet error: slot_id N not found` でのプランニングまたは評価に失敗していました。これは、内側のラムダによって引き上げられた共通部分式が、それを定義したスコープから抜け出していたためです。[#78645](https://github.com/StarRocks/starrocks/pull/78645) [#78769](https://github.com/StarRocks/starrocks/pull/78769) [#78915](https://github.com/StarRocks/starrocks/pull/78915)
- `array_sort` のコンパレータ検証は、複数のドライバ間で 1 つのエラー `Status` を共有しており、二重解放を引き起こす可能性がありました。[#78314](https://github.com/StarRocks/starrocks/pull/78314)
- `array_contains(NULL, [1, 2])` と `array_position(NULL, [1, 2])` が `class com.starrocks.type.NullType cannot be cast to class com.starrocks.type.ArrayType` で失敗していました。[#76970](https://github.com/StarRocks/starrocks/pull/76970)
- `__iceberg_transform_truncate` または `__iceberg_transform_bucket` を解析すると、共有されている組み込み関数オブジェクトがその場で変更され、最初に解析されたクエリの精度とスケールでワイルドカードの decimal シグネチャが上書きされていました。[#76777](https://github.com/StarRocks/starrocks/pull/76777)
- 空の引数リストを持つ `PARTITION BY RANGE(<partition-function>())` は、パースエラーではなく生の `IndexOutOfBoundsException` を発生させていました。[#75080](https://github.com/StarRocks/starrocks/pull/75080)
- 範囲述語が、式が順序を保持するかどうか、またどの方向に保持するかにかかわらずパーティション式にマッピングされていたため、範囲パーティションおよびリストパーティションのプルーニングが、一致する行を保持するパーティションを削除してしまう可能性がありました。また、`NULL` パーティションキーが、列の型の最小値より厳密に小さい値ではなく、その最小値で検索されていました。`DATE` や `DATETIME` をテキストにキャストする保持条件は、再び受け入れられるようになりました。[#78024](https://github.com/StarRocks/starrocks/pull/78024) [#79111](https://github.com/StarRocks/starrocks/pull/79111) [#79187](https://github.com/StarRocks/starrocks/pull/79187) [#79257](https://github.com/StarRocks/starrocks/pull/79257) [#79688](https://github.com/StarRocks/starrocks/pull/79688)
- Iceberg の述語コンバーターが `CastOperator` を無条件に取り除いていたため、`DECIMAL(5,2)` 列に対する `WHERE CAST(d AS BIGINT) = 10` が、`10.50` を保持するファイルをプルーニングしてしまう可能性がありました。[#78351](https://github.com/StarRocks/starrocks/pull/78351)
- Iceberg ベーステーブル上の増分マテリアライズドビューが、定期的な `rewrite_data_files` の Compaction によって恒久的に無効化されていました。これは、デルタ境界がスキップされた `REPLACE` スナップショットへ進んでいたためです。[#76950](https://github.com/StarRocks/starrocks/pull/76950)
- 列の最小値／最大値の統計キャッシュが、パーティションまたはスキーマの DDL によって無効化されず、古いエントリが無期限に提供される可能性がありました。[#77441](https://github.com/StarRocks/starrocks/pull/77441)
- メタストアまたは AWS Glue が列統計の読み取り権限を拒否した場合、Hive ファミリーの catalog は完全に失敗していましたが、現在は空の統計情報にフォールバックするようになりました。また、Glue の列統計リクエストには `catalogId` が付与されるようになったため、クロスアカウントデプロイメントでは設定された catalog から統計情報を読み取ります。[#78455](https://github.com/StarRocks/starrocks/pull/78455) [#78461](https://github.com/StarRocks/starrocks/pull/78461)
- Elasticsearch 外部テーブルおよび Elasticsearch catalog は、バージョン 8 または 9 を実行しているクラスタを `Unsupported/Unknown ES Cluster version` で拒否していました。また、同時実行中のクエリが互いのリクエスト URL パラメータを上書きしてしまうことがあり、これが `Column 'id' cannot be resolved` のような無関係な分析エラーとして表面化していました。[#78267](https://github.com/StarRocks/starrocks/pull/78267) [#79391](https://github.com/StarRocks/starrocks/pull/79391)
- Paimon テーブルの `DECIMAL` 列に対する述語が、その列の Paimon `DecimalType` ではなく StarRocks 型ファミリーのデフォルトの精度とスケールを使って変換されていたため、リテラルが値を表現できませんでした。[#78760](https://github.com/StarRocks/starrocks/pull/78760)
- Azure Blob の資格情報の検索は、エンドポイントが完全な URL として設定されている場合に失敗していました。これは、Hadoop の設定キーにスキームが埋め込まれていたためです。また、ADLS2 ストレージボリュームはシリアライズ時に Azure Workload Identity のトークンファイルプロパティを失ってしまい、ボリュームの再読み込み時に Managed Identity にフォールバックして `Identity not found` で失敗していました。[#78241](https://github.com/StarRocks/starrocks/pull/78241) [#79444](https://github.com/StarRocks/starrocks/pull/79444)
- `'[0].value.url'` のような配列インデックスを含む JSON パスは、配列がそのインデックスより短い行ごとに C++ 例外をスローしており、これにより大規模なスキャンがスタックアンワインダー内でのロックコンボイと化していました。現在はインデックスの範囲チェックが行われるようになりました。[#78569](https://github.com/StarRocks/starrocks/pull/78569)
- `timestampdiff(MONTH, ...)`、`timestampdiff(YEAR, ...)`、`months_diff`、`years_diff` は、両方の引数が同じ月または年に属し、かつ 2 番目の引数の方が早い場合に、`0` ではなく `-1` を返していました。[#79037](https://github.com/StarRocks/starrocks/pull/79037)
- `CAST(<varbinary constant> AS VARCHAR)` の定数畳み込みは、値ではなく、バッキングバイト配列の Java アイデンティティハッシュコード（`[B@233c25e5` のようなもの）を返していました。[#77718](https://github.com/StarRocks/starrocks/pull/77718)
- 入力列のキャッシュされた `has_null` フラグが保守的に true のままであった場合、`DICTIONARY_GET` は非NULL キーを NULL として拒否していました。[#76881](https://github.com/StarRocks/starrocks/pull/76881)
- 書き換え器が復号することのない、ネステッドループジョインの `ON` 列に対して低基数辞書が保持され続けていました。さらに、独自のコードを持つ派生辞書を介して辞書式が畳み込まれてしまい、「ローリング低基数」列がグローバル辞書を繰り返し無効化してテーブル全体の再収集を強制していました。[#78229](https://github.com/StarRocks/starrocks/pull/78229) [#78633](https://github.com/StarRocks/starrocks/pull/78633) [#78486](https://github.com/StarRocks/starrocks/pull/78486)
- BE が生の 32 バイト値から行をバケッティングする一方で、`DECIMAL256` リテラルはその文字列表現からハッシュ化されていたため、`DECIMAL256` 列で分散されたテーブルに対するポイントルックアップが誤ったバケットに送られていました。[#78495](https://github.com/StarRocks/starrocks/pull/78495)
- OLAP スキャンが、ソートキー列を列 ID ではなく列名で名付けていたため、リネームされた列が tablet スキーマにもはや一致しなくなっていました。[#77967](https://github.com/StarRocks/starrocks/pull/77967)
- `PruneComplexTypeUtil` は、記述できない単一の式に遭遇すると、文全体のすべての列に対して複合型サブフィールドのプルーニングを無効化していました。このベイルアウトは、現在それを引き起こした式のみに限定されるようになり、述語式の再利用によってスキャンから引き出された不要なサブフィールドスロットは削除されるようになりました。[#79181](https://github.com/StarRocks/starrocks/pull/79181) [#77456](https://github.com/StarRocks/starrocks/pull/77456)
- `AstToSQLBuilder` が `SELECT * EXCLUDE` リストの列を二重引用符で出力していましたが、これはどの文法でも受け入れられないため、再生成された SQL がパースできなくなっていました。[#78052](https://github.com/StarRocks/starrocks/pull/78052)
- 集計テーブルに対する `ALTER TABLE ... ADD COLUMN` は、集計関数も `KEY` キーワードも指定されていない場合、暗黙にキー列を作成しており、テーブルが集計する粒度を変更してしまっていました。このような文は現在、曖昧であるとして拒否されます。[#78359](https://github.com/StarRocks/starrocks/pull/78359)
- `JSON` のように、型に BE キーコーダーが存在しない列がソートキーとして使用でき、BE をクラッシュさせていました。このような定義は現在、all の分散タイプ全体にわたって、`CREATE TABLE` および `ALTER TABLE` で拒否されるようになりました。[#76795](https://github.com/StarRocks/starrocks/pull/76795)
- `USING GIN("IMP_LIB" = "builtin")` は `CREATE TABLE` によって受け入れられていましたが、その後、プロパティキーが元の大文字小文字のまま BE に到達していたため、すべての `Can not get inverted imp type` を伴う `INSERT` を破壊していました。[#77818](https://github.com/StarRocks/starrocks/pull/77818)
- `gram_num` プロパティを持たずに永続化されたレガシーな `NGRAMBF` インデックスは、セグメント構築中に BE に UTF-8 オフセットベクトルを超えて読み取らせ、CN をクラッシュさせていました。[#76989](https://github.com/StarRocks/starrocks/pull/76989)
- プッシュダウンされた述語がスパースな範囲を読み取った際、null フラグが序数ではなく線形にインデックス付けされていたため、ギャップの後のすべての行がスキップされた行の null フラグを取得していました。また、プッシュダウンされたフィルターが辞書全体を拒否した場合、辞書ページデコーダーが前進に失敗し、BE が異常終了していました。[#78389](https://github.com/StarRocks/starrocks/pull/78389) [#79007](https://github.com/StarRocks/starrocks/pull/79007)
- 子式のエラーが、伝播されるのではなく BE を異常終了させていました。[#77442](https://github.com/StarRocks/starrocks/pull/77442)
- `ThreadPool` はデフォルトでタスクの例外を握りつぶし、そのタスクを完了として扱っていました。[#76863](https://github.com/StarRocks/starrocks/pull/76863)
- Jaeger トレーシングが有効な場合、`OlapTableSink` を通じてロードをキャンセルすると、BE または CN が SIGSEGV でクラッシュしていました。[#76869](https://github.com/StarRocks/starrocks/pull/76869)
- `AsyncFlushOutputStream::close()` は、ストリーム自身のミューテックスを保持したままプロミスを完了させていたため、ウェイターがストリームを破棄した後に I/O スレッドが解放済みメモリ上の futex を起床させていました。[#79182](https://github.com/StarRocks/starrocks/pull/79182)
- `shutdown_staros_worker()` がグローバルワーカーを解放した後に、進行中のロードが StarOS バックエンドのファイルシステムに到達した場合、CN がグレースフルシャットダウン中に SIGSEGV でクラッシュする可能性がありました。[#79058](https://github.com/StarRocks/starrocks/pull/79058)
- 並列 Compaction が brpc の bthread 上でファイルシステム I/O を実行していたため、CN が `Resource deadlock avoided` で異常終了していました。[#76925](https://github.com/StarRocks/starrocks/pull/76925)
- ベクトルインデックスの rowset が負の `data_disk_size` を報告した場合、Compaction が SIGFPE で異常終了していました。[#78959](https://github.com/StarRocks/starrocks/pull/78959)
- レイクの txn ログ読み取りが破損に遭遇した場合、破損したローカルキャッシュのコピーが再試行のたびに再読み込みされていました。また、主キーインデックスの sstable ブロック読み取りは、ライターが常に生成していた crc32c ブロックトレーラーを一度も検証していませんでした。[#78733](https://github.com/StarRocks/starrocks/pull/78733) [#77479](https://github.com/StarRocks/starrocks/pull/77479)
- コピーに退化したパーシャルアップデートの書き換えがソースの暗号化メタデータを失わせ、どのリーダーも開けず、かつ Compaction でも書き換えられないセグメントが残ってしまう問題を修正しました。[#79053](https://github.com/StarRocks/starrocks/pull/79053)
- 共有データ schema change がデータを参照しないロウセットを記録することがあり、その後パーティション上のすべての `MERGE_TABLET` が `tablet merge source has an empty ordinary rowset` で失敗する問題を修正しました。[#79167](https://github.com/StarRocks/starrocks/pull/79167)
- Range-colocate のスプリット境界がシリアライズされた protobuf バイトとして比較されていましたが、FE と BE はこれを同じようにエンコードしません。[#76968](https://github.com/StarRocks/starrocks/pull/76968)
- 不安定な range-colocate グループのメンバーに対する単純な単一テーブルクエリが `range colocate group ... is in an unaligned state` で失敗する問題を修正しました。[#76971](https://github.com/StarRocks/starrocks/pull/76971)
- ハッシュ colocate の共有データテーブルの新しい物理パーティションのシャードが colocation 配置情報なしで作成され、後から移行が必要になる問題を修正しました。[#79223](https://github.com/StarRocks/starrocks/pull/79223)
- FE が削除された tablet のテーブルがレンジ分散であることを BE に伝えておらず、リシャード後にファイルがリークする問題を修正しました。[#76861](https://github.com/StarRocks/starrocks/pull/76861)
- リシャードジョブの更新のリプレイでリシャード対象の tablet が更新されず、FE リーダー変更後にパーティションのパブリッシュが、一度も書き込まれなかった tablet メタデータオブジェクトに対して永久に404になることがありました。また、テーブルが削除された場合にリシャードジョブのリプレイが edit log への書き込みを行うことがあり、リシャードのパブリッシュリトライがペース配分されずにスピンしていました。[#79426](https://github.com/StarRocks/starrocks/pull/79426) [#79485](https://github.com/StarRocks/starrocks/pull/79485) [#77691](https://github.com/StarRocks/starrocks/pull/77691)
- 共有データのパーティションバージョンが、変更操作がジャーナルに記録される前にメモリ上で変更されていたため、リプレイされたバージョンが後退することがありました。現在はジャーナルから導出されるようになり、常に前進のみするようになりました。[#79296](https://github.com/StarRocks/starrocks/pull/79296)
- ターゲットの tablet がレプリカを報告しなかった場合、レプリケーションジョブがゼロ除算を起こす問題を修正しました。[#79329](https://github.com/StarRocks/starrocks/pull/79329)
- Data Cache のキーハッシュがランタイムディスパッチテーブルから取得されていたため、そのディスパッチへの変更があるとアップグレードやロールバックの際にキャッシュされたすべてのブロックが孤立していました。現在は MurmurHash3 に固定されています。[#79262](https://github.com/StarRocks/starrocks/pull/79262)
- FE のスキャン範囲のヒープ安全性チェックがスキャンノードごとではなく物理パーティションごとに1回実行されていたため、物理パーティション数に対して二次的な処理量になっていました。[#76978](https://github.com/StarRocks/starrocks/pull/76978)
- ロウセットメタデータキャッシュのエントリが、遅延ロードされたインデックスによってセグメントのメモリ使用量が増加した後も初期のチャージを保持し続けていたため、キャッシュがそのフットプリントを過小報告していました。[#77893](https://github.com/StarRocks/starrocks/pull/77893)
- `Tablet::get_average_row_size()` が `_meta_lock` を保持せずに `_rs_version_map` を走査しており、マップが宣言されている契約に反していました。[#79250](https://github.com/StarRocks/starrocks/pull/79250)
- FE の thrift アクセプトループがワーカープールの飽和により数分間停止し、呼び出し元がとうに存在しなくなった後もキューに入れられていた接続をワーカーが処理し続ける問題を修正しました。[#78838](https://github.com/StarRocks/starrocks/pull/78838) [#79173](https://github.com/StarRocks/starrocks/pull/79173)
- journal applier がスタックしている非リーダー FE が、転送せずにローカルで古いメタデータを提供し続ける問題を修正しました。これは、readiness の判定がリプレイサイクルの戻り後にのみ下されていたためです。[#79041](https://github.com/StarRocks/starrocks/pull/79041)
- `getLoads` RPC がすべてのロードレコードを単一のメッセージで返していたため、ビジーなクラスタではリーダーログが `FE RPC failure ... MaxMessageSize reached` で埋め尽くされ、`loads_history` のアーカイブがロールバックして行が恒久的に失われていました。この RPC は現在ジョブ ID によってページングされるようになりました。[#78898](https://github.com/StarRocks/starrocks/pull/78898)
- `task_runs` が読み取られている間に、保留キュー、実行中マップ、メモリ内履歴、アーカイブ済み履歴の間を移動したタスク実行が、二重に報告される可能性がある問題を修正しました。[#79438](https://github.com/StarRocks/starrocks/pull/79438)
- `SHOW ALTER TABLE ROLLUP` と `SHOW ALTER MATERIALIZED VIEW` が `WHERE`、`ORDER BY`、`LIMIT` を受け付けながらそれらを無視していました。これらを適用していたのは `SHOW ALTER TABLE COLUMN` のみでした。また `FinishedTime` は `SHOW ALTER TABLE ROLLUP` 述語内でも受け付けられるようになりました。[#79281](https://github.com/StarRocks/starrocks/pull/79281) [#79498](https://github.com/StarRocks/starrocks/pull/79498)
- SQL ブラックリストのロード時に、ルールごとに不変のスナップショットを再構築してソートしていたため、ルール数に対して二次的な処理量になり、FE の起動時とチェックポイント時のたびに実行されていました。[#78752](https://github.com/StarRocks/starrocks/pull/78752)
- コンシューマスレッドが中断された後、`AsyncTaskQueue` が `getOutputs()` と `tryGetOutputs()` の間でビジースピンする問題を修正しました。これは中断が `hasMoreOutput` をクリアせずに握りつぶされていたためです。[#79297](https://github.com/StarRocks/starrocks/pull/79297)
- `sys_log_level` のランタイム変更が設定に書き込まれるものの、その項目が可変であるとドキュメントに記載されているにもかかわらず、実際には適用されない問題を修正しました。[#79200](https://github.com/StarRocks/starrocks/pull/79200)
- `has_enable_heap_profile()` がブール型の jemalloc ノード `prof.active` を `int` として読み取っていたため、常にヒーププロファイリングが無効であると報告していました。[#78820](https://github.com/StarRocks/starrocks/pull/78820)
- スクリプトの外部オブジェクトに対する `System.print()` がオブジェクトのテキストではなく `instance of <Class>` を出力する問題を修正しました。これは `toString` のバインディングがゲッターではなくメソッドとして登録されていたためです。[#78877](https://github.com/StarRocks/starrocks/pull/78877)
- マルチテーブルトランザクション Stream Load インターフェースを介した式パーティション化テーブルへのロードが、BE がパーティションを作成する必要がある場合に常にコミット時に `automatic create partition failed. error: txn ... not exist` で失敗する問題を修正しました。[#78790](https://github.com/StarRocks/starrocks/pull/78790)
- 結合された txn ログモードにおいて、自身のストリーム終端が早く到着したコーディネーターが、ロードがキャンセルされた際に txn ログコレクターを無期限に待機してしまう問題を修正しました。[#79389](https://github.com/StarRocks/starrocks/pull/79389)
- 完了した merge-commit ロードが `information_schema.loads` において完全なマージウィンドウの進捗より少ない値を報告することがある問題を修正しました。[#78723](https://github.com/StarRocks/starrocks/pull/78723)
- スキャンの `SampleTime` カウンターが経過時間ではなくブロック数を報告していた問題を修正しました。[#78732](https://github.com/StarRocks/starrocks/pull/78732)
- Arrow Flight SQL サービス用の FE クラスパスに LZ4 および ZSTD の Arrow IPC コーデックが欠落していた問題を修正しました。[#76921](https://github.com/StarRocks/starrocks/pull/76921)
- StarOS、starlet、starcache を v4.1.4 にアップグレードしました。[#79429](https://github.com/StarRocks/starrocks/pull/79429)

## 4.1.5 {#415}

リリース日：2026年9月8日

### 改善項目 {#improvements-2}

- 外部テーブルに対して `ANALYZE SAMPLE TABLE PREDICATE COLUMNS` をサポートしました。[#77243](https://github.com/StarRocks/starrocks/pull/77243)
- サンプルベースの tablet 事前分割を拡張しました：非キー式、動的上書き一時パーティション、Iceberg および上書きロードをサポートします。[#77843](https://github.com/StarRocks/starrocks/pull/77843) [#77842](https://github.com/StarRocks/starrocks/pull/77842) [#77983](https://github.com/StarRocks/starrocks/pull/77983)
- レイクのカラム更新に対してソースセグメントをストリーミングし、メモリ使用量を削減します。[#77275](https://github.com/StarRocks/starrocks/pull/77275)
- Paimon カタログのバックグラウンドメタデータリフレッシュが、各サイクルでウェアハウス全体を列挙する代わりに、最近アクセス・変更されたテーブルのみをリフレッシュするようになりました。[#78057](https://github.com/StarRocks/starrocks/pull/78057)
- パーティションプルーニング中に Paimon の `getRemoteFiles` 呼び出しに `limit` を渡すようにしました。[#76943](https://github.com/StarRocks/starrocks/pull/76943)
- GROUPING SETS のプッシュダウンを拡張し、`AVG` と `COUNT` をサポートするようにしました。[#77832](https://github.com/StarRocks/starrocks/pull/77832)
- クエリプランに `EXPLAIN COSTS` の出力を含めることができるようにしました。[#73005](https://github.com/StarRocks/starrocks/pull/73005)
- starlet のオブジェクトストアアップロードしきい値をランタイムで設定可能にしました。[#78448](https://github.com/StarRocks/starrocks/pull/78448)
- ロック競合を削減：レイクセグメントのグローバルメタキャッシュロックを削除し、SQL ブラックリスト検証をロックフリー化しました。[#78260](https://github.com/StarRocks/starrocks/pull/78260) [#78476](https://github.com/StarRocks/starrocks/pull/78476)
- コンパクションとレプリケーションのアーカイブ時に `delete_predicate` をドロップすることで、tablet メタデータを回収します。[#76870](https://github.com/StarRocks/starrocks/pull/76870)
- 共有データの削除ファイルを CRC32C でチェックサム検証します。[#77798](https://github.com/StarRocks/starrocks/pull/77798)
- キャンセルされたクエリが未報告のインスタンスを待つ時間の上限を設定しました。[#76909](https://github.com/StarRocks/starrocks/pull/76909)
- メモリ効率と可観測性向上のための jemalloc の各種改善：arm64 で 64K 版に加えて 4K ページ版をビルドし、dirty/muzzy ページおよび mapped/retained メトリクスを公開し、可変の `jemalloc_conf` オプションを実行時に適用します。[#77886](https://github.com/StarRocks/starrocks/pull/77886) [#78159](https://github.com/StarRocks/starrocks/pull/78159) [#78225](https://github.com/StarRocks/starrocks/pull/78225) [#78320](https://github.com/StarRocks/starrocks/pull/78320) [#78333](https://github.com/StarRocks/starrocks/pull/78333)
- 可観測性を追加：リーダー FE 上の `max_journal_replay_lag` ゲージと、追加のレイク公開バージョンのトレーシング。[#78382](https://github.com/StarRocks/starrocks/pull/78382) [#76658](https://github.com/StarRocks/starrocks/pull/76658) [#77048](https://github.com/StarRocks/starrocks/pull/77048)
- ジョブ完了時にリシャード出力シャードの配置ピンを解放します。[#76945](https://github.com/StarRocks/starrocks/pull/76945)
- StarOS を v4.1.2 にアップグレードしました。[#77657](https://github.com/StarRocks/starrocks/pull/77657)

### セキュリティ {#security-2}

- [CVE-2025-55163] `grpc-bom` を介して all の `io.grpc` アーティファクトを整合させ、`grpc-netty` が `grpc-core` に一致するようにしました。[#77171](https://github.com/StarRocks/starrocks/pull/77171)
- [CVE-2026-59902] Netty を 4.1.137.Final にバンプしました。[#78287](https://github.com/StarRocks/starrocks/pull/78287)
- [CVE-2026-54399] HTTP/1.1 メッセージパーサーにおけるリソース消費制御不能（DoS）脆弱性を修正するため、Apache HttpCore を 5.4.3 にバンプしました。[#77753](https://github.com/StarRocks/starrocks/pull/77753)
- [CVE-2025-48924] Broker の `commons-lang3` を 3.20.0 にアップグレードし、未使用の `commons-lang` 2.6 および `commons-configuration` 1.6 を削除しました。[#77748](https://github.com/StarRocks/starrocks/pull/77748)
- [CVE] 新しいリリースで pprof バイナリを再ビルドし、BE コンテナイメージから未使用の flamegraph ツールを削除しました。[#77846](https://github.com/StarRocks/starrocks/pull/77846)

### バグ修正 {#bug-fixes-2}

以下の問題が修正されました：

**クラッシュと安定性**

- プロセスをクラッシュ中とマークした後に致命的シグナルハンドラがハングした場合、BE を強制終了するようにしました。[#76491](https://github.com/StarRocks/starrocks/pull/76491)
- `tokenize`、`ngram_search`、`generate_series`、`corr`/`covar_pop`/`covar_samp`、`strpos`（INT32_MIN）、および decimal の配列に対する `count()` における定数または不正な引数による BE クラッシュを修正しました。[#77102](https://github.com/StarRocks/starrocks/pull/77102) [#77103](https://github.com/StarRocks/starrocks/pull/77103) [#77216](https://github.com/StarRocks/starrocks/pull/77216) [#77260](https://github.com/StarRocks/starrocks/pull/77260) [#77092](https://github.com/StarRocks/starrocks/pull/77092)
- ある `ALTER` が通常列と生成列を追加した際の BE クラッシュ、および読み取りスキーマの並び替え後のキャッシュセレクトクラッシュを修正しました。[#77436](https://github.com/StarRocks/starrocks/pull/77436) [#77245](https://github.com/StarRocks/starrocks/pull/77245)
- クエリキャッシュがパーティション化されたハッシュジョインのためにレーンを再利用する際の BE クラッシュ、およびレーンのジョインのビルド側が空の場合のハングを修正しました。[#77564](https://github.com/StarRocks/starrocks/pull/77564) [#77571](https://github.com/StarRocks/starrocks/pull/77571)
- `JsonColumn` 追加時の範囲外読み取りとサイレントな行損失、`BinaryColumnBase::replicate` における整数オーバーフロー、および 1つの配列列が複数のラムダ引数として渡された場合の `array_map` DCHECK 失敗を修正しました。[#77090](https://github.com/StarRocks/starrocks/pull/77090) [#77545](https://github.com/StarRocks/starrocks/pull/77545) [#77433](https://github.com/StarRocks/starrocks/pull/77433)
- 無効な UTF-8 に対して `substr` が隣接行のバイトを返す問題、および `array_sort` のラムダコンパレータがソートを範囲外に進める問題を修正しました。[#78722](https://github.com/StarRocks/starrocks/pull/78722) [#77687](https://github.com/StarRocks/starrocks/pull/77687)
- parquet デコーダのスクラッチバッファをスタックから外し、`SimdJsonConverter` のエラーパスにおける raw-JSON ビューの範囲を制限しました。[#77615](https://github.com/StarRocks/starrocks/pull/77615)
- 報告されたストレージメディアのないディスクでの tablet スケジューラの NPE、`INSERT ... VALUES (DEFAULT, ...)` のデパース時の NPE、およびオプティマイザにクエリ ID がない場合の `PrepareCollectMetaTask` における NPE を修正しました。[#76966](https://github.com/StarRocks/starrocks/pull/76966) [#76963](https://github.com/StarRocks/starrocks/pull/76963) [#77051](https://github.com/StarRocks/starrocks/pull/77051)

**クエリエンジンとオプティマイザ**

- 複数のオプティマイザ経路における誤った結果または結果欠落を修正しました：ストリーミング distinct グローバルリミット、ソートグループをまたぐランキングウィンドウの事前集計、マージフェーズで再追加される定数集計値、すべての子ノードに適用されない GROUP BY ルール、FOLLOWING オフセットから始まる分析フレーム。[#77086](https://github.com/StarRocks/starrocks/pull/77086) [#77068](https://github.com/StarRocks/starrocks/pull/77068) [#77069](https://github.com/StarRocks/starrocks/pull/77069) [#77099](https://github.com/StarRocks/starrocks/pull/77099) [#77342](https://github.com/StarRocks/starrocks/pull/77342)
- NL Join の null 対応アンチジョインでプローブ行が失われる問題、NL Join プローブにおける空チャンクに対する連言評価、およびグループ化キーでジョインされた `GROUPING SETS` 集計が実行可能なプランなしで失敗する問題を修正しました。[#77970](https://github.com/StarRocks/starrocks/pull/77970) [#77344](https://github.com/StarRocks/starrocks/pull/77344) [#77296](https://github.com/StarRocks/starrocks/pull/77296)
- grouping-sets-to-union-all 書き換えの順序、セット演算子としてのリミットの重複、およびハングを回避するために Set 演算子を colocate 実行グループから除外する問題を修正しました。[#77380](https://github.com/StarRocks/starrocks/pull/77380) [#77692](https://github.com/StarRocks/starrocks/pull/77692) [#77025](https://github.com/StarRocks/starrocks/pull/77025)
- ウィンドウの `PARTITION BY`/`ORDER BY` におけるサブクエリプランニング、複数列 `count(distinct)` 用に構築された `if()` の decimal 型の維持、スロットの null 許容性にスキャンヘビー式の結果列を正規化する問題を修正しました。[#77087](https://github.com/StarRocks/starrocks/pull/77087) [#77346](https://github.com/StarRocks/starrocks/pull/77346) [#77242](https://github.com/StarRocks/starrocks/pull/77242)
- 型指定されていない配列リテラルが誤った型を取得する問題（ビューを壊す）、force-inline リミットに達したときの CTE アンカリングの問題、再帰 CTE 用に推論された CTAS 列の再利用を修正しました。[#76986](https://github.com/StarRocks/starrocks/pull/76986) [#77018](https://github.com/StarRocks/starrocks/pull/77018) [#76453](https://github.com/StarRocks/starrocks/pull/76453)
- スキャンスキーマ、ジョイン述語の共通部分式、ANN 検索仕様をクエリキャッシュダイジェストに追加し、`SortNode` が TOPN フィルタを構築する際にキャッシュを無効化することで、複数のクエリキャッシュ正確性の問題を修正しました。[#77559](https://github.com/StarRocks/starrocks/pull/77559) [#77565](https://github.com/StarRocks/starrocks/pull/77565) [#77586](https://github.com/StarRocks/starrocks/pull/77586) [#77404](https://github.com/StarRocks/starrocks/pull/77404)
- 表示バージョンを決してカバーしない行数からの `COUNT(*)` の畳み込み、ジョイン並び替えにおける無条件の UKFK 制約収集、転置インデックスフィルタリング後に OR 常駐述語によって読み取られる列の保持を修正しました。[#77190](https://github.com/StarRocks/starrocks/pull/77190) [#77197](https://github.com/StarRocks/starrocks/pull/77197) [#77659](https://github.com/StarRocks/starrocks/pull/77659)
- 日時解析の正確性を修正：Teradata および Joda パーサーを入力文字列の範囲内に制限し、範囲外のユリウス値に対して `DateValue::weekday()` を範囲内に保つようにしました。[#77098](https://github.com/StarRocks/starrocks/pull/77098) [#77310](https://github.com/StarRocks/starrocks/pull/77310)
- バッチバッファが巨大になる場合に `EXCEPT`/`INTERSECT` キーを行ごとにシリアライズし、セット演算がキーをデシリアライズする際に JSON キー列を再構築するようにしました。[#77821](https://github.com/StarRocks/starrocks/pull/77821) [#77215](https://github.com/StarRocks/starrocks/pull/77215)
- 追加のオプティマイザ／式の修正：`StatusOr` をアンラップする前にステータスを確認する、プロパティ強制時に一度だけプロパティ充足を判定する、`encode_fingerprint_sha256` でインデックス 0 の定数引数を読み取る、非正の列長に対する `CHAR` 述語のゼロパディングをスキップする、非正のランキング述語を TopN にプッシュしない、推定されたパーティション述語によるプルーニング時に NULL パーティションを保持する、`_switch_context` において出力スキーマの末尾を超えて読み取らないようにする、同じ NULL 規約でランをソートしてランをマージする、prepare が失敗しうる前に `Analytor::_fns` を埋める、外部 CTAS のリストパーティション列を `NOT NULL` に強制しないようにする。[#77052](https://github.com/StarRocks/starrocks/pull/77052) [#77091](https://github.com/StarRocks/starrocks/pull/77091) [#77104](https://github.com/StarRocks/starrocks/pull/77104) [#77444](https://github.com/StarRocks/starrocks/pull/77444) [#77445](https://github.com/StarRocks/starrocks/pull/77445) [#77596](https://github.com/StarRocks/starrocks/pull/77596) [#77660](https://github.com/StarRocks/starrocks/pull/77660) [#77679](https://github.com/StarRocks/starrocks/pull/77679) [#77758](https://github.com/StarRocks/starrocks/pull/77758) [#78468](https://github.com/StarRocks/starrocks/pull/78468)
- ステートメント／デパースの問題を修正：列リスト付きの Iceberg/Hive `INSERT` の静的パーティション句を検証する、パーティション参照のデパース時に `TEMPORARY` 修飾子を保持する、プロファイル内の空の SQL ステートメントを修正するために CTAS デパースをサポートする、転送されたステートメントが元の SQL を保持するように `proxyExecute` で `isSingleStmt` を設定する。[#76659](https://github.com/StarRocks/starrocks/pull/76659) [#76987](https://github.com/StarRocks/starrocks/pull/76987) [#77978](https://github.com/StarRocks/starrocks/pull/77978) [#77982](https://github.com/StarRocks/starrocks/pull/77982)

**JSON とデータタイプ**

- 大きな `BinaryColumn` の SerDe 問題、名前にドットを含む列の統計収集、`multi_unnest`/`unnest` 配列オフセットを `int32` ではなく `uint32` として読み取る問題を修正しました。[#75504](https://github.com/StarRocks/starrocks/pull/75504) [#71301](https://github.com/StarRocks/starrocks/pull/71301) [#78479](https://github.com/StarRocks/starrocks/pull/78479) [#78480](https://github.com/StarRocks/starrocks/pull/78480)
- Flat JSON 垂直コンパクションのメモリ使用量削減、および構造体のサブフィールド述語が rowid によって評価される際に値自体を読み取る問題を修正しました。[#77682](https://github.com/StarRocks/starrocks/pull/77682) [#77891](https://github.com/StarRocks/starrocks/pull/77891)

**マテリアライズドビュー**

- 外部認可を伴う IVM リフレッシュ、MV 書き換えの鮮度ベースライン（最後に鮮度が確認された時刻を使用するように変更）、およびタイムトラベルクエリでの MV 書き換えのスキップを修正しました。[#77210](https://github.com/StarRocks/starrocks/pull/77210) [#76758](https://github.com/StarRocks/starrocks/pull/76758) [#77683](https://github.com/StarRocks/starrocks/pull/77683)
- マテリアライズドビュー上に作成された GIN インデックスへのインデックス ID 割り当て、およびビューベースの MV 書き換えで差し戻す際にビュースキャンのプロジェクションを保持する問題を修正しました。[#77340](https://github.com/StarRocks/starrocks/pull/77340) [#77613](https://github.com/StarRocks/starrocks/pull/77613)
- その他のマテリアライズドビューの修正：`default_mv_refresh_mode` の検証、インクリメンタル MV が主キーをソートではなく拡張する際の `ORDER BY` の修正、`information_schema.materialized_views` の内部クエリを `query_timeout` で制限する修正。[#77231](https://github.com/StarRocks/starrocks/pull/77231) [#77680](https://github.com/StarRocks/starrocks/pull/77680) [#77585](https://github.com/StarRocks/starrocks/pull/77585)

**データレイクコネクタ（Iceberg、Paimon、Delta、Hive）**

- Iceberg の正確性と安定性を修正：書き込みパスで NOT NULL 列を強制、プラン時のスナップショットからの書き換えコミットを検証、`TRUNCATE` 後のメタデータキャッシュの無効化、メタデータファイルが見つからないテーブルの削除、文字列パーティションへのキャストによるメタデータ削除前のネイティブテーブルのリフレッシュ、`WHERE` 句を伴う `rewrite_data_files` でのサイレントなデータ損失、`remove_orphan_files` における保持ウィンドウが小さすぎる問題。[#77589](https://github.com/StarRocks/starrocks/pull/77589) [#77709](https://github.com/StarRocks/starrocks/pull/77709) [#77385](https://github.com/StarRocks/starrocks/pull/77385) [#77786](https://github.com/StarRocks/starrocks/pull/77786) [#77335](https://github.com/StarRocks/starrocks/pull/77335) [#77797](https://github.com/StarRocks/starrocks/pull/77797) [#77394](https://github.com/StarRocks/starrocks/pull/77394)
- Delta Lake のテーブルごとの cloud 認証情報を分離しました。[#77424](https://github.com/StarRocks/starrocks/pull/77424)
- Hive 外部テーブルに対する OpenX JsonSerDe の列名マッピング、1 つの JSON フィールドを複数の Hive 列にマッピングする機能をサポートし、デフォルトの catalog が存在しない場合や CTE 参照時のコネクタビューでのテーブル修飾の問題を修正しました。[#65515](https://github.com/StarRocks/starrocks/pull/65515) [#77716](https://github.com/StarRocks/starrocks/pull/77716) [#77887](https://github.com/StarRocks/starrocks/pull/77887)
- 外部テーブルに対する Ranger のビュー / MV 認可を修正し、FE のクラスパス上で Ranger Solr の監査先を復元しました。[#56316](https://github.com/StarRocks/starrocks/pull/56316) [#77294](https://github.com/StarRocks/starrocks/pull/77294)
- 破損した `DELTA_BINARY_PACKED` ヘッダーがスローされずに返されていた問題、および Parquet リーダーでフィルタ対象外の FLBA ディクショナリ行を NULL としてマークする問題を修正しました。[#77446](https://github.com/StarRocks/starrocks/pull/77446) [#78685](https://github.com/StarRocks/starrocks/pull/78685)
- Iceberg REST catalog に対する `ALTER TABLE RENAME` での NPE、`$partitions` の decimal または null の `timestamptz` パーティション値を読み取る際の NPE、Iceberg catalog 間で 1 つの Hadoop `FileSystem` を共有する問題、および `paimon.option.cache-enabled=false` を再び尊重する問題を修正しました。[#76498](https://github.com/StarRocks/starrocks/pull/76498) [#77742](https://github.com/StarRocks/starrocks/pull/77742) [#77297](https://github.com/StarRocks/starrocks/pull/77297) [#78271](https://github.com/StarRocks/starrocks/pull/78271)

**共有データ：Compaction、Vacuum、Reshard、Replication**

- Compaction の修正：ファイルバンドリングがスキップする Compaction トランザクションログのキャッシュ、最大 tablet スコアによる候補のソート、読み取りチャンクサイズの見積もりの修正、縮小時にトークンを失う Limiter リサイズの修正、削除のみの書き込みに対するパブリッシュ時のセグメント書き換えのスキップ。[#77578](https://github.com/StarRocks/starrocks/pull/77578) [#77482](https://github.com/StarRocks/starrocks/pull/77482) [#78192](https://github.com/StarRocks/starrocks/pull/78192) [#78087](https://github.com/StarRocks/starrocks/pull/78087) [#78071](https://github.com/StarRocks/starrocks/pull/78071)
- Vacuum の修正：grace-blocked vacuum における無駄な読み取りと取り残された v1 削除を修正、reshard 引き継ぎ時に autovacuum の保持下限を維持、冗長な lake メタデータの 404 プローブを回避しました。[#78460](https://github.com/StarRocks/starrocks/pull/78460) [#78545](https://github.com/StarRocks/starrocks/pull/78545) [#78466](https://github.com/StarRocks/starrocks/pull/78466)
- Reshard / split の修正：ソースの tablet が置き換えられた materialized index に存在する reshard を拒否、クロスパブリッシュ時の tablet split フォールバックを修正、予期された reshard 履歴ルックアップの警告を抑制、フェイルオーバー後に lake alter ジョブがスタックしないようにしました。[#77512](https://github.com/StarRocks/starrocks/pull/77512) [#78270](https://github.com/StarRocks/starrocks/pull/78270) [#78059](https://github.com/StarRocks/starrocks/pull/78059) [#78517](https://github.com/StarRocks/starrocks/pull/78517)
- Replication の修正：共有 lake ファイルの保持、リモートのバンドル済み lake メタデータの読み取り、Replication 中のソース lake ファイルの再暗号化。[#77361](https://github.com/StarRocks/starrocks/pull/77361) [#77362](https://github.com/StarRocks/starrocks/pull/77362) [#78080](https://github.com/StarRocks/starrocks/pull/78080)
- 破損したローカルキャッシュの復旧：delvec ページのチェックサム失敗時に破棄して再試行し、PK インデックス SST Compaction と tablet マージの SSTable 再構築の破損時に再構築するようにしました。[#78086](https://github.com/StarRocks/starrocks/pull/78086) [#77481](https://github.com/StarRocks/starrocks/pull/77481) [#78003](https://github.com/StarRocks/starrocks/pull/78003)
- パーティション共有オブジェクトから lake version-1 メタデータを読み取る問題、スナップショットが所有する共有シャードグループの保護、および `TransactionGraph.remove` における依存関係の順序の保持を修正しました。[#78731](https://github.com/StarRocks/starrocks/pull/78731) [#78800](https://github.com/StarRocks/starrocks/pull/78800) [#78077](https://github.com/StarRocks/starrocks/pull/78077)
- その他の共有データの修正：最終コミット時のトランザクションのスロットリング、`DECOMMISSION` レプリカをロードのプライマリとして選択しないようにする修正、オンラインオプティマイズ中のシャドウ生成列を伴う DML の修正、`spill_enable_direct_io` が実際に `O_DIRECT` に関与するようにする修正、並行する `BEGIN WITH LABEL` 間のラベル一意性の競合状態の修正、クエリトラッカーの予約上限を有効なクエリメモリ上限から導出する修正、tablet マージ中の rowset ID と参照の安全な再マッピング、クロスパブリッシュされた del ファイルを子が所有するキーにのみ適用する修正、tablet マージが range-distribution の colocate を尊重するようにする修正、不完全な結合トランザクションログを拒否して `replication_num` が共有データの書き込みクォーラムを弱めないようにする修正、reshard 中に明示的なトランザクションを 1 つの tablet レイアウトに保つ修正、del ファイルのローカルキャッシュを破棄してチェックサム失敗時に再試行する修正、クローズされた lake delta writer がタスクを拒否した際に中止理由を報告する修正。[#77015](https://github.com/StarRocks/starrocks/pull/77015) [#77035](https://github.com/StarRocks/starrocks/pull/77035) [#77252](https://github.com/StarRocks/starrocks/pull/77252) [#77255](https://github.com/StarRocks/starrocks/pull/77255) [#77401](https://github.com/StarRocks/starrocks/pull/77401) [#77408](https://github.com/StarRocks/starrocks/pull/77408) [#77690](https://github.com/StarRocks/starrocks/pull/77690) [#77744](https://github.com/StarRocks/starrocks/pull/77744) [#78113](https://github.com/StarRocks/starrocks/pull/78113) [#78162](https://github.com/StarRocks/starrocks/pull/78162) [#78625](https://github.com/StarRocks/starrocks/pull/78625) [#78022](https://github.com/StarRocks/starrocks/pull/78022) [#78632](https://github.com/StarRocks/starrocks/pull/78632)
- 共有データのソートキーおよび rowset ID のエッジケースを修正：末尾のキー列追加後に split 境界を現在のソートキーに投影する、再帰 CTE 実行で内部の `INSERT` の失敗を表面化させる、クロスパブリッシュされた rowset が存在するかどうかを按分された行数で判断しないようにする、幅の狭い固定長 IVM 行 ID に対して `encode_sort_key` に到達できるようにしてその定数引数を拡張する、拡張列の delta column group をそのルート列を通じて解決する、range-split のクロスパブリッシュ統計を保持する、FE の最大 tablet Compaction スコア収集を修正する、自動増分マップを削除する際にデッドノードをスキップする、ソートされていない PK SST ライターのマップを割り当て数のカウントではなく測定する、`compression_level` を合成サブ列メタに伝播する、組み込みの転置インデックスバックエンドに対してスタンドアロンの GIN インデックスディレクトリ処理をスキップする。[#77504](https://github.com/StarRocks/starrocks/pull/77504) [#77510](https://github.com/StarRocks/starrocks/pull/77510) [#77511](https://github.com/StarRocks/starrocks/pull/77511) [#77751](https://github.com/StarRocks/starrocks/pull/77751) [#77847](https://github.com/StarRocks/starrocks/pull/77847) [#77759](https://github.com/StarRocks/starrocks/pull/77759) [#78269](https://github.com/StarRocks/starrocks/pull/78269) [#78496](https://github.com/StarRocks/starrocks/pull/78496) [#78561](https://github.com/StarRocks/starrocks/pull/78561) [#77498](https://github.com/StarRocks/starrocks/pull/77498) [#76949](https://github.com/StarRocks/starrocks/pull/76949) [#77101](https://github.com/StarRocks/starrocks/pull/77101)

**ロード、トランザクション、DML**

- データロードおよび列モードの部分更新パスでチャンク容量の境界設定とチェックを修正し、所有する子ごとに条件付き更新と部分更新を決定し、マージされた rowset の del `op_offset` をステートメントごとに記録するようにしました。[#77163](https://github.com/StarRocks/starrocks/pull/77163) [#78099](https://github.com/StarRocks/starrocks/pull/78099) [#78313](https://github.com/StarRocks/starrocks/pull/78313)
- 削除されたテーブルに対する insert-overwrite の失敗がジャーナル化されない問題を修正し、複数の部分更新パスにわたって現在の行のライフサイクルを正しく維持するようにしました。[#77212](https://github.com/StarRocks/starrocks/pull/77212)
- 切断時の kill を I/O スレッドではなくワーカーに渡すようにし、`GroupedExecutionSinkOperator::push_chunk` 内で `accept()` ステータスを伝播するように修正しました。[#77590](https://github.com/StarRocks/starrocks/pull/77590) [#77737](https://github.com/StarRocks/starrocks/pull/77737)
- エラー状態のみを記録する内部の DML / CONTROL ステートメントを失敗させるようにし、失敗がサイレントに飲み込まれることなく表面化するようにしました。[#77388](https://github.com/StarRocks/starrocks/pull/77388)

**統計情報、メトリクス、FE / メタデータ**

- 統計情報の正確性を修正：ヒストグラムにおける null のバケット / MCV を拒否し、`CancelableAnalyzeTask.cancel` が単一の勝者を選ぶようにしました。[#77024](https://github.com/StarRocks/starrocks/pull/77024) [#78593](https://github.com/StarRocks/starrocks/pull/78593)
- `pipe_driver_queue_len` メトリクスを登録し、NotFound の lake tablet メタデータ読み取りに対する BE メトリクスを追加し、テーブル以外の StarOS シャードとチェックポイントスレッドプールのメトリクスに対するテーブルメトリクスをスキップし、プロファイルディクショナリ内の誤記のある `ZoneMapIndexFilter` キーを修正しました。[#77890](https://github.com/StarRocks/starrocks/pull/77890) [#78451](https://github.com/StarRocks/starrocks/pull/78451) [#78116](https://github.com/StarRocks/starrocks/pull/78116) [#77367](https://github.com/StarRocks/starrocks/pull/77367) [#78728](https://github.com/StarRocks/starrocks/pull/78728)
- FE / メタデータの修正：メタ同期においてリサイクルされた欠落シャードグループを無視、bRPC でエンドポイントごとの複数の接続グループを復元、BE に送信する前に FE でコンピュートノードのホスト名を解決、`DROP FOLLOWER` / `OBSERVER` 時にドロップされた FE が自身を終了するようにし、colocate group スケジューリングのサンプルノブを公開しました。[#76604](https://github.com/StarRocks/starrocks/pull/76604) [#76765](https://github.com/StarRocks/starrocks/pull/76765) [#78390](https://github.com/StarRocks/starrocks/pull/78390) [#78215](https://github.com/StarRocks/starrocks/pull/78215) [#78450](https://github.com/StarRocks/starrocks/pull/78450)
- メタスキャンの tablet を拡張する際に拡張されていないアクセスパスをスキップする問題、スキャンのアクセスパスを列 ID で照合する問題、BE がプルーニングした場合にのみ OLAP スキャンの複合型を絞り込む問題、集計状態列のサブフィールドをプルーニングしない問題を修正しました。[#76593](https://github.com/StarRocks/starrocks/pull/76593) [#77987](https://github.com/StarRocks/starrocks/pull/77987) [#77894](https://github.com/StarRocks/starrocks/pull/77894) [#77901](https://github.com/StarRocks/starrocks/pull/77901)
- 集計状態列にグローバル辞書を使用しない、または `ARRAY` / `STRUCT` を返す式を辞書化しない、プッシュダウンされた述語がスパースな範囲を読み取る際にインデックスページの null フラグを序数でインデックス付けするようにしました。[#77096](https://github.com/StarRocks/starrocks/pull/77096) [#78488](https://github.com/StarRocks/starrocks/pull/78488)
- その他のメトリクスおよび FE の修正：bRPC メソッドエラーの bvar をカウンターとして報告、range-distribution パーティションの実際の tablet 数を Buckets として報告、`show proc '/global_current_queries'` の NPE を修正、中間クエリ統計情報で Data Cache の読み取り統計を報告、jemalloc `malloc_usable_size` で最小限の TSD フェッチを使用、ヒーププロファイルのトグルが `prof.thread_active_init` を書き込まないようにする、ラップ後の FE 接続 ID の衝突を防止、多くのパーティションが古い場合にテーブル統計の健全性が約 1.0 に固定される問題を修正、FE の feature-log パスに `feature_log_dir` を使用、論理ウィンドウ演算子をコピーする際に `inputIsBinary` を保持、キャッシュ無効化後の Thrift クライアントリークを修正しました。[#76956](https://github.com/StarRocks/starrocks/pull/76956) [#77465](https://github.com/StarRocks/starrocks/pull/77465) [#77407](https://github.com/StarRocks/starrocks/pull/77407) [#77696](https://github.com/StarRocks/starrocks/pull/77696) [#77625](https://github.com/StarRocks/starrocks/pull/77625) [#78384](https://github.com/StarRocks/starrocks/pull/78384) [#78217](https://github.com/StarRocks/starrocks/pull/78217) [#76927](https://github.com/StarRocks/starrocks/pull/76927) [#77221](https://github.com/StarRocks/starrocks/pull/77221) [#77058](https://github.com/StarRocks/starrocks/pull/77058) [#77072](https://github.com/StarRocks/starrocks/pull/77072)

**その他**

- プールされた Poco セッションにリクエストタイムアウトを適用し、Linux AArch64 上で Hyperscan を Vectorscan 5.4.12 にアップグレードし、`JAVA_OPTS_FOR_JDK_*` の警告をエスカレーションし、java-extensions のリーダーライブラリにテストスコープの jar を含めないようにしました。[#78361](https://github.com/StarRocks/starrocks/pull/78361) [#78326](https://github.com/StarRocks/starrocks/pull/78326) [#78328](https://github.com/StarRocks/starrocks/pull/78328) [#77752](https://github.com/StarRocks/starrocks/pull/77752)
- `dictionary_get` キーをキーサイズでシリアライズし、thrift プロセッサ内の例外をキャッチし、CTAS 用に `files()` のスキーマ推論をプランナーのメタロックの外に保つようにしました。[#78706](https://github.com/StarRocks/starrocks/pull/78706) [#78805](https://github.com/StarRocks/starrocks/pull/78805) [#78772](https://github.com/StarRocks/starrocks/pull/78772)
- ファイルに末尾の改行がない場合に最後の conf 行を読み取る、設定 YAML パスを cwd に対して解決する、GCS アップロード閾値の BE 設定名を `gs` ではなく `gcs` にする、StarOS と Starlet を 4.1.3 にアップグレードする、廃止された設定の forward-header split とドキュメントファイル名内の紛れ込んだ U+200E を削除しました。[#78065](https://github.com/StarRocks/starrocks/pull/78065) [#77132](https://github.com/StarRocks/starrocks/pull/77132) [#78452](https://github.com/StarRocks/starrocks/pull/78452) [#78038](https://github.com/StarRocks/starrocks/pull/78038) [#78780](https://github.com/StarRocks/starrocks/pull/78780) [#78612](https://github.com/StarRocks/starrocks/pull/78612)

## 4.1.4 {#414}

リリース日：2026 年 8 月 5 日

### 動作の変更 {#behavior-changes-2}

- 共有データモードにおいて、`TABLESAMPLE` / `SAMPLE` 句および `ANALYZE SAMPLE TABLE` が lake テーブルに対して有効になりました。以前はサンプルオプションが BE に到達せず、フルスキャンが実行されていました。[#71874](https://github.com/StarRocks/starrocks/pull/71874)
- `ALTER TABLE` が `flat_json` 設定に加えた変更は、バージョン管理されたタスクを通じて BE ノードに伝播されるようになり、確実に反映されます。[#74747](https://github.com/StarRocks/starrocks/pull/74747)
- GIN 転置インデックスで解決される `NOT MATCH` 述語が、NULL 行を返さなくなりました。NULL 行は結果から正しく除外されるようになりました。[#75578](https://github.com/StarRocks/starrocks/pull/75578)
- `ANALYZE ... UPDATE HISTOGRAM ON` は文字系列の列（オプティマイザが使用しない）に対してヒストグラムを計算しなくなり、MCV のみを計算するようになったため、分析コストが削減されました。[#75968](https://github.com/StarRocks/starrocks/pull/75968)
- `rewrite_manifests` は、順序を保持するパーティション範囲によって出力マニフェストをクラスタリングするようになり、マニフェストごとのパーティション範囲がより厳密になり、パーティション数の多いテーブルでのマニフェストプルーニングが改善されました。[#76193](https://github.com/StarRocks/starrocks/pull/76193)
- 複数テーブルの Stream Load は、結合トランザクションログを使用するかどうかを判断する際に、グローバルな `lake_use_combined_txn_log` 設定のみに従うのではなく、各テーブルの `file_bundling` プロパティを考慮するようになりました。[#76806](https://github.com/StarRocks/starrocks/pull/76806)
- GIN 転置インデックスと主キーテーブルでの列モード部分更新を組み合わせた場合に、破損した結果が返されなくなりました。インデックスはデルタ列グループセグメントから提供されるようになりました。[#76271](https://github.com/StarRocks/starrocks/pull/76271)
- 整数配列に対する `array_difference` は、`BIGINT` に拡張する前に隣接差分を 64 ビットで計算するようになり、int32 のオーバーフローが修正されました。[#76569](https://github.com/StarRocks/starrocks/pull/76569)
- 非定数の除数を持つ除算式が単調であると見なされなくなり、誤った結果を生む可能性があった ZoneMap プルーニングの不具合が修正されました。[#76744](https://github.com/StarRocks/starrocks/pull/76744)
- 配列およびマップコンストラクタは、チャンクのフラット化された結果が 4 GB を超えた場合に、データを黙って破損させるのではなくエラーを発生するようになりました。[#76419](https://github.com/StarRocks/starrocks/pull/76419)
- 主キーの挿入時に、L1/L2 永続性インデックスにすでに存在するキーを正しく拒否するようになりました。以前は存在チェックがスキップされていました。[#76591](https://github.com/StarRocks/starrocks/pull/76591)
- レンジコロケートテーブルでのサンプルベースの tablet 事前分割が、新しいシャードをソース tablet のワーカーに詰め込むのではなく、コンピュートノード全体に分散するようになり、ハッシュに比べて約 3 倍遅かったレンジバッチインポートの問題が修正されました。[#76608](https://github.com/StarRocks/starrocks/pull/76608)
- 増分または AUTO のマテリアライズドビューは、`AUTO` リフレッシュモードにおいて PCT フォールバック用のメンテナンスクエリを再構築するようになり、IVM がサポートしない形状に対するリフレッシュ失敗が修正されました。[#75961](https://github.com/StarRocks/starrocks/pull/75961)
- `ALTER MATERIALIZED VIEW ... ACTIVE` は非集計の INCREMENTAL マテリアライズドビューでも動作するようになりました。ストレージが埋める `__ROW_ID__` 列は MV の DDL 列リストから除外されるため、非アクティブな IVM を再アクティブ化できるようになりました。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- Iceberg パーティションキャッシュは、エントリ数だけでなくメモリによっても制限されるようになり、その使用状況も公開されるようになったため、パーティション数の多いテーブルでの無制限な増加を防げます。[#76165](https://github.com/StarRocks/starrocks/pull/76165)
- OAuth2 クライアント資格情報を使用する Iceberg REST catalog は、バックグラウンドのトークンリフレッシュタスクが停止した後、all 後続のリクエストが失敗する代わりに自己回復するようになりました。[#76457](https://github.com/StarRocks/starrocks/pull/76457)
- Lake full vacuum は、以前はスキップされていた孤立した `.lcrm`（Lake Compaction Rows Mapper）ファイルを回収するようになりました。[#76522](https://github.com/StarRocks/starrocks/pull/76522)
- 共有データの publish 処理は、tablet が欠落しているバンドル tablet メタデータファイルの書き込みを拒否するようになり、パーティションが publish 時に永続的に停止してしまう問題を防ぎます。[#76850](https://github.com/StarRocks/starrocks/pull/76850)
- 破棄された外部（Spark/Flink コネクタ）スキャンコンテキストは、回収時にそのパイプラインフラグメントをキャンセルするようになり、外部スキャンプランは QueryContext のライフタイムを制限するために `query_delivery_timeout` を設定するようになりました。[#76535](https://github.com/StarRocks/starrocks/pull/76535) [#76536](https://github.com/StarRocks/starrocks/pull/76536)
- マテリアライズドビューの pinned-range マップが（テーブル UUID によって）正しくキー付けされるようになり、OLAP テーブルで MV のブートストラップピン留めが無効になる問題や、異なるデータベースにある同名テーブルの誤処理が修正されました。[#76320](https://github.com/StarRocks/starrocks/pull/76320) [#76351](https://github.com/StarRocks/starrocks/pull/76351)
- Leader FE に転送された文について、監査ログは CTE エイリアスや完全修飾されていないテーブル名ではなく、（Leader と一致する）完全修飾されたクエリ対象リレーションを記録するようになりました。[#76387](https://github.com/StarRocks/starrocks/pull/76387)
- 完全な `STRUCT` 列を `ROLLUP`、`CUBE`、または `GROUPING SETS` と一緒に集計しても、プラン作成時に `usedStructFiledPos` エラーで失敗しなくなりました。[#76804](https://github.com/StarRocks/starrocks/pull/76804)
- FE メモリの `Estimator` はコンテナのオーバーヘッドをカウントするようになり、Parquet スキャナはバッチサイズ全体ではなく制限されたチャンクサイズを使用して欠落列をパディングするようになったため、メモリ計上の精度が向上しました。[#75971](https://github.com/StarRocks/starrocks/pull/75971) [#75981](https://github.com/StarRocks/starrocks/pull/75981)

### 改善 {#improvements-3}

- Kafka/Pulsar のメッセージメタデータ（パーティション、オフセット、タイムスタンプなど）を公開するための `INCLUDE METADATA` 句を Routine Load でサポートするようになりました。[#73840](https://github.com/StarRocks/starrocks/pull/73840)
- Routine Load メタデータエイリアスを省略可能にしました。[#76294](https://github.com/StarRocks/starrocks/pull/76294)
- `information_schema.materialized_views` に `LAST_FRESHNESS_CONFIRMED_AT` 列を追加しました。[#74585](https://github.com/StarRocks/starrocks/pull/74585)
- `/metrics` エンドポイントを通じて Data Cache のメトリクスを公開するとともに、FE の Compaction メトリクス、`ALTER TABLE` 列操作のメトリクスと所要時間、および exchange sink の `CompressedInputBytes` メトリクスを追加しました。[#58204](https://github.com/StarRocks/starrocks/pull/58204) [#72941](https://github.com/StarRocks/starrocks/pull/72941) [#76247](https://github.com/StarRocks/starrocks/pull/76247) [#76309](https://github.com/StarRocks/starrocks/pull/76309)
- コネクタのメタデータ操作における認証および接続エラーの詳細をエンドユーザーに公開するようにしました。[#75490](https://github.com/StarRocks/starrocks/pull/75490)
- 大規模列の容量制限チェックに関するエラーメッセージを改善しました。[#76303](https://github.com/StarRocks/starrocks/pull/76303)
- `hdfs_backend_selector_cache_replica_num` 変数を追加し、オブジェクトストレージクライアントのキャッシュサイズを実行時に変更可能にしました。[#75023](https://github.com/StarRocks/starrocks/pull/75023) [#75851](https://github.com/StarRocks/starrocks/pull/75851)
- サンプルベースの tablet 事前分割メタ層リーダーが、`CHAR`、複合ソートキー、UTC 調整された Parquet の `TIMESTAMP` / ORC の `TIMESTAMP_INSTANT` を含む、より多くのソートキータイプに対応するようになりました。[#75937](https://github.com/StarRocks/starrocks/pull/75937) [#76011](https://github.com/StarRocks/starrocks/pull/76011) [#76114](https://github.com/StarRocks/starrocks/pull/76114)
- 外部テーブルの統計収集を改善しました。述語列の使用状況が追跡されるようになり、複数列を 1 回のスキャンで収集できるようになり、Iceberg テーブルに対してコスト上限付きのスキャン予算が適用されるようになりました。[#75938](https://github.com/StarRocks/starrocks/pull/75938) [#76638](https://github.com/StarRocks/starrocks/pull/76638) [#76549](https://github.com/StarRocks/starrocks/pull/76549)
- 右外部、semi、anti、full-outer のレンジコロケートジョインをサポートしました。[#76040](https://github.com/StarRocks/starrocks/pull/76040)
- Query Queue V2 のコスト推定を最適化しました。[#76609](https://github.com/StarRocks/starrocks/pull/76609)
- `FILES()` のグロブに対して、ワイルドカードリテラルプレフィックスを S3 の `ListObjectsV2` 呼び出しにプッシュダウンし、列挙するオブジェクト数を削減しました。[#76210](https://github.com/StarRocks/starrocks/pull/76210)
- `remove_orphan_files` で Iceberg マニフェストエントリをスキャンする際、`file_path` 列のみが投影されるようになりました。[#76020](https://github.com/StarRocks/starrocks/pull/76020)
- lake の主キー tablet に対する base compaction、ならびに分離ソートキーの cloud ネイティブ主キーテーブルに対するロードスピルおよび積極的な PK インデックス SST をサポートしました。[#76794](https://github.com/StarRocks/starrocks/pull/76794) [#76094](https://github.com/StarRocks/starrocks/pull/76094)
- 共有データのレンジ分散テーブルにおいて、末尾のソートキー列をメタデータのみで追加する操作をサポートしました。[#76341](https://github.com/StarRocks/starrocks/pull/76341)
- フラグメントのキャンセル時に実行中の exchange sink RPC をキャンセルし、リシャード後のクリーニング中にリシャードされたパーティションの実行中の Compaction をキャンセルするようにしました。[#75613](https://github.com/StarRocks/starrocks/pull/75613) [#76759](https://github.com/StarRocks/starrocks/pull/76759)
- 複数文の Stream Load チャネルを待機前に発火させるようにし、ロードのレイテンシを削減しました。[#76715](https://github.com/StarRocks/starrocks/pull/76715)
- `Analytor::process` における列アップグレードのメモリ制限チェックを追加しました。[#75821](https://github.com/StarRocks/starrocks/pull/75821)
- 非同期 delta writer が停止した際に実際のエラーステータスを保持するようにしました。[#76216](https://github.com/StarRocks/starrocks/pull/76216)
- レイク PK インデックス SSTable に世代バージョンを記録し、レイク Primary Key の publish 時に `op_write.seg_delvecs` を適用し、Compaction の publish バージョン競合解決時に出力セグメントフッターのオープンをスキップするようにしました。[#76208](https://github.com/StarRocks/starrocks/pull/76208) [#76474](https://github.com/StarRocks/starrocks/pull/76474) [#76657](https://github.com/StarRocks/starrocks/pull/76657)
- 診断を補助するため、レイクロードおよび publish-version のパスにスタックトレースと詳細なトレースカウンタを追加しました。[#75901](https://github.com/StarRocks/starrocks/pull/75901) [#76810](https://github.com/StarRocks/starrocks/pull/76810)

### セキュリティ {#security-3}

- [CVE-2026-44891] STOMP サブフレームデコーダーにおけるメモリ枯渇（DoS）脆弱性を修正するため、Netty を 4.1.136.Final にバンプしました。[#76555](https://github.com/StarRocks/starrocks/pull/76555)
- [CVE-2026-55971] [CVE-2026-43871] C++ バインディングにおけるヒープベースのバッファオーバーフローと無限ループの脆弱性を修正するため、Apache Thrift を 0.24.0 にバンプしました。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-10050] 脆弱な Jetty の jar（Hadoop から推移的に取り込まれるクライアント側 Digest 認証バイパス）を除外し、pgjdbc を 42.7.12 にバンプしました。[#76783](https://github.com/StarRocks/starrocks/pull/76783)
- [CVE-2011-4969] [CVE-2014-6071] 脆弱な jQuery 1.4.2（および他の jQuery XSS CVE）をバンドルしていた未使用の `avro-ipc` jar を除外しました。[#76270](https://github.com/StarRocks/starrocks/pull/76270)
- [CVE-2024-29857] 修正済みのバージョンと並存していた古い脆弱な推移的依存関係（例：`bcprov-jdk15on` 1.70 や EOL の `okhttp` 2.x）を削除し、再発を防ぐために依存関係の禁止設定を追加しました。[#76097](https://github.com/StarRocks/starrocks/pull/76097)

### バグ修正 {#bug-fixes-3}

以下の問題が修正されました：

- ジョイン述語導出時の空レンジでプランナーがクラッシュする問題。[#75011](https://github.com/StarRocks/starrocks/pull/75011)
- null でない定数 `ELSE` を持つ `CASE` に対して集計が誤ってプッシュダウンされる問題。[#75037](https://github.com/StarRocks/starrocks/pull/75037)
- `PARTITION-TOP-N` の partition-by がプルーニングされた辞書スロットに書き換えられる問題。[#75956](https://github.com/StarRocks/starrocks/pull/75956)
- ジョインのシャッフルジョイン出力プロパティ分岐での演算子優先順位の誤り、および `predicateCommonOperators` がジョイン演算子構築時に引き継がれない問題。[#76203](https://github.com/StarRocks/starrocks/pull/76203) [#76330](https://github.com/StarRocks/starrocks/pull/76330) [#76388](https://github.com/StarRocks/starrocks/pull/76388)
- ビュー列および `ROLLUP` キーの解析時における null 許容性の誤り。[#75684](https://github.com/StarRocks/starrocks/pull/75684) [#76149](https://github.com/StarRocks/starrocks/pull/76149)
- 長さゼロのキャプチャグループに対する `regexp_extract_all` の無限ループ問題。[#75798](https://github.com/StarRocks/starrocks/pull/75798)
- `LargeOrCalculatingVisitor` における `nullsFraction` のクランプが誤っている問題。[#75864](https://github.com/StarRocks/starrocks/pull/75864)
- 文字列から数値への schema change 変換において `CAST` のセマンティクスが誤っている問題。[#75538](https://github.com/StarRocks/starrocks/pull/75538)
- 同一列に対する `min`/`max` の同期 MV 書き換えで rollup 列が失われる問題。[#75528](https://github.com/StarRocks/starrocks/pull/75528)
- スキャン述語を引き上げる際に、`array_map` のラムダ内で書き換えが誤って適用される問題。[#76380](https://github.com/StarRocks/starrocks/pull/76380)
- バケット単位の実行下で Iceberg バケットテーブルにおいて `COUNT(DISTINCT)` が過剰にカウントされる問題。[#76601](https://github.com/StarRocks/starrocks/pull/76601)
- 必須の Iceberg 列に対して `UNNEST` と `GROUP BY` を組み合わせた際、nullable な出力でエラーが発生する問題。[#76730](https://github.com/StarRocks/starrocks/pull/76730)
- `INSERT OVERWRITE` コードパス全体でのロック競合を軽減し、CBO テーブルプルーニング時の Memo 自己参照を避けるため `Operator` のソルトを復元しました。[#75828](https://github.com/StarRocks/starrocks/pull/75828) [#76542](https://github.com/StarRocks/starrocks/pull/76542)
- 文字列型の日付パーティション列を時間値と比較した場合、Iceberg のパーティションプルーニング、マニフェストの行数推定、メタデータ削除、等価削除の適用、および Delta Lake のパーティションプルーニングが誤っていた問題。[#76068](https://github.com/StarRocks/starrocks/pull/76068) [#76107](https://github.com/StarRocks/starrocks/pull/76107) [#76197](https://github.com/StarRocks/starrocks/pull/76197) [#76280](https://github.com/StarRocks/starrocks/pull/76280) [#76348](https://github.com/StarRocks/starrocks/pull/76348)
- Iceberg の読み取りが対象スナップショットのスキーマおよびパーティション仕様に従わない問題、パーティションフィールド削除後に Iceberg V1 テーブルでクエリが失敗する問題、および `rollback_to_snapshot` の後に MV の書き換えが古い結果を返す問題。[#74711](https://github.com/StarRocks/starrocks/pull/74711) [#75149](https://github.com/StarRocks/starrocks/pull/75149) [#75924](https://github.com/StarRocks/starrocks/pull/75924)
- Iceberg マニフェストのデータファイルキャッシュが不完全なファイルセットを返していた問題、インクリメンタルスキャンレンジイテレータの並行クローズが安全でなかった問題、および解析時のタイムトラベルスナップショットのバインディングをベストエフォートに変更した問題。[#76215](https://github.com/StarRocks/starrocks/pull/76215) [#75953](https://github.com/StarRocks/starrocks/pull/75953) [#76448](https://github.com/StarRocks/starrocks/pull/76448)
- Iceberg/Delta のメタデータ由来の統計情報に `StatsSource=TABLE_METADATA` タグが付与されるようになりました。[#76560](https://github.com/StarRocks/starrocks/pull/76560)
- Delta Lake および Kudu の非パーティションマテリアライズドビューのクエリ書き換え。[#76359](https://github.com/StarRocks/starrocks/pull/76359)
- `gcs-connector` 3.x の設定名変更により GCS の vended credentials が無視される問題。[#75979](https://github.com/StarRocks/starrocks/pull/75979)
- Hive `getTable()` が `get_table_req` フォールバック前に再接続するようになり、テーブルが見つからない状態をメッセージテキストではなく例外タイプで検出するようになりました。[#76456](https://github.com/StarRocks/starrocks/pull/76456) [#76459](https://github.com/StarRocks/starrocks/pull/76459)
- Parquet の列インデックス統計情報で `BOOLEAN` の最小値／最大値がサポートされていない問題。[#74752](https://github.com/StarRocks/starrocks/pull/74752)
- キー名に `.` を含む場合の Flat-JSON サブフィールド読み取り、および NULL を返す代わりに中間的な flat-JSON オブジェクトが再構築される問題。サブフィールドキーが大文字小文字を区別せずに衝突する場合、JSON サブフィールドのプッシュダウンがスキップされるようになりました。[#75583](https://github.com/StarRocks/starrocks/pull/75583) [#75764](https://github.com/StarRocks/starrocks/pull/75764) [#76594](https://github.com/StarRocks/starrocks/pull/76594)
- 片方の分岐のみがビットマップインデックス化されている場合に OR がネストされた述語を保持するようにしました。[#76275](https://github.com/StarRocks/starrocks/pull/76275)
- Primary Key の自動増分列を使った部分更新の適用時に BE がクラッシュする問題。[#76119](https://github.com/StarRocks/starrocks/pull/76119)
- ロードのスピル処理中に `LoadChunkSpiller` の初期化競合により BE がクラッシュする問題。[#76098](https://github.com/StarRocks/starrocks/pull/76098)
- 不完全なネストされたレイクスキーマに対する内部 Parquet リーダーのクラッシュ、および Avro の複合型列にネストされた `BOOLEAN` における ASAN クラッシュ。[#76455](https://github.com/StarRocks/starrocks/pull/76455) [#76041](https://github.com/StarRocks/starrocks/pull/76041)
- 無効な文字列を `NOT NULL` 数値列に変換する際、schema change の実行中に `bad_variant_access` が発生する問題。[#76707](https://github.com/StarRocks/starrocks/pull/76707)
- `SimdJsonConverter` のエラーパスにおけるヒープバッファオーバーフロー、およびバッファ拡張をまたぐ複数文字 CSV 区切り文字での use-after-free 問題。[#76752](https://github.com/StarRocks/starrocks/pull/76752) [#76718](https://github.com/StarRocks/starrocks/pull/76718)
- スキャンの終了処理中に `MorselQueueFactory` で発生する use-after-free、キャンセル時にスピル可能なジョインビルド `set_finishing` で発生する use-after-free、および `PipelineDriver` デストラクタにおけるグローバルランタイムフィルタータイマーのリーク／未スケジュールタイマーを修正しました。[#76259](https://github.com/StarRocks/starrocks/pull/76259) [#76633](https://github.com/StarRocks/starrocks/pull/76633) [#76252](https://github.com/StarRocks/starrocks/pull/76252)
- 物理分割時の空 tablet による CN クラッシュを修正するため `SparseRangeIterator::has_more()` を null-safe にし、bRPC スタブキャッシュのクリーンタイマーリークを修正しました。[#75985](https://github.com/StarRocks/starrocks/pull/75985) [#75973](https://github.com/StarRocks/starrocks/pull/75973)
- クエリデプロイワーカーで `ConnectContext` が復元されない問題を修正しました。[#76366](https://github.com/StarRocks/starrocks/pull/76366)
- 共有データの主キーテーブルにおいて、トランザクション内での upsert/delete の順序を保持するようにしました。これにはスピルマージの操作認識対応も含まれます。[#75338](https://github.com/StarRocks/starrocks/pull/75338) [#75366](https://github.com/StarRocks/starrocks/pull/75366)
- ダングリングした `prev_garbage_version` を回避するために `cal_new_base_version` で永続メタデータを読み取るようにし、`publish_version` 内で `base_version` を `base_metadata` と同期させ、通常パスでマージされた並列コンパクションのトランザクションログを永続化するようにしました。[#75904](https://github.com/StarRocks/starrocks/pull/75904) [#76313](https://github.com/StarRocks/starrocks/pull/76313) [#76460](https://github.com/StarRocks/starrocks/pull/76460)
- レンジコロケートに関する修正：アライメントジョブのストームを停止し、アラインされていないコロケートジョインを fail-close するようにし、バケットシャッフル下で null-safe ジョインがマッチを取りこぼす問題を修正し、プランフィードバックにおけるレンジコロケートジョインの検出を改善しました。[#75930](https://github.com/StarRocks/starrocks/pull/75930) [#76104](https://github.com/StarRocks/starrocks/pull/76104) [#76121](https://github.com/StarRocks/starrocks/pull/76121)
- tablet 分割／リシャードに関する修正：分割時にテーブルの楽観バージョンをインクリメントして同時実行中のクエリが再プランされるようにし、同一 tablet でのリシャード前に PK インデックスのメモリテーブルをフラッシュするようにし、リシャードをまたいでバージョン区間ごとに lake vacuum ファイルを保持するようにし、分割不可能なレンジ分散 tablet での自動分割ジョブのループを停止するようにし、分割後のレンジ分散ソートキーにおける `IS NULL` プルーニングを修正しました。[#76123](https://github.com/StarRocks/starrocks/pull/76123) [#76367](https://github.com/StarRocks/starrocks/pull/76367) [#76209](https://github.com/StarRocks/starrocks/pull/76209) [#76663](https://github.com/StarRocks/starrocks/pull/76663) [#76797](https://github.com/StarRocks/starrocks/pull/76797)
- キー派生のレンジ分散テーブルのキー列を並べ替えてしまう全列 `ORDER BY` を拒否するようにしました。[#76256](https://github.com/StarRocks/starrocks/pull/76256)
- ロードスピルの並列マージ結果をフラッシュ順に統合し、ファイルバンドリングの引き継ぎにおいて `metaId` によってタッチされたインデックスを除外するようにしました。[#75951](https://github.com/StarRocks/starrocks/pull/75951) [#76368](https://github.com/StarRocks/starrocks/pull/76368)
- NULL を吸収する辞書マッピングの group-by キー用に辞書サイズ + 1 を予約するようにし、プロファイルレポートの再スケジュール前にドライバーの準備状態を確認するようにしました。[#75357](https://github.com/StarRocks/starrocks/pull/75357) [#75725](https://github.com/StarRocks/starrocks/pull/75725)
- `PlannerMetaLocker` において、ロックが一度も成功していない場合にアンロックをスキップしてしまう問題を修正しました。[#74041](https://github.com/StarRocks/starrocks/pull/74041)
- ウェアハウスによってリソースグループをフィルタリングするようにしました。[#73209](https://github.com/StarRocks/starrocks/pull/73209)
- `SHOW CREATE ROUTINE LOAD` の出力において `jsonpaths` の値をエスケープするようにしました。[#75755](https://github.com/StarRocks/starrocks/pull/75755)
- Arrow Flight のプリペアドステートメントの転送に関する問題を修正しました。[#76310](https://github.com/StarRocks/starrocks/pull/76310)
- 辞書のリフレッシュ間隔のオーバーフローおよび意図しない自動リフレッシュの問題を修正しました。[#76634](https://github.com/StarRocks/starrocks/pull/76634)
- `ERROR_IF_OVERFLOW` 配下で min/max が空の場合に統計キャッシュのロードが失敗する問題を修正しました。[#76684](https://github.com/StarRocks/starrocks/pull/76684)
- catalog 削除時の存在チェックを書き込みロック下でアトミックに行うようにしました。[#76778](https://github.com/StarRocks/starrocks/pull/76778)

## 4.1.3 {#413}

リリース日：2026年7月14日

### 動作の変更 {#behavior-changes-3}

- `CTAS` は、明示的に宣言された `VARCHAR(N)` 列の長さを `VARCHAR(MAX)` に広げるのではなく、その長さを保持するようになりました。既存のテーブルには影響しません。`CTAS` で作成された新しいテーブルでは、以降の書き込みで宣言された長さが適用されます。[#73498](https://github.com/StarRocks/starrocks/pull/73498)
- `OPERATE ON SYSTEM` 権限なしで `sys.fe_memory_usage` または `sys.fe_locks` をクエリした場合、紛らわしいノード検索失敗ではなく、明確なアクセス拒否エラーが返されるようになりました。[#73567](https://github.com/StarRocks/starrocks/pull/73567)
- `FILES()` および Broker Load/Stream Load では、`isAdjustedToUTC=false` で書き込まれた `INT64` の Parquet タイムスタンプにセッションのタイムゾーンシフトを適用しなくなりました。これらのタイムスタンプは現在、壁時計値として扱われ、そのままロードされます。v4.1.3 より前にこのようなファイルからロードされたデータは、アップグレード後にロードされたデータと異なる場合があります。整合性が必要な場合は再ロードしてください。[#73674](https://github.com/StarRocks/starrocks/pull/73674)
- 正常にコミットされたマルチテーブルトランザクションの Stream Load ジョブが、`PREPARING` のまま固定されるのではなく、`information_schema.loads` と `SHOW STREAM LOAD` において正しく `VISIBLE` として表示されるようになりました。[#74386](https://github.com/StarRocks/starrocks/pull/74386)
- コネクタのインクリメンタルスキャンレンジのスケジューリングが、デプロイ済みフラグメントのドライバーレイアウトを一貫して再利用するようになり、スキャンレンジが存在しないドライバーに誤って割り当てられることを防止します。[#74674](https://github.com/StarRocks/starrocks/pull/74674)
- `LIKE` の定数畳み込みが MySQL 8 のバックスラッシュエスケープのセマンティクスに一致するようになり、`'a\\\\b'` のようなパターンが以前は逆の結果を返していた問題が修正されました。[#74814](https://github.com/StarRocks/starrocks/pull/74814)
- Routine Load は `property.kafka_partition_discovery` プロパティをサポートするようになりました。これにより、`kafka_partitions` と `kafka_offsets` を指定して正確な開始オフセットを設定している場合でも、パーティションの自動検出を継続できます。`property.kafka_default_offsets` が設定されていない場合、ジョブがすでに消費進捗を持った後に検出されたパーティションのデフォルト開始オフセットは `OFFSET_END` から `OFFSET_BEGINNING` に変更されます。これは、この新しいプロパティを使用するジョブに限らず、**all** 自動検出ジョブすべてに適用されます。[#74729](https://github.com/StarRocks/starrocks/pull/74729)
- 非 group-by 集計が、マージ前に `UNION ALL` の分岐を経由してプッシュダウンされるようになり、union に対して集計を行うクエリのネットワーク転送量とメモリ使用量が削減されます。[#73930](https://github.com/StarRocks/starrocks/pull/73930)
- IVM メンテナンスクエリは、`CREATE` 時に保存された固定済みのクエリテキストを使用するのではなく、リフレッシュのたびに現在のビュー定義から再導出されるようになりました。既存の MV は再作成する必要なく、書き換えエンジンのバグ修正の恩恵を自動的に受けられます。[#74881](https://github.com/StarRocks/starrocks/pull/74881)
- サンプルベースの tablet の事前分割では、プレスプリットされたシャードをソース tablet のワーカーに詰め込む（`PACK` 配置）のではなく、all のコンピュートノードに分散させる（`SPREAD` 配置）ようになり、ロードの並列性が向上しました。[#75514](https://github.com/StarRocks/starrocks/pull/75514)
- 列コメントのみを変更する `ALTER TABLE ... MODIFY COLUMN` は、完全な schema change ジョブを発生させるのではなく、軽量なメタデータのみのパスを取るようになり、主キー列でも動作するようになりました。[#75325](https://github.com/StarRocks/starrocks/pull/75325)
- `FLOOR` と `CEIL` は非予約キーワードとして扱われるようになり、クォートなしで列名として使用できるようになりました。[#75241](https://github.com/StarRocks/starrocks/pull/75241)
- `SHOW FUNCTIONS` の出力には、UDF および UDAF の Properties 列に常に `isolation` プロパティ（`shared` または `isolated`）が含まれるようになりました。[#75255](https://github.com/StarRocks/starrocks/pull/75255)
- `lake_vacuum_min_batch_delete_size` のデフォルト値が 100 から 200 に引き上げられ、`DeleteObjects` リクエストごとにより多くの古いファイル削除をバッチ処理することで、オブジェクトストレージでの vacuum スループットが向上しました。[#74304](https://github.com/StarRocks/starrocks/pull/74304)
- ベンドクレデンシャルを持つ Iceberg REST catalog テーブルがキャッシュされ、そのクレデンシャルがバックグラウンドでリフレッシュされるようになりました。これにより、AWS Lake Formation のレート制限を引き起こしていた `getTable()` ごとの `GetDataAccess` 呼び出しが不要になりました。[#75431](https://github.com/StarRocks/starrocks/pull/75431)
- IVM の `bitmap_union`、`hll_union`、`percentile_union` の集計状態が、マテリアライズドビュー内で2回（可視列 + 隠し `__AGG_STATE_` 列）ではなく1回だけ格納されるようになり、これらのスケッチタイプのストレージが半減しました。[#75760](https://github.com/StarRocks/starrocks/pull/75760)
- 増分マテリアライズドビューが `bitmap_agg`、`hll_union`、`percentile_union`、`bitmap_union` の集計関数をサポートするようになり、正確な重複排除カウントおよびスケッチベースの集計を増分的に維持できるようになりました。[#75587](https://github.com/StarRocks/starrocks/pull/75587) [#75610](https://github.com/StarRocks/starrocks/pull/75610)
- サンプルベースの tablet の事前分割における tablet 数が、均等分散のためにアクティブなコンピュートノード数の最も近い倍数に切り上げられるようになり、小規模ロードでの過度な断片化を避けるために最小 tablet サイズによって下限が設定されるようになりました。[#75360](https://github.com/StarRocks/starrocks/pull/75360) [#75584](https://github.com/StarRocks/starrocks/pull/75584)

### 改善 {#improvements-4}

- `ngram_search` 関数が非定数の needle 引数を受け付けるようになりました。[#74675](https://github.com/StarRocks/starrocks/pull/74675)
- `enable_http_auth` FE 設定によって制御される HTTP 認証フレームワークを追加し、all 外部 HTTP エンドポイントでの認証および RBAC 強制をゲーティングします。[#73822](https://github.com/StarRocks/starrocks/pull/73822)
- `information_schema.materialized_views` にリフレッシュおよび配置の可観測性カラム（`refresh_warehouse`、`refresh_resource_group`、`refresh_mode`、`refresh_type`、`last_refresh_details`）を追加しました。[#74342](https://github.com/StarRocks/starrocks/pull/74342)
- ジャーナルリプレイ時に外部統計キャッシュをオプトインで遅延リフレッシュする機能を追加し、新しい FE 設定で制御できるようにしました。これにより、遅い、またはスタックした外部メタストアが FE のジャーナルリプレイや起動を停止させることを防ぎます。[#74371](https://github.com/StarRocks/starrocks/pull/74371)
- レンジ分散（共有データ）のソートキー列において、`VARCHAR` の長さ増加がデータの書き換えなしに高速スキーマ変更経由で許可されるようになりました。[#74698](https://github.com/StarRocks/starrocks/pull/74698)
- 共有データのトランザクションログ書き込みが設定可能なしきい値を超えた場合にスタックトレースダンプを追加し、遅い `put_txn_log` / `put_combined_txn_log` 呼び出しの診断を容易にしました。[#74704](https://github.com/StarRocks/starrocks/pull/74704)
- タブレット事前分割のメタ層フッターリーダーが `DATE`、`DATETIME`、`DECIMAL`、`VARCHAR`、および ORC の `TIMESTAMP` ソートキーをサポートするようになり、データ層サンプリングへのフォールバックが必要なロードの数を削減しました。[#74710](https://github.com/StarRocks/starrocks/pull/74710) [#74739](https://github.com/StarRocks/starrocks/pull/74739) [#74792](https://github.com/StarRocks/starrocks/pull/74792) [#74902](https://github.com/StarRocks/starrocks/pull/74902) [#74955](https://github.com/StarRocks/starrocks/pull/74955) [#75186](https://github.com/StarRocks/starrocks/pull/75186) [#75209](https://github.com/StarRocks/starrocks/pull/75209) [#75427](https://github.com/StarRocks/starrocks/pull/75427) [#75697](https://github.com/StarRocks/starrocks/pull/75697)
- サンプルベースの tablet 事前分割が `INSERT INTO ... SELECT ... FROM <OLAP table>` ロードに適用されるようになり、all ソートキー列を含む列リストの `INSERT` 文にも適用されるようになりました。[#74828](https://github.com/StarRocks/starrocks/pull/74828) [#75345](https://github.com/StarRocks/starrocks/pull/75345)
- 共有データの tablet メタデータおよびトランザクションログファイルに Adler-32 チェックサム保護を追加し、読み取り時にサイレント破損を検出できるようにしました。[#74924](https://github.com/StarRocks/starrocks/pull/74924)
- データベースごとの `txn_max_committed_pending_publish_ms` FE メトリクスを追加し、コミット済みだがまだパブリッシュされていないトランザクションのうち最も古いものの経過時間を報告することで、バージョンパブリッシュの停滞検出を支援します。[#75025](https://github.com/StarRocks/starrocks/pull/75025)
- タブレットの分割・マージがパブリッシュバージョンのレスポンスからリアルタイムでトリガーされるようになり、ロード完了から自動分割・マージ開始までの遅延が削減されました。[#75010](https://github.com/StarRocks/starrocks/pull/75010)
- SST なしの条件マージタスクを `pk_index_execution` スレッドプールにルーティングすることで、レイクプライマリキーテーブルの条件更新比較フェーズを最適化しました。[#74572](https://github.com/StarRocks/starrocks/pull/74572)
- レイクスキーマ変更およびロールアップジョブのロックスコープをデータベース全体ではなくテーブルレベルにし、同じデータベース内の他のテーブルに対する同時実行操作でのロック競合を削減しました。[#75087](https://github.com/StarRocks/starrocks/pull/75087)
- 共有なしモードにおいて、いくつかのデータベースレベルの書き込みロックをテーブルスコープの集中書き込みロックに狭めることで、BE レポートコールバックおよびクールダウン操作中のロック競合を削減しました。[#74521](https://github.com/StarRocks/starrocks/pull/74521) [#74523](https://github.com/StarRocks/starrocks/pull/74523)
- Avro の Routine Load が、ネイティブな `MAP` および `STRUCT` ターゲット列をサポートするようになりました。[#74901](https://github.com/StarRocks/starrocks/pull/74901)
- レンジコロケートの tablet 安定性ゲーティングが、グループを安定としてマークする前に StarOS の配置収束を待機するようになり、コロケートジョインがホストローカル実行を達成できるようになりました。[#75290](https://github.com/StarRocks/starrocks/pull/75290) [#75656](https://github.com/StarRocks/starrocks/pull/75656) [#75883](https://github.com/StarRocks/starrocks/pull/75883)
- 外部テーブル向けの CBO 統計を改善しました：オプティマイザは、ファイルを完全に列挙することなく Iceberg マニフェストから行数を推定できるようになり、Parquet/ORC 圧縮に対する Hive/Hudi の行数過小評価を修正し、JDBC コネクタ向けに非同期の行数統計を追加し、Puffin 統計が利用できない場合に Iceberg および外部コネクタ向けの NDV 推定フォールバックを提供します。[#75280](https://github.com/StarRocks/starrocks/pull/75280) [#75082](https://github.com/StarRocks/starrocks/pull/75082) [#75083](https://github.com/StarRocks/starrocks/pull/75083) [#75092](https://github.com/StarRocks/starrocks/pull/75092) [#75097](https://github.com/StarRocks/starrocks/pull/75097) [#75382](https://github.com/StarRocks/starrocks/pull/75382) [#75474](https://github.com/StarRocks/starrocks/pull/75474)
- Iceberg マニフェスト列統計が、クラスタ化列のみに対して選択的にキャッシュされるようになり、多数のデータファイルを持つ幅広いテーブルに対する FE ヒープ消費を削減しました。[#75395](https://github.com/StarRocks/starrocks/pull/75395)
- 外部テーブルの統計収集が、FE 再起動や HA フェイルオーバーをまたいだ永続的な述語列の追跡をサポートするようになり、auto-ANALYZE が正しい列を対象にできるようになりました。[#75653](https://github.com/StarRocks/starrocks/pull/75653)
- スケジューリングから実行までの外部テーブル統計収集の全ライフサイクルをカバーする構造化された `[ExternalStats]` ログ行を追加しました。[#75335](https://github.com/StarRocks/starrocks/pull/75335) [#75529](https://github.com/StarRocks/starrocks/pull/75529)
- `SHOW ANALYZE STATUS` が、外部テーブル統計ジョブの Properties 列にパーティション、列、およびスナップショットのメタデータを含むようになりました。[#75630](https://github.com/StarRocks/starrocks/pull/75630)
- 各外部テーブルの統計ソース（`TABLE_METADATA`、`ANALYZE`、または `NONE`）が、クエリランタイムプロファイルに公開されるようになりました。[#75253](https://github.com/StarRocks/starrocks/pull/75253)
- Iceberg および Delta Lake 外部テーブルに対するパーティションフィルタ要件とパーティション数制限のサポートを追加しました（以前は Hive、Hudi、Paimon でのみ利用可能でした）。[#75790](https://github.com/StarRocks/starrocks/pull/75790)
- `TABLE SAMPLE` およびヒストグラムの `ANALYZE` において1%未満のサンプリング率をサポートし、計算された比率がゼロに切り捨てられてしまう大規模テーブルでの失敗を修正しました。[#74551](https://github.com/StarRocks/starrocks/pull/74551)
- `jemalloc_conf` BE 設定項目を追加し、`information_schema.be_configs` 経由で jemalloc のランタイムオプションを可視化できるようにしました。[#75344](https://github.com/StarRocks/starrocks/pull/75344)
- 保持されたチャンク容量を解放することで、共有なしモードにおけるプライマリキー Compaction 時のメモリ使用量を削減する `compaction_chunk_reset_memory_tracker_threshold_percent` BE 設定を追加しました。[#75091](https://github.com/StarRocks/starrocks/pull/75091)
- staros を v4.1.1 にアップグレードしました。これには、再起動をまたいだ `datacache.enable` の永続化、ワーカーグループごとのシャードウォームアップタイムアウトのオーバーライド、および S3 リトライジッターの改善が含まれます。[#75204](https://github.com/StarRocks/starrocks/pull/75204)
- SQL 文字列に認証情報マーカーが存在しない場合は正規表現スキャンをスキップすることで、監査ホットパス上の SQL 認証情報のマスキングを最適化しました。[#74812](https://github.com/StarRocks/starrocks/pull/74812)
- Parquet スキャナ向けの式駆動型オンデマンド遅延列ロードにより、マルチブランチの `OR` クエリにおける不要な I/O を削減しました。[#74886](https://github.com/StarRocks/starrocks/pull/74886)
- `ds_hll_count_distinct` / `DataSketchesHll` が、順序依存の HIP 推定器ではなく複合推定器を使用することで、安定したカーディナリティ推定を生成するようになりました。[#75053](https://github.com/StarRocks/starrocks/pull/75053)

### セキュリティ {#security-4}

- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] SNI ハンドラのヒープ枯渇（DoS）、IPv6 サブネットフィルタのバイパス、および DNS キャッシュポイズニングを修正するため、Netty を4.1.135.Final にアップグレードしました。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- [CVE-2026-54512] [CVE-2026-54513] 2つのデシリアライゼーション脆弱性を修正するため、`jackson-databind` を2.21.4 にアップグレードしました。[#75373](https://github.com/StarRocks/starrocks/pull/75373)
- [GHSA-2r2c-cx56-8933] [GHSA-47qp-hqvx-6r3f] 未認証の Telnet サーバ DoS 脆弱性を是正するため、`org.jline:jline-remote-telnet` を Hadoop の推移的依存関係から除外しました。[#75066](https://github.com/StarRocks/starrocks/pull/75066)
- [CVE-2026-39822] pprof バイナリの脆弱性を修正するため、pprof プリビルドを更新しました。[#76248](https://github.com/StarRocks/starrocks/pull/76248) [#74669](https://github.com/StarRocks/starrocks/pull/74669)
- `information_schema.task_runs` において、述語値内のシングルクォートがリテラル境界をエスケープできてしまう SQL インジェクションを修正しました。[#75520](https://github.com/StarRocks/starrocks/pull/75520)
- `tencent.cos.access_key`、`tencent.cos.secret_key`、および `iceberg.catalog.jdbc.password` が、`SHOW CREATE CATALOG` の出力内でマスクされるようになりました。[#74696](https://github.com/StarRocks/starrocks/pull/74696)
- 入力が切り詰められたパーセントエスケープシーケンスで終わる場合に `url_decode` で発生していた範囲外読み取りを修正しました。[#75139](https://github.com/StarRocks/starrocks/pull/75139)
- `HyperLogLog::deserialize` が範囲外の `SPARSE` レジスタインデックスを受け入れてしまい、不正な入力によってヒープメモリが破損して BE がクラッシュする可能性があった問題を修正しました。[#75521](https://github.com/StarRocks/starrocks/pull/75521)
- `bar()` が負の幅の値を拒否しない問題を修正しました。これにより、以前は無制限の文字列増大と BE のメモリ枯渇が許容されていました。[#75143](https://github.com/StarRocks/starrocks/pull/75143)

### バグ修正 {#bug-fixes-4}

以下の問題が修正されました：

- `add_files` は、論理的な型付き値ではなく Parquet の物理エンコーディングバイトを使用して Iceberg のファイル範囲を設定していたため、ファイルレベルの最小値/最大値プルーニングが不正確になっていました（たとえば `DECIMAL` 列の場合）。[#69207](https://github.com/StarRocks/starrocks/pull/69207)
- `ApplyTuningGuideRule` は、入力リストが不変の `List.of(...)` として構築されたプランノードを走査する際に `UnsupportedOperationException` をスローしていました。[#70785](https://github.com/StarRocks/starrocks/pull/70785)
- `INSERT OVERWRITE` の2フェーズ再プランにおいて、最初のプランニングセッションから古いラムダ引数の列参照 ID が生成されることがあり、`expr_type does not match slot_type` エラーが発生していました。[#73273](https://github.com/StarRocks/starrocks/pull/73273)
- GIN（転置）インデックスを持つテーブルに対する部分更新で、GIN インデックス付き列が更新から除外されている場合に、クエリが無期限にハングするか失敗していました。[#73773](https://github.com/StarRocks/starrocks/pull/73773)
- ロウセットスキーマと tablet スキーマの間でスキーマドリフトが発生した場合、Lake PCU（部分列更新）がクラッシュするか、データが静かに破損していました。[#74005](https://github.com/StarRocks/starrocks/pull/74005)
- 複数のスキーマ句を持つ外部 Iceberg テーブルに対する結合 `ALTER TABLE` が、句のディスパッチごとに all の以前にキューに入れられたアクションを誤って再実行していました。[#74036](https://github.com/StarRocks/starrocks/pull/74036)
- パーティションフラッシュとリソースグループキャンセルが重なった際に、`num_rows` スナップショットが実際のチャンク行数を超えた場合、`PartitionedSpillerWriter` が `SIGSEGV` でクラッシュしていました。[#74081](https://github.com/StarRocks/starrocks/pull/74081)
- BE のシグナル初期化処理で SIGPIPE が無視されていなかったため、起動時（通常はデプロイ直後）に BE プロセスが予期せず終了することがありました。[#74424](https://github.com/StarRocks/starrocks/pull/74424)
- 行範囲フィルタリングにより struct の VARCHAR サブフィールドの充填がスキップされた場合、Parquet の一時的な辞書コード列が上位レイヤーに漏れ出し、型の不一致が発生していました。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `SELECT ... INTO OUTFILE` は、実際にエクスポートされた行数ではなく `ReturnRows=0` を監査ログに記録していました。[#74467](https://github.com/StarRocks/starrocks/pull/74467)
- `TabletChecker.doCheck()` は、ロックタイプの不一致により `blockingAddTabletCtxToScheduler` で `IllegalMonitorStateException` をスローし、チェッカーのラウンド全体が静かに中断していました。[#74596](https://github.com/StarRocks/starrocks/pull/74596)
- `information_schema.COLUMNS` は `DATETIME_PRECISION` に対して常に `NULL` を返しており、そのフィールドから列サイズを導出する MySQL プロトコルクライアントが壊れていました。[#74623](https://github.com/StarRocks/starrocks/pull/74623)
- 異なるデータベースまたはカタログにある修飾されていない同名の2つのテーブルをクエリがジョインした場合、マテリアライズドビューのリフレッシュが `Duplicate key` で失敗していました。[#74730](https://github.com/StarRocks/starrocks/pull/74730)
- 特定の条件下で、スピル可能なハッシュジョインのプローブがクラッシュしていました。[#74978](https://github.com/StarRocks/starrocks/pull/74978) [#75140](https://github.com/StarRocks/starrocks/pull/75140)
- 幅またはバケット数の引数がゼロの場合、Iceberg の `truncate` および `bucket` の変換関数が `SIGFPE` で BE をクラッシュさせていました。[#74998](https://github.com/StarRocks/starrocks/pull/74998)
- 被除数が `TYPE_MIN` で除数が `-1` の場合、`mod()` および `pmod()` が `SIGFPE` で BE をクラッシュさせていました。[#74980](https://github.com/StarRocks/starrocks/pull/74980)
- `bucket_num` がゼロまたは負の値の場合、`histogram()` が `SIGFPE` で BE をクラッシュさせていました。[#75041](https://github.com/StarRocks/starrocks/pull/75041)
- all の入力行が `NULL` の場合、`encode_fingerprint_sha256` が `SIGSEGV` でクラッシュしていました。[#75042](https://github.com/StarRocks/starrocks/pull/75042)
- 単一文字ワイルドカード `_` を含む `LIKE` パターンが、GIN 転置インデックスを介して評価された場合に誤った結果を返していました。[#75551](https://github.com/StarRocks/starrocks/pull/75551)
- GIN 転置インデックスに対する AND のみの `MATCH` クエリは、対象のセグメントが空の場合に誤ったエラーを返していました。[#75161](https://github.com/StarRocks/starrocks/pull/75161)
- CLucene の `match_all` クエリが誤った結果を返していましたが、CLucene の依存関係をアップグレードすることで解決しました。[#75180](https://github.com/StarRocks/starrocks/pull/75180)
- ベクトルインデックスの書き換えが、共有テーブルスキーマ上に直接合成距離列を登録していたため、同一テーブルに対する無関係な同時実行クエリで `Multiple entries with same key` エラーが発生していました。[#74785](https://github.com/StarRocks/starrocks/pull/74785)
- ジョイン並べ替えプルーニングが、スキャン述語によってまだ参照されている列をプルーニングすることがあり、統計推定で `missing statistic of col` がスローされていました。[#74791](https://github.com/StarRocks/starrocks/pull/74791)
- `avg(DISTINCT x)` が sum/count マテリアライズドビュー経由で誤って書き換えられ、`DISTINCT` が静かに削除され、重複が存在する場合に誤った結果を返していました。[#75071](https://github.com/StarRocks/starrocks/pull/75071)
- `ALTER TABLE ... MODIFY COLUMN ... AFTER <nonexistent_col>` が、適切なセマンティックエラーではなく内部の `NullPointerException` をスローしていました。[#75073](https://github.com/StarRocks/starrocks/pull/75073)
- `SHOW CREATE ROUTINE LOAD` は、`COLUMNS TERMINATED BY` 句を持たないジョブに対して、最初のロード説明句の前に誤って先頭カンマを出力していました。[#75522](https://github.com/StarRocks/starrocks/pull/75522)
- CTE を含む `SECURITY INVOKER` ビューでは、CTE 名が実際のテーブル参照と誤認された場合、NPE により権限チェックが失敗することがありました。[#74813](https://github.com/StarRocks/starrocks/pull/74813)
- 日付/日時の境界リテラルのシフトが表現可能な範囲をオーバーフローする場合（たとえば `<= '9999-12-31'`）、`ReduceCastRule` が `SemanticException` でクエリプランニングを中断していました。[#75036](https://github.com/StarRocks/starrocks/pull/75036)
- ジョイン条件が null セーフ等価演算子（`<=>`）の論理和を使用していた場合、`SplitJoinORToUnionRule` が重複行を出力していました。[#75038](https://github.com/StarRocks/starrocks/pull/75038)
- 複数テーブル外部クエリの並列メタデータ準備スレッド間で共有されていた `Tracers` が、`enable_profile=true` 下で `IllegalStateException` を引き起こしていました。[#74746](https://github.com/StarRocks/starrocks/pull/74746)
- `ChunksPartitioner` 内のパーティションコンシューマーエラーが静かに破棄されていたため、パーティション化された TopN がエラーを表面化せずに部分的または誤った結果を返すことがありました。[#74693](https://github.com/StarRocks/starrocks/pull/74693)
- BE のバキュームタスクが、FE 呼び出し元のタイムアウト経過後もゾンビとして実行され続け、`RELEASE_SNAPSHOT` スレッドプールを枯渇させ、バキュームのスループットを崩壊させていました。[#74694](https://github.com/StarRocks/starrocks/pull/74694)
- autovacuum の競合状態により、進行中のトランザクションより1つ大きい `minActiveTxnId` が一時的に計算されることがあり、BE がまだ必要な結合トランザクションログを削除してしまい、パブリッシュが恒久的に行き詰まることがありました。[#74906](https://github.com/StarRocks/starrocks/pull/74906)
- FE の EOS キャンセルと BE のステージ2デプロイの間の競合状態により、クエリが正常に完了した後でもキャンセルされたと誤って記録されていました。[#75009](https://github.com/StarRocks/starrocks/pull/75009)
- 集計 TopN のランタイムフィルタ構築キーが `ConstColumn` の場合、BE が `AggTopNRuntimeFilterUpdaterImpl` 内で `SIGSEGV` によりクラッシュしていました。[#74809](https://github.com/StarRocks/starrocks/pull/74809) [#74941](https://github.com/StarRocks/starrocks/pull/74941)
- all の非 null 配列が空の場合、`array_map` / `transform` が `NULL` 行を静かに削除し、誤った行数を返していました。[#75141](https://github.com/StarRocks/starrocks/pull/75141)
- 2^64 を超える `LARGEINT` / `DECIMAL128` のリテラルが、JIT コンパイルされた式において静かに64ビットに切り詰められていました。[#75137](https://github.com/StarRocks/starrocks/pull/75137)
- UTF-8 文字列関数（`split`、`split_part`、`str_to_map`）は、最後の文字が切り詰められたまたは無効なマルチバイト先頭バイトを持ち、区切り文字が空の場合に、文字列の終端を超えて読み取っていました。[#75068](https://github.com/StarRocks/starrocks/pull/75068)
- `parse_json()` は、不正な形式の JSON に対して、クエリを失敗させる代わりに `ALLOW_THROW_EXCEPTION` SQL モードであっても静かに `NULL` を返していました。[#74976](https://github.com/StarRocks/starrocks/pull/74976)
- 厳格モードの数値縮小キャストは、スロットデータが未定義の `NULL` 行に対して誤ってオーバーフローエラーを発生させていました。[#74903](https://github.com/StarRocks/starrocks/pull/74903)
- サブ秒部分が非ゼロの1970年以前の Parquet `INT64` タイムスタンプは、負の切り捨て除算の剰余により不正な値にデコードされていました。[#75207](https://github.com/StarRocks/starrocks/pull/75207)
- 1970年以前の ORC `TIMESTAMP` 値は、ロード時にサブ秒成分が欠落していました。[#75432](https://github.com/StarRocks/starrocks/pull/75432)
- ORC ストライプの最小値／最大値のタイムスタンプ統計は、1970年以前とサブ秒の境界に対して誤ってデコードされ、データファイルが誤ってプルーニングされる原因となっていました。[#75543](https://github.com/StarRocks/starrocks/pull/75543)
- `ARRAY`、`MAP`、または `STRUCT` 列内にネストされた `INT96` Parquet タイムスタンプは、ロード時にセッションタイムゾーンのオフセットが1回失われていました。[#74868](https://github.com/StarRocks/starrocks/pull/74868)
- Parquet の `UINT_32` 値は、`BIGINT` 列にロードされる際にゼロ拡張ではなく符号拡張されており、ハイビットの符号なし整数に対して負の値がサイレントに格納されていました。[#75002](https://github.com/StarRocks/starrocks/pull/75002)
- `HiveDataSource` のデストラクタは、`_scanner_ctx`（これらのノードを参照する述語を保持する）より前に `_pool`（およびその `Expr` ノード）を破棄していたため、ヒープの use-after-free を引き起こしていました。[#74818](https://github.com/StarRocks/starrocks/pull/74818)
- OpenX SerDe を使用して gzip 圧縮された JSON Hive 外部テーブルを読み取る際、マルチバイト UTF-8 文字が8MBの解凍バッファ境界をまたぐと `UTF8_ERROR` で失敗していました。[#74827](https://github.com/StarRocks/starrocks/pull/74827)
- ADLS2 `ListPaths` は、クライアントが無条件にアクセスする JSON フィールドが欠落していたため、非 HNS アカウントで `SIGSEGV` を伴いクラッシュしていました。[#75166](https://github.com/StarRocks/starrocks/pull/75166)
- 複数の `UNNEST` オペレーターが同じ入力配列列を共有し、異なるサブフィールドを消費する場合、`unnest` がクラッシュするか誤った結果を返していました。[#75012](https://github.com/StarRocks/starrocks/pull/75012) [#75445](https://github.com/StarRocks/starrocks/pull/75445) [#76002](https://github.com/StarRocks/starrocks/pull/76002)
- `unnest` の実行中に `query_mem_limit` が適用されておらず、大規模な配列に対する `unnest` がクエリを失敗させる代わりに BE を OOM キルさせることが可能でした。[#75179](https://github.com/StarRocks/starrocks/pull/75179)
- ランク制限がチャンク境界にちょうど一致する場合、`RANK` 境界を伴う `TopN` は1行を欠落させていました。[#75045](https://github.com/StarRocks/starrocks/pull/75045)
- `PushDownDistinctAggregateRule` の後の列プルーニングが空の分析（ウィンドウ）オペレーターを生成し、プランニングまたは実行エラーを引き起こす可能性がありました。[#74810](https://github.com/StarRocks/starrocks/pull/74810)
- `EliminateSortColumnWithEqualityPredicateRule` はグローバルな制限を設定せずにスキャンオペレーターにのみ行数制限を設定していたため、制限付きサブクエリに対する `COUNT(*)` が並行実行時に想定より多くの行を返していました。[#74983](https://github.com/StarRocks/starrocks/pull/74983)
- Lake プライマリキーの永続性インデックス再構築は、セグメント範囲モードで誤ったセグメントイテレータ位置を使用しており、キー範囲フィルターの適用が不正になっていました。[#74887](https://github.com/StarRocks/starrocks/pull/74887) [#75206](https://github.com/StarRocks/starrocks/pull/75206)
- `DROP PERSISTENT INDEX` はテーブルロックなしで `rebuildPindexVersion` を変更していました。`RestoreJob` はリストア後に DB READ ロックのみの下で MV ベーステーブル情報を変更していました。`FinalizeCreateTableAction` はイテレータ作成をまたいで DB レベルのロックを渡していました。[#74968](https://github.com/StarRocks/starrocks/pull/74968)
- `dumpImage` は、ループ中にデータベースごとのロック取得で例外がスローされた場合、グローバルメタロックを無期限に取り残す可能性がありました。[#75488](https://github.com/StarRocks/starrocks/pull/75488)
- マルチステートメントのストリームロードは、トランザクションごとに `TxnStateCallbackFactory` エントリを1つリークし、無制限に増加して最終的に FE のヒープを枯渇させていました。[#75188](https://github.com/StarRocks/starrocks/pull/75188)
- catalog、データベース、テーブル、またはパーティション名が長い場合、ヒストグラム統計の `information_schema.task_runs` 行数が `primary_key_limit_size`（128バイト）をオーバーフローする可能性がありました。[#75735](https://github.com/StarRocks/starrocks/pull/75735)
- BE の JVM メトリクスは、無効な Prometheus `# TYPE` 行（メトリクス名内にラベルセットが含まれる）を出力しており、Prometheus がスクレイプ全体を中断する原因となっていました。[#75240](https://github.com/StarRocks/starrocks/pull/75240)
- `SHOW PARTITIONS` と `information_schema.partitions_meta` は、共有データテーブルにおいて all の物理パーティションのバケット数を、各パーティションの実際のバケット数ではなくテーブルレベルのデフォルトとして報告していました。[#75734](https://github.com/StarRocks/starrocks/pull/75734)
- `SHOW PROC '.../index_schema/<id>'` は、共有データ（`CLOUD_NATIVE`）テーブル上の all ロールアップインデックスに対してベーステーブルのスキーマを返していました。[#76069](https://github.com/StarRocks/starrocks/pull/76069)
- `ALTER TABLE ... MODIFY COLUMN` の no-op 句が誤って軽量コメントパスにルーティングされており、バッチ `ALTER TABLE` ステートメントで `MODIFY COLUMN COMMENT can not be combined with other alter operations` エラーを引き起こしていました。[#75736](https://github.com/StarRocks/starrocks/pull/75736)
- `isCommentOnlyModification` は、`isKey` ／ `aggregationType` の正規化が不正であったため、キー列や集計列をコメントのみの変更と誤認識する可能性がありました。[#75545](https://github.com/StarRocks/starrocks/pull/75545)
- `ALTER VIEW` は循環するビュー定義をコミットしてしまい、その後の `SELECT` が `StackOverflowError` をスローする原因となっていました。[#75033](https://github.com/StarRocks/starrocks/pull/75033)
- `OrderedPartitionExchanger` は、下流のコンシューマーが前のチャンクを変更する際に `accept()` がまだそのチャンクへのポインタを保持していたため、ヒープの use-after-free を引き起こしていました。[#75279](https://github.com/StarRocks/starrocks/pull/75279)
- ビルド側のスロット記述子が non-nullable であるにもかかわらず、ランタイム状態が nullable である場合、NLJoin がクラッシュしていました。[#75343](https://github.com/StarRocks/starrocks/pull/75343) [#75788](https://github.com/StarRocks/starrocks/pull/75788)
- 構造体フィールド名が解析可能な JSON パスでない場合、`CAST(json/variant AS struct)` はフラグメント準備時に BE をクラッシュさせていました。[#75355](https://github.com/StarRocks/starrocks/pull/75355)
- ネストされた辞書式のための Dict-decode は、プロデューサーとコンシューマーのフラグメント間で互換性のない辞書変換を生成する可能性があり、実行時に `Dict Decode failed` エラーを引き起こしていました。[#75246](https://github.com/StarRocks/starrocks/pull/75246)
- `get_rowset_by_version` が `nullptr` を返し、`gtid` の比較が null チェックより前に配置されていたため、schema change が null ポインタの参照解除でクラッシュしていました。[#74855](https://github.com/StarRocks/starrocks/pull/74855)
- スナップショット manager が親 tablet メタデータを回収するかどうかを決定する際にリシャードジョブを考慮していなかったため、tablet の分割／マージ後に共有データクラスタのスナップショットが復元不能になっていました。[#75638](https://github.com/StarRocks/starrocks/pull/75638)
- ファイルバンドリングの vacuum は、兄弟 tablet のゼロ行バンドルセグメントを誤って非共有としてフラグ付けしており、他の tablet がまだ参照している間にそのバンドルファイルを削除していました。[#75689](https://github.com/StarRocks/starrocks/pull/75689)
- 共有データテーブルの Compaction のパブリッシュは、Compaction トランザクション開始後に可視化されたロールアップ／同期マテリアライズドビューのインデックスを削除しており、バンドルファイルにそれらのインデックスが欠落していました。[#76105](https://github.com/StarRocks/starrocks/pull/76105)
- tablet のリシャード中に Compaction がドロップされた場合、共有データの永続性インデックス Compaction がパススルーで再利用された SSTable ファイルを誤って削除していました。[#75726](https://github.com/StarRocks/starrocks/pull/75726)
- `NOT NULL` から nullable へのフラット JSON 列のスキーマ進化が、Compaction の読み取りパスで `CHECK` クラッシュを引き起こしていました。[#75680](https://github.com/StarRocks/starrocks/pull/75680)
- nullable 列に対する `count_combine` は、ストリーミング事前集計のパススルーパスで `SIGSEGV` を伴い BE をクラッシュさせていました。[#75298](https://github.com/StarRocks/starrocks/pull/75298)
- JDK 21で削除されたリフレクティブな `DirectByteBuffer` コンストラクタの検索が原因で、Java UDF が JDK 21以上でロードに失敗していました。[#75666](https://github.com/StarRocks/starrocks/pull/75666)
- パーサーが `CREATE TABLE AS SELECT` 内の `ENGINE` 句をサポートしていなかったため、Unified catalog（Hive メタストア）への `CTAS` は常に失敗していました。[#75771](https://github.com/StarRocks/starrocks/pull/75771)
- `JoinTuningGuide` フィードバック駆動のジョイン再構築が `predicateCommonOperators` を失っており、共通部分式の再利用を伴うプランで `InputDependenciesChecker` の検証エラーを引き起こしていました。[#75773](https://github.com/StarRocks/starrocks/pull/75773)
- 空のサブパーティションがバージョンリストの構築前にプルーニングされる場合、サブパーティションを持つテーブルでクエリキャッシュの正規化が `Preconditions.checkState` を伴いクラッシュしていました。[#75789](https://github.com/StarRocks/starrocks/pull/75789)
- `replayFromJson` はレガシーエイリアスで保存されたセッション変数を黙って無視したため、クエリダンプのリプレイがデフォルト値にフォールバックしていました。[#75813](https://github.com/StarRocks/starrocks/pull/75813)
- Iceberg の `_row_id` 仮想列は、複数の Parquet 行グループを持つデータファイルに対して、行グループの開始オフセットが二重にカウントされたため、誤った値を返していました。[#75758](https://github.com/StarRocks/starrocks/pull/75758)
- Iceberg DELETE/UPDATE プランナーは、物理的なテーブル識別子ではなく合成テーブル ID で一致を試みていたため、対象のスキャンノードを特定できず、ベーススナップショット ID と競合検出フィルターを失っていました。[#76013](https://github.com/StarRocks/starrocks/pull/76013)
- `PipelineExecutorSet::start()` が呼び出される前に `cancel_plan_fragment` RPC が到着した場合、`FragmentContext::set_final_status` が `SIGSEGV` でクラッシュしていました。[#75030](https://github.com/StarRocks/starrocks/pull/75030)
- `FragmentExecutor` がまだフラグメントを解体している最中に `QueryContext` が回収される可能性があり、`ResGuard::reset()` でヒープ use-after-free が発生していました。[#74978](https://github.com/StarRocks/starrocks/pull/74978)
- `StringSearch::_pattern` が初期化されておらず、デフォルトコンストラクタで作成された `search()` が未初期化のポインタを参照できてしまっていました。[#75614](https://github.com/StarRocks/starrocks/pull/75614)
- `DATETIME` マイクロ秒は JVM のデフォルトロケールの数字セットを使ってレンダリングされていたため、アラビア語やペルシャ語などのロケールで非 ASCII の数字が使用され、tablet の事前分割で境界値のパースが壊れていました。[#75001](https://github.com/StarRocks/starrocks/pull/75001)
- tablet の統計情報がリフレッシュされる前に `INSERT OVERWRITE` を実行すると、パーティションの行数が `_statistics_.column_statistics` にゼロとして書き込まれる場合があり、オプティマイザがパーティションのカーディナリティ推定を崩壊させていました。[#74801](https://github.com/StarRocks/starrocks/pull/74801)
- `enable_statistic_collect_on_first_load` のテーブルレベルのオーバーライドは、グローバル設定で無効化されている場合に初回ロード時の統計情報収集を有効にできませんでした。[#74794](https://github.com/StarRocks/starrocks/pull/74794)
- `PushDownNonGroupedAggregateBelowUnion` は、union ブランチに入力行がない場合、非 nullable と宣言されたタイプに対して nullable な出力を生成し、BE の `CHECK` の失敗を引き起こしていました。[#76101](https://github.com/StarRocks/starrocks/pull/76101)

## 4.1.2 {#412}

リリース日：2026 年 6 月 18 日

### 動作の変更 {#behavior-changes-4}

- ユーザーが権限を持たないデータベースへの接続時、以前は ERROR 2013 で接続が閉じられていましたが、現在は適切な MySQL エラーパケットが返されるようになりました。[#70072](https://github.com/StarRocks/starrocks/pull/70072)
- `SHOW FUNCTIONS` は、関数レベルの権限により関数を参照できるものの、関数作成スコープの権限を保持していないユーザーに対して、UDF ファイルおよびオブジェクトファイルのパスを `***` としてマスクするようになりました。[#73425](https://github.com/StarRocks/starrocks/pull/73425)
- 外部 catalog から Hive ビューをクエリする際、Ranger の行フィルターおよび列マスキングポリシーが正しく適用されるようになりました。[#73265](https://github.com/StarRocks/starrocks/pull/73265)
- `ALTER TABLE ... ADD COLUMN ... DEFAULT current_timestamp` は `current_timestamp` の生成式を正しく保持するようになりました。`DESCRIBE` および `information_schema` は、バックフィル時のリテラルではなく式を反映するようになりました。[#73455](https://github.com/StarRocks/starrocks/pull/73455)
- `information_schema.loads` のロード時フィルタリングは、セッションのタイムゾーンが UTC+8 と異なるクラスタでフィルター範囲をずらさなくなりました。ロード時刻は、FE と BE の境界間で UTC エポックミリ秒としてやり取りされるようになりました。[#73365](https://github.com/StarRocks/starrocks/pull/73365)
- `connector_max_split_size` セッション変数は、常にデフォルト値を使用するのではなく、Paimon のスキャン分割計算に正しく適用されるようになりました。[#71756](https://github.com/StarRocks/starrocks/pull/71756)
- `pipeline_enable_large_column_checker` はデフォルトで有効になりました。[#72798](https://github.com/StarRocks/starrocks/pull/72798)
- Hive のパーティション統計情報は、タイマーによってキーごとに自動でリフレッシュされなくなりました。パーティション統計情報は、明示的な `refreshTable()` 呼び出し時にのみリフレッシュされるようになり、大規模なパーティションテーブルでの HMS の負荷が軽減されました。[#73563](https://github.com/StarRocks/starrocks/pull/73563)
- Iceberg または外部 catalog のベーステーブルでスキーマドリフト（列タイプの変更、列の削除、またはテーブルの削除）が発生した場合、依存するマテリアライズドビューは、NULL 行を黙って生成したり不明瞭なエラーを出したりする代わりに、次回のリフレッシュ時に非アクティブとしてマークされるようになりました。[#73770](https://github.com/StarRocks/starrocks/pull/73770)
- Iceberg コネクタは、片側のみ変換可能な場合に述語全体を破棄するのではなく、`AND` の複合述語のうち変換可能な側を部分的にプッシュダウンするようになり、パーティションプルーニングとデータスキッピングが改善されました。[#70293](https://github.com/StarRocks/starrocks/pull/70293)
- 明示的トランザクションの `COMMIT` は、データベースの書き込みロックに対して正しく最大 `query_timeout` 秒（ミリ秒ではなく）待機するようになり、短時間の同時書き込みアクティビティによる誤ったロックタイムアウト失敗が防止されるようになりました。[#73549](https://github.com/StarRocks/starrocks/pull/73549)
- IVM のリフレッシュは、フィルターされた行を黙って破棄するのではなく、厳密ロードのフィルターエラーを呼び出し元に通知するようになりました。[#73938](https://github.com/StarRocks/starrocks/pull/73938)
- `count_combine(nullable_col)` は `COUNT(col)` のセマンティクスに合わせて NULL 行を正しく除外するようになりました。`COUNT(<nullable column>)` を基にしたインクリメンタル MV は、以前は水増しされたカウントをマテリアライズしていました。[#74029](https://github.com/StarRocks/starrocks/pull/74029)
- `SHOW ALTER TABLE COLUMN` は、cloud ネイティブ（共有データ）テーブルの `file_bundling` や `enable_persistent_index` などのプロパティに対して `ALTER TABLE ... SET (...)` によってトリガーされた非同期のメタデータのみの alter ジョブも表示するようになりました。[#74198](https://github.com/StarRocks/starrocks/pull/74198)
- 集計関数を参照する `HAVING` 句を使用してインクリメンタル MV を作成すると、初回リフレッシュ時に内部プランエラーを生成するのではなく、`CREATE` の時点で明確なエラーとともに失敗するようになりました。[#74054](https://github.com/StarRocks/starrocks/pull/74054)
- IVM は、インクリメンタルマテリアライズドビューで `MIN`/`MAX(DECIMAL)` 集計関数をサポートするようになりました。[#73969](https://github.com/StarRocks/starrocks/pull/73969)
- IVM の適応型リフレッシュは、最初のデルタ特性がすでに `mv_max_rows_per_refresh` を超えている場合にデルタウィンドウを正しく制限するようになり、1 回のタスク実行でバックログ全体がリフレッシュされることを防ぐようになりました。[#74464](https://github.com/StarRocks/starrocks/pull/74464)
- GROUP-BY のみのインクリメンタル MV（例：`SELECT k FROM t GROUP BY k`）は、`__ROW_ID__` を VARCHAR として正しくエンコードするようになり、2 回目のリフレッシュでのクラッシュが修正されました。[#74030](https://github.com/StarRocks/starrocks/pull/74030)

### 改善 {#improvements-5}

- Paimon ビューをサポートし、`CREATE`/`REPLACE`/`DROP`、`SHOW`/`DESC`、および外部 catalog からの Paimon ビューのクエリが可能になりました。Paimon ビュー内のテーブル参照は、`default_catalog` ではなく Paimon の catalog に対して解決されるようになりました。[#56058](https://github.com/StarRocks/starrocks/pull/56058) [#70217](https://github.com/StarRocks/starrocks/pull/70217)
- スキーマドリフトや複雑なネストされたタイプを持つファイルを読み取る際の安定したスキーマ制御のために、`FILES()` で明示的な `schema` パラメーターをサポートするようになりました。[#72033](https://github.com/StarRocks/starrocks/pull/72033)
- `get_query_profile()` は、接続中の FE だけでなく、all の FE ノード全体のクエリプロファイル情報を取得するようになりました。[#71123](https://github.com/StarRocks/starrocks/pull/71123)
- 現在実行中のクエリの UUID を返す組み込み関数 `query_id()` を追加しました。[#73621](https://github.com/StarRocks/starrocks/pull/73621)
- 共有データモードの `CREATE`/`ALTER STORAGE VOLUME` は、メタデータを永続化する前にストレージの場所へのアクセス可能性（認証情報とエンドポイント）を検証するようになり、設定ミスを早期に検出して失敗するようになりました。[#70053](https://github.com/StarRocks/starrocks/pull/70053)
- BE の AWS S3 認証情報に対する `WebIdentity` トークンプロバイダーのサポートを追加し、`AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE` に対する既存の FE サポートと整合させました。[#69966](https://github.com/StarRocks/starrocks/pull/69966)
- 不足している `txnlog`、セグメントの消失、またはリモート I/O の遅延によって publish が恒久的にブロックされている場合に、共有データテーブル上で行き詰まった `COMMITTED` トランザクションのブロックを解除するための `ADMIN SKIP COMMITTED TRANSACTION` コマンドを追加しました。[#73553](https://github.com/StarRocks/starrocks/pull/73553)
- `information_schema.tables_config` は `table_name` 述語を FE にプッシュダウンするようになり、単一テーブルのルックアップのオーバーヘッドが大幅に削減されました。[#73210](https://github.com/StarRocks/starrocks/pull/73210)
- MySQL 8 のスキーマを接続時に検査する BI ツールや JDBC ドライバとの互換性を向上させるため、`information_schema` テーブルに不足していた MySQL 8 の列を追加しました。[#73370](https://github.com/StarRocks/starrocks/pull/73370)
- `false` に設定するとセッション単位の変数を上書きする、クラスタ全体のキルスイッチとして機能する `enable_pipeline_event_scheduler` BE 設定を追加しました。[#73264](https://github.com/StarRocks/starrocks/pull/73264)
- 複数のワイド文字列列を持つテーブルで統計情報を収集する際のクエリごとのメモリピークを削減するため、統計情報収集向けのオプトインのワイド文字列列分離機能を追加しました。[#73258](https://github.com/StarRocks/starrocks/pull/73258)
- ロック競合が激しい状況下で JVM セーフポイントの停滞を防ぐため、スローロックのロギングがイベント単位のレート制限と設定可能なスタックキャプチャ制御をサポートするようになりました。[#73647](https://github.com/StarRocks/starrocks/pull/73647)
- MV のリフレッシュログのエントリに、プレフィックスとしてデータベース名が含まれるようになりました。これにより、同じ MV 名が複数のスキーマに存在するマルチテナント環境でもログ行を区別できます。[#73521](https://github.com/StarRocks/starrocks/pull/73521)
- `enable_profile_log` FE 設定が可変になり、FE の再起動なしに `ADMIN SET FRONTEND CONFIG` 経由で実行時に切り替えられるようになりました。[#73894](https://github.com/StarRocks/starrocks/pull/73894)
- ロードプロファイル（stream load、routine load、broker load、merge-commit load）を `fe.profile.log` に書き込むための `enable_print_load_profile_to_log` FE 設定（デフォルト `false`）を追加しました。これにより、クエリプロファイルのバーストによってインメモリストアが退避された場合でも、プロファイルを保持できます。[#74150](https://github.com/StarRocks/starrocks/pull/74150)
- `SHOW ROUTINE LOAD` は、Java オブジェクト参照の代わりに `JobProperties` 内の列マッピングを正しくレンダリングするようになりました。[#74199](https://github.com/StarRocks/starrocks/pull/74199)
- `CachingIcebergCatalog` は catalog 単位のロックの代わりにテーブル単位のロックを使用するようになり、多数のテーブルが同時にアクティブな catalog でのリフレッシュのシリアライズ遅延を削減しました。[#73079](https://github.com/StarRocks/starrocks/pull/73079)
- メタスキャン（バックグラウンドの統計情報収集）は、変更後のセグメントファイルで not-found エラーが発生する代わりに、`ADD COLUMN`、`DROP COLUMN`、`RENAME COLUMN`、`REORDER COLUMN` の schema change を適切に処理できるようになりました。[#72901](https://github.com/StarRocks/starrocks/pull/72901)
- サンプルベースの tablet の事前分割が、複数パーティションのレンジ分散テーブルと Broker Load をカバーするようになり、既存のデータ層のベースラインがなくても初回ロード時の並列性を実現できます。[#73101](https://github.com/StarRocks/starrocks/pull/73101) [#73912](https://github.com/StarRocks/starrocks/pull/73912) [#74048](https://github.com/StarRocks/starrocks/pull/74048)
- MySQL の結果シリアライズでは、行単位の仮想ディスパッチが使用されなくなりました。型付きの列ライターがチャンクごとに一度だけ構築されるようになり、幅広または大規模な結果セットに対するシリアライズのオーバーヘッドが削減されます。[#66316](https://github.com/StarRocks/starrocks/pull/66316)
- `DATETIME`/`DATE` から文字列へのキャストが出力バッファへ直接書き込まれるようになり、行単位のヒープアロケーションがなくなりました。[#73801](https://github.com/StarRocks/starrocks/pull/73801)
- クエリ統計情報のマージ処理では、`SpinLock` の代わりにロックフリーの並列マップが使用されるようになり、ワーカーが中間または最終の統計情報を送信する際の大規模クラスタでの CPU 使用率が削減されます。[#73796](https://github.com/StarRocks/starrocks/pull/73796)
- 集計用のハッシュマップおよびハッシュセットのプリフェッチが L2 キャッシュの常駐状況に基づいて制御されるようになり、バケット配列が L2 に収まる場合の 4 〜 9% のリグレッションが回避されます。プリフェッチ距離は設定可能になりました。[#73943](https://github.com/StarRocks/starrocks/pull/73943)
- 共有データの Primary Key テーブルにおけるライトコンパクションのパブリッシュ向けに、パイプライン化されたセグメント単位の `.lcrm` 読み取りが導入され、連続したオブジェクトストレージへのラウンドトリップが削減されます。[#73992](https://github.com/StarRocks/starrocks/pull/73992)
- 共有データモードにおけるコールド PK インデックスの再構築スキャンがセグメント間で並列化されるようになり、セグメントの読み取りがリモート I/O に律速される場合の再構築時間が削減されます。[#74249](https://github.com/StarRocks/starrocks/pull/74249)
- 内部クエリ（統計情報収集、タスク実行、MV のリフレッシュ）が `SHOW PROC '/current_queries'` に表示されるようになり、`KILL QUERY` で kill できるようになりました。[#74488](https://github.com/StarRocks/starrocks/pull/74488)
- S3 のスロットリングを監視し `lake_vacuum_min_batch_delete_size` をチューニングするための lake vacuum のバッチサイズおよびリトライ回数の bvar メトリクスを追加しました。[#74112](https://github.com/StarRocks/starrocks/pull/74112)
- recycle-bin の増加が FE のヒープを圧迫する前に把握できるよう、`CatalogRecycleBin` のサイズゲージメトリクスを追加しました。[#74440](https://github.com/StarRocks/starrocks/pull/74440)
- `LIST` でパーティション化されたテーブルは、レンジパーティションテーブル向けに設計された最新 N 件のヒューリスティックを適用する代わりに、all パーティションを `OlapTableSink` で開くようになり、インクリメンタルオープンの RPC オーバーヘッドが削減されます。[#74099](https://github.com/StarRocks/starrocks/pull/74099)
- `FILES()` または Broker Load 経由で、`LARGE_LIST` および `FIXED_SIZE_LIST` の Arrow タイプを JSON 列へロードすることをサポートします。[#73714](https://github.com/StarRocks/starrocks/pull/73714) [#73718](https://github.com/StarRocks/starrocks/pull/73718)
- 共有データテーブルにおける merge-commit（`FRONTEND_STREAMING`）ロードで、トランザクションログとファイルのバンドルを組み合わせた処理をサポートし、他のロードタイプと動作を統一しました。[#74460](https://github.com/StarRocks/starrocks/pull/74460)
- FE の再起動なしに lake publish のフェーズ内訳の警告しきい値を制御するための可変 FE 設定 `slow_publish_partition_log_threshold_ms`（デフォルト 3000 ms）を追加しました。[#74043](https://github.com/StarRocks/starrocks/pull/74043)

### セキュリティ {#security-5}

- [CVE-2026-43869] 証明書ホストの検証が不適切な問題に対処するため、`libthrift` を 0.23.0 にバンプしました。[#73243](https://github.com/StarRocks/starrocks/pull/73243)
- [CVE-2026-41293] HTTP/2 のリクエストヘッダー検証の問題に対処するため、Apache Tomcat を 9.0.118 にバンプしました。[#73797](https://github.com/StarRocks/starrocks/pull/73797)
- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] SNI ハンドラのヒープ枯渇（DoS）、IPv6 サブネットフィルタのバイパス、DNS キャッシュポイズニングの問題に対処するため、Netty を 4.1.135.Final にバンプしました。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- Go 標準ライブラリのセキュリティ修正を含めるため、pprof のプリビルドバイナリを Go 1.25.11 にアップグレードしました。[#73545](https://github.com/StarRocks/starrocks/pull/73545) [#74669](https://github.com/StarRocks/starrocks/pull/74669)

### バグ修正 {#bug-fixes-5}

以下の問題が修正されました：

- URL に `host:port` パターン外で `:` が含まれる場合、`parse_url()` が誤ったホストを返していました。[#63542](https://github.com/StarRocks/starrocks/pull/63542)
- 辞書変換された式で、それが成立しない式（例：`IF(col = '1', NULL, 'ok')`）に対しても誤って `f(null) = null` を前提としていました。[#69376](https://github.com/StarRocks/starrocks/pull/69376)
- トランザクション stream load がユーザー指定のタイムアウトではなくデフォルトの RPC タイムアウトを使用していたため、タイムアウトが早期に発生していました。[#67584](https://github.com/StarRocks/starrocks/pull/67584)
- `NULL = NULL` が結合述語で UNKNOWN と評価されるため、identity 列に NULL 値を持つ Iceberg の equality delete ファイルで、一致する行を削除できませんでした。[#67321](https://github.com/StarRocks/starrocks/pull/67321)
- `INJECTED` パーティションプロジェクション列を持つテーブルのエラーメッセージが、問題の原因となっている列を示すよう、より説明的になりました。[#68052](https://github.com/StarRocks/starrocks/pull/68052)
- insert-overwrite 操作が認識されなかったため、insert-only の ACID Hive テーブルへのクエリが期待より多くの行を返していました。[#71460](https://github.com/StarRocks/starrocks/pull/71460)
- 同時読み取り中に Iceberg のメタデータエントリがピン留めされた場合、ディスクキャッシュが設定された容量を超えてしまう問題がありました。[#71651](https://github.com/StarRocks/starrocks/pull/71651)
- external catalog へのクエリ時に、Paimon の主キー列が誤って非 nullable としてマークされていました。[#71660](https://github.com/StarRocks/starrocks/pull/71660)
- 複数の `ARRAY_AGG(DISTINCT <const>)` 入力を持つクエリに対して `MultiDistinctByMultiFuncRewriter` が同じルールを繰り返し適用した際、オプティマイザがタイムアウトしていました。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- `DATE`/`TIMESTAMP` キーワードなしでプッシュダウンされた Oracle JDBC の日付述語が NLS フォーマットエラーを引き起こしていた問題。[#71412](https://github.com/StarRocks/starrocks/pull/71412)
- Partition TopN が子 Operator から必要な出力列を失う可能性があった問題。[#72848](https://github.com/StarRocks/starrocks/pull/72848)
- パーティション進化（partition evolution）を持つ Iceberg テーブルに対して、パーティション化されていない MV を作成できなかった問題。[#72285](https://github.com/StarRocks/starrocks/pull/72285)
- `information_schema.be_cloud_native_compactions` において、並列サブタスクの Compaction タスク統計が上書きされて失われていた問題。[#72331](https://github.com/StarRocks/starrocks/pull/72331)
- 同期 MV 向けの `SHOW CREATE MATERIALIZED VIEW` が「Table is not found」で失敗していた問題。[#73396](https://github.com/StarRocks/starrocks/pull/73396)
- ステートメントの `.log` ファイルが 4 セグメントのパスに配置された場合、schema change 中に Lake publish のマルチステートメントトランザクションがデッドロックしていた問題。[#73423](https://github.com/StarRocks/starrocks/pull/73423)
- Sort merge プロバイダーのエラーがフラグメントコンテキストに伝播されず、クエリが無言で失敗していた問題。[#73337](https://github.com/StarRocks/starrocks/pull/73337)
- 長時間稼働している Follower FE 上で `ConnectorTableId` が `int` から負の値にオーバーフローし、Iceberg および Hive のクエリが「Invalid table type」という誤解を招くエラーで失敗していた問題。[#73344](https://github.com/StarRocks/starrocks/pull/73344)
- 空の optimize 句（分散またはパーティション指定なし）を持つ `ALTER TABLE` が誤って解析され、FE リプレイ時にテーブルのデフォルト分散を破損させる可能性があった問題。[#73352](https://github.com/StarRocks/starrocks/pull/73352)
- ADLS2 の共有データディザスタリカバリ中に、`AZURE_PATH_KEY` が有効な `StorageVolumeMgr` パラメータとして認識されず、FE の起動が失敗していた問題。[#73509](https://github.com/StarRocks/starrocks/pull/73509)
- オプティマイザがネスト型の一部を `UNKNOWN_TYPE` にプルーニングした場合、または NULL 許容の配列、マップ、構造体のスキーマが使用された場合に、Avro の複合型デコードが失敗していた問題。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- 2 つの `NullableColumn` が同じ `NullColumn` オブジェクトを共有していたため、COW カラムの変更最適化が `map_apply` および類似の関数でクラッシュを引き起こしていた問題。[#73480](https://github.com/StarRocks/starrocks/pull/73480)
- カスタムの `LocationProvider` を持つ Iceberg テーブルで、プロバイダーが FE 上で即座にインスタンス化されていたため、`ClassNotFoundException` を伴う `SELECT` クエリが失敗していた問題。[#73482](https://github.com/StarRocks/starrocks/pull/73482)
- キャッシュミスのたびに JDBC の `getTable()` が余分な `getTableComment()` のラウンドトリップを実行し、負荷の高いプランニングフェーズのロック保持時間を延長して、並行する DDL をブロックしていた問題。[#73488](https://github.com/StarRocks/starrocks/pull/73488)
- ネストされた MV が `FULL` または `UNKNOWN` のタイムリネスを返した場合に、ネスト MV のリフレッシュが `NullPointerException` をスローしていた問題。[#73644](https://github.com/StarRocks/starrocks/pull/73644)
- 低速な MySQL クライアントへのクエリ結果送信中に FE ワーカーが無期限にブロックされていた問題。結果送信パスに書き込みタイムアウトが適用されるようになりました。[#73646](https://github.com/StarRocks/starrocks/pull/73646)
- 主キーテーブルを V1 エンコード（共有なし）から V2 エンコード（共有データ）のクラスタへ、または 2 つの共有データクラスタ間でレプリケートする際に、PK の `.del` ファイルがトランスコードされていなかった問題。[#73649](https://github.com/StarRocks/starrocks/pull/73649) [#73958](https://github.com/StarRocks/starrocks/pull/73958)
- 古いレプリカの参照がライブレプリカを追加する前に削除されなかったため、`VERSION_INCOMPLETE` のリカバリ中に `TabletInvertedIndex` に重複レプリカが蓄積していた問題。[#73661](https://github.com/StarRocks/starrocks/pull/73661)
- `REPLICATE_SNAPSHOT` タスクとファイルごとのコピーサブタスクが同じスレッドプールを共有していたため、共有データレイクのレプリケーションにおけるファイルコピーが CN をクラッシュさせていた問題。[#73666](https://github.com/StarRocks/starrocks/pull/73666)
- BE によってユニットカウンタが `.000` の小数接尾辞でフォーマットされた場合に、`RuntimeProfileParser` が `NumberFormatException` をスローしていた問題。[#73683](https://github.com/StarRocks/starrocks/pull/73683)
- 共有データの主キーの tablet 分割における共有セグメントの物理 rowid エンコーディングが不正で、誤った `rss_rowid` エントリを引き起こしていた問題。[#73686](https://github.com/StarRocks/starrocks/pull/73686)
- レンジコロケート × ハッシュ分散が混在する `JOIN` クエリが、有効な結果の代わりに `Unknown error` を返していた問題。[#73702](https://github.com/StarRocks/starrocks/pull/73702)
- `TimeUtils.longToTimeString` が固定の UTC+8 フォーマッタを使用していた問題。出力がセッションの `time_zone` を尊重するようになりました。[#73619](https://github.com/StarRocks/starrocks/pull/73619)
- all の値が `NULL` で、かつその列が NULL 許容の単項関数パスを通過した場合に、Decimal 型の列がスケールを失い、後続の結果型が破損していた問題。[#73789](https://github.com/StarRocks/starrocks/pull/73789)
- ネスト型に対する JSON の部分追加が ASAN クラッシュを引き起こしていた問題。[#73715](https://github.com/StarRocks/starrocks/pull/73715)
- `public` ロールの権限キャッシュが `GRANT`/`REVOKE` 時に無効化されず、有効期限が切れるまで古い権限が有効のままになっていた問題。[#73717](https://github.com/StarRocks/starrocks/pull/73717)
- サブライターに追加が存在しない場合に `FlatJson` がクラッシュしていた問題。[#73730](https://github.com/StarRocks/starrocks/pull/73730)
- MV 自体が `HAVING` 述語を持つ場合に、集計 MV の書き換えが誤って適用され、不完全な結果が返される可能性があった問題。[#73610](https://github.com/StarRocks/starrocks/pull/73610)
- 並列マージモードに入る際の spill writer の `auto_flush` フラグにおけるデータ競合が、ARM 上で意図しないセグメントフラッシュを引き起こしていた問題。[#73616](https://github.com/StarRocks/starrocks/pull/73616)
- Routine Load のスケジューラが、Kafka または Pulsar のパーティションメタデータを取得するためのブロッキング BE RPC 実行中にジョブごとの書き込みロックを保持しており、ロック保持時間が最大 33.6 秒に達していた問題。[#73591](https://github.com/StarRocks/starrocks/pull/73591)
- `tablet_sched_disable_colocate_balance` が有効な場合に、停止している BE 上のコロケートされた tablet が誤って `HEALTHY` として報告されていた問題。[#73550](https://github.com/StarRocks/starrocks/pull/73550)
- MISSING（phantom）レプリカの行が存在する場合に `ADMIN SHOW REPLICA STATUS` が MySQL の結果ストリームを非同期化し、クライアントのハングまたは切断を引き起こしていた問題。[#74393](https://github.com/StarRocks/starrocks/pull/74393)
- 共有データモードにおいて、各送信者の `open` RPC ごとにパーティション単位のコーディネータ主張が再記録されず、コーディネータ選出の際に一部の送信者が見落とされ、`combined_txn_log` ファイルが書き込まれないままになっていた問題。[#73962](https://github.com/StarRocks/starrocks/pull/73962)
- `_statistics_` のデータベースまたはテーブルが削除された後に、`_statistics_.pipe_file_list` の内部テーブルが再作成されていなかった問題。[#73970](https://github.com/StarRocks/starrocks/pull/73970)
- `TaskCleaner` によって強制終了されたタスク実行がアーカイブされず、痕跡なしに `information_schema.task_runs` から消えていた問題。[#74146](https://github.com/StarRocks/starrocks/pull/74146)
- `RENAME TABLE` および `SWAP TABLE`/`SWAP MATERIALIZED VIEW` が、データベース書き込みロックではなく負荷の高いテーブルロックのみを保持していたため、並行する読み取り側が途中状態の名前からテーブルへのマッピングを観測できてしまっていた問題。[#74100](https://github.com/StarRocks/starrocks/pull/74100)
- PK インデックスのコンパクション出力 sstable が tablet メタデータなしで開かれ、永続的な `metadata is null when loading delvec` の失敗を引き起こしていた問題。[#74037](https://github.com/StarRocks/starrocks/pull/74037)
- 同一トランザクション内ですでに変更されたテーブルを対象とした明示的トランザクション内の部分更新 `INSERT` が、`COMMIT` において無言でデータを破損させていた問題。[#74344](https://github.com/StarRocks/starrocks/pull/74344)
- レンジ分散と互換性のない `ALTER TABLE` 操作（schema change、ソートキーの変更）が、メタデータを無言で破損させるのではなく、適切なエラーで拒否されるようになりました。[#74020](https://github.com/StarRocks/starrocks/pull/74020)
- オプティマイザ内で子の型が一致しない集計関数が、誤ったクエリ結果を引き起こしていた問題。[#74159](https://github.com/StarRocks/starrocks/pull/74159)
- 予約語のテーブル名を使用した `ALTER ROUTINE LOAD` がパースできない `origStmt` を書き込み、FE 再起動後にカラムマッピングが失われていました。[#74188](https://github.com/StarRocks/starrocks/pull/74188)
- IVM の `state_union` 互換性チェックがネストされた型（例：`ARRAY<VARCHAR>`）に対して再帰的に処理を行わず、`ARRAY_AGG` の IMV で `CREATE MATERIALIZED VIEW` が失敗していました。[#73627](https://github.com/StarRocks/starrocks/pull/73627)
- スキャン範囲が完全にフィルタされた場合、Parquet の一時的な辞書コード列が上位層に漏れ出し、下流で型の不一致が発生していました。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- float と integer が混在する `WHEN` と結果タイプを持つ `CASE WHEN` が無効な JIT IR を生成し、結果が不正になったりクラッシュが発生したりしていました。[#74382](https://github.com/StarRocks/starrocks/pull/74382)
- JIT コンパイル失敗により `LLVMContext` の use-after-free が発生し、SIGSEGV を引き起こしていました。[#74396](https://github.com/StarRocks/starrocks/pull/74396)
- バックグラウンドの統計タスクがセッションの `WAREHOUSE` 設定を上書きし、同じ接続コンテキストでの以降のユーザークエリに影響を与えていました。[#74385](https://github.com/StarRocks/starrocks/pull/74385)
- クラスタスナップショットが一度も正常に完了していない場合、`CatalogRecycleBin` がエントリの削除を停止し、高負荷の `INSERT OVERWRITE` ワークロード下で FE のメモリが際限なく増加していました。[#74379](https://github.com/StarRocks/starrocks/pull/74379)
- 非 PK レプリカのバージョンホールが FE によって検出されず、クエリが固定された `max_version` を持つホールのあるレプリカに永続的にルーティングされていました。[#74408](https://github.com/StarRocks/starrocks/pull/74408)
- `MaterializedIndexMeta.updateSchemaBackendId`（共有読み取りロック下で変更される `HashSet`）に対するデータ競合により、エントリの消失やセットの破損が発生する可能性がありました。[#74412](https://github.com/StarRocks/starrocks/pull/74412)
- 保持境界メタデータがすでにバキューム済みの場合、バキュームウォーターマークが正しく報告されず、`file_bundling` のバージョン切り替えクリーンアップが停滞していました。[#74429](https://github.com/StarRocks/starrocks/pull/74429)
- Lake のバキュームリトライが決定論的な指数バックオフを使用していましたが、S3 スロットリング下でリトライを CN 間に分散させるため、非相関ジッターが追加されました。[#74108](https://github.com/StarRocks/starrocks/pull/74108)
- クエリメモリプールから割り当てられた RPC リクエストがプロセスコンテキストで解放されていたため、`OlapTableSink` のメモリ計上が膨張していました。[#73807](https://github.com/StarRocks/starrocks/pull/73807)
- 自動パーティション作成が `_incremental_open_node_channel` を同時にトリガーした場合、`TabletSinkSender::_send_chunk_by_node` で競合状態が発生していました。[#73820](https://github.com/StarRocks/starrocks/pull/73820)
- 上流の変更のバックポートによって作成された UDAF コンテキストが、`unique_ptr::release` を介してメモリリークを引き起こしていました。[#74025](https://github.com/StarRocks/starrocks/pull/74025)
- `append_selective` でのメモリ計上が不正確なことが原因で、パーティション化されたジョインのプローブで境界外アクセスが発生する可能性がありました。[#74315](https://github.com/StarRocks/starrocks/pull/74315)
- `azure_adls2_oauth2_client_endpoint` 設定フィールドの名前にタイプミスがありました。[#74581](https://github.com/StarRocks/starrocks/pull/74581)
- `StarMgrMetaSyncer` が range-colocate PACK シャードグループを誤って孤立として回収し、共有データモードでアクティブなシャードを永続的に削除していました。[#74117](https://github.com/StarRocks/starrocks/pull/74117)
- colocate の tablet 分割のソートキーの要素数が、PRIMARY KEY テーブルおよび明示的な `ORDER BY` を持たないテーブルに対して、実体化されたスキーマではなくベーススキーマから解決されており、分割ジョブが tablet サイズを縮小せずに完了していました。[#74409](https://github.com/StarRocks/starrocks/pull/74409)
- パーティションデータがマージ閾値を下回っている場合、自動マージデーモンが事前分割された tablet を再びマージしてしまい、サンプルベースの事前分割による並列性のメリットが無効化されていました。[#74583](https://github.com/StarRocks/starrocks/pull/74583)
- 関数の `FunctionName.db` がソースデータベースを参照したままだったため、`RESTORE ... AS <new_db>` の後、データベースレベルの UDF が Follower FE で欠落していました。[#74313](https://github.com/StarRocks/starrocks/pull/74313)
- ウェアハウスに複数の CN グループがある場合、共有データモードの `DISTRIBUTED BY RANDOM` CTAS/INSERT で、不変パーティションの tablet のロケーションに誤った CN グループが割り当てられていました。[#74316](https://github.com/StarRocks/starrocks/pull/74316)
- 統計推定中にパーティションが同時にドロップされた場合、`StatisticsCalcUtils` で `NullPointerException` が発生していました。[#73711](https://github.com/StarRocks/starrocks/pull/73711)
- `InformationSchemaDataSource` と `FrontendServiceImpl` のメタデータ RPC ハンドラがデータベース全体の READ ロックを保持し、無関係なテーブルの DDL をブロックしていました。[#73936](https://github.com/StarRocks/starrocks/pull/73936) [#73913](https://github.com/StarRocks/starrocks/pull/73913)
- 完了状態を切り替えた際に共有コンテキストのオブザーバーに通知しないパイプラインオペレーターが、イベントスケジューラ配下でピアドライバをスタックさせる可能性がありました。[#74055](https://github.com/StarRocks/starrocks/pull/74055) [#74056](https://github.com/StarRocks/starrocks/pull/74056)
- 述語プッシュダウンにおける非ルートの複合述語が、スキャンレベルの EOF ではなく `NotPushDown` を生成し、UNION の下で不可能なネストされた AND 分岐が存在する場合に `OlapScanNode` が行を出力しなくなっていました。[#74218](https://github.com/StarRocks/starrocks/pull/74218)
- 単一の記憶媒体を持つ BE 上で `BackendLoadStatistic.init` がレプリカごとに高コストなスキャンを実行していましたが、均質なディスク構成の BE ではこのチェックが O(1) になりました。[#73555](https://github.com/StarRocks/starrocks/pull/73555)
- データディレクトリのロードスレッドにおけるスレッド名設定の競合により、BE 起動のたびにノイズの多い `failed to set thread name` 警告が発生していました。[#73862](https://github.com/StarRocks/starrocks/pull/73862)
- タスクの manager が不正な `RUNNING→RUNNING` エディットログを書き込み、タスク実行が実行中マップに無期限でスタックしているように見えていました。[#73882](https://github.com/StarRocks/starrocks/pull/73882)
- PK のマルチステートメントバッチトランザクションが、複合ロウセットにわたって `num_rows`、`data_size`、`num_dels` を累積していなかったため、共有データモードの主キーテーブルで行数の統計が不正確になっていました。[#74059](https://github.com/StarRocks/starrocks/pull/74059)
- Lake ロードのスピルクリーンアップは、トランザクション ID ベースのバキューム駆動回収を使用するようになり、BE のクラッシュや OOM の後に孤立したスピルファイルが残ることを防ぎます。[#73064](https://github.com/StarRocks/starrocks/pull/73064)
- `0000` 年以降の PostgreSQL JDBC の時刻値で、型マッピングの結果が不正になっていました。[#70842](https://github.com/StarRocks/starrocks/pull/70842)
- schema change 中にロウセットから `gtid` を読み込む前に null チェックが欠落しており、NPE クラッシュを引き起こしていました。[#74855](https://github.com/StarRocks/starrocks/pull/74855)

## 4.1.1 {#411}

リリース日：2026 年 5 月 29 日

### 動作の変更 {#behavior-changes-5}

- Hive コネクタは、デフォルトで JNI の Avro スキャナーの代わりにネイティブ C++ の Avro スキャナーを使用するようになりました。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- INCREMENTAL/AUTO マテリアライズドビューに対するクエリの書き換えが無効化され、INCREMENTAL/AUTO マテリアライズドビューに対する FORCE リフレッシュおよびパーティションリフレッシュが拒否されるようになりました。[#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### 改善 {#improvements-6}

- Java の UDF/UDAF/UDTF がより多くの型をサポートするようになりました：UDAF/UDTF の引数と戻り値としての STRUCT、ネストされた ARRAY/MAP 型、DATE/DATETIME、DECIMAL、および可変長引数。[#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- スカラー UDF が STRUCT 引数をサポートするようになりました。[#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDF が入れ子の ARRAY/MAP タイプをサポートするようになりました。[#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAF が一度だけロード・初期化され、クエリ間で再利用されるようになり、クエリごとのオーバーヘッドが削減されました。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- Hive コネクタ用の JNI Avro スキャナをネイティブ C++ スキャナに置き換え、直接バイナリデコードと `avro.schema.literal` および `avro.schema.url` のサポートを追加しました。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- CTAS 文で Trino の `WITH` 句をサポートします。[#71960](https://github.com/StarRocks/starrocks/pull/71960)
- シンクパスにおける Iceberg `timestamptz` パーティション変換のサポートを完成させました。[#73397](https://github.com/StarRocks/starrocks/pull/73397)
- Iceberg テーブルの集計に対して TopN ランタイムフィルタのプッシュダウンを有効にしました。[#72332](https://github.com/StarRocks/starrocks/pull/72332)
- Iceberg の日時の最小値／最大値最適化をサポートします。[#71870](https://github.com/StarRocks/starrocks/pull/71870)
- 複数の HDFS クラスタへのアクセスをサポートするため、INSERT と BE での HDFS HA 設定のパススルーを許可しました。
- 外部テーブルクエリに対してパーティションスキャン数の上限を追加しました。[#68480](https://github.com/StarRocks/starrocks/pull/68480)
- サポートされていない Iceberg V3 の機能に対して早期にエラーを返すようにしました。[#70242](https://github.com/StarRocks/starrocks/pull/70242)
- INSERT INTO FILES 経由の CSV エクスポートで `csv.enclose` と `csv.escape` をサポートします。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- `files()` への完全なスキーマプッシュダウンのため、`enable_push_down_schema` INSERT プロパティを追加しました。[#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load ジョブは、リトライ不可能なエラー（たとえば主キーサイズの超過など）が発生した場合に一時停止されるようになりました。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- 2 つの子要素からなる複雑な式に対するジョインの並べ替え（join reorder）をサポートします。[#71615](https://github.com/StarRocks/starrocks/pull/71615)
- `date_trunc`、`array_map`、CASE WHEN、IS NULL、UNION、および定数に対する MCV／null 割合の伝搬を含め、CBO の統計推定を改善しました。[#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- スキュージョインの検出を改善しました。all のジョインキーがスキューしている場合にのみスキューが検出されるようになり、スキューのルールを強制的に適用する `force_group_by_skew_eliminate_when_skewed` スイッチが追加されました。[#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- FE での `regexp_replace` に対する定数畳み込みをサポートします。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 定数のパーティション値を持つ日付パーティション列に対する MIN/MAX を最適化しました。[#69880](https://github.com/StarRocks/starrocks/pull/69880)
- マテリアライズドビューのリフレッシュにおいて、`ASYNC` の同義語として `SCHEDULE` キーワードを導入しました。[#72329](https://github.com/StarRocks/starrocks/pull/72329)
- 共有データモードの Lake テーブルに対する tablet 作成のリトライをサポートします。[#71068](https://github.com/StarRocks/starrocks/pull/71068)
- Lake の列モード部分更新に対する条件付き更新をサポートします。[#71961](https://github.com/StarRocks/starrocks/pull/71961)
- 取り込みスループットを向上させるため、部分更新の公開（publish）、永続性インデックスの初期化、および SSTable のオープン処理を並列化しました。[#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- 共有なしモードから共有データモードへのレプリケーション時における DCG ファイルの同期をサポートします。[#69339](https://github.com/StarRocks/starrocks/pull/69339)
- キー列・非キー列の両方で VARCHAR 長を拡張するスキーマ変更（schema change）をサポートします。[#70747](https://github.com/StarRocks/starrocks/pull/70747)
- クラスタスナップショットの整合性チェック用に `snapshot_meta.json` マーカーを追加しました。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- DN パターンを介した LDAP ダイレクトバインド認証をサポートします。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- クエリのトラブルシューティングを容易にするため、`get_query_dump_from_query_id` メタ関数を追加しました。[#72875](https://github.com/StarRocks/starrocks/pull/72875)
- 監査ログでクエリされたリレーションの監査をサポートします。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- MySQL のバイナリ結果エンコーディング用のセッション変数を追加しました。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 可観測性向上のため、共有データクラスタ用の `tablet_num`、`MemtableIOSpeed`、`staros_shard_count`、および Iceberg メタデータテーブルのクエリメトリクスなどのメトリクスを追加しました。[#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- FE の設定項目 `deploy_serialization_min_thread_pool_size` を追加しました。[#72274](https://github.com/StarRocks/starrocks/pull/72274)
- MergeTabletJob の作成を無効化するための `tablet_reshard_enable_tablet_merge` 設定を追加しました。[#70906](https://github.com/StarRocks/starrocks/pull/70906)
- `SO_REUSEPORT` によって HTTP サーバーの accept における thundering herd 問題を解消しました。[#72956](https://github.com/StarRocks/starrocks/pull/72956)
- `CREATE FUNCTION ... AS <sql_body>` 経由での SQL UDF の作成をサポートします。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
- S3 からの UDF ロードをサポートします。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
- 時系列順の UUID v7 値を生成する `uuid_v7` 関数を追加しました。[#67694](https://github.com/StarRocks/starrocks/pull/67694)
- external catalog の可観測性向上のため、catalog タイプごとのクエリメトリクスを追加しました。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- ウィンドウ関数に対する明示的なスキューヒントをサポートし、スキューしたパーティションキーを持つウィンドウ関数を UNION に分割することで自動的に最適化します。[#68739](https://github.com/StarRocks/starrocks/pull/68739)

### セキュリティ {#security-6}

- [CVE] Netty を 4.1.133.Final にアップグレードしました。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] pgjdbc を 42.7.11（無制限の SCRAM PBKDF2 反復回数によるクライアント側 DoS）、BouncyCastle を 1.84（FrodoKEM 秘密鍵の漏洩）にバンプしました。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- [CVE-2026-32280] [CVE-2026-32282] Golang の CVE を解消するため、pprof を go1.25.9 でビルドしました。[#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- jetty-http を 9.4.58.v20250814 にアップグレードしました。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- Broker の依存関係の CVE をクリーンアップし、`wildfly-openssl` を削除しました。[#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- INSERT INTO FILES のエラーメッセージ内の認証情報を隠蔽しました。[#71245](https://github.com/StarRocks/starrocks/pull/71245)

### バグ修正 {#bug-fixes-6}

以下の問題が修正されました：

- `hash_util` の静的初期化順序に起因する、起動時の CN のセグフォルト。[#71825](https://github.com/StarRocks/starrocks/pull/71825)
- 物理分割が有効な状態で空の tablet をスキャンした際の CN クラッシュ。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- `information_schema.warehouse_queries` をクエリした際の BE クラッシュ。[#72019](https://github.com/StarRocks/starrocks/pull/72019)
- rowset の `num_rows` がゼロの場合に Lake Compaction で発生する SIGFPE。[#71742](https://github.com/StarRocks/starrocks/pull/71742)
- ExecutionDAG フラグメント接続におけるゼロ除算。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- SinkBuffer における正常終了時のクラッシュ。[#73202](https://github.com/StarRocks/starrocks/pull/73202)
- スピル可能なハッシュジョインのプローブ時のクラッシュ。[#72397](https://github.com/StarRocks/starrocks/pull/72397)
- 一時的な `std::string` へのフォーマット時に発生するスタックバッファオーバーフロー。[#72728](https://github.com/StarRocks/starrocks/pull/72728)
- `reverse(DecimalV3)` 内でのクラッシュ。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- 一時的な `shared_ptr` の破棄に起因する `LoadChannel::get_load_replica_status` での use-after-free。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- スレッド作成が失敗した場合の `ThreadPool::do_submit` における use-after-free。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- フラグメントの終了処理中に発生する Hive パーティションディスクリプタの use-after-free。[#73176](https://github.com/StarRocks/starrocks/pull/73176)
- Information Schema シンクの use-after-free。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- HttpClient インスタンスの再利用による FE ファイルディスクリプタのリーク。[#73239](https://github.com/StarRocks/starrocks/pull/73239)
- `JDBCScanner::_init_jdbc_scanner` 内での JNI ローカル参照のリーク。[#72913](https://github.com/StarRocks/starrocks/pull/72913)
- MV プランコンテキストをキャッシュする際のメモリリーク。[#72300](https://github.com/StarRocks/starrocks/pull/72300)
- ローカル交換における予期しないメモリの過剰使用。[#72262](https://github.com/StarRocks/starrocks/pull/72262)
- Lake の `publish_version` における `response->tablet_metas` のレース。[#73274](https://github.com/StarRocks/starrocks/pull/73274)
- `DeltaWriter::commit()` における並行 `SegmentFlushTask` のレース。[#73371](https://github.com/StarRocks/starrocks/pull/73371)
- シリアライズ時の `RuntimeProfile` の最小値／最大値のレース。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- クエリコンテキストの破棄時における `PipelineTimerTask` 内の競合状態。[#73082](https://github.com/StarRocks/starrocks/pull/73082)
- `_all_global_rf_ready_or_timeout` における競合状態。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- `map_apply` と `array_length` における共有 `NullColumn` の問題。[#71258](https://github.com/StarRocks/starrocks/pull/71258)
- パーティションバージョンのギャップに起因するバッチパブリッシュのデッドロック。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- 共有なしモードで rowset メタデータの LRU キャッシュをウォームアップする際のデッドロック。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `Locker` のロールバックが例外安全でなく、アンロックの順序が誤っている問題。[#72789](https://github.com/StarRocks/starrocks/pull/72789)
- 読み取り専用パスおよびメタデータパスにおける複数の DB ロックに起因する、DDL および StarOS RPC とのロック競合。[#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- プロジェクトノードの欠落による不正なシャッフル分散。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- AGG TopN ランタイムフィルターの `exprOrder` 不一致によるクラッシュと誤った結果。[#71479](https://github.com/StarRocks/starrocks/pull/71479)
- dict-merge GROUP BY による誤った結果。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- Query Cache がローカルシャッフル集計と競合する問題。[#73194](https://github.com/StarRocks/starrocks/pull/73194)
- flat JSON におけるグローバル辞書生成の不整合。[#72953](https://github.com/StarRocks/starrocks/pull/72953)
- Flat JSON のマージ時の空データの不整合。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- キー/値のタイプが明示的に宣言されている場合に、マップリテラルでタイプの不一致が発生する問題。[#71316](https://github.com/StarRocks/starrocks/pull/71316)
- JOIN USING トランスフォーマーで COALESCE の子要素が共通タイプにキャストされない問題。[#72338](https://github.com/StarRocks/starrocks/pull/72338)
- グローバル変数を使用した reduce-cast 後に VARCHAR の長さが保持されない問題。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- MySQL の結果セット内のネストされたタイプで VARBINARY が誤ってエンコードされる問題。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- 小さな LIMIT で集計スピルを無効化した際の、having 句のチェックに関する問題。[#72705](https://github.com/StarRocks/starrocks/pull/72705)
- 日付解析前に引用符が除去されない問題、および PostgreSQL の日付／時刻のバグ。[#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)
- データファイルの shared フラグが失われ、vacuum が兄弟の分割 tablet によってまだ参照されているファイルを削除してしまう問題。[#71585](https://github.com/StarRocks/starrocks/pull/71585)
- split→compaction→merge のシーケンスにおける tablet マージの正確性に関する問題。[#72350](https://github.com/StarRocks/starrocks/pull/72350)
- tablet の分割時に、クロスパブリッシュされたトランザクションログの num_rows/data_size が膨張する問題。[#71144](https://github.com/StarRocks/starrocks/pull/71144)
- 同一のパブリッシュバッチ内で Compaction 前に書き込みが行われたことによる Delvec の孤立エントリの問題。[#71001](https://github.com/StarRocks/starrocks/pull/71001)
- StarMgr ジャーナルのリプレイを同期させることで、Follower FE で発生する「no queryable replica」の問題を解消。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 通常の rowset コミットを適用する際に `merge_condition` が保持されない問題。[#72542](https://github.com/StarRocks/starrocks/pull/72542)
- 不正なスナップショット ID とフィルターを使用した Iceberg DELETE の競合検出の問題。[#73354](https://github.com/StarRocks/starrocks/pull/73354)
- 無効な Iceberg transform 引数による NPE。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- プランナーによって注入された余分な列が原因で、Iceberg の min/max 最適化がスキップされる問題。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- Iceberg ベーステーブルにおける集計ジョインプッシュダウン MV 書き換えの問題。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- INSERT OVERWRITE のコミット前に Hive パーティションディレクトリが欠落する問題。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- JNI スキャナーに AWS assume-role が適用されない問題。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- プルーニングされた子要素およびネストされた null 許容スキーマに対する Avro の複合タイプのデコードに関する問題。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- Parquet の Broker Load エラーにおいて、ファイル／列／行のコンテキストが欠落している問題。[#73236](https://github.com/StarRocks/starrocks/pull/73236)
- Parquet スキャナーが Arrow の辞書値をサポートしていない問題。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- Paimon テーブルの主キーが SHOW CREATE および DESC の戻り値に表示されない問題。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- PostgreSQL／Oracle の JDBC タイプ互換性、および末尾にスラッシュが付いた JDBC URL の構築に関する問題。[#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)
- JDBC catalog 内の SQL Server テーブルに関するマテリアライズドビューのリフレッシュの問題。[#72962](https://github.com/StarRocks/starrocks/pull/72962)
- 外部ジョインに対するマテリアライズドビューにおける、遅延実体化スロットの null 許容性の問題。[#72621](https://github.com/StarRocks/starrocks/pull/72621)
- AUTO および INCREMENTAL マテリアライズドビューのパーティションリフレッシュが拒否される問題。[#71355](https://github.com/StarRocks/starrocks/pull/71355)
- マテリアライズドビューが非アクティブになった後もマテリアライズドビュースケジューラーが停止しない問題。[#71265](https://github.com/StarRocks/starrocks/pull/71265)
- MySQL クライアント互換性のための `SHOW GRANTS FOR CURRENT_USER()` がサポートされていない問題。[#71959](https://github.com/StarRocks/starrocks/pull/71959)
- 明示的なトランザクション内で SHOW 文が許可されない問題。[#72954](https://github.com/StarRocks/starrocks/pull/72954)
- 空の結果セットに対して Arrow Flight が列名 `r` を返す問題。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Java UDF コードにおける JNI 例外処理チェックの欠如。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `ai_query` 関数の登録に関する問題。[#72103](https://github.com/StarRocks/starrocks/pull/72103)
- `enable_load_profile` 使用時の Stream Load プロファイル収集の問題。[#71952](https://github.com/StarRocks/starrocks/pull/71952)
- プロファイルの START_TIME/END_TIME がセッションのタイムゾーンで表示されない問題。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `star_mgr_meta_sync_interval_sec` がランタイムで可変ではない問題。[#71675](https://github.com/StarRocks/starrocks/pull/71675)
- `information_schema.tables` が等価述語内の特殊文字をエスケープしない問題。[#71273](https://github.com/StarRocks/starrocks/pull/71273)
- エラーパスでの並列セグメント／rowset ロードにおける use-after-free の問題。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 集計スピル `set_finishing` におけるハッシュテーブルのデータ損失の可能性。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- ディスクの再マイグレーション（A→B→A）中の GC レースによって生じる PK tablet の rowset メタ損失。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- `SharedDataStorageVolumeMgr` における DB 読み取りロックのリーク。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- IVM リフレッシュが不完全な PCT パーティションメタデータを記録する問題。[#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 参照先の列が存在しない場合、Stream Load/Broker Load で生成列を解析する際に NPE が発生する問題。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- 短絡ポイントルックアップでパーティション述語が欠落する問題。[#71124](https://github.com/StarRocks/starrocks/pull/71124)

## 4.1.0 {#410}

リリース日：2026年4月13日

### 共有データアーキテクチャ {#shared-data-architecture}

- **新しいマルチテナントデータ管理**

  共有データクラスタが、レンジベースのデータ分散、および tablet の自動分割・マージをサポートするようになりました。tablet はサイズが過大になったりホットスポットになったりした場合に、スキーマ変更や SQL の変更、データの再取り込みを必要とせずに自動的に分割できます。この機能により使いやすさが大幅に向上し、マルチテナントワークロードにおけるデータスキューやホットスポットの問題に直接対処できます。[#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量 Tablet サポート（フェーズ1）**

  共有データクラスタが tablet あたりに保持できるデータ量を大幅に増やし、長期的には tablet あたり100 GBを目標としています。フェーズ1では、取り込み、Primary Key 更新、Compaction の全パイプラインにわたって tablet 内の並列処理を導入しており、単一の Lake tablet がデータ増加に伴って単一スレッドのボトルネックになることがなくなりました。改善点としては、単一 tablet 内での並列 Compaction（セグメントレベルでの分割を伴う）、Lake ロード（ロードスピルパスを含む）における MemTable のファイナライズ・フラッシュ・マージの並列化、Primary Key テーブル向けの tablet 内部での並列パブリッシュおよび並列条件更新、さらにリモートストレージマッパーファイルをサポートした cloud ネイティブの Primary Key インデックス向けのレンジ分割・並列・サイズ階層型 Compaction が挙げられます。これらの変更により、大規模な tablet ワークロードにおける取り込みメモリのオーバーヘッド、Compaction の増幅、FE メタデータへの負荷が大幅に軽減されます。[#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **高速スキーマ進化 V2**

  共有データクラスタが高速スキーマ進化 V2 をサポートするようになりました。これにより、スキーマ操作の秒単位での DDL 実行が可能になり、さらにマテリアライズドビューへのサポートも拡張されます。[#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **【ベータ】共有データにおける転置インデックス**

  共有データクラスタで組み込みの転置インデックスをサポートし、テキストフィルタリングや全文検索のワークロードを高速化します。[#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **キャッシュの可観測性**

  クエリ単位でのキャッシュヒット率が、監査ログおよび監視システムで確認できるようになり、キャッシュの透明性とレイテンシ診断が向上しました。追加された Data Cache メトリクスには、メモリおよびディスクのクォータ使用状況、ページキャッシュの統計情報が含まれます。[#63964](https://github.com/StarRocks/starrocks/pull/63964)

- Lake テーブル向けにセグメントメタデータフィルターを追加し、スキャン時にソートキーの範囲に基づいて無関係なセグメントをスキップすることで、範囲述語クエリの I/O を削減します。[#68124](https://github.com/StarRocks/starrocks/pull/68124)

- Lake DeltaWriter の高速キャンセルをサポートし、共有データクラスタでキャンセルされた取り込みジョブのレイテンシを削減します。[#68877](https://github.com/StarRocks/starrocks/pull/68877)

- 自動クラスタスナップショットに対する間隔ベースのスケジューリングのサポートを追加しました。[#67525](https://github.com/StarRocks/starrocks/pull/67525)

- MemTable のフラッシュおよびマージのパイプライン実行をサポートし、共有データクラスタにおける cloud ネイティブテーブルの取り込みスループットを向上させます。[#67878](https://github.com/StarRocks/starrocks/pull/67878)

- cloud ネイティブテーブルの修復に `dry_run` モードをサポートし、実行前に修復アクションをプレビューできるようになりました。[#68494](https://github.com/StarRocks/starrocks/pull/68494)

- 共有なしクラスタにおけるトランザクションのパブリッシュ用スレッドプールを追加し、パブリッシュのスループットを向上させました。[#67797](https://github.com/StarRocks/starrocks/pull/67797)

### データレイク分析 {#data-lake-analytics}

- **Iceberg DELETE サポート**

  Iceberg テーブル向けにポジションデリートファイルの書き込みをサポートし、StarRocks から直接 Iceberg テーブルに対して DELETE 操作を実行できるようになりました。Plan、Sink、Commit、Audit の全パイプラインをカバーしています。[#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **Hive および Iceberg テーブル向けの TRUNCATE**

  外部 Hive および Iceberg テーブルに対する TRUNCATE TABLE をサポートします。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Iceberg における増分マテリアライズドビュー**

  増分マテリアライズドビューリフレッシュのサポートを Iceberg の追記専用テーブルにも拡張し、テーブル全体をリフレッシュすることなくクエリアクセラレーションを実現します。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **Iceberg における半構造化データ向けの VARIANT タイプ**

  Iceberg Catalog における VARIANT データタイプをサポートし、半構造化データに対する柔軟な schema-on-read 方式のストレージとクエリを可能にします。読み取り、書き込み、タイプキャスト、Parquet との統合をサポートします。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3 サポート**

  Iceberg v3 のデフォルト値機能および行系列（row lineage）のサポートを追加しました。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Iceberg テーブルメンテナンスプロシージャ**

  `rewrite_manifests` プロシージャのサポートを追加し、よりきめ細かいテーブルメンテナンスのために `expire_snapshots` および `remove_orphan_files` プロシージャに追加の引数を拡張しました。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- Iceberg テーブルからファイルパスおよび行位置のメタデータ列を読み取る機能をサポートします。[#67003](https://github.com/StarRocks/starrocks/pull/67003)

- Iceberg v3 テーブルから `_row_id` を読み取る機能をサポートし、Iceberg v3 のグローバルな後期実体化をサポートします。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)

- カスタムプロパティを持つ Iceberg ビューの作成をサポートし、SHOW CREATE VIEW の出力にプロパティを表示します。[#65938](https://github.com/StarRocks/starrocks/pull/65938)

- 特定のブランチ、タグ、バージョン、またはタイムスタンプを指定した Paimon テーブルのクエリをサポートします。[#63316](https://github.com/StarRocks/starrocks/pull/63316)

- Paimon テーブルの複合型（ARRAY、MAP、STRUCT）をサポートします。[#66784](https://github.com/StarRocks/starrocks/pull/66784)

- Iceberg テーブル作成時に括弧構文を使用した Partition Transforms をサポートしました。[#68945](https://github.com/StarRocks/starrocks/pull/68945)

- データ編成を改善するため、Transform Partition に基づく Iceberg のグローバルシャッフルをサポートしました。[#70009](https://github.com/StarRocks/starrocks/pull/70009)

- Iceberg テーブルシンクでグローバルシャッフルを動的に有効化することをサポートしました。[#67442](https://github.com/StarRocks/starrocks/pull/67442)

- Iceberg テーブルシンクに Commit キューを導入し、Commit の競合を回避できるようにしました。[#68084](https://github.com/StarRocks/starrocks/pull/68084)

- データ編成と読み取り性能を向上させるため、Iceberg テーブルシンクにホストレベルのソートを追加しました。[#68121](https://github.com/StarRocks/starrocks/pull/68121)

- ETL 実行モードで追加の最適化をデフォルトで有効化し、明示的な設定なしで INSERT INTO SELECT や CREATE TABLE AS SELECT などの類似のバッチ操作のパフォーマンスを向上させました。[#66841](https://github.com/StarRocks/starrocks/pull/66841)

- Iceberg テーブルに対する INSERT および DELETE 操作のコミット監査情報を追加しました。[#69198](https://github.com/StarRocks/starrocks/pull/69198)

- Iceberg REST Catalog でビューエンドポイント操作の有効化・無効化をサポートしました。[#66083](https://github.com/StarRocks/starrocks/pull/66083)

- CachingIcebergCatalog におけるキャッシュ検索の効率を最適化しました。[#66388](https://github.com/StarRocks/starrocks/pull/66388)

- さまざまな Iceberg catalog タイプに対して EXPLAIN をサポートしました。[#66563](https://github.com/StarRocks/starrocks/pull/66563)

- AWS Glue Catalog 内のテーブルに対するパーティションプロジェクションをサポートしました。[#67601](https://github.com/StarRocks/starrocks/pull/67601)

- AWS Glue `GetDatabases` API のリソース共有タイプのサポートを追加しました。[#69056](https://github.com/StarRocks/starrocks/pull/69056)

- エンドポイントインジェクション（`azblob`/`adls2`）による Azure ABFS/WASB パスマッピングをサポートしました。[#67847](https://github.com/StarRocks/starrocks/pull/67847)

- リモート RPC のオーバーヘッドと外部システム障害の影響を低減するため、JDBC catalog にデータベースメタデータキャッシュを追加しました。[#68256](https://github.com/StarRocks/starrocks/pull/68256)

- `information_schema` において PostgreSQL テーブルの列コメントをサポートしました。[#70520](https://github.com/StarRocks/starrocks/pull/70520)

- Oracle および PostgreSQL の JDBC タイプマッピングを改善しました。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### クエリエンジン {#query-engine}

- **再帰 CTE**

  階層走査、グラフクエリ、反復的な SQL 計算のために、再帰的な共通テーブル式（CTE）をサポートしました。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 統計ベースのスキュー検出、ヒストグラムサポート、NULL スキュー認識を備えた Skew Join v2 の書き換えを改善しました。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)

- ウィンドウにおける COUNT DISTINCT を改善し、複数の distinct を融合した集計のサポートを追加しました。[#67453](https://github.com/StarRocks/starrocks/pull/67453)

- ウィンドウ関数に対する明示的なスキューヒントをサポートし、スキューしたパーティションキーを持つウィンドウ関数を UNION に分割することで自動最適化します。[#67944](https://github.com/StarRocks/starrocks/pull/67944)

- Trino Parser の INSERT ステートメントに対して EXPLAIN と EXPLAIN ANALYZE をサポートしました。[#70174](https://github.com/StarRocks/starrocks/pull/70174)

- クエリキューの可視性のために EXPLAIN をサポートしました。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 関数と SQL 構文 {#functions-and-sql-syntax}

- 以下の関数を追加しました：
  - `array_top_n`：値でランク付けされた配列の上位 N 個の要素を返します。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：複数の配列を要素ごとに結合し、構造体の配列にします。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`：インデントを付けて JSON 文字列をフォーマットします。[#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`：JSON 文字列内の指定されたパスに値を設定します。[#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`：各単語の最初の文字を大文字に変換します。[#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`：同じキーを持つ行間で MAP の値を合計します。[#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`：現在のセッションのタイムゾーンを返します。[#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`：現在の warehouse の名前を返します。[#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`：秒数を TIME の値に変換します。[#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`：推論ワークロードのために SQL から外部 AI モデルを呼び出します。[#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `raise_error`：SQL 式内でユーザー定義のエラーを発生させます。[#69661](https://github.com/StarRocks/starrocks/pull/69661)
- 以下の関数または構文の拡張を提供します：
  - カスタムソート順序のために `array_sort` 内でラムダ比較器をサポートしました。[#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - SQL 標準のセマンティクスで FULL OUTER JOIN に対する USING 句をサポートしました。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - DISTINCT と ORDER BY/PARTITION BY を使用したフレームウィンドウ関数での集計をサポートします。[#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - `lead`/`lag`/`first_value`/`last_value` ウィンドウ関数での ARRAY タイプをサポートします。[#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - 重複排除カウント系の集計関数に対する VARBINARY をサポートします。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - IN 式での日付型および文字列型のキャストをサポートします。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - BEGIN/START TRANSACTION 向けの WITH LABEL 構文をサポートします。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - SHOW ステートメントでの WHERE/ORDER/LIMIT 句をサポートします。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - タスク管理のための `ALTER TASK` ステートメントをサポートします。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - CSV ファイルエクスポートにおける複数の圧縮フォーマット（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）をサポートします。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 名前ベースの構造体フィールドマッチングのための `STRUCT_CAST_BY_NAME` SQL モードをサポートします。[#69845](https://github.com/StarRocks/starrocks/pull/69845)

### 管理と可観測性 {#management--observability}

- 複数ウェアハウスの CPU リソース分離を改善するため、リソースグループに対して `warehouses`、`cpu_weight_percent`、`exclusive_cpu_weight` 属性をサポートします。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- FE のスレッド状態を確認するための `information_schema.fe_threads` システムビューを導入します。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- クラスタレベルで特定のクエリパターンをブロックするための SQL Digest Blacklist をサポートします。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- ネットワークトポロジの制約によりアクセスできないノードからの Arrow Flight データ取得をサポートします。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 再接続せずに既存の接続へグローバル変数の変更を伝播するための REFRESH CONNECTIONS コマンドを導入します。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- クエリプロファイルを分析し、フォーマットされた SQL を表示するための組み込み UI 関数を追加し、クエリチューニングをより容易にしました。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 構造化されたクラスタ概要を提供する `ClusterSummaryActionV2` API エンドポイントを実装します。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 現在のクラスタ実行モード（共有データまたは共有なし）を照会するためのグローバル読み取り専用システム変数 `@@run_mode` を追加しました。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- クエリキュー管理を改善するため、デフォルトで `query_queue_v2` を有効にしました。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- Stream Load および Merge Commit 操作に対するユーザーレベルのデフォルトウェアハウスをサポートします。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 必要に応じてバックエンドのブラックリスト検証をバイパスするための `skip_black_list` セッション変数を追加しました。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- メトリクス API 用の `enable_table_metrics_collect` オプションを追加しました。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- クエリ詳細 HTTP API に対するユーザーなりすましのサポートを追加しました。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- テーブルレベルのプロパティとして `table_query_timeout` を追加しました。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- FE オブザーバーノードの追加をサポートします。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- ロードジョブの可視性を向上させるため、`information_schema.loads` に Merge Commit 情報を表示することをサポートします。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- トラブルシューティングを改善するため、cloud ネイティブテーブルでの tablet ステータスの表示をサポートします。[#69616](https://github.com/StarRocks/starrocks/pull/69616)

### セキュリティ {#security-7}

- [CVE-2026-33870] [CVE-2026-33871] AWS バンドルを置き換え、Netty を 4.1.132.Final にバンプしました。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] Hadoop を v3.4.2 にアップグレードしました。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] `spark-core_2.12` を 3.5.7 にアップグレードしました。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### バグ修正 {#bug-fixes-7}

以下の問題が修正されました：

- レンジ分散のタブレットに対するデータファイル削除をスキップすることで、tablet 分割後のデータ損失を修正しました。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 複合型に対する `DefaultValueColumnIterator` でのメモリリークを修正しました。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- `BatchUnit` と `FetchTaskContext` の間の `shared_ptr` サイクルによって引き起こされるメモリリークを修正しました。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- 並行した getline アクセスによる SystemMetrics での二重解放クラッシュを修正しました。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- eager merge が all ブロックを消費する際の SpillMemTableSink でのクラッシュを修正しました。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- 自動作成されたパーティションが TTL クリーナーによって削除された際の NPE を修正しました。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- スナップショットが期限切れの場合に `IcebergCatalog.getPartitionLastUpdatedTime` で発生する NPE を修正しました。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 定数側の列参照を伴う外部結合における不正な述語書き換えを修正しました。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- 共有データ環境で CHAR 列長を変更した後にクエリ結果が誤っていた問題を修正しました。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 複数テーブルの場合における MV リフレッシュのバグを修正しました。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 強制リフレッシュ時に MV のリサイクル時間が不正になる問題を修正しました。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 同期 MV における all -null 値処理のバグを修正しました。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- 高速 schema change ADD COLUMN 後に MV をクエリする際に重複した列 ID エラーが発生する問題を修正しました。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- 共有の DecodeInfo に起因する低基数書き換えの NPE を修正しました。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 低基数ジョイン述語のタイプ不一致を修正しました。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- `null_counts` が空の場合に Parquet Page Index Filter でセグメンテーション違反が発生する問題を修正しました。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- 同一パス上での JSON フラット化配列とオブジェクトの競合を修正しました。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- Iceberg キャッシュウェイヤーの不正確さを修正しました。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- Iceberg テーブルキャッシュのメモリ制限を修正しました。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- Iceberg 削除列の null 許容性に関する問題を修正しました。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- コンテナを含むように Azure ABFS/WASB FileSystem のキャッシュキーを修正しました。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- HMS 接続プールが満杯の場合のデッドロックを修正しました。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- Paimon Catalog における VARCHAR フィールドタイプの長さが不正な問題を修正しました。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- ObjectTable において ClassCastException が発生し Paimon catalog のリフレッシュがクラッシュする問題を修正しました。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- 定数サブクエリを伴う FULL OUTER JOIN USING の問題を修正しました。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- CTE スコープにおける結合の on 句のバグを修正しました。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- bindScope() パターンを使用することで ConnectContext のメモリリークを修正しました。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- 共有なしクラスタ向けの `CatalogRecycleBin.asyncDeleteForTables` におけるメモリリークを修正しました。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- 例外が発生した際に Thrift の accept スレッドが終了してしまう問題を修正しました。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- ルーチンロードの列マッピングにおける UDF 解決を修正しました。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- `DROP FUNCTION IF EXISTS` が `ifExists` フラグを無視する問題を修正しました。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- 辞書ページが大きすぎる場合にスキャン結果がエラーになる問題を修正しました。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- レンジパーティション化の重複を修正しました。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- クエリキューの割り当て時間と保留タイムアウトを修正しました。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- null リテラル配列を処理する際に `array_map` がクラッシュする問題を修正しました。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- `to_base64` のスタックオーバーフローを修正しました。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- LDAP 認証における大文字小文字を区別しないユーザー名の正規化を修正しました。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- API `proc_file` における SSRF リスクを軽減しました。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 監査ログおよび SQL のマスキングにおいて、ユーザー認証文字列をマスクするようにしました。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 動作の変更 {#behavior-changes-6}

- ETL 実行モードの最適化がデフォルトで有効になりました。これにより、INSERT INTO SELECT、CREATE TABLE AS SELECT、および同様のバッチワークロードが、明示的な設定変更なしで恩恵を受けられるようになりました。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead` ウィンドウ関数の第 3 引数は、定数値に加えて列参照もサポートするようになりました。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING が SQL 標準のセマンティクスに従うようになりました。USING 列は出力中に 2 回ではなく 1 回だけ表示されます。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- `query_queue_v2` がデフォルトで有効になりました。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQL トランザクションは、デフォルトでセッション変数 `enable_sql_transaction` によって制御されるようになりました。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
- これまで、FE 設定項目 `max_scalar_operator_flat_children`（デフォルト `10000`）は `CASE WHEN` によって構築される式のノード数のみを制限していました。現在は、オプティマイザが書き換えるすべてのスカラー式を制限するようになり、ノード数はキャッシュされた値を再利用するのではなく、各書き換え後に再計算されます。その結果、アップグレード後、以前は実行できていたクエリが、ビュー、CTE、またはサブクエリのインライン化によって式ツリーが拡大した場合、分析フェーズで `Expression too complex. Current nodes: N, Limit: M.` として拒否されることがあります。式を単純化するか、動的に変更可能な `max_scalar_operator_flat_children` を増やしてください。[#66324](https://github.com/StarRocks/starrocks/pull/66324)
