---
displayed_sidebar: docs
description: "StarRocks 4.1 リリースノート：マルチテナントのレンジベース tablet 自動分割、大容量 tablet サポート（100 GB 目標）、Fast Schema Evolution V2 for..."
---

# StarRocks バージョン 4.1

:::danger
**コンテナイメージの問題（v4.1.0）**

v4.1.0 コンテナイメージにおける不安定なロード順序の問題により、コンテナ環境で BE プロセスが確実に起動しない場合があります。**コンテナ環境のユーザーは v4.1.0 への NOT アップグレードを行わないでください。**この修正を含む v4.1.1（[#71825](https://github.com/StarRocks/starrocks/pull/71825)）をお待ちください。
:::

:::warning
**ダウングレードに関する注意事項**

- StarRocks を v4.1 にアップグレードした後、v4.0.6 未満のいかなる v4.0 バージョンにも NOT ダウングレードしないでください。

  v4.1 で導入された内部的なデータレイアウトの変更（tablet の分割および分散メカニズムに関連するもの）により、v4.1 にアップグレードされたクラスタは、以前のバージョンと完全には互換性のないメタデータおよびストレージ構造を生成する可能性があります。その結果、v4.1 からのダウングレードは v4.0.6 以降のバージョンにのみサポートされます。v4.0.6 より前のバージョンへのダウングレードはサポートされていません。この制限は、以前のバージョンが tablet のレイアウトおよび分散メタデータを解釈する方法における後方互換性の制約によるものです。
:::

## 4.1.8 {/* #418 */}

リリース日：2026 年 11 月 16 日

### 動作の変更 {/* #behavior-changes */}

- 共有データの主キーインデックスのキャッシュ退避ポリシーが変更されました。キャッシュには、すべての publish の後にチェックされる緊急レベルが追加されました。[#81143](https://github.com/StarRocks/starrocks/pull/81143) [#81168](https://github.com/StarRocks/starrocks/pull/81168)
- ローカル（共有なし）の主キー挿入では、デフォルトで、重複キーについて不変の L1 および L2 永続性インデックスファイルをチェックしなくなりました。新しい BE 設定項目 `skip_pindex_l1_l2_check_in_insert` のデフォルトは `true` です。以前の動作に戻すには `false` に設定してください。[#81304](https://github.com/StarRocks/starrocks/pull/81304)
- `lake_pk_preload_memory_limit_percent`、`lake_pk_index_block_cache_limit_percent`、および `put_combined_txn_log_thread_pool_num_max` は、BE または CN を再起動せずに実行時に有効になるようになりました。`lake_pk_index_block_cache_limit_percent` は可変になりました。[#81164](https://github.com/StarRocks/starrocks/pull/81164) [#81244](https://github.com/StarRocks/starrocks/pull/81244) [#81677](https://github.com/StarRocks/starrocks/pull/81677)
- マテリアライズドビューのリフレッシュ処理が変更されました。削除されたベースパーティションは、そのマテリアライズドビューパーティションがリフレッシュされるまで追跡され続け、どのベースパーティションにも対応しないマテリアライズドビューパーティションはリフレッシュされ、インクリメンタルなマテリアライズドビュー再構築のうち上書きされたバッチは失敗ではなくスキップされるようになりました。[#81187](https://github.com/StarRocks/starrocks/pull/81187) [#81192](https://github.com/StarRocks/starrocks/pull/81192) [#81262](https://github.com/StarRocks/starrocks/pull/81262)
- インクリメンタルなマテリアライズドビューは、`__ROW_ID__` に対してプレフィックスフリーの行 ID フィンガープリントを使用します。[#81259](https://github.com/StarRocks/starrocks/pull/81259)
- マテリアライズドビューは FE 起動時にレイヤーごとに有効化されるようになったため、`mv-plan-cache` プールを枯渇させることがなくなりました。[#81497](https://github.com/StarRocks/starrocks/pull/81497)
- external catalog テーブルおよび外部データベース・テーブルの一覧表示における `DESCRIBE` は、メタデータ呼び出しに呼び出し元のセッションコンテキストを使用するようになりました。[#80628](https://github.com/StarRocks/starrocks/pull/80628)
- `bool_or` は、NULL を保持する中間列をマージできるようになりました。[#81237](https://github.com/StarRocks/starrocks/pull/81237)
- サポートされていない間隔演算は、`Unknown error` の代わりに説明的なエラーを報告するようになりました。[#81024](https://github.com/StarRocks/starrocks/pull/81024)
- `CREATE BASELINE` が follower FE 上で動作するようになりました。[#81157](https://github.com/StarRocks/starrocks/pull/81157)
- FE が複数の FE 間でグローバル辞書を誤って更新する問題が解消されました。[#79511](https://github.com/StarRocks/starrocks/pull/79511)
- 削除された共有データ tablet のバージョン 1 を読み取ると、`NotFound` が返されるようになりました。[#81196](https://github.com/StarRocks/starrocks/pull/81196)
- 共有データの主キー tablet のリカバリが改修されました。publish の Delete Vector はリカバリと整合性が保たれ、マルチステートメントトランザクションのバッチ適用は主キーの不整合時にリカバリされ、物理行 ID が使用され、Compaction によって継承された削除ファイルは破棄されるようになりました。[#81204](https://github.com/StarRocks/starrocks/pull/81204) [#81206](https://github.com/StarRocks/starrocks/pull/81206) [#81211](https://github.com/StarRocks/starrocks/pull/81211) [#81250](https://github.com/StarRocks/starrocks/pull/81250)
- Kafka の Routine Load は、パーティションごとに EOF を追跡し、空のロード（宛先行を生成しないバッチ）を異なる方法で処理するようになりました。[#81412](https://github.com/StarRocks/starrocks/pull/81412)
- 明示的なトランザクションのロード ID が永続化されるようになったため、FE の再起動やリーダー変更後でも、publish がトランザクションログを見つけられるようになりました。[#81413](https://github.com/StarRocks/starrocks/pull/81413)
- レガシーなユーザー DN ごとの LDAP ユーザーに対して、グループプロバイダが保持されるようになりました。[#81399](https://github.com/StarRocks/starrocks/pull/81399)
- FE がイメージをロードする際、拒否されたセッション変数の値は FE の起動を失敗させる代わりにスキップされるようになりました。[#81655](https://github.com/StarRocks/starrocks/pull/81655)
- Arrow が事前バッファリングする Parquet の範囲は、`FILES()` および Broker Load においてスキャンスレッド上で読み取られるようになりました。[#81771](https://github.com/StarRocks/starrocks/pull/81771)
- Iceberg のパーティション値は、各データファイル固有のパーティション仕様でデコードされるようになりました。[#81789](https://github.com/StarRocks/starrocks/pull/81789)
- 単一トランザクションの lake publish バッチが、再試行のバックオフを尊重するようになりました。[#81803](https://github.com/StarRocks/starrocks/pull/81803)
- トライ境界によって以前は除外されていた、フラットな JSON パスが、JSON 型として扱われるようになりました。[#81816](https://github.com/StarRocks/starrocks/pull/81816)
- Lake メタデータキャッシュのキーおよびメタデータ削除時のキャッシュ退避に関する問題が修正されました。[#81978](https://github.com/StarRocks/starrocks/pull/81978)

### 改善項目 {/* #improvements */}

- スキャンエグゼキュータのワーカースレッドに関するメトリクスが追加されました。[#77435](https://github.com/StarRocks/starrocks/pull/77435)
- 共有データの publish バージョンのタイムアウトが設定可能になりました。[#79332](https://github.com/StarRocks/starrocks/pull/79332)
- 大きなフラット JSON の Compaction 出力が、セグメント全体をバッファリングする代わりにストリーミングされるようになりました。[#80470](https://github.com/StarRocks/starrocks/pull/80470)
- `update_memory_limit_percent` が可変になりました。[#81118](https://github.com/StarRocks/starrocks/pull/81118)
- JVM のコミット済みメモリが、BE プロセスのメモリトラッカーでカウントされるようになりました。[#81127](https://github.com/StarRocks/starrocks/pull/81127)
- 不変インデックスの存在チェックが `get` を再利用するようになりました。[#81307](https://github.com/StarRocks/starrocks/pull/81307)
- データファイルが ORC である Iceberg テーブルに対する `DELETE` をサポートしました。[#81435](https://github.com/StarRocks/starrocks/pull/81435)
- ORC データファイルからの Iceberg v3 行系列（row lineage）の読み取りをサポートしました。[#81440](https://github.com/StarRocks/starrocks/pull/81440)
- ランタイムフィルターによって拒否されたパーティションの Iceberg equality delete ファイルはスキップされ、Iceberg スキャンレンジのプランニング時にファイルごとのパーティション処理もスキップされます。[#81437](https://github.com/StarRocks/starrocks/pull/81437) [#81438](https://github.com/StarRocks/starrocks/pull/81438)
- 共有データバックアップノードは tablet ハッシュによって分散されます。[#81436](https://github.com/StarRocks/starrocks/pull/81436)

### セキュリティ {/* #security */}

- FE の ZooKeeper を 3.9.6 にバンプしました。[#81292](https://github.com/StarRocks/starrocks/pull/81292)
- pprof を release/20261022 にバンプしました。[#81300](https://github.com/StarRocks/starrocks/pull/81300)
- guava を 33.7.2 に、snappy-java を 1.1.10.9 にバンプしました。[#81859](https://github.com/StarRocks/starrocks/pull/81859)

### バグ修正 {/* #bug-fixes */}

以下の問題が修正されました：

- Iceberg の読み込みおよび挿入において、不明なパーティショントランスフォーム、ORC フィールド ID、昇格された境界値、不明なタイプ、デフォルト値に関する問題を修正しました。[#81257](https://github.com/StarRocks/starrocks/pull/81257) [#81311](https://github.com/StarRocks/starrocks/pull/81311) [#81432](https://github.com/StarRocks/starrocks/pull/81432)
- パーティション進化が行われたテーブルに対する Iceberg の読み書きを修正しました。これには、位置削除がデータファイルのスペックの下に保持されることを含みます。[#81401](https://github.com/StarRocks/starrocks/pull/81401) [#81361](https://github.com/StarRocks/starrocks/pull/81361)
- Iceberg DML のターゲット検索、row id でペアリングされた v3 検索行、および write-default を持たない列に対する NULL 書き込みの問題を修正しました。[#81165](https://github.com/StarRocks/starrocks/pull/81165) [#81188](https://github.com/StarRocks/starrocks/pull/81188) [#81362](https://github.com/StarRocks/starrocks/pull/81362)
- 2 つの Iceberg FE ハング（explain パーティションリスト表示と OSS の read-ahead 待機）、およびアイドル状態の Iceberg コミットエグゼキュータースレッドの問題を修正しました。[#81711](https://github.com/StarRocks/starrocks/pull/81711) [#81805](https://github.com/StarRocks/starrocks/pull/81805)
- Iceberg、Delta、Hudi の `INSERT` ソースは、テーブルロック中にリフレッシュされるのではなく無効化されるようになりました。[#81923](https://github.com/StarRocks/starrocks/pull/81923)
- Iceberg REST catalog のユーザーエージェントが `NodeMgr` を使わずに構築されるようになりました。[#80660](https://github.com/StarRocks/starrocks/pull/80660)
- `OR` 転置インデックスフォールバック内で否定された `MATCH` が NULL 行を返していた問題を修正しました。[#76350](https://github.com/StarRocks/starrocks/pull/76350)
- 主キーテーブルの Compaction におけるレベル -1 の rowset 選択が無制限になっていた問題を修正しました。[#80471](https://github.com/StarRocks/starrocks/pull/80471)
- バッファリングされたパススルーチャンクがクエリおよびプロセスのメモリに計上されていなかった問題を修正しました。[#80910](https://github.com/StarRocks/starrocks/pull/80910)
- 集計状態コンビネーターが、バッチ破棄時にラップされた状態をリークしていた問題を修正しました。[#81054](https://github.com/StarRocks/starrocks/pull/81054)
- Compaction 処理がマージされた JSON 列を書き込んでいる最中に、その列が解放される可能性があった問題を修正しました。[#81117](https://github.com/StarRocks/starrocks/pull/81117)
- フラッシュが並行して完了した場合、スピラーの flush-all コールバックが 2 回実行される可能性があった問題を修正しました。[#81134](https://github.com/StarRocks/starrocks/pull/81134)
- min/max ランタイムフィルターの境界値が、スライスロックを保持せずに読み取られていた問題を修正しました。[#81138](https://github.com/StarRocks/starrocks/pull/81138)
- スピル可能な集計ソースが、解放済みのスピラーにアクセスする可能性があった問題を修正しました。[#81199](https://github.com/StarRocks/starrocks/pull/81199)
- `getShardInfo` 呼び出しが、バッチあたりのシャード数で制限されていなかった問題を修正しました。[#81197](https://github.com/StarRocks/starrocks/pull/81197)
- ロードシンクのクローズ送信時の例外により、BE または CN プロセスが終了する可能性があった問題を修正しました。[#81251](https://github.com/StarRocks/starrocks/pull/81251)
- Trino の `TIMESTAMP WITH TIME ZONE` リテラルが誤ったタイムゾーンの方向を使用していた問題を修正しました。[#81256](https://github.com/StarRocks/starrocks/pull/81256)
- アクティベーションが失敗した際、`ALTER MATERIALIZED VIEW ... ACTIVE` が `Can not active materialized view` メッセージを失っていた問題を修正しました。[#81258](https://github.com/StarRocks/starrocks/pull/81258)
- イメージのロード時に、`mv_rewrite_staleness_second` がマテリアライズドビューのプロパティから復元されていなかった問題を修正しました。[#81263](https://github.com/StarRocks/starrocks/pull/81263)
- `DOUBLE` または `FLOAT` を `DECIMAL` にキャストした際、aarch64 と x86 で結果が異なっていた問題を修正しました。[#81261](https://github.com/StarRocks/starrocks/pull/81261)
- 結果が `DATETIME` の範囲を超える場合、`convert_tz` が NULL を返すようになりました。[#81264](https://github.com/StarRocks/starrocks/pull/81264)
- `VARBINARY` 列に対する等価フィルターでプランニングが失敗していた問題を修正しました。[#81310](https://github.com/StarRocks/starrocks/pull/81310)
- マテリアライズドビューの命名と検索について：参照先ベーステーブルのパーティション列が解析されていない場合に MV が命名されるようにし、`inspect_mv_refresh_info` が同名のベーステーブルを完全名でキー管理するようにしました。[#81357](https://github.com/StarRocks/starrocks/pull/81357) [#81358](https://github.com/StarRocks/starrocks/pull/81358)
- 共有なしモードにおいて、グローバル後期実体化の検索がコンピュートノードに到達できなかった問題を修正しました。[#81359](https://github.com/StarRocks/starrocks/pull/81359)
- `lastSchemaUpdateTime` のバンプが遅すぎたため、共有データ schema change 中に並行クエリが再プランニングされない問題を修正しました。[#81363](https://github.com/StarRocks/starrocks/pull/81363)
- Ranger のサービス定義で、ビューおよびマテリアライズドビューに対する `SELECT` の付与が許可されていなかった問題を修正しました。[#81400](https://github.com/StarRocks/starrocks/pull/81400)
- `UNION` のマージで値の順序が誤る可能性があった問題を修正しました。[#81420](https://github.com/StarRocks/starrocks/pull/81420)
- タスク実行プロセスの開始時刻が、Follower で再生されていなかった問題を修正しました。[#81433](https://github.com/StarRocks/starrocks/pull/81433)
- `IF EXISTS` の下で、ロード不可な外部テーブルに対して `DROP` 権限がチェックされるようになりました。[#81434](https://github.com/StarRocks/starrocks/pull/81434)
- クエリがキャンセルされた後、Lake セグメントが開かれなくなりました。[#81598](https://github.com/StarRocks/starrocks/pull/81598)
- リフレッシュによって行われたマテリアライズドビューの非アクティブ化がジャーナルに記録され、そのタスクがスケジュール済みのままになるようになりました。[#81660](https://github.com/StarRocks/starrocks/pull/81660)
- キープレフィックスのみが制約されている場合に、末尾の NULL を保持するパーティションが削除されていた問題を修正しました。[#81675](https://github.com/StarRocks/starrocks/pull/81675)
- 文字列 `IN` 述語が、ランタイムフィルターパスで辞書コードとして読み取られていました。[#81676](https://github.com/StarRocks/starrocks/pull/81676)
- 後期マテリアライズドビュープルーニングが、その述語に必要な列を保持していませんでした。[#81687](https://github.com/StarRocks/starrocks/pull/81687)
- 定数 `IN` 述語の値リストに列参照が含まれる可能性がありました。[#81825](https://github.com/StarRocks/starrocks/pull/81825)
- 定数の GROUP BY キーが畳み込まれた後、`HAVING` が正規化されていませんでした。[#81919](https://github.com/StarRocks/starrocks/pull/81919)
- 月単位の日付演算が、あたかも可逆であるかのように書き換えられていました。[#81979](https://github.com/StarRocks/starrocks/pull/81979)
- 保持期間が 0 の場合、マテリアライズドビューの強制削除が再帰していました。[#81953](https://github.com/StarRocks/starrocks/pull/81953)
- StarOS と StarCache を v4.1.6 にアップグレードしました。[#81508](https://github.com/StarRocks/starrocks/pull/81508)

## 4.1.7 {/* #417 */}

リリース日：2026年10月26日

### 動作変更 {/* #behavior-changes-1 */}

- ローカル（共有なし）主キーテーブルの INSERT は、デフォルトでは不変の L1 および L2 永続性インデックスファイルに対する重複キーのチェックを行わなくなりました。新しい BE 設定 `skip_pindex_l1_l2_check_in_insert` はデフォルトで `true` になります。L0（メムテーブル）のチェックは常に実行されます。以前の動作に戻すには、`false` に設定してください。[#81304](https://github.com/StarRocks/starrocks/pull/81304)
- BE の JVM メトリクスはデフォルトで有効になりました（`enable_jvm_metrics` のデフォルトが `true` になりました）。これらは JVM が存在する場合にのみ収集されるため、有効にしても、本来 JVM を必要としない BE 上で JVM が起動することはなくなりました。[#81113](https://github.com/StarRocks/starrocks/pull/81113) [#81074](https://github.com/StarRocks/starrocks/pull/81074)
- `spill_max_dir_bytes_ratio` のデフォルト値が `0.8` から `0.5` に引き下げられ、スピルは各スピルディレクトリの容量の最大 50% を使用するようになりました。[#80189](https://github.com/StarRocks/starrocks/pull/80189)
- Lake レプリケーションのファイルコピーは、現在は CN 全体専用のプールで実行されるようになりました。`lake_replication_file_copy_threads` のデフォルトは `0`（CPU コア数の 4 倍を意味していた）ではなく `16` になり、新しい `lake_replication_max_parallel_files_per_tablet`（デフォルト `4`）が tablet ごとに並行してコピーされるファイル数の上限を定めます。[#78078](https://github.com/StarRocks/starrocks/pull/78078)
- 共有データの主キーテーブルタブレットにおける削除トリガーのベースコンパクションは、新しい BE 設定 `lake_pk_compaction_base_enable_delete_trigger` によってオプトイン方式となり、デフォルトは `false` です。[#79926](https://github.com/StarRocks/starrocks/pull/79926)
- `pk_index_parallel_execution_threadpool_max_threads` が `0` の場合、主キーインデックスの並列実行プールは、CPU コア数の半分ではなく all の CPU コア数にサイズ設定されます。[#80460](https://github.com/StarRocks/starrocks/pull/80460)
- `CAST(x AS CHAR(N))` は、MySQL と同様に結果を `N` 文字に切り詰めるようになりました。FE での定数畳み込みと BE での実行の両方で、この切り詰めが適用されます。[#75909](https://github.com/StarRocks/starrocks/pull/75909)
- 構成やタスク定義が表示またはログ出力される箇所では、資格情報の値がマスクされます：`ADMIN SHOW FRONTEND CONFIG`、FE の `/variable` ページ、BE の `/varz`、`information_schema.be_configs`、`task_run_history` にアーカイブされたタスク実行定義、および FE と BE の HTTP ログに書き込まれるリクエスト URI とヘッダーです。`ADMIN SET FRONTEND CONFIG`、FE の `/api/_set_config` エンドポイント、BE の `/api/update_config` エンドポイントを通じて設定された値もマスクされます。[#80097](https://github.com/StarRocks/starrocks/pull/80097) [#80040](https://github.com/StarRocks/starrocks/pull/80040) [#80492](https://github.com/StarRocks/starrocks/pull/80492) [#80444](https://github.com/StarRocks/starrocks/pull/80444) [#80280](https://github.com/StarRocks/starrocks/pull/80280) [#80447](https://github.com/StarRocks/starrocks/pull/80447) [#80515](https://github.com/StarRocks/starrocks/pull/80515) [#80203](https://github.com/StarRocks/starrocks/pull/80203)
- FE の HTTP エンドポイント `/service_id`、`/dump`、`/dump_starmgr` は、管理者ユーザーを必要とするようになりました。[#80749](https://github.com/StarRocks/starrocks/pull/80749) [#80701](https://github.com/StarRocks/starrocks/pull/80701)

### 改善 {/* #improvements-1 */}

- テーブルプロパティ `enable_statistic_auto_collect` を追加しました。`CREATE TABLE` または `ALTER TABLE` を使用してこれを `false` に設定すると、テーブルを自動統計収集から除外できます。データロードによってトリガーされる収集には影響しません。[#81027](https://github.com/StarRocks/starrocks/pull/81027)
- 新しい FE 設定 `authentication_ldap_case_insensitive` を `true`（デフォルトは `false`）に設定することで、LDAP および Active Directory のユーザー名とグループ名を大文字小文字を区別せずにマッチングできます。[#79255](https://github.com/StarRocks/starrocks/pull/79255)
- グループプロバイダーのプロパティをその場で更新するための `ALTER GROUP PROVIDER` を追加しました。[#79166](https://github.com/StarRocks/starrocks/pull/79166)
- Query Queue V2 では、`query_queue_mem_used_pct_limit` がクエリを実行するすべてのノードに適用されます。[#81026](https://github.com/StarRocks/starrocks/pull/81026)
- パブリッシュ時の大規模な主キーインデックス削除が、並列逆引きとトゥームストーン sstable パスを使用することで高速化されました。[#76785](https://github.com/StarRocks/starrocks/pull/76785)
- 検索された `CASE WHEN` は、各分岐に到達する行に対してのみ評価されます。[#79484](https://github.com/StarRocks/starrocks/pull/79484)
- `convert_tz` は、すべての行でタイムゾーンを変換する代わりに、夏時間オフセットのルックアップをキャッシュするようになりました。[#78459](https://github.com/StarRocks/starrocks/pull/78459)
- `UNNEST` は null 許容配列に対してゼロコピーであり、`LEFT JOIN` を使用する場合、入力チャンク全体を実体化する代わりに一度に1つのチャンクで複数の配列を展開します。また、残りのテーブル関数はメモリ不足に対して保護されています。[#79221](https://github.com/StarRocks/starrocks/pull/79221) [#79222](https://github.com/StarRocks/starrocks/pull/79222)
- フラグメントのプランは、そのインスタンスリクエストの all に対して一度だけシリアライズされます。[#80716](https://github.com/StarRocks/starrocks/pull/80716)
- Colocate 集計関数とウィンドウ関数は、レンジコロケートテーブル上で計画できるようになりました。[#79946](https://github.com/StarRocks/starrocks/pull/79946)
- メインブランチから tablet 事前分割の改善をバックポートしました。これには、サンプルベースの tablet 事前分割がスキャンするデータ量を制限する新しい FE 設定（`tablet_pre_split_data_tier_scan_byte_limit`、`tablet_pre_split_data_tier_min_scan_files`、`tablet_pre_split_data_tier_max_scan_files`、`tablet_pre_split_meta_tier_footer_read_parallelism`）が含まれます。[#79949](https://github.com/StarRocks/starrocks/pull/79949)

### セキュリティ {/* #security-1 */}

- pprof を release/20261012 にバンプしました。[#80439](https://github.com/StarRocks/starrocks/pull/80439)
- lz4-java を 1.11.4 にバンプしました。[#80319](https://github.com/StarRocks/starrocks/pull/80319)
- BouncyCastle を 1.86、Jackson を 2.21.7、zstd-jni を 1.5.7-15、Parquet を 1.18.1 にバンプしました。[#80248](https://github.com/StarRocks/starrocks/pull/80248)
- Broker で使用される Tomcat を 9.0.121 にバンプしました。[#79719](https://github.com/StarRocks/starrocks/pull/79719)
- ZooKeeper を 3.8.7 および 3.9.6 にバンプしました。[#79707](https://github.com/StarRocks/starrocks/pull/79707)
- `lookup_string` の引数は SQL インジェクションを防ぐためにエスケープされ、その内部クエリは呼び出し元の ID で実行されます。[#79991](https://github.com/StarRocks/starrocks/pull/79991) [#79959](https://github.com/StarRocks/starrocks/pull/79959)
- FE の Web コンソールは、`/system` ページや `/query` ページを含め、管理者やホストが提供する値、およびテーブルセルを HTML エンコードし、`/proc_profile` 上の `node` パラメータを検証・エンコードします。[#80656](https://github.com/StarRocks/starrocks/pull/80656) [#80639](https://github.com/StarRocks/starrocks/pull/80639) [#80582](https://github.com/StarRocks/starrocks/pull/80582) [#80565](https://github.com/StarRocks/starrocks/pull/80565) [#80689](https://github.com/StarRocks/starrocks/pull/80689)

### バグ修正 {/* #bug-fixes-1 */}

以下の問題が修正されました：

- Iceberg の equality delete がパーティションに限定されていませんでした。[#81139](https://github.com/StarRocks/starrocks/pull/81139)
- 空き容量がある場合でも、datacache へのメモリ書き込みが eviction probability によって拒否されることがありました。[#80542](https://github.com/StarRocks/starrocks/pull/80542)
- バケットプロパティが、パイプラインソースのパーティションタイプとともに伝播されていませんでした。[#80099](https://github.com/StarRocks/starrocks/pull/80099)
- 送信側ですでに整列済みだった Bucket Shuffle Join の入力が、再度パーティション化されていました。[#79769](https://github.com/StarRocks/starrocks/pull/79769)
- ドライバーのパーティション化が、`EXCEPT`/`INTERSECT` とハッシュジョインの入力間で整合していませんでした。[#78920](https://github.com/StarRocks/starrocks/pull/78920)
- 不正な `str_to_date` 入力に対して `yearweek` が BE をクラッシュさせていました。[#78358](https://github.com/StarRocks/starrocks/pull/78358)
- spill channel がクローズ中であっても、キューに入れられた spill データタスクが実行され得ました。[#80252](https://github.com/StarRocks/starrocks/pull/80252)
- Lake compaction の読み取りバッファが、ワーカーごとのメモリ予算によって制限されていませんでした。[#80229](https://github.com/StarRocks/starrocks/pull/80229)
- 状態をラップできなかった Java UDAF のマージが、失敗する代わりに `NULL` を返していました。[#80098](https://github.com/StarRocks/starrocks/pull/80098)
- spill の追記またはパーティション分割に失敗した際、それが失われることがありました。[#79457](https://github.com/StarRocks/starrocks/pull/79457)
- `char` が符号なしであるプラットフォームにおいて、ORC `TINYINT` の最小／最大統計値が誤っていました。[#81086](https://github.com/StarRocks/starrocks/pull/81086)
- flat JSON のパス導出用トライが無制限になっていました。[#80316](https://github.com/StarRocks/starrocks/pull/80316)
- compaction が読み取りレイアウトを導出する際に、スカラーの flat JSON サブカラムがドロップされることがありました。[#80295](https://github.com/StarRocks/starrocks/pull/80295)
- scan predicate builder が唯一の conjunct を畳み込んだ際に、スキャン対象の列が出力されませんでした。[#80572](https://github.com/StarRocks/starrocks/pull/80572)
- `PSlotDescriptor.global_dict_words` が bytes として宣言されておらず、UTF-8 の警告が発生していました。[#78657](https://github.com/StarRocks/starrocks/pull/78657)
- プライマリインデックスキャッシュを `update_memory_limit_percent` でリサイズする際に、100 での除算が行われていませんでした。[#81067](https://github.com/StarRocks/starrocks/pull/81067)
- `TRUE` の透過モードにある MV が、ベーステーブルが削除された際に MV のスキャンを停止してしまいました。[#81088](https://github.com/StarRocks/starrocks/pull/81088)
- 範囲外のベクトル検索クエリベクトルが、拒否される代わりに BE を異常終了させていました。[#78658](https://github.com/StarRocks/starrocks/pull/78658)
- ANN の距離列が、再利用されたスキャンチャンクを許容していませんでした。[#78686](https://github.com/StarRocks/starrocks/pull/78686)
- flat JSON 列のルートに対する `json_query` が、ドキュメント全体を再構築していませんでした。[#80772](https://github.com/StarRocks/starrocks/pull/80772)
- `tokenize()` の結果のサイズが、行数ではなく viewer のデータ列によって決定されていました。[#80337](https://github.com/StarRocks/starrocks/pull/80337)
- MV の refresh がスケジュールしたタスクが、refresh によって MV が非アクティブ化された際に失われていました。これにより、planner lock および Hive `INSERT` の変更に関する 2 件の追加修正も行われています。[#81033](https://github.com/StarRocks/starrocks/pull/81033)
- `INSERT` が Hive のソース行を見逃すことがあり、また `SELECT *` がリソースマッピングの Hive 列をドロップしていました。[#81032](https://github.com/StarRocks/starrocks/pull/81032)
- スコープをまたぐ再帰 CTE の参照が、拒否される代わりにスタックオーバーフローを引き起こしていました。[#78533](https://github.com/StarRocks/starrocks/pull/78533)
- 壊れた Ranger の audit 設定により catalog アクセスが失敗し、また Solr audit に必要な ZooKeeper クラスが同梱されていませんでした。[#81075](https://github.com/StarRocks/starrocks/pull/81075)
- 参照先ではないベーステーブルに対するロックタイムアウトが発生しても MV の refresh が失敗しておらず、また `inspect_mv_refresh_info` が保持中のロック下でベーステーブルのロックを取得していました。[#81036](https://github.com/StarRocks/starrocks/pull/81036)
- `DATETIME` の値域外にある統計境界値が拒否されていませんでした。[#79388](https://github.com/StarRocks/starrocks/pull/79388)
- `INT64_MAX` のしきい値を超える JSON 数値の比較で例外がスローされていました。[#78969](https://github.com/StarRocks/starrocks/pull/78969)
- ある selection がその選択元の列より長くならないことを確認するアサーションが、失敗することがありました。[#80324](https://github.com/StarRocks/starrocks/pull/80324)
- 保持できないほど大きい `array_generate` の範囲が、拒否される代わりに BE を異常終了させていました。[#80205](https://github.com/StarRocks/starrocks/pull/80205)
- 定数の JSON/VARIANT から MAP へのキャストが、その入力と同じだけの期間維持されていませんでした。[#80679](https://github.com/StarRocks/starrocks/pull/80679)
- scalar shuttle が、複製すべき dict オペレーターを登録していませんでした。[#79310](https://github.com/StarRocks/starrocks/pull/79310)
- fast schema evolution の後に、グローバル辞書が無効化されていませんでした。[#78599](https://github.com/StarRocks/starrocks/pull/78599)
- CTE consume project において、繰り返し列が複製されていませんでした。[#80158](https://github.com/StarRocks/starrocks/pull/80158)
- 緩和された辞書が、符号なしバイト順でマージされていませんでした。[#78659](https://github.com/StarRocks/starrocks/pull/78659)
- 集計の struct フィールドが、`GROUP BY` のチェックによって拒否されていました。[#79649](https://github.com/StarRocks/starrocks/pull/79649)
- すべての MV の非アクティブ化とそのバージョンマップのクリアがジャーナルに記録されていたわけではありませんでした。[#81030](https://github.com/StarRocks/starrocks/pull/81030)
- 物理パーティション ID のルックアップにより、MV のバージョンマップ修復がスキップされていました。[#78469](https://github.com/StarRocks/starrocks/pull/78469)
- analytic の結果チャンクが公開される前に、ウィンドウ関数のエラーがチェックされていませんでした。[#81022](https://github.com/StarRocks/starrocks/pull/81022)
- MIN/MAX の LIST パーティション列が、データが保持していない可能性のあるパーティション値から読み取られることがありました。[#80558](https://github.com/StarRocks/starrocks/pull/80558)
- 1 つのテーブルで失敗した後、analyze ジョブが残りのテーブルの収集を停止していました。[#81028](https://github.com/StarRocks/starrocks/pull/81028)
- ベーステーブルのパーティション変動とバッチリフレッシュの状況下で、MV のリフレッシュと鮮度が不正になっていました。[#81034](https://github.com/StarRocks/starrocks/pull/81034)
- 3 つの小規模な既存の FE メタデータ並行処理の問題がありました。[#81037](https://github.com/StarRocks/starrocks/pull/81037)
- skew join v2 がシャッフルエクスチェンジを置き換えた際に、グローバル辞書の式が失われていました。[#78672](https://github.com/StarRocks/starrocks/pull/78672)
- 削除されたベースパーティションより後まで存続した MV パーティションがリフレッシュされていませんでした。[#79057](https://github.com/StarRocks/starrocks/pull/79057)
- アライメント済みメモリ割り当てに失敗した際に `std::bad_alloc` がスローされていませんでした。[#79054](https://github.com/StarRocks/starrocks/pull/79054)
- ALTER の完了レポートがジョブとマッチしておらず、キューに入った ALTER タスクが期限切れになっていました。[#81018](https://github.com/StarRocks/starrocks/pull/81018)
- スワップインされた一時パーティションの tablet が強制削除されていました。[#81014](https://github.com/StarRocks/starrocks/pull/81014)
- 残存する `INSERT` ジョブが、そのトランザクションに応じて完了またはキャンセルされていませんでした。[#80711](https://github.com/StarRocks/starrocks/pull/80711)
- インクリメンタル MV の複合型の行識別キーが拒否されていませんでした。[#79312](https://github.com/StarRocks/starrocks/pull/79312)
- `greplog` が年プレフィックス付きの glog エントリをサポートしていませんでした。[#80990](https://github.com/StarRocks/starrocks/pull/80990)
- アーカイブされた MV タスク実行履歴内のリストパーティション値が縮小されていませんでした。[#80944](https://github.com/StarRocks/starrocks/pull/80944)
- インクリメンタルに維持されるマテリアライズドビューに対して `auto_refresh_partitions_limit` が受け入れられていました。[#79051](https://github.com/StarRocks/starrocks/pull/79051)
- 読み取りが検証に失敗した際に、バンドルメタデータが再読み込みされていませんでした。[#79928](https://github.com/StarRocks/starrocks/pull/79928)
- コミット時にソースパーティションが存在しなくなった場合、`INSERT OVERWRITE` が明確なエラーを報告していませんでした。[#79902](https://github.com/StarRocks/starrocks/pull/79902)
- MV のバージョンマップのリセット、`TRUE` の透過的な書き換え、タイムライネスキャッシュが古い結果を返す可能性がありました。[#80893](https://github.com/StarRocks/starrocks/pull/80893)
- リストパーティションのドリフトがライブテーブルと比較されていませんでした。[#78470](https://github.com/StarRocks/starrocks/pull/78470)
- 軽量 Compaction でプライマリインデックスのサイズが保持されていませんでした。[#80746](https://github.com/StarRocks/starrocks/pull/80746)
- 固定された force 設定により、インクリメンタルに維持されるマテリアライズドビューが再マテリアライズされていました。[#79366](https://github.com/StarRocks/starrocks/pull/79366)
- インクリメンタルに維持されるマテリアライズドビュー上の列 `ALTER` が拒否されていませんでした。[#78726](https://github.com/StarRocks/starrocks/pull/78726)
- MV の `lastRefreshTime` が、リプレイ時にログに記録される代わりに再計算されていました。[#78467](https://github.com/StarRocks/starrocks/pull/78467)
- IVM リフレッシュのウィンドウが、コミットされるトランザクションに記録されていませんでした。[#79315](https://github.com/StarRocks/starrocks/pull/79315)
- V2 エンコードされた単一列固定長キーでプライマリキーのリカバリが失敗していました。[#80446](https://github.com/StarRocks/starrocks/pull/80446)
- cloud ネイティブのプライマリキー tablet において、競合する Compaction の all ではないファイルが孤児として記録されていました。[#80445](https://github.com/StarRocks/starrocks/pull/80445)
- 共有データ MV のベースパーティションに対して、FE ローカルのバージョン時刻が比較されていました。[#79945](https://github.com/StarRocks/starrocks/pull/79945)
- `MultiGet` が早期リターンした際に、インデックスブロックのイテレーターが解放されていませんでした。[#79380](https://github.com/StarRocks/starrocks/pull/79380)
- memtable が受け取っていない行が、その行数制限にカウントされていました。[#79705](https://github.com/StarRocks/starrocks/pull/79705)
- ネストされた Iceberg REST 名前空間が一覧表示中に削除された場合、エラーが発生していました。[#80846](https://github.com/StarRocks/starrocks/pull/80846)
- プランナーロックの修正によって残されたポリシー検索の繰り返しと無制限の待機がありました。[#80943](https://github.com/StarRocks/starrocks/pull/80943)
- tablet メタデータのダンプがローカルキャッシュに限定されていませんでした。[#78449](https://github.com/StarRocks/starrocks/pull/78449)
- リソースグループの更新をまたいでドライバーキューのカウンターが保持されていませんでした。[#80889](https://github.com/StarRocks/starrocks/pull/80889)
- 不正な形式の数値 HTTP パラメーターが BE をクラッシュさせる可能性がありました。[#79022](https://github.com/StarRocks/starrocks/pull/79022)
- `convert_hash_set_to_chunk` での読み取りオーバーフローがありました。[#80709](https://github.com/StarRocks/starrocks/pull/80709)
- `ALTER VIEW`、`INSERT OVERWRITE`、MV リフレッシュにおける既存のメタデータロック競合、および残っていたプランナーと MV メタデータロックのギャップがありました。[#80891](https://github.com/StarRocks/starrocks/pull/80891) [#80892](https://github.com/StarRocks/starrocks/pull/80892)
- lake レプリケーションにおけるファイルハンドルの再利用がありました。[#79359](https://github.com/StarRocks/starrocks/pull/79359)
- ブルームフィルター下でサポートされていない列タイプが受け入れられていました。[#80871](https://github.com/StarRocks/starrocks/pull/80871)
- ロードシンクのクローズがパイプラインポーラー上で実行されていました。[#80761](https://github.com/StarRocks/starrocks/pull/80761)
- 単一の収集クエリが失敗した際に、収集済みの外部統計情報が破棄されていました。[#79518](https://github.com/StarRocks/starrocks/pull/79518)
- BE Thrift RPC のリトライにジッターが適用されておらず、FE Thrift RPC のリトライがジッター付きのバックオフを行っていませんでした。[#80847](https://github.com/StarRocks/starrocks/pull/80847) [#80714](https://github.com/StarRocks/starrocks/pull/80714)
- パイプラインドライバーキューのレベルによって、レベル0が枯渇する可能性がありました。[#80760](https://github.com/StarRocks/starrocks/pull/80760)
- `INSERT ... SELECT` ソースのリフレッシュ時に、リソースマッピングされた外部テーブルが宣言通りに保持されていませんでした。[#80546](https://github.com/StarRocks/starrocks/pull/80546)
- 互いに依存する重い式がプッシュダウンされていました。[#80710](https://github.com/StarRocks/starrocks/pull/80710)
- brpc の future タイムアウト契約が破られていました。[#80783](https://github.com/StarRocks/starrocks/pull/80783)
- 解析済みの `INSERT` のソースがプランナーロックを取得する前にリフレッシュされておらず、ビューのベーステーブルがプラン作成で読み取られる前にロックされていませんでした。[#80786](https://github.com/StarRocks/starrocks/pull/80786) [#80784](https://github.com/StarRocks/starrocks/pull/80784)
- Iceberg REST の catalog がコネクタのシャットダウン時にクローズされておらず、Iceberg メタデータキャッシュのバックグラウンドエグゼキューターが catalog のシャットダウン時に停止していませんでした。[#80712](https://github.com/StarRocks/starrocks/pull/80712) [#80598](https://github.com/StarRocks/starrocks/pull/80598)
- `SHOW PROC` のジョブ詳細では、表示できない ALTER ジョブを拒否する代わりにキャストに失敗していました。[#80719](https://github.com/StarRocks/starrocks/pull/80719)
- rowset 移行時にベクトルインデックスファイルがコピーされていませんでした。[#80391](https://github.com/StarRocks/starrocks/pull/80391)
- `ADD ROLLUP` が失敗した際に、lake rollup の tablet が転置インデックスから削除されていませんでした。[#80717](https://github.com/StarRocks/starrocks/pull/80717)
- optimize ジョブおよび merge-partition ジョブの書き換えタスクが期限切れになっていませんでした。[#80718](https://github.com/StarRocks/starrocks/pull/80718)
- リモート読み取りに失敗した後、共有バッファが再利用される可能性がありました。[#79965](https://github.com/StarRocks/starrocks/pull/79965)
- 列ごとの最小値・最大値統計の収集がスロットリングされていませんでした。[#78544](https://github.com/StarRocks/starrocks/pull/78544)
- プールされたアイドル状態の FE Thrift コネクションが、置き換えられる代わりに再利用されていました。[#80715](https://github.com/StarRocks/starrocks/pull/80715)
- 可視状態の `DELETE` トランザクションが適用された際に、`hasDelete` が復元されていませんでした。[#80592](https://github.com/StarRocks/starrocks/pull/80592)
- decimal 型の `map_agg` キーの精度が失われていました。[#80673](https://github.com/StarRocks/starrocks/pull/80673)
- FE の brpc クライアントは、現在 `com.starrocks:jprotobuf-rpc-core` を使用するようになりました。[#80684](https://github.com/StarRocks/starrocks/pull/80684)
- トランザクション Stream Load 上の非ブール値の `partial_update` ヘッダーが拒否されていませんでした。[#79350](https://github.com/StarRocks/starrocks/pull/79350)
- 分割された lake スキャンにおいて、物理セグメント数および rowset 数が報告されていませんでした。[#80359](https://github.com/StarRocks/starrocks/pull/80359)
- すべての lake rollup ジョブで lake compaction がスキップされていませんでした。[#80655](https://github.com/StarRocks/starrocks/pull/80655)
- 主キーテーブルの compaction における行ソースのオーバーフロー。[#80472](https://github.com/StarRocks/starrocks/pull/80472)
- FE Web コンソールの `/system` ページが、生の proc パスを参照していませんでした。[#80648](https://github.com/StarRocks/starrocks/pull/80648)
- 統計 DML が内部コンテキストにバインドされていませんでした。[#80473](https://github.com/StarRocks/starrocks/pull/80473)
- インスタンス化前にドライバーの容量が確保されていませんでした。[#80495](https://github.com/StarRocks/starrocks/pull/80495)
- ブロックバッファリングが無効な場合、ピアキャッシュデータがコピーされていませんでした。[#80438](https://github.com/StarRocks/starrocks/pull/80438)
- パーティションに複数のインデックスがある場合、すべての create タスクがカウントされていませんでした。[#80640](https://github.com/StarRocks/starrocks/pull/80640)
- 主キーテーブルの並列実行スレッドに対して、実行時の自動リセットが機能していませんでした。[#80567](https://github.com/StarRocks/starrocks/pull/80567)
- イベントスケジューラーを用いたローカル交換において、sink のウェイクアップが失われていました。[#80547](https://github.com/StarRocks/starrocks/pull/80547)
- FE と範囲が一致しない tablet のマージおよび tablet の分割結果が拒否されていませんでした。[#80538](https://github.com/StarRocks/starrocks/pull/80538) [#80418](https://github.com/StarRocks/starrocks/pull/80418)
- パーティションが32ビットのバイナリオフセットを超えて成長した際に、ハッシュジョインがパーティション化された状態を維持していませんでした。[#80488](https://github.com/StarRocks/starrocks/pull/80488)
- バイパス書き込みトランザクションの開始タイムアウトヘッダーが秒単位として扱われていませんでした。[#80548](https://github.com/StarRocks/starrocks/pull/80548)
- FE メモリに保持される alter ジョブの履歴が無制限でした。[#80465](https://github.com/StarRocks/starrocks/pull/80465)
- 記録された NULL の拒否に基づいて、NULL 拡張された行を拒否していない場合でも外部ジョインが変換されており、また limit、top-N、または window の下にあるジョイン NULL の拒否が外部ジョインを変換していました。[#80510](https://github.com/StarRocks/starrocks/pull/80510) [#79974](https://github.com/StarRocks/starrocks/pull/79974)
- 列を持たない、または非決定的なジョイン述語がジョインの並べ替えによって削除されていました。[#80511](https://github.com/StarRocks/starrocks/pull/80511)
- BE がエンコードできない列タイプに対して `BITMAP` インデックスが受け入れられていました。[#80459](https://github.com/StarRocks/starrocks/pull/80459)
- 同じキーを繰り返すショートサーキットの `IN` リストが、行を複数回返していました。[#80318](https://github.com/StarRocks/starrocks/pull/80318)
- `enclose` または `escape` が設定されている場合、CSV ファイルがバイト範囲に分割されていました。[#80386](https://github.com/StarRocks/starrocks/pull/80386)
- 列ごとの ZSTD 圧縮辞書を読み取ることができませんでした。[#77355](https://github.com/StarRocks/starrocks/pull/77355)
- マルチテーブルトランザクション Stream Load のコミット失敗時のエラーが正しく報告されていませんでした。[#80351](https://github.com/StarRocks/starrocks/pull/80351)
- リクエスト URI が、アクションと同じクエリパーサーでサニタイズされていませんでした。[#80461](https://github.com/StarRocks/starrocks/pull/80461)
- ID を指定しない `CANCEL ALTER TABLE ROLLUP` では、all の rollup ジョブがキャンセルされていませんでした。[#80450](https://github.com/StarRocks/starrocks/pull/80450)
- クエリされた catalog から `information_schema.columns` が `LIMIT` で応答されませんでした。[#80448](https://github.com/StarRocks/starrocks/pull/80448)
- Paimon ブランチの状態がテーブルごとに分離されておらず、削除された Paimon パーティションがメタデータキャッシュから削除されていませんでした。[#79879](https://github.com/StarRocks/starrocks/pull/79879) [#79442](https://github.com/StarRocks/starrocks/pull/79442)
- レンジ分散の tablet が独自の初期メタデータで作成されていませんでした。[#80421](https://github.com/StarRocks/starrocks/pull/80421)
- `INSERT INTO FILES` を使用して CSV にアンロードする際、重複する出力列名が拒否されていました。[#79897](https://github.com/StarRocks/starrocks/pull/79897)
- `FULL OUTER JOIN ... USING` 列が結合キーで独自の型を保持していませんでした。[#79655](https://github.com/StarRocks/starrocks/pull/79655)
- コネクタ I/O が FE メタデータロック下で実行されていました。[#80416](https://github.com/StarRocks/starrocks/pull/80416)
- SQL ダイアレクトが Trino の場合に Iceberg のタイムトラベルが失われていました。[#79171](https://github.com/StarRocks/starrocks/pull/79171)
- Iceberg の decimal マニフェスト範囲が最小長の2の補数でエンコードされていませんでした。[#78456](https://github.com/StarRocks/starrocks/pull/78456)
- Parquet、ORC、CSV の読み取りパスにおけるデータ駆動のアロケーションがクエリのメモリ制限に制約されていませんでした。[#78573](https://github.com/StarRocks/starrocks/pull/78573)
- アンロードまたは失敗した統計情報が統計情報なしとしてキャッシュされていました。[#80236](https://github.com/StarRocks/starrocks/pull/80236)
- パーティションキーまたはソートキー列が生成列であるロードが事前分割されておらず、tablet の事前分割サンプルがロードのタイムゾーンで実行されていませんでした。[#80390](https://github.com/StarRocks/starrocks/pull/80390) [#80362](https://github.com/StarRocks/starrocks/pull/80362)
- 主キーの tablet のクリーンアップがエラー状態としてカウントされていました。[#79558](https://github.com/StarRocks/starrocks/pull/79558)
- FE の再起動後、または AD リファラルで LDAP グループプロバイダーが使用不能になっていました。[#80361](https://github.com/StarRocks/starrocks/pull/80361)
- インラインサービスアカウントキーを使用した GCS からの Broker Load が gcs-connector 3.x で失敗していました。[#78649](https://github.com/StarRocks/starrocks/pull/78649)
- 年0より前の datetime で週関数が中断していました。[#80228](https://github.com/StarRocks/starrocks/pull/80228)
- StarOS と starlet を v4.1.5 にアップグレードしました。[#80347](https://github.com/StarRocks/starrocks/pull/80347)
- 破棄時に MemSpaceMonitor スレッドが停止されず、BE の起動失敗時に SIGABRT が発生していました。[#79691](https://github.com/StarRocks/starrocks/pull/79691)
- コネクタクエリによってトリガーされた analyze が実行中のタスクをリークし、スケジューラーがデッドのまま残っていました。[#80329](https://github.com/StarRocks/starrocks/pull/80329)
- StarMgr からの応答なしで失敗した場合、`createShard` が一度もリプレイされていませんでした。[#80312](https://github.com/StarRocks/starrocks/pull/80312)
- ASOF ジョインの右側のフィルターがジョインより上位に保持されていませんでした。[#80303](https://github.com/StarRocks/starrocks/pull/80303)
- `NOT LIKE` に対する NGRAMBF プルーニングが不正確でした。[#79243](https://github.com/StarRocks/starrocks/pull/79243)
- ジョインおよびレンジ述語の基数推定が誤っていました。[#80242](https://github.com/StarRocks/starrocks/pull/80242)
- top-N 集計が分割された際に NULL グループが失われていました。[#80143](https://github.com/StarRocks/starrocks/pull/80143)
- 適応的なマテリアライズドビューのパーティションリフレッシュが cloud 内部テーブルをサポートしていませんでした。[#80270](https://github.com/StarRocks/starrocks/pull/80270)
- クリーンアップが必要なテーブルがない場合に `AnalyzeMgr.clearStalePartitionStats` が `IllegalStateException` をスローしていました。[#80265](https://github.com/StarRocks/starrocks/pull/80265)
- `ViewsSystemTable` がインテンシブなテーブルロックではなくデータベースの読み取りロックを保持していました。[#78344](https://github.com/StarRocks/starrocks/pull/78344)
- `DROP PARTITIONS WHERE` が条件に一致しない行を削除していました。[#79958](https://github.com/StarRocks/starrocks/pull/79958)
- Lake の `delete_tablets` の記帳が tablet とバージョンの合計に制約されていませんでした。[#80243](https://github.com/StarRocks/starrocks/pull/80243)
- 最後の送信者が終了中の `SinkBuffer` から離れた際、シンクのオブザーバーに通知されていませんでした。[#80155](https://github.com/StarRocks/starrocks/pull/80155)
- レガシーなマテリアライズドビュータスクの `postRun` がリフレッシュ中に無視されていませんでした。[#79943](https://github.com/StarRocks/starrocks/pull/79943)
- グループプロバイダーのファイル I/O が無制限であり、リプレイされたプロバイダーがウォームになるまでブリッジされていませんでした。[#79896](https://github.com/StarRocks/starrocks/pull/79896)
- Compaction が適用を待機している間に、主キーの tablet の `get_compaction_status` で BE がクラッシュしていました。[#80167](https://github.com/StarRocks/starrocks/pull/80167)
- キャッシュの退避時にファイルシステムが切断されていました。[#80087](https://github.com/StarRocks/starrocks/pull/80087)
- 0行のコミットがテーブル全体をロードされた行としてカウントしていました。[#80182](https://github.com/StarRocks/starrocks/pull/80182)
- バンドルされたステートメントと単独のステートメントが混在するマルチステートメントトランザクションが公開されていませんでした。[#79894](https://github.com/StarRocks/starrocks/pull/79894)
- 完了したトランザクションが `finalStatusTransactionStateDeque` に複数回エンキューされることがありました。[#79988](https://github.com/StarRocks/starrocks/pull/79988)
- スキーマの解決、推論、ライター、リーダーにおける Parquet のネスト型のエッジケース。[#80085](https://github.com/StarRocks/starrocks/pull/80085)
- 特殊文字を含む struct キャストの variant パス。[#74168](https://github.com/StarRocks/starrocks/pull/74168)
- null化されたグルーピングキーに対して、NULL を通過するランタイムフィルターが `RepeatNode` より下にプッシュされていました。[#80051](https://github.com/StarRocks/starrocks/pull/80051)
- 主キーの重複排除書き換えが他の集計を削除していました。[#79895](https://github.com/StarRocks/starrocks/pull/79895)
- Iceberg テーブルキャッシュのリフレッシュが、同時に発生した無効化を上書きしてしまうことがありました。[#79968](https://github.com/StarRocks/starrocks/pull/79968)
- `SET_VAR` がヒントされた `custom_query_id` が監査ログに記録されていませんでした。[#79871](https://github.com/StarRocks/starrocks/pull/79871)
- JSON の拡張カラムが、共有される catalog テーブルに登録されていました。[#78611](https://github.com/StarRocks/starrocks/pull/78611)
- `COM_STMT_CLOSE` でプリペアドステートメントがリークしていました。[#79860](https://github.com/StarRocks/starrocks/pull/79860)
- レンジコロケートのバケット割り当てが古くなったクエリが再プランされていませんでした。[#79927](https://github.com/StarRocks/starrocks/pull/79927)
- マルチキャスト交換が完了するまで、MV スピルの I/O が維持されていませんでした。[#79899](https://github.com/StarRocks/starrocks/pull/79899)
- セット演算または null-safe ジョインのレンジ子ノードの前にある exchange が削除されていました。[#79901](https://github.com/StarRocks/starrocks/pull/79901)
- イベントスケジューラがオフのとき、BE が `LookUpOperator` でクラッシュしていました。[#79711](https://github.com/StarRocks/starrocks/pull/79711)
- `sys.fe_memory_usage` が、収集する代わりに空のマップを読み込んでいました。[#79809](https://github.com/StarRocks/starrocks/pull/79809)
- スピルソートの完了が同期されておらず、終了エラーが保持されていませんでした。[#79521](https://github.com/StarRocks/starrocks/pull/79521)
- 圧縮された group-by キーのシリアライザが、null のみの定数を受け付けていませんでした。[#79499](https://github.com/StarRocks/starrocks/pull/79499)
- マテリアライズドビューの書き換えによって、プロジェクションに集計が残ってしまうことがありました。[#79611](https://github.com/StarRocks/starrocks/pull/79611)
- ADLS2 のアカウントスコープが保持されず、その認証情報も秘匿化されていませんでした。[#79526](https://github.com/StarRocks/starrocks/pull/79526)
- `QueryCumulativeCpuTime` と `QuerySpillBytes` が、フラグメントインスタンスごとに 1 回ずつカウントされていました。[#79774](https://github.com/StarRocks/starrocks/pull/79774)
- `SHOW` 文で `LIMIT` のオフセットが末尾を超えている場合に、空の結果が返されていませんでした。[#79764](https://github.com/StarRocks/starrocks/pull/79764)
- ロードのスピルマージタスクがキャンセルされたとき、終了コールバックに応答していませんでした。[#79360](https://github.com/StarRocks/starrocks/pull/79360)
- `SHOW ALTER TABLE OPTIMIZE` が、自身のカラムに対して `ORDER BY` を解決していませんでした。[#79687](https://github.com/StarRocks/starrocks/pull/79687)
- セキュリティ統合（LDAP）ユーザーが、MySQL 以外のチャネルで認証できませんでした。[#79165](https://github.com/StarRocks/starrocks/pull/79165)
- `EvHttpServer` がシャットダウン時に無関係なファイルディスクリプタをクローズしていました。[#79582](https://github.com/StarRocks/starrocks/pull/79582)
- proc ディレクトリの行の幅が、その行が応答となる箇所でチェックされていませんでした。[#79619](https://github.com/StarRocks/starrocks/pull/79619)
- 非正の `compact_threads` が拒否されず、CN をクラッシュさせていました。[#79641](https://github.com/StarRocks/starrocks/pull/79641)
- `TableKeeper` が `jdk.jshell` をインポートしていました。[#79520](https://github.com/StarRocks/starrocks/pull/79520)

## 4.1.6 {/* #416 */}

リリース日：2026 年 9 月 30 日

### 動作の変更 {/* #behavior-changes-2 */}

- AWS SDK のデフォルトの HTTP クライアントが Poco から SDK 自体のクライアントに戻されました： `enable_poco_client_for_aws_sdk` のデフォルトが `false` になりました。以前の動作に戻すには `true` に設定してください。[#78375](https://github.com/StarRocks/starrocks/pull/78375)
- オンラインでの optimize table はオプトイン方式になりました：FE の設定 `enable_online_optimize_table` のデフォルトが `false` になったため、この設定を有効にしない限り `ALTER TABLE ... DISTRIBUTED BY ...` は再び書き込みをブロックします。[#78206](https://github.com/StarRocks/starrocks/pull/78206)
- jemalloc の専用オーバーサイズアリーナがデフォルトで有効になったため、大きなアロケーションが呼び出し元スレッドの CPU ごとのアリーナに配置されなくなりました。これにより大きなバッファの再利用が向上し、断片化が削減される一方、BE と CN のメモリがアリーナ間でどのように分配されるかが変わります。[#78396](https://github.com/StarRocks/starrocks/pull/78396)
- 共有データの tablet メタデータおよびトランザクションログファイルは、デフォルトで Adler-32 チェックサム付きで書き込まれるようになり（`lake_enable_protobuf_file_checksum`）、読み取り時に破損を検出できるようになりました。リーダー側はファイルにチェックサムが付いていれば常に検証していたため、ダウングレード手順は不要です。[#75334](https://github.com/StarRocks/starrocks/pull/75334)
- 同じキーに対して `UPSERT` と `DELETE` の行が混在する共有データの主キーロードは、デフォルトでそれらのトランザクション内の順序を保持するようになりました（`lake_enable_pk_preserve_txn_delete_order`）。[#76211](https://github.com/StarRocks/starrocks/pull/76211)
- マルチテーブルトランザクションは、1 つずつトランザクションを公開するのではなく、共有データのバッチ公開に参加するようになり、CDC のマイクロバッチが多数のテーブルにファンアウトする際に発生していたシリアライズが解消されました。[#77029](https://github.com/StarRocks/starrocks/pull/77029)
- 共有データのパーティションは、各中間バージョンを経由するのではなく、バッチ公開の最終バージョンへ直接進むようになったため、クエリが tablet メタデータオブジェクトを持たない中間バージョンを要求することがなくなりました。これは以前 `The specified key does not exist` として表面化していました。[#79175](https://github.com/StarRocks/starrocks/pull/79175)
- 失敗し続ける共有データの公開処理は、試行のたびに `previous publish version task has not finished` メッセージを出力するのではなく、再試行のログ出力をバックオフしてスロットリングするようになりました。これは障害発生中に FE リーダーの CPU とログ量を圧迫する可能性がありました。[#79299](https://github.com/StarRocks/starrocks/pull/79299)
- Del-file のトゥームストーン行が `cloud_native_pk_index_rebuild_rows_threshold` にカウントされるようになったため、削除の多い共有データのワークロードで、本来想定されていた主キーインデックスの早期フラッシュがトリガーされるようになりました。[#76222](https://github.com/StarRocks/starrocks/pull/76222)
- 自身のセグメントを置き換える共有データのカラムアップサート公開は、削除ベクトルをマージするようになりました。以前は 1 つの主キーに対して 2 つの有効な行が残ってしまうことがありました。この問題は、CN の再起動時に永続性インデックスが再構築され `insert found duplicate key` が報告されたときになって初めて、ずっと後になって表面化していました。[#78797](https://github.com/StarRocks/starrocks/pull/78797)
- 共有データの主キーインデックスコンパクターが、フラッシュ側が sstable を生成し続けている間も静かに停止しなくなり、tablet の永続性インデックスが際限なく増大する問題が解消されました。[#78812](https://github.com/StarRocks/starrocks/pull/78812)
- HDFS ファイルは、クローズされる前に `hsync` されなくなりました。`hsync` は、別の変更によってすでに修正済みだったクラッシュに対する回避策であり、クローズのたびに各 DataNode レプリカに対して `fsync()` を強制していました。[#78864](https://github.com/StarRocks/starrocks/pull/78864)
- NULL を過小報告する Parquet の `null_count` が、デコードおよびプルーニング時にそのまま鵜呑みにされなくなりました。古いバージョンの parquet-mr で書き込まれたファイルでは、以前は値が破損したり、行が失われたりすることがありました。[#78329](https://github.com/StarRocks/starrocks/pull/78329)
- `!=`、`NOT IN`、`IS NOT NULL` のリストパーティションプルーニングが、値のリストに `NULL` が含まれているという理由だけで複数値パーティションを破棄することがなくなりました。このようなテーブルに対するクエリ（ジョインキーで `IS NOT NULL` を導出するジョインを含む）は、以前は失われていた非 null 行を返すようになりました。[#78946](https://github.com/StarRocks/starrocks/pull/78946)
- バッチ DDL で、`DATE` の有効な値である年 `0000` から始まる日付レンジパーティションを構築できるようになりました。このような文は、以前は `the range of the partition is invalid` で失敗していました。[#78746](https://github.com/StarRocks/starrocks/pull/78746)
- `max_partitions_in_one_batch` とパーティション数の上限は、現在のリクエストが作成しようとしているパーティションをカウントするようになりました。サイズ超過のバッチは、受け入れられた後に続くバッチが失敗するのではなく、事前に拒否されるようになりました。[#79379](https://github.com/StarRocks/starrocks/pull/79379)
- `TRUNCATE TABLE <external_catalog>.<db>.<table>` は、セッションの現在の catalog ではなく、ステートメント内で指定された catalog に対して `DELETE` 権限をチェックするようになりました。これまで許可されていたユーザーが拒否されるようになったり、その逆のケースが発生する可能性があります。[#79047](https://github.com/StarRocks/starrocks/pull/79047)
- ストリーミング集計のチューニングは、`HAVING` 後に残った行数ではなく、group-by の基数によって駆動されるようになりました。これにより、`HAVING` 句でほとんどのグループがフィルタリングされるクエリでも、ローカルの事前集計が失われなくなりました。[#79064](https://github.com/StarRocks/starrocks/pull/79064)
- 外部 Iceberg または Hive テーブル、`FILES()` テーブル関数を対象とした DML、および非同期マテリアライズドビューのリフレッシュについて、監査ログの行に `scanBytes`、`scanRows`、`returnRows`、`cpuCostNs`、`memCostBytes` が付与されるようになりました。これらのフィールドは以前は `0` を報告していました。[#74072](https://github.com/StarRocks/starrocks/pull/74072) [#74954](https://github.com/StarRocks/starrocks/pull/74954)
- クエリプロファイルと `EXPLAIN ANALYZE` は、最初の試行のプランではなく、再試行されたクエリが実際に実行したプランを報告するようになりました。[#79225](https://github.com/StarRocks/starrocks/pull/79225)
- Broker Load は、`enable_tablet_pre_split` の送信時点のスナップショットを、送信元クライアントの有効なセッション変数に書き戻さなくなりました。[#79177](https://github.com/StarRocks/starrocks/pull/79177)
- `ALTER MATERIALIZED VIEW ... ACTIVE` が非集計の増分マテリアライズドビューで再び動作するようになりました。ストレージが埋める `__ROW_ID__` 列は、マテリアライズドビューの DDL 列リストから除外されます。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- `SHOW ALTER TABLE OPTIMIZE` の `Operation` 列は、`com.starrocks.sql.ast.OptimizeClause@b5dc069` のような内部オブジェクトのアドレスではなく、optimize 操作の読みやすい説明を表示するようになりました。[#75948](https://github.com/StarRocks/starrocks/pull/75948)
- tablet スキーマに存在しない列があっても、述語のプッシュダウン時に BE が異常終了しなくなりました。該当の述語は単にプッシュダウンされません。[#79234](https://github.com/StarRocks/starrocks/pull/79234)
- `OlapTableSink` の初期化に失敗した場合、部分的に初期化されたシンクは完全なクローズ処理を経由しなくなりました。これにより、作成されていない状態を参照して BE が異常終了することがなくなりました。代わりに元の初期化エラーが返されます。[#79370](https://github.com/StarRocks/starrocks/pull/79370)
- ショートキーインデックスページのジオメトリは 32 ビットのオーバーフローなしに検証されるようになったため、破損したページフッターはラップしてヒープの範囲外書き込みを引き起こすのではなく、拒否されるようになりました。[#79048](https://github.com/StarRocks/starrocks/pull/79048)
- TLS ハンドシェイクがタイムアウトした HTTPS リクエストは、接続済みとして扱われ同じソケットで再試行されることがなくなりました。停止したリクエストは、`connect_timeout` に加えて 2 回の受信タイムアウトではなく、約 1 回のタイムアウト後に失敗するようになりました。[#79369](https://github.com/StarRocks/starrocks/pull/79369)

### 改善 {/* #improvements-2 */}

- LDAP および Active Directory のグループは、ユーザーの `memberOf` 属性から解決できるようになったため、新しいディレクトリグループでクラスター上に対応するグループプロバイダー設定が不要になりました。[#79254](https://github.com/StarRocks/starrocks/pull/79254)
- 監査ログは、大まかな `ErrorCode` だけでなく、クエリのエラーメッセージも記録するようになりました。これにより、構文エラー、不明なテーブル、権限エラーを `fe.log` に照会することなく区別できるようになりました。[#78798](https://github.com/StarRocks/starrocks/pull/78798)
- all のスピル可能なオペレーター間でスピルアクティビティを集計し、ローカルストレージとリモートストレージのバックエンド別に内訳を示す、サーバーレベルのスピルメトリクスを追加しました。[#71827](https://github.com/StarRocks/starrocks/pull/71827)
- Hudi のメタデータリーダーは、パーティションごとに再構築するのではなく、1 回のスキャンのパーティション間で再利用されるようになりました。これにより、Hudi メタデータテーブルが有効な場合、パーティション数の多いテーブルのプランニング時間が短縮されます。[#79367](https://github.com/StarRocks/starrocks/pull/79367)
- tablet の分割ポイントは、専用の完全なソートキーインデックスページではなく、セグメントの既存のインデックスから計算されるようになったため、一度も分割されないテーブルは、そのページの書き込みと保存のコストを負わなくなりました。[#79050](https://github.com/StarRocks/starrocks/pull/79050)
- `/memz` が jemalloc 独自の統計オプションを受け付けるようになり、アリーナごとの dirty および muzzy ページ数、エクステントのサイズクラス内訳を、実行中の BE から取得できるようになりました。[#79090](https://github.com/StarRocks/starrocks/pull/79090)

### セキュリティ {/* #security-2 */}

- [CVE-2026-55971] C++ バインディングの `THeaderTransport::untransform()` におけるヒープベースのバッファオーバーフローを修正するため、Apache Thrift を 0.24.0 にアップグレードしました。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-42537、CVE-2026-59646] JDBC URL インジェクションによる RCE を修正するため Apache Ranger を 2.9.0 に、DTLS リアセンブラーのバッファオーバーランを修正するため Bouncy Castle を 1.85 にアップグレードしました。[#79331](https://github.com/StarRocks/starrocks/pull/79331)
- [CVE-2026-71290] Apache HttpClient5 を 5.6.4 にアップグレードしました。[#79538](https://github.com/StarRocks/starrocks/pull/79538)

### バグ修正 {/* #bug-fixes-2 */}

以下の問題が修正されました：

- `SHOW PROFILELIST`、`ANALYZE PROFILE`、`get_query_profile()`、`/api/profile` は、キャッシュされたすべてのプロファイル（他のユーザーの SQL テキストとプランを含む）を、認証済みの任意のユーザーに返していました。新しい FE 設定 `authorization_enable_query_profile_access_check` により、キャッシュされたプロファイルはクエリを実行したユーザーと `SYSTEM OPERATE` の保持者に制限されるようになりました。[#79375](https://github.com/StarRocks/starrocks/pull/79375)
- 認可チェック、`INSERT` のプランニング、ビューの展開、権限チェック、および `inspect_mv_refresh_info` は、FE のメタデータロックを保持したまま外部 catalog とのラウンドトリップを行っていたため、メタストアが遅いと同じロックを待つすべての DDL が停止していました。これらの呼び出しは、ロックの外で行われるようになりました。[#79319](https://github.com/StarRocks/starrocks/pull/79319) [#79226](https://github.com/StarRocks/starrocks/pull/79226)
- gtid とパーティションバージョンのエポックが非 Leader の FE で生成される可能性があり、2 つのノードが同時に同じ番号を生成することがありました。[#78799](https://github.com/StarRocks/starrocks/pull/78799)
- `FULL OUTER JOIN ... USING (c1, c2, ...)` は誤った値を返していました。最初の `USING` 列は正しいものの、それ以降の列はすべて直前の列の値を引き継いでいました。同じクエリに `ORDER BY <ordinal>` を追加すると `IndexOutOfBoundsException` で失敗していました。[#79298](https://github.com/StarRocks/starrocks/pull/79298)
- `HAVING`、`GROUPING SETS`、`ROLLUP`、または `CUBE` がユニオンに書き換えられた際、`HAVING` 述語が失われ、本来フィルタリングされるはずの行がクエリに返されていました。[#79415](https://github.com/StarRocks/starrocks/pull/79415)
- `GROUP BY ROLLUP(a, b, a)`、および `CUBE` または `GROUPING SETS` が 1 つのセット内で同じ列を 2 回指定すると、同じキーを 2 回含むグルーピングセットが生成されていました。[#78261](https://github.com/StarRocks/starrocks/pull/78261)
- 集計キーテーブルの上の集計を取り除くと、クエリの `LIMIT` がストレージ層に到達する前に失われるため、BE がテーブル全体を読み込んでいました。[#79414](https://github.com/StarRocks/starrocks/pull/79414)
- `OR` ジョインをユニオンに分割すると、ユニオンの出力列と最初のブランチが列 ID でソートされる一方、他のブランチは projection map のイテレーション順に従っていたため、ブランチの整合性が取れていませんでした。[#79473](https://github.com/StarRocks/starrocks/pull/79473)
- スキュージョインの書き換えが outer join の保持側を複製していたため、1 つの入力行が複数回返され、結果が実行ごとに変動することがありました。また、その salt テーブルも `generate_series` の引数が誤った順序で渡されていたため、シリーズが何も出力しませんでした。[#79168](https://github.com/StarRocks/starrocks/pull/79168) [#79268](https://github.com/StarRocks/starrocks/pull/79268)
- `SELECT DISTINCT` は、`DISTINCT` が削除する列に対する `ORDER BY` を受け入れてしまい、ウィンドウの下での distinct のプッシュダウンにより、projection に削除済みの列が残っていました。[#77217](https://github.com/StarRocks/starrocks/pull/77217) [#77218](https://github.com/StarRocks/starrocks/pull/77218)
- `bitmap_union_count()` のような、常に非 null となる集計に対するウィンドウが、宣言された null 許容性を失っていたため、その上での `DISTINCT` または `GROUP BY` が `error nullablel column` で失敗していました。[#79009](https://github.com/StarRocks/starrocks/pull/79009)
- `percentile_cont` のマージが、消費完了を位置ではなく値によって追跡していたため、BE が異常終了することがありました。[#77261](https://github.com/StarRocks/starrocks/pull/77261)
- `count(DISTINCT percentile_hash(...))`、および `PERCENTILE` 値に対する `DISTINCT` 集計は BE をクラッシュさせていました。このような呼び出しは現在拒否されます。[#78795](https://github.com/StarRocks/starrocks/pull/78795)
- スカラーの agg-state コンビネーターは、null のメモリプールでクラッシュし、ネストされた関数コンテキストで競合状態が発生していました。[#76910](https://github.com/StarRocks/starrocks/pull/76910)
- 集計テーブル上の JSON サブフィールド集計が、生の `NullPointerException` で失敗していました。これは、合成されたサブフィールド列に集計タイプがないためです。[#77952](https://github.com/StarRocks/starrocks/pull/77952)
- ストリーミング事前集計は、`enable_spill = true` の下でメモリ不足になる可能性がありました。これは、メモリ予算の上限が一度固定されると、その後見直されることがなかったためです。[#76702](https://github.com/StarRocks/starrocks/pull/76702)
- ビルドパーティションを復元中にキャンセルされたスピルハッシュジョインが BE をクラッシュさせていました。これは、プローブオペレーターがスピル I/O タスクの完了を待たずにクローズしていたためです。[#79085](https://github.com/StarRocks/starrocks/pull/79085)
- ローカルパススルー exchange は、レシーバーがキャンセルされた後もチャンクを追加し続けており、シンクとレシーバーが所有権を共有していなかったため、そのチャネルが解放後に使用される可能性がありました。シンクの準備通知も、シンクの observable を選択しながらソースイベントを発行していたため、`OUTPUT_FULL` でブロックされているドライバーがブロックされたままになることがありました。[#76603](https://github.com/StarRocks/starrocks/pull/76603) [#77088](https://github.com/StarRocks/starrocks/pull/77088) [#76782](https://github.com/StarRocks/starrocks/pull/76782)
- Query Cache は tablet ごとの不完全な結果を保存し、それを後続のクエリに提供する可能性がありました。また、ASOF ジョインを含むフラグメントは、時間条件がダイジェストに到達していなかったにもかかわらずキャッシュされていました。[#77066](https://github.com/StarRocks/starrocks/pull/77066) [#77563](https://github.com/StarRocks/starrocks/pull/77563)
- 両辺を比較しない時間条件を持つ ASOF ジョインが BE をクラッシュさせていました。このような条件は現在拒否されるようになっています。[#79049](https://github.com/StarRocks/starrocks/pull/79049)
- 親タプルを持たないネストされたラムダスロットが、ランタイムフィルターの構築時に TopN ノードのタプルセットによってバインドされていると誤って扱われていました。また、order-by ヒントが、重い式としてスキャンにプッシュされたカラムを持たないスロットを参照外ししていました。[#78050](https://github.com/StarRocks/starrocks/pull/78050) [#78527](https://github.com/StarRocks/starrocks/pull/78527)
- ネストされた `array_map` と `array_sort` のラムダは、`Input dependency cols check failed` または `Expr evaluate meet error: slot_id N not found` を使用した場合にプランニングまたは評価に失敗していました。これは、内側のラムダによって引き上げられた共通部分式が、それを定義したスコープを抜け出してしまっていたためです。[#78645](https://github.com/StarRocks/starrocks/pull/78645) [#78769](https://github.com/StarRocks/starrocks/pull/78769) [#78915](https://github.com/StarRocks/starrocks/pull/78915)
- `array_sort` のコンパレータ検証は、ドライバー間で 1 つのエラー `Status` を共有していたため、二重解放される可能性がありました。[#78314](https://github.com/StarRocks/starrocks/pull/78314)
- `array_contains(NULL, [1, 2])` と `array_position(NULL, [1, 2])` が `class com.starrocks.type.NullType cannot be cast to class com.starrocks.type.ArrayType` で失敗していました。[#76970](https://github.com/StarRocks/starrocks/pull/76970)
- `__iceberg_transform_truncate` または `__iceberg_transform_bucket` を解析すると、共有されているビルトイン関数オブジェクトがその場で変更され、ワイルドカードの decimal シグネチャに最初に解析されたクエリの精度とスケールが刻印されていました。[#76777](https://github.com/StarRocks/starrocks/pull/76777)
- 引数リストが空の `PARTITION BY RANGE(<partition-function>())` は、パースエラーではなく生の `IndexOutOfBoundsException` を発生させていました。[#75080](https://github.com/StarRocks/starrocks/pull/75080)
- 範囲述語は、その式が順序を保持するかどうか、またどの方向に保持するかにかかわらずパーティション式にマッピングされていたため、レンジパーティション化とリストパーティション化のプルーニングで、一致する行を保持するパーティションが削除される可能性がありました。また、`NULL` のパーティションキーは、厳密にその値未満ではなく、列のタイプの最小値で検索されていました。`DATE` または `DATETIME` をテキストにキャストする保持条件は、再び受け入れられるようになりました。[#78024](https://github.com/StarRocks/starrocks/pull/78024) [#79111](https://github.com/StarRocks/starrocks/pull/79111) [#79187](https://github.com/StarRocks/starrocks/pull/79187) [#79257](https://github.com/StarRocks/starrocks/pull/79257) [#79688](https://github.com/StarRocks/starrocks/pull/79688)
- Iceberg 述語コンバーターが無条件に `CastOperator` を取り除いていたため、`DECIMAL(5,2)` カラムに対する `WHERE CAST(d AS BIGINT) = 10` が、`10.50` を保持するファイルをプルーニングしてしまう可能性がありました。[#78351](https://github.com/StarRocks/starrocks/pull/78351)
- Iceberg ベーステーブル上のインクリメンタルマテリアライズドビューが、定期的な `rewrite_data_files` Compaction によって恒久的に非アクティブ化されていました。これは、デルタの境界がスキップされた `REPLACE` スナップショットへと進んでしまっていたためです。[#76950](https://github.com/StarRocks/starrocks/pull/76950)
- カラムの min/max 統計情報のキャッシュは、パーティションまたはスキーマの DDL によって無効化されなかったため、古いエントリが無期限に提供される可能性がありました。[#77441](https://github.com/StarRocks/starrocks/pull/77441)
- メタストアまたは AWS Glue がカラム統計情報の読み取り権限を拒否した場合、Hive 系のカタログは完全に失敗していましたが、現在は空の統計情報にデグレードするようになりました。また、Glue のカラム統計情報リクエストに `catalogId` が付与されるようになったため、クロスアカウントのデプロイメントでは設定された catalog から統計情報が読み取られます。[#78455](https://github.com/StarRocks/starrocks/pull/78455) [#78461](https://github.com/StarRocks/starrocks/pull/78461)
- Elasticsearch 外部テーブルおよび Elasticsearch カタログは、バージョン 8 または 9 を実行しているクラスタを `Unsupported/Unknown ES Cluster version` で拒否していました。また、同時実行クエリが互いのリクエスト URL パラメータを上書きすることがあり、これが `Column 'id' cannot be resolved` のような無関係な分析エラーとして現れていました。[#78267](https://github.com/StarRocks/starrocks/pull/78267) [#79391](https://github.com/StarRocks/starrocks/pull/79391)
- Paimon テーブルの `DECIMAL` カラムに対する述語は、カラムの Paimon `DecimalType` ではなく StarRocks タイプファミリーのデフォルトの精度とスケールを使用して変換されていたため、そのリテラルが値を表現できませんでした。[#78760](https://github.com/StarRocks/starrocks/pull/78760)
- エンドポイントが完全な URL として設定されていた場合、Azure Blob の資格情報の検索が失敗していました。これは、Hadoop の設定キーにスキームが埋め込まれていたためです。また、ADLS2 ストレージボリュームはシリアル化時に Azure Workload Identity のトークンファイルプロパティを失っていたため、ボリュームの再読み込み時に Managed Identity にフォールバックし、`Identity not found` で失敗していました。[#78241](https://github.com/StarRocks/starrocks/pull/78241) [#79444](https://github.com/StarRocks/starrocks/pull/79444)
- `'[0].value.url'` のような配列インデックスを持つ JSON パスは、配列がそのインデックスより短いすべての行に対して C++ 例外をスローしており、これが大規模スキャンをスタックアンワインダー内のロックコンボイに変えてしまっていました。現在、このインデックスは境界チェックされるようになっています。[#78569](https://github.com/StarRocks/starrocks/pull/78569)
- `timestampdiff(MONTH, ...)`、`timestampdiff(YEAR, ...)`、`months_diff`、`years_diff` は、両方の引数が同じ月または年に収まり、かつ 2 番目の引数の方が早い場合に、`0` ではなく `-1` を返していました。[#79037](https://github.com/StarRocks/starrocks/pull/79037)
- `CAST(<varbinary constant> AS VARCHAR)` の定数畳み込みは、値ではなく `[B@233c25e5` のような、バッキングするバイト配列の Java アイデンティティハッシュコードを返していました。[#77718](https://github.com/StarRocks/starrocks/pull/77718)
- `DICTIONARY_GET` は、入力カラムのキャッシュされた `has_null` フラグが保守的に true のままであった場合、非 NULL キーを NULL として拒否していました。[#76881](https://github.com/StarRocks/starrocks/pull/76881)
- 書き換え機がデコードすることのないネストループジョインの `ON` カラムに低基数ディクショナリが保持され続けていました。また、独自のコードを持つ派生ディクショナリを介してディクショナリ式が畳み込まれてしまい、「ローリング低基数」カラムがグローバル辞書を繰り返し無効化してテーブル全体の再収集を強制していました。[#78229](https://github.com/StarRocks/starrocks/pull/78229) [#78633](https://github.com/StarRocks/starrocks/pull/78633) [#78486](https://github.com/StarRocks/starrocks/pull/78486)
- `DECIMAL256` リテラルはその文字列表現からハッシュ化されていましたが、BE は生の 32 バイト値から行をバケット化するため、`DECIMAL256` カラムで分散されたテーブルに対するポイントルックアップが誤ったバケットに送信されていました。[#78495](https://github.com/StarRocks/starrocks/pull/78495)
- OLAP スキャンは、そのソートキーカラムをカラム ID ではなくカラム名で名付けていたため、名前が変更されたカラムは tablet スキーマと一致しなくなっていました。[#77967](https://github.com/StarRocks/starrocks/pull/77967)
- `PruneComplexTypeUtil` は、説明できない単一の式に遭遇すると、ステートメント内のすべてのカラムに対して複合タイプのサブフィールドプルーニングを無効化していました。現在、このバイルアウトは原因となった式の範囲に限定され、述語式の再利用によってスキャンから引き出されたデッドなサブフィールドスロットは削除されるようになりました。[#79181](https://github.com/StarRocks/starrocks/pull/79181) [#77456](https://github.com/StarRocks/starrocks/pull/77456)
- `AstToSQLBuilder` は `SELECT * EXCLUDE` リストのカラムを二重引用符で出力していましたが、これは文法上どこにも許容されていなかったため、再生成された SQL がパースできなくなっていました。[#78052](https://github.com/StarRocks/starrocks/pull/78052)
- 集計テーブルに対する `ALTER TABLE ... ADD COLUMN` は、集計関数も `KEY` キーワードも指定されていない場合、黙ってキーカラムを作成しており、テーブルが集計する粒度を変更してしまっていました。このようなステートメントは現在、曖昧であるとして拒否されます。[#78359](https://github.com/StarRocks/starrocks/pull/78359)
- `JSON` のように BE のキーコーダーを持たないタイプのカラムがソートキーとして使用可能になっており、BE をクラッシュさせていました。このような定義は現在、all の各分散タイプにおいて `CREATE TABLE` および `ALTER TABLE` で拒否されるようになっています。[#76795](https://github.com/StarRocks/starrocks/pull/76795)
- `USING GIN("IMP_LIB" = "builtin")` は `CREATE TABLE` によって受け入れられていましたが、その後 `Can not get inverted imp type` を使ったすべての `INSERT` を壊していました。これは、プロパティキーが元の大文字小文字のまま BE に到達していたためです。[#77818](https://github.com/StarRocks/starrocks/pull/77818)
- `gram_num` プロパティを持たずに永続化されたレガシーな `NGRAMBF` インデックスにより、セグメントを構築する際に BE が UTF-8 オフセットベクターを読み越してしまい、CN がクラッシュしていました。[#76989](https://github.com/StarRocks/starrocks/pull/76989)
- プッシュダウンされた述語がスパースな範囲を読み取る際、null フラグが序数ではなく線形にインデックス化されていたため、ギャップの後のすべての行がスキップされた行の null フラグを取得していました。また、プッシュダウンされたフィルターがディクショナリ全体を拒否した場合、ディクショナリページデコーダーが進行せず、BE が中断していました。[#78389](https://github.com/StarRocks/starrocks/pull/78389) [#79007](https://github.com/StarRocks/starrocks/pull/79007)
- 子式のエラーが伝播されずに BE を中断させていました。[#77442](https://github.com/StarRocks/starrocks/pull/77442)
- `ThreadPool` はデフォルトでタスクの例外を握りつぶし、そのタスクを完了済みとしてカウントしていました。[#76863](https://github.com/StarRocks/starrocks/pull/76863)
- Jaeger トレーシングが有効な場合、`OlapTableSink` によるロードのキャンセルが、SIGSEGV で BE または CN をクラッシュさせていました。[#76869](https://github.com/StarRocks/starrocks/pull/76869)
- `AsyncFlushOutputStream::close()` はストリーム自身のミューテックスを保持したままプロミスを満たしていたため、ウェイターがストリームを破棄した後に I/O スレッドが解放済みメモリ上の futex を起床させていました。[#79182](https://github.com/StarRocks/starrocks/pull/79182)
- `shutdown_staros_worker()` がグローバルワーカーを解放した後に、実行中のロードが StarOS バックエンドのファイルシステムに到達すると、CN がグレースフルシャットダウン中に SIGSEGV でクラッシュする可能性がありました。[#79058](https://github.com/StarRocks/starrocks/pull/79058)
- 並列コンパクションが brpc の bthread 上でファイルシステム I/O を実行していたため、CN が `Resource deadlock avoided` で中断していました。[#76925](https://github.com/StarRocks/starrocks/pull/76925)
- ベクトルインデックスの rowset が負の `data_disk_size` を報告した場合、Compaction が SIGFPE で中断していました。[#78959](https://github.com/StarRocks/starrocks/pull/78959)
- lake トランザクションログの読み取りが破損にヒットするたびに、破損したローカルキャッシュのコピーが再読み込みされていました。また、主キーインデックスの sstable ブロック読み取りでは、ライターが常に生成していたはずの crc32c ブロックトレーラーが一度も検証されていませんでした。[#78733](https://github.com/StarRocks/starrocks/pull/78733) [#77479](https://github.com/StarRocks/starrocks/pull/77479)
- 部分更新の書き換えがコピーに退化した際、ソースの暗号化メタデータが失われ、どのリーダーも開くことができず、Compaction による書き換えもできないセグメントが残ってしまいました。[#79053](https://github.com/StarRocks/starrocks/pull/79053)
- 共有データの schema change が、データを一切参照しない rowset を記録してしまうことがあり、その後そのパーティションに対するすべての `MERGE_TABLET` が `tablet merge source has an empty ordinary rowset` で失敗していました。[#79167](https://github.com/StarRocks/starrocks/pull/79167)
- レンジ・コロケートの分割境界がシリアライズされた protobuf バイト列として比較されていましたが、FE と BE はこれを同一のエンコーディングで扱っていませんでした。[#76968](https://github.com/StarRocks/starrocks/pull/76968)
- 不安定なレンジ・コロケートグループのメンバーに対する単純な単一テーブルクエリが `range colocate group ... is in an unaligned state` で失敗していました。[#76971](https://github.com/StarRocks/starrocks/pull/76971)
- ハッシュ・コロケート共有データテーブルの新しい物理パーティションのシャードがコロケーション配置情報なしで作成されてしまい、後でマイグレーションが必要になっていました。[#79223](https://github.com/StarRocks/starrocks/pull/79223)
- FE が、ドロップされた tablet のテーブルがレンジ分散されていることを BE に伝えていなかったため、reshard 後にファイルがリークしていました。[#76861](https://github.com/StarRocks/starrocks/pull/76861)
- reshard ジョブの更新をリプレイしても reshard 対象の tablet が更新されなかったため、FE の Leader 変更後、パーティションの publish が、一度も書き込まれたことのない tablet メタデータオブジェクトに対して永久に 404 を返し続けることがありました。また、reshard ジョブのリプレイが、テーブルがすでにドロップされている場合でも編集ログへの書き込みを行ってしまい、reshard の publish リトライがペース調整されずに空回りすることもありました。[#79426](https://github.com/StarRocks/starrocks/pull/79426) [#79485](https://github.com/StarRocks/starrocks/pull/79485) [#77691](https://github.com/StarRocks/starrocks/pull/77691)
- 共有データのパーティションバージョンが、変更操作がジャーナルに記録される前にメモリ上で変更されていたため、リプレイされたバージョンが後退することがありました。現在はジャーナルから導出されるようになり、常に前進のみとなります。[#79296](https://github.com/StarRocks/starrocks/pull/79296)
- 対象の tablet がレプリカなしと報告した際、レプリケーションジョブがゼロ除算を起こしていました。[#79329](https://github.com/StarRocks/starrocks/pull/79329)
- DataCache のキーハッシュがランタイムのディスパッチテーブルから取得されていたため、そのディスパッチに変更があると、アップグレードやロールバックの際にすべてのキャッシュ済みブロックが孤立してしまっていました。現在は MurmurHash3 に固定されています。[#79262](https://github.com/StarRocks/starrocks/pull/79262)
- FE のスキャン範囲ヒープ安全性チェックが、スキャンノードごとに 1 回ではなく物理パーティションごとに 1 回実行されていたため、物理パーティション数に対して二次的な計算量になっていました。[#76978](https://github.com/StarRocks/starrocks/pull/76978)
- 遅延ロードされたインデックスによってセグメントのメモリ使用量が増加した後も、rowset メタデータキャッシュのエントリが初期チャージを保持し続けていたため、キャッシュが自身のフットプリントを過小報告していました。[#77893](https://github.com/StarRocks/starrocks/pull/77893)
- `Tablet::get_average_row_size()` が、マップが宣言されている契約に反して `_meta_lock` を保持せずに `_rs_version_map` を走査していました。[#79250](https://github.com/StarRocks/starrocks/pull/79250)
- FE の thrift accept ループが、ワーカープールの飽和により数分間停止してしまい、ワーカーは呼び出し元がすでにいなくなってから長時間キューに入っていた接続を処理し続けていました。[#78838](https://github.com/StarRocks/starrocks/pull/78838) [#79173](https://github.com/StarRocks/starrocks/pull/79173)
- ジャーナルアプライアが停止している非 Leader の FE が、転送を行わずにローカルで古いメタデータを提供し続けていました。これは、レディネス判定がリプレイサイクルの完了後にしか得られなかったためです。[#79041](https://github.com/StarRocks/starrocks/pull/79041)
- `getLoads` RPC は、すべてのロードレコードを 1 つのメッセージで返していたため、ビジーなクラスタでは Leader のログが `FE RPC failure ... MaxMessageSize reached` で埋め尽くされ、`loads_history` のアーカイブがロールバックして行を永久に失うことがありました。この RPC は現在、ジョブ ID によってページングされるようになりました。[#78898](https://github.com/StarRocks/starrocks/pull/78898)
- `task_runs` が読み取られている間に、保留キュー、実行中マップ、メモリ内履歴、アーカイブ済み履歴の間を移動するタスク実行が、二重に報告されることがありました。[#79438](https://github.com/StarRocks/starrocks/pull/79438)
- `SHOW ALTER TABLE ROLLUP` と `SHOW ALTER MATERIALIZED VIEW` は `WHERE`、`ORDER BY`、`LIMIT` を受け付けていましたが、実際には無視していました。これらを適用していたのは `SHOW ALTER TABLE COLUMN` のみでした。また、`FinishedTime` は `SHOW ALTER TABLE ROLLUP` 述語でも受け付けられるようになりました。[#79281](https://github.com/StarRocks/starrocks/pull/79281) [#79498](https://github.com/StarRocks/starrocks/pull/79498)
- SQL のブラックリストを読み込む際、不変のスナップショットがルールごとに再構築・ソートされていたため、ルール数に対して二次的な計算量となり、FE の起動およびチェックポイントのたびに実行されていました。[#78752](https://github.com/StarRocks/starrocks/pull/78752)
- `AsyncTaskQueue` は、コンシューマスレッドが割り込まれた後、`getOutputs()` と `tryGetOutputs()` の間でビジースピンしていました。これは、割り込みが `hasMoreOutput` をクリアせずに握りつぶされていたためです。[#79297](https://github.com/StarRocks/starrocks/pull/79297)
- `sys_log_level` のランタイム変更が設定には書き込まれていたものの、実際には適用されていませんでした。この項目は可変であるとドキュメントに記載されているにもかかわらずです。[#79200](https://github.com/StarRocks/starrocks/pull/79200)
- `has_enable_heap_profile()` が、ブール値の jemalloc ノードである `prof.active` を `int` として読み取っていたため、常にヒーププロファイリングが無効であると報告していました。[#78820](https://github.com/StarRocks/starrocks/pull/78820)
- スクリプトの外部オブジェクトに対する `System.print()` が、オブジェクトのテキストではなく `instance of <Class>` を出力していました。これは、`toString` バインディングがゲッターではなくメソッドとして登録されていたためです。[#78877](https://github.com/StarRocks/starrocks/pull/78877)
- 複数テーブルトランザクションの Stream Load インターフェースを介して式に基づくパーティション化テーブルへロードする際、BE がパーティションを作成する必要がある場合には常に、コミット時に `automatic create partition failed. error: txn ... not exist` で失敗していました。[#78790](https://github.com/StarRocks/starrocks/pull/78790)
- 結合トランザクションログモードでは、自身のストリーム終端が早く到着したコーディネーターが、ロードがキャンセルされた際にトランザクションログコレクターを無期限に待機してしまうことがありました。[#79389](https://github.com/StarRocks/starrocks/pull/79389)
- 完了済みの merge-commit ロードが、`information_schema.loads` においてマージウィンドウの進捗を完全な状態より少なく報告することがありました。[#78723](https://github.com/StarRocks/starrocks/pull/78723)
- スキャンの `SampleTime` カウンタが、経過時間ではなくブロック数を報告していました。[#78732](https://github.com/StarRocks/starrocks/pull/78732)
- LZ4 および ZSTD の Arrow IPC コーデックが、Arrow Flight SQL サービス用の FE クラスパスから欠落していました。[#76921](https://github.com/StarRocks/starrocks/pull/76921)
- StarOS、starlet、および starcache を v4.1.4 にアップグレードしました。[#79429](https://github.com/StarRocks/starrocks/pull/79429)

## 4.1.5 {/* #415 */}

リリース日：2026 年 9 月 8 日

### 改善 {/* #improvements-3 */}

- 外部テーブルに対して `ANALYZE SAMPLE TABLE PREDICATE COLUMNS` をサポートしました。[#77243](https://github.com/StarRocks/starrocks/pull/77243)
- サンプルベースの tablet 事前分割を拡張し、キー以外の式、動的上書き一時パーティション、および Iceberg と上書きロードをサポートしました。[#77843](https://github.com/StarRocks/starrocks/pull/77843) [#77842](https://github.com/StarRocks/starrocks/pull/77842) [#77983](https://github.com/StarRocks/starrocks/pull/77983)
- lake の列更新向けに、ソースセグメントをストリーム処理することでメモリ使用量を削減しました。[#77275](https://github.com/StarRocks/starrocks/pull/77275)
- Paimon カタログのバックグラウンドメタデータ更新は、サイクルごとにウェアハウス全体を列挙するのではなく、最近アクセスされ変更されたテーブルのみを更新するようになりました。[#78057](https://github.com/StarRocks/starrocks/pull/78057)
- パーティションプルーニングの際、Paimon の `getRemoteFiles` 呼び出しに `limit` を渡すようになりました。[#76943](https://github.com/StarRocks/starrocks/pull/76943)
- GROUPING の SETS プッシュダウンを拡張し、`AVG` と `COUNT` をサポートしました。[#77832](https://github.com/StarRocks/starrocks/pull/77832)
- クエリプロファイルに `EXPLAIN COSTS` の出力を含められるようになりました。[#73005](https://github.com/StarRocks/starrocks/pull/73005)
- starlet のオブジェクトストレージアップロードしきい値を実行時に設定可能にしました。[#78448](https://github.com/StarRocks/starrocks/pull/78448)
- ロック競合を削減：lake セグメントのグローバルメタキャッシュロックを削除し、SQL のブラックリスト検証をロックフリーにしました。[#78260](https://github.com/StarRocks/starrocks/pull/78260) [#78476](https://github.com/StarRocks/starrocks/pull/78476)
- Compaction およびレプリケーションアーカイブ時に `delete_predicate` をドロップすることで、tablet のメタデータを解放します。[#76870](https://github.com/StarRocks/starrocks/pull/76870)
- 共有データの削除ファイルを CRC32C でチェックサム検証します。[#77798](https://github.com/StarRocks/starrocks/pull/77798)
- キャンセルされたクエリが未報告のインスタンスを待つ時間を制限します。[#76909](https://github.com/StarRocks/starrocks/pull/76909)
- メモリ効率と可観測性向上のための jemalloc の各種改善：arm64 上で 64K 版に加えて 4K ページ版をビルドし、dirty/muzzy ページおよび mapped/retained メトリクスを公開し、実行時に可変な `jemalloc_conf` オプションを適用できるようにしました。[#77886](https://github.com/StarRocks/starrocks/pull/77886) [#78159](https://github.com/StarRocks/starrocks/pull/78159) [#78225](https://github.com/StarRocks/starrocks/pull/78225) [#78320](https://github.com/StarRocks/starrocks/pull/78320) [#78333](https://github.com/StarRocks/starrocks/pull/78333)
- 可観測性を追加：Leader FE 上の `max_journal_replay_lag` ゲージと、追加の lake publish-version トレーシング。[#78382](https://github.com/StarRocks/starrocks/pull/78382) [#76658](https://github.com/StarRocks/starrocks/pull/76658) [#77048](https://github.com/StarRocks/starrocks/pull/77048)
- ジョブ完了時に reshard 出力シャードの配置ピンを解放します。[#76945](https://github.com/StarRocks/starrocks/pull/76945)
- StarOS を v4.1.2 にアップグレードしました。[#77657](https://github.com/StarRocks/starrocks/pull/77657)

### セキュリティ {/* #security-3 */}

- [CVE-2025-55163] `grpc-bom` を介して all の `io.grpc` アーティファクトを整合させ、`grpc-netty` が `grpc-core` と一致するようにしました。[#77171](https://github.com/StarRocks/starrocks/pull/77171)
- [CVE-2026-59902] Netty を 4.1.137.Final に更新しました。[#78287](https://github.com/StarRocks/starrocks/pull/78287)
- [CVE-2026-54399] HTTP/1.1 メッセージパーサーにおけるリソース消費無制御（DoS）脆弱性を修正するため、Apache HttpCore を 5.4.3 に更新しました。[#77753](https://github.com/StarRocks/starrocks/pull/77753)
- [CVE-2025-48924] Broker の `commons-lang3` を 3.20.0 にアップグレードし、未使用の `commons-lang` 2.6 と `commons-configuration` 1.6 を削除しました。[#77748](https://github.com/StarRocks/starrocks/pull/77748)
- [CVE] pprof バイナリを新しいリリースで再ビルドし、BE コンテナイメージから未使用の flamegraph ツールを削除しました。[#77846](https://github.com/StarRocks/starrocks/pull/77846)

### バグ修正 {/* #bug-fixes-3 */}

以下の問題が修正されました：

**クラッシュと安定性**

- 致命的シグナルハンドラがプロセスをクラッシュ中としてマークした後にハングする場合、BE を強制終了するようにしました。[#76491](https://github.com/StarRocks/starrocks/pull/76491)
- `tokenize`、`ngram_search`、`generate_series`、`corr`/`covar_pop`/`covar_samp`、`strpos`（INT32_MIN）、および decimal の配列に対する `count()` における定数/不正な引数による BE クラッシュを修正しました。[#77102](https://github.com/StarRocks/starrocks/pull/77102) [#77103](https://github.com/StarRocks/starrocks/pull/77103) [#77216](https://github.com/StarRocks/starrocks/pull/77216) [#77260](https://github.com/StarRocks/starrocks/pull/77260) [#77092](https://github.com/StarRocks/starrocks/pull/77092)
- ある `ALTER` が通常の列と生成列の両方を追加する際の BE クラッシュ、および read-schema の並べ替え後に発生する cache-select のクラッシュを修正しました。[#77436](https://github.com/StarRocks/starrocks/pull/77436) [#77245](https://github.com/StarRocks/starrocks/pull/77245)
- クエリキャッシュがパーティション化されたハッシュジョインのレーンを再利用する際の BE クラッシュ、およびレーンのジョインのビルド側が空の場合のハングを修正しました。[#77564](https://github.com/StarRocks/starrocks/pull/77564) [#77571](https://github.com/StarRocks/starrocks/pull/77571)
- `JsonColumn` の追加における範囲外読み取りとサイレントな行の消失、`BinaryColumnBase::replicate` における整数オーバーフロー、および 1 つの配列列が複数のラムダ引数として渡された場合の `array_map` DCHECK 失敗を修正しました。[#77090](https://github.com/StarRocks/starrocks/pull/77090) [#77545](https://github.com/StarRocks/starrocks/pull/77545) [#77433](https://github.com/StarRocks/starrocks/pull/77433)
- 不正な UTF-8 に対して `substr` が隣接する行のバイトを返す問題、および `array_sort` のラムダコンパレータがソートを範囲外に導く問題を修正しました。[#78722](https://github.com/StarRocks/starrocks/pull/78722) [#77687](https://github.com/StarRocks/starrocks/pull/77687)
- parquet デコーダのスクラッチバッファをスタックから外し、`SimdJsonConverter` のエラーパスにおける raw-JSON ビューに境界を設けました。[#77615](https://github.com/StarRocks/starrocks/pull/77615)
- 報告されたストレージ媒体を持たないディスクに対する tablet スケジューラの NPE、および `INSERT ... VALUES (DEFAULT, ...)` のデパース時と、オプティマイザがクエリ ID を持たない場合の `PrepareCollectMetaTask` における NPE を修正しました。[#76966](https://github.com/StarRocks/starrocks/pull/76966) [#76963](https://github.com/StarRocks/starrocks/pull/76963) [#77051](https://github.com/StarRocks/starrocks/pull/77051)

**クエリエンジンとオプティマイザ**

- 複数のオプティマイザパスにおける誤った結果や欠落した結果を修正：ストリーミング distinct グローバル limit、ソートグループをまたぐランキングウィンドウの事前集計、マージフェーズで再追加される定数集計値、すべての子に適用されていなかった GROUP BY ルール、FOLLOWING オフセットから開始する分析フレーム。[#77086](https://github.com/StarRocks/starrocks/pull/77086) [#77068](https://github.com/StarRocks/starrocks/pull/77068) [#77069](https://github.com/StarRocks/starrocks/pull/77069) [#77099](https://github.com/StarRocks/starrocks/pull/77099) [#77342](https://github.com/StarRocks/starrocks/pull/77342)
- NL Join の null 対応アンチジョインがプローブ行を失う問題、NL Join プローブにおける空チャンクに対する連言評価、およびグループ化キーでジョインした `GROUPING SETS` 集計が実行可能プランなしで失敗する問題を修正しました。[#77970](https://github.com/StarRocks/starrocks/pull/77970) [#77344](https://github.com/StarRocks/starrocks/pull/77344) [#77296](https://github.com/StarRocks/starrocks/pull/77296)
- grouping-sets-to-union-all の書き換え順序、limit がセットオペレータとして重複する問題、および Set オペレータが colocate 実行グループに含まれてハングを引き起こす問題を修正しました。[#77380](https://github.com/StarRocks/starrocks/pull/77380) [#77692](https://github.com/StarRocks/starrocks/pull/77692) [#77025](https://github.com/StarRocks/starrocks/pull/77025)
- ウィンドウの `PARTITION BY`/`ORDER BY` におけるサブクエリ計画、複数列 `count(distinct)` 用に構築された `if()` の decimal タイプの維持、スキャンの重い式の結果列をスロットの nullability に正規化する問題を修正しました。[#77087](https://github.com/StarRocks/starrocks/pull/77087) [#77346](https://github.com/StarRocks/starrocks/pull/77346) [#77242](https://github.com/StarRocks/starrocks/pull/77242)
- 型のない配列リテラルが誤ったタイプを取得してビューを壊す問題、force-inline の上限に達した際の CTE アンカリングの問題、および再帰 CTE に対する推論済み CTAS 列の再利用を修正しました。[#76986](https://github.com/StarRocks/starrocks/pull/76986) [#77018](https://github.com/StarRocks/starrocks/pull/77018) [#76453](https://github.com/StarRocks/starrocks/pull/76453)
- スキャンスキーマ、ジョイン述語の共通部分式、および ANN 検索仕様をクエリキャッシュダイジェストに追加し、`SortNode` が TOPN フィルタを構築する際にキャッシュを無効化することで、複数のクエリキャッシュの正確性に関する問題を修正しました。[#77559](https://github.com/StarRocks/starrocks/pull/77559) [#77565](https://github.com/StarRocks/starrocks/pull/77565) [#77586](https://github.com/StarRocks/starrocks/pull/77586) [#77404](https://github.com/StarRocks/starrocks/pull/77404)
- 可視バージョンを一度もカバーしていない行数からの `COUNT(*)` の畳み込み、ジョインの並べ替えにおける無条件の UKFK 制約収集、および転置インデックスフィルタリング後に OR 常駐述語によって読み取られる列を保持する問題を修正しました。[#77190](https://github.com/StarRocks/starrocks/pull/77190) [#77197](https://github.com/StarRocks/starrocks/pull/77197) [#77659](https://github.com/StarRocks/starrocks/pull/77659)
- 日時解析の正確性を修正：Teradata と Joda のパーサーを入力文字列の範囲内に制限し、範囲外のユリウス値に対して `DateValue::weekday()` を範囲内に保持するようにしました。[#77098](https://github.com/StarRocks/starrocks/pull/77098) [#77310](https://github.com/StarRocks/starrocks/pull/77310)
- バッチバッファが巨大になる場合に `EXCEPT`/`INTERSECT` キーを行ごとにシリアライズし、セットオペレーションがキーをデシリアライズする際に JSON キー列を再構築するようにしました。[#77821](https://github.com/StarRocks/starrocks/pull/77821) [#77215](https://github.com/StarRocks/starrocks/pull/77215)
- その他のオプティマイザ/式の修正：`StatusOr` をアンラップする前にステータスを確認、property 強制時にプロパティの充足性を一度だけ判定、`encode_fingerprint_sha256` でインデックス 0 の定数引数を読み取り、非正の列長に対して `CHAR` 述語のゼロパディングをスキップ、非正のランキング述語を TopN にプッシュしない、推定されたパーティション述語によるプルーニング時に NULL パーティションを保持、`_switch_context` で出力スキーマの末尾を超えて読み取らないようにする、同一の NULL 規約でランをソートしランをマージ、prepare が失敗する前に `Analytor::_fns` を埋める、外部 CTAS のリストパーティション列を `NOT NULL` に強制しないようにする。[#77052](https://github.com/StarRocks/starrocks/pull/77052) [#77091](https://github.com/StarRocks/starrocks/pull/77091) [#77104](https://github.com/StarRocks/starrocks/pull/77104) [#77444](https://github.com/StarRocks/starrocks/pull/77444) [#77445](https://github.com/StarRocks/starrocks/pull/77445) [#77596](https://github.com/StarRocks/starrocks/pull/77596) [#77660](https://github.com/StarRocks/starrocks/pull/77660) [#77679](https://github.com/StarRocks/starrocks/pull/77679) [#77758](https://github.com/StarRocks/starrocks/pull/77758) [#78468](https://github.com/StarRocks/starrocks/pull/78468)
- ステートメント/デパースの問題を修正：列リストを持つ Iceberg/Hive の `INSERT` における静的パーティション句を検証、パーティション参照のデパース時に `TEMPORARY` 修飾子を保持、プロファイル内の空の SQL ステートメントを修正するために CTAS デパースをサポート、転送されたステートメントが元の SQL を保持するように `proxyExecute` で `isSingleStmt` を設定。[#76659](https://github.com/StarRocks/starrocks/pull/76659) [#76987](https://github.com/StarRocks/starrocks/pull/76987) [#77978](https://github.com/StarRocks/starrocks/pull/77978) [#77982](https://github.com/StarRocks/starrocks/pull/77982)

**JSON とデータタイプ**

- 大きな `BinaryColumn` の SerDe の問題、名前にドットを含む列の統計情報収集、および `multi_unnest`/`unnest` の配列オフセットを `int32` ではなく `uint32` として読み取る問題を修正しました。[#75504](https://github.com/StarRocks/starrocks/pull/75504) [#71301](https://github.com/StarRocks/starrocks/pull/71301) [#78479](https://github.com/StarRocks/starrocks/pull/78479) [#78480](https://github.com/StarRocks/starrocks/pull/78480)
- 行IDによって struct サブフィールド述語が評価される際の Flat JSON 垂直コンパクションのメモリ使用量と値自体の読み取りに関する問題を修正しました。[#77682](https://github.com/StarRocks/starrocks/pull/77682) [#77891](https://github.com/StarRocks/starrocks/pull/77891)

**マテリアライズドビュー**

- 外部認可を伴う IVM リフレッシュ、MV 書き換えの古さの基準（現在は最終鮮度確認時刻を使用）、タイムトラベルクエリに対する MV 書き換えのスキップに関する問題を修正しました。[#77210](https://github.com/StarRocks/starrocks/pull/77210) [#76758](https://github.com/StarRocks/starrocks/pull/76758) [#77683](https://github.com/StarRocks/starrocks/pull/77683)
- マテリアライズドビュー上に作成された GIN インデックスへのインデックス ID 割り当て、およびビューベースの MV 書き換えで置き換え戻す際にビュースキャンプロジェクションを保持する問題を修正しました。[#77340](https://github.com/StarRocks/starrocks/pull/77340) [#77613](https://github.com/StarRocks/starrocks/pull/77613)
- その他のマテリアライズドビュー修正：`default_mv_refresh_mode` の検証、`ORDER BY` が増分 MV の主キーをソートではなく拡張してしまう問題の修正、`information_schema.materialized_views` の内部クエリを `query_timeout` で制限する修正。[#77231](https://github.com/StarRocks/starrocks/pull/77231) [#77680](https://github.com/StarRocks/starrocks/pull/77680) [#77585](https://github.com/StarRocks/starrocks/pull/77585)

**データレイクコネクタ（Iceberg、Paimon、Delta、Hive）**

- Iceberg の正確性と安定性を修正：書き込みパスで NOT NULL 列を強制する、プラン時点のスナップショットから書き換えコミットを検証する、`TRUNCATE` の後にメタデータキャッシュを無効化する、メタデータファイルが欠落しているテーブルを削除する、cast-on-string パーティションのメタデータ削除前に内部テーブルをリフレッシュする、`WHERE` 句を伴う `rewrite_data_files` でのサイレントなデータ損失、`remove_orphan_files` における保持期間が短すぎる問題。[#77589](https://github.com/StarRocks/starrocks/pull/77589) [#77709](https://github.com/StarRocks/starrocks/pull/77709) [#77385](https://github.com/StarRocks/starrocks/pull/77385) [#77786](https://github.com/StarRocks/starrocks/pull/77786) [#77335](https://github.com/StarRocks/starrocks/pull/77335) [#77797](https://github.com/StarRocks/starrocks/pull/77797) [#77394](https://github.com/StarRocks/starrocks/pull/77394)
- Delta Lake のテーブルごとの cloud 認証情報を分離しました。[#77424](https://github.com/StarRocks/starrocks/pull/77424)
- Hive 外部テーブルに対する OpenX JsonSerDe の列名マッピング、1 つの JSON フィールドを複数の Hive 列にマッピングする機能をサポートし、デフォルトの catalog がない場合や CTE 参照時のコネクタビューにおけるテーブル修飾の問題を修正しました。[#65515](https://github.com/StarRocks/starrocks/pull/65515) [#77716](https://github.com/StarRocks/starrocks/pull/77716) [#77887](https://github.com/StarRocks/starrocks/pull/77887)
- 外部テーブルに対する Ranger ビュー／MV 認可、および FE クラスパス上の Ranger Solr 監査宛先の復元に関する問題を修正しました。[#56316](https://github.com/StarRocks/starrocks/pull/56316) [#77294](https://github.com/StarRocks/starrocks/pull/77294)
- 破損した `DELTA_BINARY_PACKED` ヘッダーが例外としてスローされず返されてしまう問題、および Parquet リーダーでフィルター除外された FLBA 辞書行を NULL としてマークする問題を修正しました。[#77446](https://github.com/StarRocks/starrocks/pull/77446) [#78685](https://github.com/StarRocks/starrocks/pull/78685)
- Iceberg REST catalog における `ALTER TABLE RENAME` の NPE、decimal または null の `timestamptz` パーティション値を持つ Iceberg `$partitions` 読み取り時の NPE、複数の Iceberg catalog 間で 1 つの Hadoop `FileSystem` を共有する問題、`paimon.option.cache-enabled=false` が再び尊重されるようにする問題を修正しました。[#76498](https://github.com/StarRocks/starrocks/pull/76498) [#77742](https://github.com/StarRocks/starrocks/pull/77742) [#77297](https://github.com/StarRocks/starrocks/pull/77297) [#78271](https://github.com/StarRocks/starrocks/pull/78271)

**共有データ：コンパクション、vacuum、リシャード、レプリケーション**

- Compaction の修正：ファイルバンドリングがスキップするコンパクション txn ログをキャッシュする、最大 tablet スコアで候補をソートする、read-chunk-size の見積もりを修正する、縮小時にトークンを失う Limiter のリサイズを修正する、削除のみの書き込みに対してパブリッシュ時のセグメント書き換えをスキップする。[#77578](https://github.com/StarRocks/starrocks/pull/77578) [#77482](https://github.com/StarRocks/starrocks/pull/77482) [#78192](https://github.com/StarRocks/starrocks/pull/78192) [#78087](https://github.com/StarRocks/starrocks/pull/78087) [#78071](https://github.com/StarRocks/starrocks/pull/78071)
- Vacuum の修正：grace-blocked vacuum における無駄な読み取りと迷子の v1 削除を修正、リシャード引き継ぎ時にも autovacuum の保持下限を維持、冗長な lake メタデータの 404 プローブを回避。[#78460](https://github.com/StarRocks/starrocks/pull/78460) [#78545](https://github.com/StarRocks/starrocks/pull/78545) [#78466](https://github.com/StarRocks/starrocks/pull/78466)
- リシャード／分割の修正：ソースの tablet が置き換え済みのマテリアライズドインデックスにあるリシャードを拒否する、クロスパブリッシュ時の tablet 分割フォールバックを修正する、想定されるリシャード履歴検索の警告を抑制する、フェイルオーバー後に lake alter ジョブがスタックするのを防止する。[#77512](https://github.com/StarRocks/starrocks/pull/77512) [#78270](https://github.com/StarRocks/starrocks/pull/78270) [#78059](https://github.com/StarRocks/starrocks/pull/78059) [#78517](https://github.com/StarRocks/starrocks/pull/78517)
- レプリケーションの修正：共有 lake ファイルを保持する、リモートのバンドルされた lake メタデータを読み取る、レプリケーション中にソース lake ファイルを再暗号化する。[#77361](https://github.com/StarRocks/starrocks/pull/77361) [#77362](https://github.com/StarRocks/starrocks/pull/77362) [#78080](https://github.com/StarRocks/starrocks/pull/78080)
- 破損したローカルキャッシュの復旧：delvec ページのチェックサム失敗時にドロップして再試行する、PK インデックスの SST コンパクションおよび tablet マージの SSTable 再構築で破損時に再構築する。[#78086](https://github.com/StarRocks/starrocks/pull/78086) [#77481](https://github.com/StarRocks/starrocks/pull/77481) [#78003](https://github.com/StarRocks/starrocks/pull/78003)
- パーティション共有オブジェクトから lake バージョン 1 のメタデータを読み取る問題、スナップショットが所有する共有シャードグループを保護する問題、`TransactionGraph.remove` における依存関係の順序を保持する問題を修正しました。[#78731](https://github.com/StarRocks/starrocks/pull/78731) [#78800](https://github.com/StarRocks/starrocks/pull/78800) [#78077](https://github.com/StarRocks/starrocks/pull/78077)
- その他の共有データ修正：最終コミット時にトランザクションをスロットルする、`DECOMMISSION` レプリカをロードのプライマリとして選ばないようにする、オンライン最適化中のシャドウ生成列を伴う DML を修正する、`spill_enable_direct_io` が実際に `O_DIRECT` を有効にするようにする、並行する `BEGIN WITH LABEL` 間のラベル一意性の競合状態を修正する、クエリトラッカーの予約上限を実効クエリメモリ上限から導出する、tablet マージ中に rowset ID と参照を安全に再マッピングする、クロスパブリッシュされた del ファイルを子が所有するキーにのみ適用する、tablet マージが range-distribution コロケーションを尊重するようにする、不完全な結合済み txn ログを拒否し `replication_num` が共有データの書き込みクォーラムを弱めるのを防ぐ、リシャード中は明示的トランザクションを 1 つの tablet レイアウトに保つ、del ファイルのローカルキャッシュを削除してチェックサム失敗時に再試行する、クローズされた lake デルタライターがタスクを拒否した際に中止理由を報告する。[#77015](https://github.com/StarRocks/starrocks/pull/77015) [#77035](https://github.com/StarRocks/starrocks/pull/77035) [#77252](https://github.com/StarRocks/starrocks/pull/77252) [#77255](https://github.com/StarRocks/starrocks/pull/77255) [#77401](https://github.com/StarRocks/starrocks/pull/77401) [#77408](https://github.com/StarRocks/starrocks/pull/77408) [#77690](https://github.com/StarRocks/starrocks/pull/77690) [#77744](https://github.com/StarRocks/starrocks/pull/77744) [#78113](https://github.com/StarRocks/starrocks/pull/78113) [#78162](https://github.com/StarRocks/starrocks/pull/78162) [#78625](https://github.com/StarRocks/starrocks/pull/78625) [#78022](https://github.com/StarRocks/starrocks/pull/78022) [#78632](https://github.com/StarRocks/starrocks/pull/78632)
- 共有データのソートキーおよび rowset ID に関するエッジケースを修正：末尾のキー列追加後、現在のソートキーに分割境界を投影する、再帰 CTE 実行中の内部 `INSERT` 失敗を表面化する、割り当てられた行数によってクロスパブリッシュされた rowset が存在するかどうかを判断しないようにする、狭い固定長 IVM 行 ID に対して `encode_sort_key` を到達可能にしその定数引数を拡張する、拡張列のデルタ列グループをそのルート列を通じて解決する、範囲分割のクロスパブリッシュ統計を保持する、FE の最大 tablet コンパクションスコア収集を修正する、自動増分マップをドロップする際にデッドノードをスキップする、未ソートの PK SST ライターのマップを割り当て数のカウントではなく計測する、`compression_level` を合成サブカラムメタに伝播する、組み込みの転置インデックスバックエンドに対してスタンドアロンの GIN インデックスディレクトリ処理をスキップする。[#77504](https://github.com/StarRocks/starrocks/pull/77504) [#77510](https://github.com/StarRocks/starrocks/pull/77510) [#77511](https://github.com/StarRocks/starrocks/pull/77511) [#77751](https://github.com/StarRocks/starrocks/pull/77751) [#77847](https://github.com/StarRocks/starrocks/pull/77847) [#77759](https://github.com/StarRocks/starrocks/pull/77759) [#78269](https://github.com/StarRocks/starrocks/pull/78269) [#78496](https://github.com/StarRocks/starrocks/pull/78496) [#78561](https://github.com/StarRocks/starrocks/pull/78561) [#77498](https://github.com/StarRocks/starrocks/pull/77498) [#76949](https://github.com/StarRocks/starrocks/pull/76949) [#77101](https://github.com/StarRocks/starrocks/pull/77101)

**ロード、トランザクション、DML**

- データロードおよび列モードの部分更新パスにおけるチャンク容量の制限とチェック、所有する子ごとの条件更新と部分更新の決定、ステートメントごとのマージされた rowset の del `op_offset` の記録に関する問題を修正しました。[#77163](https://github.com/StarRocks/starrocks/pull/77163) [#78099](https://github.com/StarRocks/starrocks/pull/78099) [#78313](https://github.com/StarRocks/starrocks/pull/78313)
- 削除されたテーブルに対する insert-overwrite の失敗がジャーナル化されない問題、および複数の部分更新パス全体で現在の行のライフサイクルを正しく保つ問題を修正しました。[#77212](https://github.com/StarRocks/starrocks/pull/77212)
- 切断の kill を I/O スレッドではなくワーカーに引き渡す問題、`GroupedExecutionSinkOperator::push_chunk` における `accept()` ステータスの伝播に関する問題を修正しました。[#77590](https://github.com/StarRocks/starrocks/pull/77590) [#77737](https://github.com/StarRocks/starrocks/pull/77737)
- エラー状態の記録のみを行う内部 DML／CONTROL ステートメントを失敗させるようにし、失敗が黙って握りつぶされるのではなく表面化するようにしました。[#77388](https://github.com/StarRocks/starrocks/pull/77388)

**統計、メトリクス、FE／メタデータ**

- 統計の正確性を修正：ヒストグラムにおける null バケット／MCV を拒否する、`CancelableAnalyzeTask.cancel` が単一の勝者を選ぶようにする。[#77024](https://github.com/StarRocks/starrocks/pull/77024) [#78593](https://github.com/StarRocks/starrocks/pull/78593)
- `pipe_driver_queue_len` メトリクスを登録し、NotFound の lake tablet メタデータ読み取りに対する BE メトリクスを追加し、テーブルでない StarOS シャードに対するテーブルメトリクスとチェックポイントスレッドプールメトリクスをスキップし、プロファイル辞書内の誤記された `ZoneMapIndexFilter` キーを修正しました。[#77890](https://github.com/StarRocks/starrocks/pull/77890) [#78451](https://github.com/StarRocks/starrocks/pull/78451) [#78116](https://github.com/StarRocks/starrocks/pull/78116) [#77367](https://github.com/StarRocks/starrocks/pull/77367) [#78728](https://github.com/StarRocks/starrocks/pull/78728)
- FE／メタデータの修正：メタ同期においてリサイクルされ欠落したシャードグループを無視する、bRPC でエンドポイントごとに複数の接続グループを復元する、BE に送る前に FE でコンピュートノードのホスト名を解決する、`DROP FOLLOWER`／`OBSERVER` 時にドロップされた FE が自身を終了するようにする、コロケーショングループのスケジューリングサンプルノブを公開する。[#76604](https://github.com/StarRocks/starrocks/pull/76604) [#76765](https://github.com/StarRocks/starrocks/pull/76765) [#78390](https://github.com/StarRocks/starrocks/pull/78390) [#78215](https://github.com/StarRocks/starrocks/pull/78215) [#78450](https://github.com/StarRocks/starrocks/pull/78450)
- メタスキャンの tablet を拡張する際に拡張されていないアクセスパスをスキップする問題、スキャンのアクセスパスを列 ID で照合する問題、BE がプルーニングした場合にのみ OLAP スキャンの複合タイプを狭める問題、集計状態列のサブフィールドをプルーニングしない問題を修正しました。[#76593](https://github.com/StarRocks/starrocks/pull/76593) [#77987](https://github.com/StarRocks/starrocks/pull/77987) [#77894](https://github.com/StarRocks/starrocks/pull/77894) [#77901](https://github.com/StarRocks/starrocks/pull/77901)
- 集計状態列にグローバル辞書を使用しない、`ARRAY`／`STRUCT` を返す式を辞書化しない、プッシュダウンされた述語がスパースな範囲を読み取る際にページの null フラグを序数でインデックス化する、という修正を行いました。[#77096](https://github.com/StarRocks/starrocks/pull/77096) [#78488](https://github.com/StarRocks/starrocks/pull/78488)
- その他のメトリクスおよび FE の修正：bRPC メソッドのエラー bvars をカウンターとして報告する、range-distribution パーティションの実際の tablet 数を Buckets として報告する、`show proc '/global_current_queries'` における NPE を修正する、中間クエリ統計で Data Cache の読み取り統計を報告する、jemalloc `malloc_usable_size` で最小限の TSD フェッチを使用する、ヒーププロファイルのトグルが `prof.thread_active_init` を書き込まないようにする、ラップ後の FE 接続 ID の衝突を防ぐ、多くのパーティションが古い場合にテーブル統計のヘルスが ~1.0 に固定される問題を修正する、FE の features-log パスに `feature_log_dir` を使用する、論理ウィンドウオペレータをコピーする際に `inputIsBinary` を保持する、キャッシュ無効化後の Thrift クライアントのリークを修正する。[#76956](https://github.com/StarRocks/starrocks/pull/76956) [#77465](https://github.com/StarRocks/starrocks/pull/77465) [#77407](https://github.com/StarRocks/starrocks/pull/77407) [#77696](https://github.com/StarRocks/starrocks/pull/77696) [#77625](https://github.com/StarRocks/starrocks/pull/77625) [#78384](https://github.com/StarRocks/starrocks/pull/78384) [#78217](https://github.com/StarRocks/starrocks/pull/78217) [#76927](https://github.com/StarRocks/starrocks/pull/76927) [#77221](https://github.com/StarRocks/starrocks/pull/77221) [#77058](https://github.com/StarRocks/starrocks/pull/77058) [#77072](https://github.com/StarRocks/starrocks/pull/77072)

**その他**

- プールされた Poco セッションにリクエストタイムアウトを適用し、Linux AArch64 上で Hyperscan を Vectorscan 5.4.12 にアップグレードし、`JAVA_OPTS_FOR_JDK_*` の警告をエスカレートし、java-extensions リーダーライブラリでテストスコープの jar を出荷しないようにしました。[#78361](https://github.com/StarRocks/starrocks/pull/78361) [#78326](https://github.com/StarRocks/starrocks/pull/78326) [#78328](https://github.com/StarRocks/starrocks/pull/78328) [#77752](https://github.com/StarRocks/starrocks/pull/77752)
- `dictionary_get` キーをキーサイズでシリアライズし、thrift プロセッサ内の例外をキャッチし、CTAS においてプランナーのメタロックから `files()` のスキーマ推論を外しました。[#78706](https://github.com/StarRocks/starrocks/pull/78706) [#78805](https://github.com/StarRocks/starrocks/pull/78805) [#78772](https://github.com/StarRocks/starrocks/pull/78772)
- ファイルに末尾の改行がない場合に最後の設定行を読み込むようにし、設定 YAML パスを cwd に対して解決し、GCS アップロードしきい値の BE 設定を `gcs`（`gs` ではなく）という名前にし、StarOS と Starlet を4.1.3にアップグレードし、不要になった設定であるフォワードヘッダースプリットと、ドキュメントファイル名内の紛れ込んだ U+200E を削除しました。[#78065](https://github.com/StarRocks/starrocks/pull/78065) [#77132](https://github.com/StarRocks/starrocks/pull/77132) [#78452](https://github.com/StarRocks/starrocks/pull/78452) [#78038](https://github.com/StarRocks/starrocks/pull/78038) [#78780](https://github.com/StarRocks/starrocks/pull/78780) [#78612](https://github.com/StarRocks/starrocks/pull/78612)

## 4.1.4 {/* #414 */}

リリース日：2026年8月5日

### 動作の変更 {/* #behavior-changes-3 */}

- 共有データモードにおいて、`TABLESAMPLE` / `SAMPLE` 句および `ANALYZE SAMPLE TABLE` が lake テーブルに対して有効になりました。以前はサンプルオプションが BE に到達しなかったため、フルスキャンが実行されていました。[#71874](https://github.com/StarRocks/starrocks/pull/71874)
- `ALTER TABLE` から `flat_json` 設定への変更が、バージョン管理されたタスクを通じて BE ノードに伝播されるようになり、確実に反映されるようになりました。[#74747](https://github.com/StarRocks/starrocks/pull/74747)
- GIN 転置インデックスで解決される `NOT MATCH` 述語が NULL 行を返さなくなりました。NULL 行は結果から正しく除外されるようになりました。[#75578](https://github.com/StarRocks/starrocks/pull/75578)
- `ANALYZE ... UPDATE HISTOGRAM ON` は（オプティマイザが使用しない）char 系列の列に対してヒストグラムを計算しなくなり、MCV のみを計算するようになったため、分析コストが削減されました。[#75968](https://github.com/StarRocks/starrocks/pull/75968)
- `rewrite_manifests` が出力マニフェストを順序保持のパーティション範囲でクラスタリングするようになり、マニフェストごとのパーティション境界がより厳密になり、パーティション数の多いテーブルでのマニフェストプルーニングが向上しました。[#76193](https://github.com/StarRocks/starrocks/pull/76193)
- 複数テーブルの Stream Load は、結合トランザクションログを使用するかどうかを判断する際に、グローバルな `lake_use_combined_txn_log` 設定のみに従うのではなく、各テーブルの `file_bundling` プロパティに従うようになりました。[#76806](https://github.com/StarRocks/starrocks/pull/76806)
- Primary Key テーブル上の列モード部分更新と組み合わせた GIN 転置インデックスが、破損した結果を返さなくなりました。インデックスはデルタ列グループセグメントから提供されるようになりました。[#76271](https://github.com/StarRocks/starrocks/pull/76271)
- 整数配列に対する `array_difference` が、`BIGINT` に拡張する前に64ビットで隣接差分を計算するようになり、int32のオーバーフローを修正しました。[#76569](https://github.com/StarRocks/starrocks/pull/76569)
- 非定数の除数を持つ除算式が単調であると見なされなくなり、誤った結果を生む可能性があった誤った ZoneMap プルーニングを修正しました。[#76744](https://github.com/StarRocks/starrocks/pull/76744)
- 配列およびマップコンストラクタは、チャンクの平坦化結果が4GBを超えた場合、データを黙って破損させるのではなく、エラーを発生させるようになりました。[#76419](https://github.com/StarRocks/starrocks/pull/76419)
- Primary Key の挿入は、L1/L2永続性インデックスにすでに存在するキーを正しく拒否するようになりました。以前はこの存在チェックがスキップされていました。[#76591](https://github.com/StarRocks/starrocks/pull/76591)
- レンジコロケートテーブルに対するサンプルベースの tablet 事前分割が、新しいシャードをソースの tablet のワーカーに詰め込むのではなく、コンピュートノード全体に分散させるようになり、ハッシュに比べて約3倍遅かったレンジバッチインポートを修正しました。[#76608](https://github.com/StarRocks/starrocks/pull/76608)
- 増分または AUTO のマテリアライズドビューが、`AUTO` リフレッシュモードにおける PCT フォールバック用のメンテナンスクエリを再構築するようになり、IVM がサポートしない形状に対するリフレッシュの失敗を修正しました。[#75961](https://github.com/StarRocks/starrocks/pull/75961)
- `ALTER MATERIALIZED VIEW ... ACTIVE` が非集計の INCREMENTAL マテリアライズドビューに対して機能するようになりました。ストレージ充填された `__ROW_ID__` 列が MV の DDL 列リストから除外され、非アクティブな IVM を再アクティブ化できるようになりました。[#77017](https://github.com/StarRocks/starrocks/pull/77017)
- Iceberg パーティションキャッシュが（エントリ数だけでなく）メモリで制限されるようになり、その使用状況が公開され、パーティション数の多いテーブルでの無制限の増加を防止しました。[#76165](https://github.com/StarRocks/starrocks/pull/76165)
- OAuth2クライアントクレデンシャルを使用する Iceberg REST catalog が、バックグラウンドのトークン更新タスクが停止した後に自己回復するようになり、以降のリクエストが all 失敗することがなくなりました。[#76457](https://github.com/StarRocks/starrocks/pull/76457)
- Lake のフルバキュームが、以前はスキップされていた孤立した `.lcrm`（Lake Compaction Rows Mapper）ファイルを回収するようになりました。[#76522](https://github.com/StarRocks/starrocks/pull/76522)
- 共有データの publish が、tablet が欠落しているバンドル tablet メタデータファイルの書き込みを拒否するようになり、パーティションが publish 時に永続的に停止するのを防止しました。[#76850](https://github.com/StarRocks/starrocks/pull/76850)
- 放棄された外部（Spark/Flink connector）スキャンコンテキストが、回収時にパイプラインフラグメントをキャンセルするようになり、外部スキャンプランは QueryContext の有効期間を制限するために `query_delivery_timeout` を設定するようになりました。[#76535](https://github.com/StarRocks/starrocks/pull/76535) [#76536](https://github.com/StarRocks/starrocks/pull/76536)
- マテリアライズドビューのピン留め範囲マップが（テーブル UUID によって）正しくキー付けされるようになり、OLAP テーブルに対して MV のブートストラップピン留めが無効になっていた問題や、異なるデータベース内の同名テーブルの誤った扱いを修正しました。[#76320](https://github.com/StarRocks/starrocks/pull/76320) [#76351](https://github.com/StarRocks/starrocks/pull/76351)
- leader FE に転送されたステートメントの監査ログが、CTE エイリアスや未修飾のテーブル名ではなく、（leader と一致する）完全修飾されたクエリ対象のリレーションを記録するようになりました。[#76387](https://github.com/StarRocks/starrocks/pull/76387)
- 完全な `STRUCT` 列を `ROLLUP`、`CUBE`、または `GROUPING SETS` と一緒に集計しても、プラン作成時に `usedStructFiledPos` エラーで失敗しなくなりました。[#76804](https://github.com/StarRocks/starrocks/pull/76804)
- FE メモリの `Estimator` がコンテナのオーバーヘッドをカウントするようになり、Parquet スキャナが欠落した列をフルバッチサイズではなく制限されたチャンクサイズを使用してパディングするようになり、メモリ会計の精度が向上しました。[#75971](https://github.com/StarRocks/starrocks/pull/75971) [#75981](https://github.com/StarRocks/starrocks/pull/75981)

### 改善 {/* #improvements-4 */}

- Routine Load において、Kafka/Pulsar のメッセージメタデータ（パーティション、オフセット、タイムスタンプなど）を公開するための `INCLUDE METADATA` 句のサポートを追加しました。[#73840](https://github.com/StarRocks/starrocks/pull/73840)
- Routine Load メタデータエイリアスをオプションにしました。[#76294](https://github.com/StarRocks/starrocks/pull/76294)
- `information_schema.materialized_views` に `LAST_FRESHNESS_CONFIRMED_AT` 列を追加しました。[#74585](https://github.com/StarRocks/starrocks/pull/74585)
- `/metrics` エンドポイントを通じてデータキャッシュのメトリクスを公開し、FE の Compaction メトリクス、`ALTER TABLE` の列操作メトリクスとその所要時間、および exchange sink の `CompressedInputBytes` メトリクスを追加しました。[#58204](https://github.com/StarRocks/starrocks/pull/58204) [#72941](https://github.com/StarRocks/starrocks/pull/72941) [#76247](https://github.com/StarRocks/starrocks/pull/76247) [#76309](https://github.com/StarRocks/starrocks/pull/76309)
- connector メタデータ操作からの認証および接続性エラーの詳細をエンドユーザーに公開しました。[#75490](https://github.com/StarRocks/starrocks/pull/75490)
- 大きな列の容量制限チェックに関するエラーメッセージを改善しました。[#76303](https://github.com/StarRocks/starrocks/pull/76303)
- `hdfs_backend_selector_cache_replica_num` 変数を追加し、オブジェクトストレージクライアントのキャッシュサイズを実行時に変更可能にしました。[#75023](https://github.com/StarRocks/starrocks/pull/75023) [#75851](https://github.com/StarRocks/starrocks/pull/75851)
- サンプルベースの tablet 事前分割のメタ層リーダーを、`CHAR`、複合ソートキー、UTC 調整された Parquet の `TIMESTAMP` / ORC の `TIMESTAMP_INSTANT` を含む、より多くのソートキータイプに拡張しました。[#75937](https://github.com/StarRocks/starrocks/pull/75937) [#76011](https://github.com/StarRocks/starrocks/pull/76011) [#76114](https://github.com/StarRocks/starrocks/pull/76114)
- 外部テーブルの統計収集を改善しました：述語列の使用状況が追跡されるようになり、複数の列が単一のスキャンで収集されるようになり、Iceberg テーブルに対してコスト制限付きのスキャン予算が適用されるようになりました。[#75938](https://github.com/StarRocks/starrocks/pull/75938) [#76638](https://github.com/StarRocks/starrocks/pull/76638) [#76549](https://github.com/StarRocks/starrocks/pull/76549)
- 右外部、semi、anti、full-outer のレンジコロケートジョインをサポートします。[#76040](https://github.com/StarRocks/starrocks/pull/76040)
- Query Queue V2 のコスト推定を最適化しました。[#76609](https://github.com/StarRocks/starrocks/pull/76609)
- `FILES()` グロブに対して、ワイルドカードリテラルのプレフィックスを S3 の `ListObjectsV2` 呼び出しにプッシュダウンし、リストされるオブジェクト数を削減しました。[#76210](https://github.com/StarRocks/starrocks/pull/76210)
- `remove_orphan_files` で Iceberg マニフェストエントリをスキャンする際、`file_path` 列のみが投影されるようになりました。[#76020](https://github.com/StarRocks/starrocks/pull/76020)
- レイク上の主キーテーブルの tablet に対するベースコンパクションをサポートし、個別ソートキーを持つ cloud ネイティブの主キーテーブルに対してロードのスピルと先行的な PK インデックス SST をサポートします。[#76794](https://github.com/StarRocks/starrocks/pull/76794) [#76094](https://github.com/StarRocks/starrocks/pull/76094)
- 共有データモードのレンジ分散テーブルにおいて、末尾のソートキー列をメタデータのみで追加できるようになりました。[#76341](https://github.com/StarRocks/starrocks/pull/76341)
- フラグメントのキャンセル時に実行中の exchange sink RPC をキャンセルし、リシャードのクリーニング中にリシャードされたパーティション上の実行中の Compaction をキャンセルするようになりました。[#75613](https://github.com/StarRocks/starrocks/pull/75613) [#76759](https://github.com/StarRocks/starrocks/pull/76759)
- 複数ステートメントの Stream Load チャネルを待機前に発火させることで、ロードのレイテンシを削減しました。[#76715](https://github.com/StarRocks/starrocks/pull/76715)
- `Analytor::process` における列アップグレードに対してメモリ上限チェックを追加しました。[#75821](https://github.com/StarRocks/starrocks/pull/75821)
- 非同期のデルタライターが停止した際に、実際のエラーステータスを保持するようになりました。[#76216](https://github.com/StarRocks/starrocks/pull/76216)
- レイクの主キーインデックス SSTable に世代バージョンを記録し、レイクの主キーの publish 時に `op_write.seg_delvecs` を適用し、Compaction の publish における競合解決時に出力セグメントのフッターを開く処理をスキップするようになりました。[#76208](https://github.com/StarRocks/starrocks/pull/76208) [#76474](https://github.com/StarRocks/starrocks/pull/76474) [#76657](https://github.com/StarRocks/starrocks/pull/76657)
- 診断を容易にするため、レイクのロードおよび publish-version の経路にスタックトレースと細粒度のトレースカウンタを追加しました。[#75901](https://github.com/StarRocks/starrocks/pull/75901) [#76810](https://github.com/StarRocks/starrocks/pull/76810)

### セキュリティ {/* #security-4 */}

- [CVE-2026-44891] STOMP サブフレームデコーダーにおけるメモリ枯渇（DoS）脆弱性を修正するため、Netty を 4.1.136.Final にアップグレードしました。[#76555](https://github.com/StarRocks/starrocks/pull/76555)
- [CVE-2026-55971] [CVE-2026-43871] C++ バインディングにおけるヒープベースのバッファオーバーフローと無限ループの脆弱性を修正するため、Apache Thrift を 0.24.0 にアップグレードしました。[#76922](https://github.com/StarRocks/starrocks/pull/76922)
- [CVE-2026-10050] 脆弱性のある Jetty jar（Hadoop から推移的に取り込まれるクライアント側 Digest 認証バイパス）を除外し、pgjdbc を 42.7.12 にアップグレードしました。[#76783](https://github.com/StarRocks/starrocks/pull/76783)
- [CVE-2011-4969] [CVE-2014-6071] 脆弱性のある jQuery 1.4.2（および他の jQuery XSS CVE）を同梱していた、未使用の `avro-ipc` jar を除外しました。[#76270](https://github.com/StarRocks/starrocks/pull/76270)
- 修正版と並行して出荷されていた、脆弱で古い推移的依存関係（例：`bcprov-jdk15on` 1.70 や EOL の `okhttp` 2.x）を削除し、回帰を防ぐための依存関係禁止ルールを追加しました。[#76097](https://github.com/StarRocks/starrocks/pull/76097)

### バグ修正 {/* #bug-fixes-4 */}

以下の問題が修正されました：

- ジョイン述語の導出において、空の範囲に対してプランナがクラッシュする問題。[#75011](https://github.com/StarRocks/starrocks/pull/75011)
- 非 NULL 定数の `ELSE` を持つ `CASE` 上に集計が誤ってプッシュダウンされる問題。[#75037](https://github.com/StarRocks/starrocks/pull/75037)
- `PARTITION-TOP-N` の partition-by が、プルーニングされた辞書スロットに書き換えられる問題。[#75956](https://github.com/StarRocks/starrocks/pull/75956)
- ジョインの Shuffle Join 出力プロパティ分岐における演算子の優先順位の誤り、および `predicateCommonOperators` がジョイン演算子の構築を通じて引き継がれない問題。[#76203](https://github.com/StarRocks/starrocks/pull/76203) [#76330](https://github.com/StarRocks/starrocks/pull/76330) [#76388](https://github.com/StarRocks/starrocks/pull/76388)
- 分析時におけるビューの列、および `ROLLUP` キーの null 許容性の誤り。[#75684](https://github.com/StarRocks/starrocks/pull/75684) [#76149](https://github.com/StarRocks/starrocks/pull/76149)
- 長さゼロのキャプチャグループに対する `regexp_extract_all` の無限ループ。[#75798](https://github.com/StarRocks/starrocks/pull/75798)
- `LargeOrCalculatingVisitor` における `nullsFraction` のクランプの誤り。[#75864](https://github.com/StarRocks/starrocks/pull/75864)
- 文字列から数値への schema change 変換における `CAST` のセマンティクスの誤り。[#75538](https://github.com/StarRocks/starrocks/pull/75538)
- 同一列の `min`/`max` に対して、同期マテリアライズドビューの書き換えがロールアップ列を失う問題。[#75528](https://github.com/StarRocks/starrocks/pull/75528)
- スキャン述語を引き上げる際に、`array_map` ラムダの内部で書き換えが誤って適用される問題。[#76380](https://github.com/StarRocks/starrocks/pull/76380)
- バケット認識実行下における Iceberg バケットテーブルでの `COUNT(DISTINCT)` の過剰カウント。[#76601](https://github.com/StarRocks/starrocks/pull/76601)
- 必須の Iceberg 列の `UNNEST` + `GROUP BY` において、null 許容の出力でエラーが発生する問題。[#76730](https://github.com/StarRocks/starrocks/pull/76730)
- `INSERT OVERWRITE` コードパス全体でのロック競合を削減し、CBO のテーブルプルーニング中に Memo の自己参照を避けるため `Operator` のソルトを復元しました。[#75828](https://github.com/StarRocks/starrocks/pull/75828) [#76542](https://github.com/StarRocks/starrocks/pull/76542)
- 文字列型の日付パーティション列を時間値と比較した場合、Iceberg のパーティションプルーニング、マニフェストの行数推定、メタデータの削除、イコリティ削除の適用、および Delta Lake のパーティションプルーニングが誤っていた問題。[#76068](https://github.com/StarRocks/starrocks/pull/76068) [#76107](https://github.com/StarRocks/starrocks/pull/76107) [#76197](https://github.com/StarRocks/starrocks/pull/76197) [#76280](https://github.com/StarRocks/starrocks/pull/76280) [#76348](https://github.com/StarRocks/starrocks/pull/76348)
- Iceberg の読み取りが対象スナップショットのスキーマとパーティション仕様を尊重していなかった問題、Iceberg V1 テーブルでパーティションフィールドを削除した後にクエリが失敗する問題、および `rollback_to_snapshot` の後にマテリアライズドビューの書き換えが古い結果を返す問題。[#74711](https://github.com/StarRocks/starrocks/pull/74711) [#75149](https://github.com/StarRocks/starrocks/pull/75149) [#75924](https://github.com/StarRocks/starrocks/pull/75924)
- Iceberg マニフェストのデータファイルキャッシュが不完全なファイルセットを返す問題、インクリメンタルスキャンレンジイテレーターが並行してクローズするには安全でなかった問題、および分析時のタイムトラベルスナップショットのバインディングがベストエフォートになった問題。[#76215](https://github.com/StarRocks/starrocks/pull/76215) [#75953](https://github.com/StarRocks/starrocks/pull/75953) [#76448](https://github.com/StarRocks/starrocks/pull/76448)
- Iceberg/Delta のメタデータに由来する統計情報に、`StatsSource=TABLE_METADATA` のタグが付与されるようになりました。[#76560](https://github.com/StarRocks/starrocks/pull/76560)
- Delta Lake および Kudu の非パーティション化マテリアライズドビューのクエリ書き換え。[#76359](https://github.com/StarRocks/starrocks/pull/76359)
- `gcs-connector` 3.x の設定名変更により、GCS の委譲済み認証情報が無視されていた問題。[#75979](https://github.com/StarRocks/starrocks/pull/75979)
- Hive の `getTable()` が、`get_table_req` へのフォールバック前に再接続するようになり、テーブルが見つからない場合の検出をメッセージテキストではなく例外タイプに基づいて行うようになりました。[#76456](https://github.com/StarRocks/starrocks/pull/76456) [#76459](https://github.com/StarRocks/starrocks/pull/76459)
- Parquet の列インデックス統計において `BOOLEAN` の最小値／最大値がサポートされていなかった問題。[#74752](https://github.com/StarRocks/starrocks/pull/74752)
- 名前に `.` を含むキーに対する Flat- JSON のサブフィールド読み取り、および NULL を返す代わりに中間の Flat- JSON オブジェクトを再構築してしまう問題。サブフィールドキーが大文字小文字を区別せずに衝突する場合、JSON のサブフィールドプッシュダウンがスキップされるようになりました。[#75583](https://github.com/StarRocks/starrocks/pull/75583) [#75764](https://github.com/StarRocks/starrocks/pull/75764) [#76594](https://github.com/StarRocks/starrocks/pull/76594)
- 一方のブランチのみがビットマップインデックス化されている場合に、OR がネストされた述語を保持するようにしました。[#76275](https://github.com/StarRocks/starrocks/pull/76275)
- 主キー自動増分部分更新の適用で BE がクラッシュする問題。[#76119](https://github.com/StarRocks/starrocks/pull/76119)
- ロードスピル中の `LoadChunkSpiller` 初期化競合による BE クラッシュ。[#76098](https://github.com/StarRocks/starrocks/pull/76098)
- 不完全なネストされた lake スキーマでのネイティブ Parquet リーダーのクラッシュ、および Avro 複合型カラムにネストされた `BOOLEAN` の ASAN クラッシュ。[#76455](https://github.com/StarRocks/starrocks/pull/76455) [#76041](https://github.com/StarRocks/starrocks/pull/76041)
- 無効な文字列を `NOT NULL` 数値カラムに変換する際、schema change 中に `bad_variant_access` が発生する問題。[#76707](https://github.com/StarRocks/starrocks/pull/76707)
- `SimdJsonConverter` エラーパスでのヒープバッファオーバーフロー、およびバッファ拡張をまたぐ複数文字 CSV 区切り文字での use-after-free。[#76752](https://github.com/StarRocks/starrocks/pull/76752) [#76718](https://github.com/StarRocks/starrocks/pull/76718)
- スキャン終了時の `MorselQueueFactory` での use-after-free、キャンセル時のスピル可能なジョインビルド `set_finishing` での use-after-free、および `PipelineDriver` デストラクタにおけるグローバルランタイムフィルタータイマーのリーク / スケジュールされないタイマー。[#76259](https://github.com/StarRocks/starrocks/pull/76259) [#76633](https://github.com/StarRocks/starrocks/pull/76633) [#76252](https://github.com/StarRocks/starrocks/pull/76252)
- 物理分割時の空の tablet による CN クラッシュを修正するための null セーフな `SparseRangeIterator::has_more()`、および bRPC スタブキャッシュのクリーンタイマーのリーク。[#75985](https://github.com/StarRocks/starrocks/pull/75985) [#75973](https://github.com/StarRocks/starrocks/pull/75973)
- クエリデプロイワーカーで `ConnectContext` が復元されない問題。[#76366](https://github.com/StarRocks/starrocks/pull/76366)
- 共有データの主キーテーブルにおいて、スピルマージをオペレーション認識にすることを含め、トランザクション内で upsert / delete の順序を保持するようにしました。[#75338](https://github.com/StarRocks/starrocks/pull/75338) [#75366](https://github.com/StarRocks/starrocks/pull/75366)
- ダングリングした `prev_garbage_version` を回避するため `cal_new_base_version` で永続メタデータを読み取るようにし、`publish_version` 内の `base_version` を `base_metadata` と同期させ、通常パスでマージされた並列 Compaction トランザクションログを永続化するようにしました。[#75904](https://github.com/StarRocks/starrocks/pull/75904) [#76313](https://github.com/StarRocks/starrocks/pull/76313) [#76460](https://github.com/StarRocks/starrocks/pull/76460)
- レンジコロケートの修正：アライメントジョブの急増を停止し、アライメントされていないコロケートジョインをフェイルクローズするようにし、バケットシャッフル下で null セーフジョインがマッチを失う問題を修正し、プランフィードバックにおけるレンジコロケートジョインの検出を改善しました。[#75930](https://github.com/StarRocks/starrocks/pull/75930) [#76104](https://github.com/StarRocks/starrocks/pull/76104) [#76121](https://github.com/StarRocks/starrocks/pull/76121)
- tablet 分割 / リシャードの修正：分割時にテーブルの楽観バージョンを引き上げて並行クエリが再プランされるようにし、同一の tablet リシャード前に PK インデックスの memtable をフラッシュし、リシャード中も lake vacuum ファイルをバージョン区間で保持し、分割不可能なレンジ分散 tablet での自動分割ジョブループを停止し、分割後のレンジ分散ソートキーにおける `IS NULL` プルーニングを修正しました。[#76123](https://github.com/StarRocks/starrocks/pull/76123) [#76367](https://github.com/StarRocks/starrocks/pull/76367) [#76209](https://github.com/StarRocks/starrocks/pull/76209) [#76663](https://github.com/StarRocks/starrocks/pull/76663) [#76797](https://github.com/StarRocks/starrocks/pull/76797)
- キー派生のレンジ分散テーブルのキー列の順序を変更する全カラム `ORDER BY` を拒否するようにしました。[#76256](https://github.com/StarRocks/starrocks/pull/76256)
- フラッシュ順序でロードスピルの並列マージ結果を統合し、ファイルバンドリングの引き継ぎにおいて `metaId` によってタッチされたインデックスを除外するようにしました。[#75951](https://github.com/StarRocks/starrocks/pull/75951) [#76368](https://github.com/StarRocks/starrocks/pull/76368)
- NULL を吸収する辞書マッピングの group-by キーのために辞書サイズ + 1 を予約し、プロファイルレポートの再スケジュール前にドライバーの準備状態をチェックするようにしました。[#75357](https://github.com/StarRocks/starrocks/pull/75357) [#75725](https://github.com/StarRocks/starrocks/pull/75725)
- `PlannerMetaLocker` が、ロックが一度も成功しなかった場合にアンロックをスキップしていた問題。[#74041](https://github.com/StarRocks/starrocks/pull/74041)
- ウェアハウスによってリソースグループをフィルタリングするようにしました。[#73209](https://github.com/StarRocks/starrocks/pull/73209)
- `SHOW CREATE ROUTINE LOAD` の出力で `jsonpaths` の値をエスケープするようにしました。[#75755](https://github.com/StarRocks/starrocks/pull/75755)
- Arrow Flight のプリペアドステートメント転送を修正しました。[#76310](https://github.com/StarRocks/starrocks/pull/76310)
- ディクショナリのリフレッシュ間隔のオーバーフローと意図しない自動リフレッシュを修正しました。[#76634](https://github.com/StarRocks/starrocks/pull/76634)
- `ERROR_IF_OVERFLOW` 下での空の min / max における統計キャッシュロードの失敗を修正しました。[#76684](https://github.com/StarRocks/starrocks/pull/76684)
- 書き込みロック下で catalog のドロップ存在チェックをアトミックにしました。[#76778](https://github.com/StarRocks/starrocks/pull/76778)

## 4.1.3 {/* #413 */}

リリース日：2026年7月14日

### 動作の変更 {/* #behavior-changes-4 */}

- `CTAS` は、明示的に宣言された `VARCHAR(N)` カラムの長さを `VARCHAR(MAX)` に拡張するのではなく、そのまま保持するようになりました。既存のテーブルには影響しません。`CTAS` で作成された新しいテーブルでは、以降の書き込みで宣言された長さが適用されます。[#73498](https://github.com/StarRocks/starrocks/pull/73498)
- `OPERATE ON SYSTEM` 権限なしで `sys.fe_memory_usage` または `sys.fe_locks` をクエリすると、誤解を招くノード検索失敗の代わりに、明確なアクセス拒否エラーが返されるようになりました。[#73567](https://github.com/StarRocks/starrocks/pull/73567)
- `FILES()` と broker / stream load は、`isAdjustedToUTC=false` で書き込まれた `INT64` の Parquet タイムスタンプにセッションタイムゾーンのシフトを適用しなくなりました。これらのタイムスタンプは、壁時計の値として扱われ、そのままロードされます。v4.1.3 より前にこのようなファイルからロードされたデータは、アップグレード後にロードされたデータと異なる可能性があります。一貫性が必要な場合は再ロードしてください。[#73674](https://github.com/StarRocks/starrocks/pull/73674)
- 正常にコミットされた複数テーブルトランザクションの stream load ジョブは、`PREPARING` のまま止まるのではなく、`information_schema.loads` と `SHOW STREAM LOAD` において `VISIBLE` として正しく表示されるようになりました。[#74386](https://github.com/StarRocks/starrocks/pull/74386)
- コネクタのインクリメンタルスキャン範囲のスケジューリングは、デプロイされたフラグメントのドライバーレイアウトを一貫して再利用するようになり、スキャン範囲が存在しないドライバーに誤って割り当てられることを防ぐようになりました。[#74674](https://github.com/StarRocks/starrocks/pull/74674)
- `LIKE` の定数畳み込みが MySQL 8 のバックスラッシュエスケープのセマンティクスに一致するようになり、`'a\\\\b'` のようなパターンが以前は逆の結果を返していたケースを修正しました。[#74814](https://github.com/StarRocks/starrocks/pull/74814)
- Routine Load が `property.kafka_partition_discovery` プロパティをサポートするようになりました。これにより、`kafka_partitions` と `kafka_offsets` が正確な開始オフセットを指定するために設定されている場合でも、パーティションの自動検出を継続できます。`property.kafka_default_offsets` が設定されていない場合、ジョブにすでに消費の進捗がある状態で検出されたパーティションのデフォルト開始オフセットは `OFFSET_END` から `OFFSET_BEGINNING` に変更されます。これは、新しいプロパティを使用するジョブだけでなく、**all** のすべての自動検出ジョブに適用されます。[#74729](https://github.com/StarRocks/starrocks/pull/74729)
- group-by を伴わない集計は、マージの前に `UNION ALL` の分岐を通してプッシュダウンされるようになり、ユニオンに対して集計を行うクエリのネットワーク転送とメモリ使用量を削減します。[#73930](https://github.com/StarRocks/starrocks/pull/73930)
- IVM メンテナンスクエリは、`CREATE` 時に保存された固定のクエリテキストを使用するのではなく、各リフレッシュ時に現在のビュー定義から再導出されるようになりました。既存の MV は、再作成する必要なく、リライターのバグ修正の恩恵を自動的に受けられます。[#74881](https://github.com/StarRocks/starrocks/pull/74881)
- サンプルベースの tablet の事前分割では、事前分割されたシャードをソース tablet のワーカーに詰め込む（`PACK` 配置）のではなく、all のコンピュートノード全体に分散させる（`SPREAD` 配置）ようになり、ロードの並列性が向上しました。[#75514](https://github.com/StarRocks/starrocks/pull/75514)
- カラムコメントのみを変更する `ALTER TABLE ... MODIFY COLUMN` は、完全な schema change ジョブを起動するのではなく、軽量なメタデータのみのパスを取るようになり、これは主キーカラムでも動作するようになりました。[#75325](https://github.com/StarRocks/starrocks/pull/75325)
- `FLOOR` と `CEIL` は非予約キーワードとして扱われるようになり、引用符なしでカラム名として使用できるようになりました。[#75241](https://github.com/StarRocks/starrocks/pull/75241)
- `SHOW FUNCTIONS` の出力には、UDF と UDAF の Properties カラムに `isolation` プロパティ（`shared` または `isolated`）が常に含まれるようになりました。[#75255](https://github.com/StarRocks/starrocks/pull/75255)
- `lake_vacuum_min_batch_delete_size` のデフォルト値が100から200に引き上げられ、`DeleteObjects` リクエストごとにより多くの古いファイル削除をバッチ処理することで、オブジェクトストレージ上の vacuum スループットが向上しました。[#74304](https://github.com/StarRocks/starrocks/pull/74304)
- vended credentials を持つ Iceberg REST の catalog テーブルがキャッシュされ、その認証情報がバックグラウンドでリフレッシュされるようになり、AWS Lake Formation のレート制限を引き起こしていた `getTable()` ごとの `GetDataAccess` 呼び出しがなくなりました。[#75431](https://github.com/StarRocks/starrocks/pull/75431)
- IVM の `bitmap_union`、`hll_union`、`percentile_union` 集計状態は、マテリアライズドビュー内で（可視列と非表示の `__AGG_STATE_` 列の 2 回ではなく）1 回だけ格納されるようになり、これらのスケッチタイプのストレージが半減しました。[#75760](https://github.com/StarRocks/starrocks/pull/75760)
- 増分マテリアライズドビューが `bitmap_agg`、`hll_union`、`percentile_union`、`bitmap_union` 集計関数をサポートするようになり、正確な重複排除カウントとスケッチベースの集計を増分的に維持できるようになりました。[#75587](https://github.com/StarRocks/starrocks/pull/75587) [#75610](https://github.com/StarRocks/starrocks/pull/75610)
- サンプルベースの tablet 事前分割数は、均等な分散のためにアクティブなコンピュートノード数の倍数に切り上げられるようになり、小規模なロードでの過度な断片化を避けるため最小 tablet サイズで下限を設けるようになりました。[#75360](https://github.com/StarRocks/starrocks/pull/75360) [#75584](https://github.com/StarRocks/starrocks/pull/75584)

### 改善 {/* #improvements-5 */}

- `ngram_search` 関数が非定数の needle 引数を受け付けるようになりました。[#74675](https://github.com/StarRocks/starrocks/pull/74675)
- `enable_http_auth` FE 設定で制御される HTTP 認証フレームワークを追加し、all 外部 HTTP エンドポイントでの認証と RBAC 適用をゲートできるようにしました。[#73822](https://github.com/StarRocks/starrocks/pull/73822)
- `information_schema.materialized_views` にリフレッシュおよび配置の可観測性カラム（`refresh_warehouse`、`refresh_resource_group`、`refresh_mode`、`refresh_type`、`last_refresh_details`）を追加しました。[#74342](https://github.com/StarRocks/starrocks/pull/74342)
- ジャーナルリプレイ時に外部統計キャッシュのオプトインの遅延リフレッシュを追加しました。これは新しい FE 設定で制御され、低速または停止した外部メタストアが FE ジャーナルリプレイや起動を停滞させることを防ぎます。[#74371](https://github.com/StarRocks/starrocks/pull/74371)
- `VARCHAR` の長さ増加が、データの書き換えなしに高速スキーマ進化経由でレンジ分散（共有データ）ソートキー列に対して許可されるようになりました。[#74698](https://github.com/StarRocks/starrocks/pull/74698)
- 共有データのトランザクションログ書き込みが設定可能なしきい値を超えた場合にスタックトレースダンプを追加し、低速な `put_txn_log` / `put_combined_txn_log` 呼び出しの診断を容易にしました。[#74704](https://github.com/StarRocks/starrocks/pull/74704)
- tablet 事前分割メタ層フッターリーダーが `DATE`、`DATETIME`、`DECIMAL`、`VARCHAR`、および ORC の `TIMESTAMP` ソートキーをサポートするようになり、データ層サンプリングへのフォールバックが必要なロード数を削減しました。[#74710](https://github.com/StarRocks/starrocks/pull/74710) [#74739](https://github.com/StarRocks/starrocks/pull/74739) [#74792](https://github.com/StarRocks/starrocks/pull/74792) [#74902](https://github.com/StarRocks/starrocks/pull/74902) [#74955](https://github.com/StarRocks/starrocks/pull/74955) [#75186](https://github.com/StarRocks/starrocks/pull/75186) [#75209](https://github.com/StarRocks/starrocks/pull/75209) [#75427](https://github.com/StarRocks/starrocks/pull/75427) [#75697](https://github.com/StarRocks/starrocks/pull/75697)
- サンプルベースの tablet 事前分割が `INSERT INTO ... SELECT ... FROM <OLAP table>` ロードにも、また all ソートキー列を含む列リストの `INSERT` ステートメントにも適用されるようになりました。[#74828](https://github.com/StarRocks/starrocks/pull/74828) [#75345](https://github.com/StarRocks/starrocks/pull/75345)
- 共有データの tablet メタデータとトランザクションログファイルに Adler-32 チェックサム保護を追加し、読み取り時にサイレントな破損を検出できるようにしました。[#74924](https://github.com/StarRocks/starrocks/pull/74924)
- データベースごとの `txn_max_committed_pending_publish_ms` FE メトリクスを追加し、バージョン公開の停滞検出に役立つよう、コミット済みだが未公開のトランザクションのうち最も古いものの経過時間を報告します。[#75025](https://github.com/StarRocks/starrocks/pull/75025)
- tablet の分割 / マージがバージョン公開レスポンスからリアルタイムでトリガーされるようになり、ロード完了から自動分割 / マージが開始されるまでの遅延が削減されました。[#75010](https://github.com/StarRocks/starrocks/pull/75010)
- SST なしの条件マージタスクを `pk_index_execution` スレッドプールにルーティングすることで、レイクプライマリキーテーブルの条件付き更新比較フェーズを最適化しました。[#74572](https://github.com/StarRocks/starrocks/pull/74572)
- レイクスキーマ変更とロールアップジョブのロックのスコープをデータベース全体ではなくテーブルレベルに限定し、同一データベース内の他のテーブルに対する並行操作でのロック競合を削減しました。[#75087](https://github.com/StarRocks/starrocks/pull/75087)
- 共有なしモードにおいて、いくつかのデータベースレベルの書き込みロックをテーブルスコープの集中的な書き込みロックに縮小し、BE レポートコールバックおよびクールダウン操作中のロック競合を削減しました。[#74521](https://github.com/StarRocks/starrocks/pull/74521) [#74523](https://github.com/StarRocks/starrocks/pull/74523)
- Avro Routine Load がネイティブの `MAP` および `STRUCT` ターゲット列をサポートするようになりました。[#74901](https://github.com/StarRocks/starrocks/pull/74901)
- レンジコロケート tablet の安定性ゲーティングが、グループを安定とマークする前に StarOS の配置収束を待機するようになり、Colocate Join がホストローカル実行を達成できるようになりました。[#75290](https://github.com/StarRocks/starrocks/pull/75290) [#75656](https://github.com/StarRocks/starrocks/pull/75656) [#75883](https://github.com/StarRocks/starrocks/pull/75883)
- 外部テーブルの CBO 統計を改善しました：オプティマイザが完全なファイル列挙なしに Iceberg マニフェストから行数を推定できるようになり、Parquet / ORC 圧縮における Hive / Hudi の行数過小見積もりを修正し、JDBC コネクタ向けの非同期行数統計を追加し、Puffin 統計が利用できない場合の Iceberg および外部コネクタ向け NDV 推定フォールバックを提供します。[#75280](https://github.com/StarRocks/starrocks/pull/75280) [#75082](https://github.com/StarRocks/starrocks/pull/75082) [#75083](https://github.com/StarRocks/starrocks/pull/75083) [#75092](https://github.com/StarRocks/starrocks/pull/75092) [#75097](https://github.com/StarRocks/starrocks/pull/75097) [#75382](https://github.com/StarRocks/starrocks/pull/75382) [#75474](https://github.com/StarRocks/starrocks/pull/75474)
- Iceberg マニフェスト列統計が、クラスタ化列のみに対して選択的にキャッシュされるようになり、データファイルが多い幅広テーブルに対する FE ヒープ消費を削減しました。[#75395](https://github.com/StarRocks/starrocks/pull/75395)
- 外部テーブル統計収集が、FE 再起動や HA フェイルオーバーをまたいで永続的な述語列追跡をサポートするようになり、auto-ANALYZE が正しい列を対象にできるようになりました。[#75653](https://github.com/StarRocks/starrocks/pull/75653)
- スケジューリングから実行までの外部テーブル統計収集の全ライフサイクルをカバーする構造化された `[ExternalStats]` ログ行を追加しました。[#75335](https://github.com/StarRocks/starrocks/pull/75335) [#75529](https://github.com/StarRocks/starrocks/pull/75529)
- `SHOW ANALYZE STATUS` が、外部テーブル統計ジョブの Properties 列にパーティション、列、スナップショットのメタデータを含むようになりました。[#75630](https://github.com/StarRocks/starrocks/pull/75630)
- 各外部テーブルの統計ソース（`TABLE_METADATA`、`ANALYZE`、または `NONE`）が、クエリのランタイムプロファイルに表示されるようになりました。[#75253](https://github.com/StarRocks/starrocks/pull/75253)
- Iceberg および Delta Lake 外部テーブルに対するパーティションフィルター要件とパーティション数制限のサポートを追加しました（これまでは Hive、Hudi、Paimon でのみ利用可能でした）。[#75790](https://github.com/StarRocks/starrocks/pull/75790)
- `TABLE SAMPLE` およびヒストグラムの `ANALYZE` で 1% 未満のサンプリング比率をサポートし、計算された比率がゼロに切り捨てられてしまう大規模テーブルでの失敗を修正しました。[#74551](https://github.com/StarRocks/starrocks/pull/74551)
- `jemalloc_conf` BE 設定項目を追加し、jemalloc のランタイムオプションを `information_schema.be_configs` 経由で可視化できるようにしました。[#75344](https://github.com/StarRocks/starrocks/pull/75344)
- 保持されたチャンク容量を解放することで、共有なしモードでの主キーテーブルの Compaction 中のメモリ使用量を削減する `compaction_chunk_reset_memory_tracker_threshold_percent` BE 設定を追加しました。[#75091](https://github.com/StarRocks/starrocks/pull/75091)
- staros を v4.1.1 にアップグレードしました。これには再起動をまたいだ永続的な `datacache.enable`、ワーカーグループごとのシャードウォームアップタイムアウトのオーバーライド、S3 リトライジッターの改善が含まれます。[#75204](https://github.com/StarRocks/starrocks/pull/75204)
- SQL 文字列に資格情報マーカーが存在しない場合に正規表現スキャンをスキップすることで、監査ホットパス上の SQL 資格情報の編集処理を最適化しました。[#74812](https://github.com/StarRocks/starrocks/pull/74812)
- Parquet スキャナーの式駆動によるオンデマンドの遅延列ロードにより、マルチブランチの `OR` クエリでの不要な I/O を削減しました。[#74886](https://github.com/StarRocks/starrocks/pull/74886)
- `ds_hll_count_distinct` / `DataSketchesHll` が、順序依存の HIP 推定器ではなく複合推定器を使用することで、安定した基数推定を生成するようになりました。[#75053](https://github.com/StarRocks/starrocks/pull/75053)

### セキュリティ {/* #security-5 */}

- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] SNI ハンドラーのヒープ枯渇（DoS）、IPv6 サブネットフィルターのバイパス、DNS キャッシュポイズニングを修正するため、Netty を 4.1.135.Final にアップグレードしました。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- [CVE-2026-54512] [CVE-2026-54513] 2 件のデシリアライゼーション脆弱性を修正するため、`jackson-databind` を 2.21.4 にアップグレードしました。[#75373](https://github.com/StarRocks/starrocks/pull/75373)
- [GHSA-2r2c-cx56-8933] [GHSA-47qp-hqvx-6r3f] 認証なしの Telnet サーバー DoS 脆弱性を是正するため、Hadoop の推移的依存関係から `org.jline:jline-remote-telnet` を除外しました。[#75066](https://github.com/StarRocks/starrocks/pull/75066)
- [CVE-2026-39822] pprof バイナリの脆弱性を修正するため、pprof のプリビルドを更新しました。[#76248](https://github.com/StarRocks/starrocks/pull/76248) [#74669](https://github.com/StarRocks/starrocks/pull/74669)
- `information_schema.task_runs` において、述語の値に含まれる単一引用符がリテラル境界をエスケープしてしまう SQL インジェクションを修正しました。[#75520](https://github.com/StarRocks/starrocks/pull/75520)
- `tencent.cos.access_key`、`tencent.cos.secret_key`、および `iceberg.catalog.jdbc.password` が `SHOW CREATE CATALOG` の出力でマスクされるようになりました。[#74696](https://github.com/StarRocks/starrocks/pull/74696)
- 入力が切り詰められたパーセントエスケープシーケンスで終わる場合に `url_decode` で発生していた境界外読み取りを修正しました。[#75139](https://github.com/StarRocks/starrocks/pull/75139)
- `HyperLogLog::deserialize` が範囲外の `SPARSE` レジスタインデックスを受け入れてしまい、不正な入力によってヒープメモリが破損し BE がクラッシュする可能性があった問題を修正しました。[#75521](https://github.com/StarRocks/starrocks/pull/75521)
- `bar()` が負の幅の値を拒否しておらず、文字列が無制限に増大して BE のメモリが枯渇する可能性があった問題を修正しました。[#75143](https://github.com/StarRocks/starrocks/pull/75143)

### バグ修正 {/* #bug-fixes-5 */}

以下の問題が修正されました：

- `add_files` が Iceberg のファイル境界値に、論理型の値ではなく Parquet の物理エンコーディングのバイト列を設定しており、ファイルレベルの最小値/最大値プルーニングが誤って行われていました（例えば `DECIMAL` 列など）。[#69207](https://github.com/StarRocks/starrocks/pull/69207)
- `ApplyTuningGuideRule` が、入力リストが不変の `List.of(...)` として構築されたプランノードを走査する際に `UnsupportedOperationException` をスローしていました。[#70785](https://github.com/StarRocks/starrocks/pull/70785)
- `INSERT OVERWRITE` の 2 段階再プランニングで、最初のプランニングセッションから古いラムダ引数の列参照 ID が生成されることがあり、`expr_type does not match slot_type` エラーが発生していました。[#73273](https://github.com/StarRocks/starrocks/pull/73273)
- GIN（転置）インデックスを持つテーブルに対する部分更新で、更新対象から GIN インデックス付き列が除外されている場合、クエリが無期限にハングしたり失敗したりしていました。[#73773](https://github.com/StarRocks/starrocks/pull/73773)
- Lake PCU（部分列更新）で、rowset のスキーマと tablet のスキーマとの間にスキーマドリフトが発生した場合に、クラッシュしたり、データが静かに破損したりしていました。[#74005](https://github.com/StarRocks/starrocks/pull/74005)
- 外部 Iceberg テーブルに対する複数のスキーマ句を含む結合された `ALTER TABLE` が、各句のディスパッチのたびに all ですでにキューに入っていたアクションを誤って再実行していました。[#74036](https://github.com/StarRocks/starrocks/pull/74036)
- パーティションフラッシュとリソースグループキャンセルの処理が交錯した際に、`num_rows` のスナップショットが実際のチャンク行数を超えると `PartitionedSpillerWriter` が `SIGSEGV` でクラッシュしていました。[#74081](https://github.com/StarRocks/starrocks/pull/74081)
- BE のシグナル初期化処理で SIGPIPE が無視されていなかったため、起動中（通常はデプロイ直後）に BE プロセスが予期せず終了することがありました。[#74424](https://github.com/StarRocks/starrocks/pull/74424)
- 行範囲フィルタリングによって struct VARCHAR サブフィールドの埋め込みがスキップされた場合、Parquet の一時的な dict-code 列が上位レイヤーに漏れ出し、タイプの不一致を引き起こしていました。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `SELECT ... INTO OUTFILE` が監査ログに実際のエクスポート行数ではなく `ReturnRows=0` を記録していました。[#74467](https://github.com/StarRocks/starrocks/pull/74467)
- `TabletChecker.doCheck()` が `blockingAddTabletCtxToScheduler` 内でロックタイプの不一致により `IllegalMonitorStateException` をスローし、チェッカーのラウンド全体が黙って中断していました。[#74596](https://github.com/StarRocks/starrocks/pull/74596)
- `information_schema.COLUMNS` が `DATETIME_PRECISION` に対して常に `NULL` を返しており、このフィールドから列サイズを取得する MySQL プロトコルクライアントが壊れていました。[#74623](https://github.com/StarRocks/starrocks/pull/74623)
- クエリが異なるデータベースまたはカタログにまたがる同名（非修飾名）の 2 つのテーブルを結合した場合、MV の更新が `Duplicate key` で失敗していました。[#74730](https://github.com/StarRocks/starrocks/pull/74730)
- 特定の条件下で、スピル可能なハッシュジョインの探索処理がクラッシュしていました。[#74978](https://github.com/StarRocks/starrocks/pull/74978) [#75140](https://github.com/StarRocks/starrocks/pull/75140)
- Iceberg の `truncate` および `bucket` 変換関数で、幅またはバケット数の引数がゼロの場合に BE が `SIGFPE` でクラッシュしていました。[#74998](https://github.com/StarRocks/starrocks/pull/74998)
- `mod()` および `pmod()` で、被除数が `TYPE_MIN` かつ除数が `-1` の場合に BE が `SIGFPE` でクラッシュしていました。[#74980](https://github.com/StarRocks/starrocks/pull/74980)
- `bucket_num` がゼロまたは負の場合に `histogram()` が BE を `SIGFPE` でクラッシュさせていました。[#75041](https://github.com/StarRocks/starrocks/pull/75041)
- all の入力行が `NULL` の場合に `encode_fingerprint_sha256` が `SIGSEGV` でクラッシュしていました。[#75042](https://github.com/StarRocks/starrocks/pull/75042)
- 単一文字のワイルドカード `_` を含む `LIKE` パターンが、GIN の転置インデックスを介して評価された場合に誤った結果を返していました。[#75551](https://github.com/StarRocks/starrocks/pull/75551)
- AND のみの `MATCH` クエリが GIN の転置インデックスに対して実行された際、対象のセグメントが空の場合に偽のエラーを返していました。[#75161](https://github.com/StarRocks/starrocks/pull/75161)
- CLucene の `match_all` クエリが誤った結果を返していましたが、CLucene の依存関係をアップグレードすることで解決しました。[#75180](https://github.com/StarRocks/starrocks/pull/75180)
- ベクトルインデックスの書き換えで、共有テーブルスキーマに直接合成の距離列が登録されてしまい、同一テーブルに対する無関係な並行クエリで `Multiple entries with same key` エラーが発生していました。[#74785](https://github.com/StarRocks/starrocks/pull/74785)
- ジョインの並べ替えによるプルーニングで、スキャン述語がまだ参照している列がプルーニングされてしまうことがあり、統計情報の推定時に `missing statistic of col` がスローされていました。[#74791](https://github.com/StarRocks/starrocks/pull/74791)
- `avg(DISTINCT x)` が sum/count マテリアライズドビューを介して誤って書き換えられ、`DISTINCT` が黙って削除され、重複が存在する場合に誤った結果を返していました。[#75071](https://github.com/StarRocks/starrocks/pull/75071)
- `ALTER TABLE ... MODIFY COLUMN ... AFTER <nonexistent_col>` が、明確なセマンティックエラーではなく内部的な `NullPointerException` をスローしていました。[#75073](https://github.com/StarRocks/starrocks/pull/75073)
- `COLUMNS TERMINATED BY` 句を持たないジョブに対して、`SHOW CREATE ROUTINE LOAD` が最初のロード記述句の前に不要な先頭カンマを出力していました。[#75522](https://github.com/StarRocks/starrocks/pull/75522)
- CTE を含む `SECURITY INVOKER` のビューで、CTE 名が実在のテーブル参照と誤認された場合、NPE により権限チェックが失敗することがありました。[#74813](https://github.com/StarRocks/starrocks/pull/74813)
- 日時/日付時刻の境界リテラルのシフトにより表現可能な範囲をオーバーフローする場合（例えば `<= '9999-12-31'`）、`ReduceCastRule` がクエリプランニングを `SemanticException` で中断していました。[#75036](https://github.com/StarRocks/starrocks/pull/75036)
- ジョイン条件で null セーフイコール（`<=>`）の論理和（disjunct）が使用されている場合、`SplitJoinORToUnionRule` が重複した行を出力していました。[#75038](https://github.com/StarRocks/starrocks/pull/75038)
- 複数テーブルにまたがる外部クエリの並行メタデータ準備スレッド間で共有されている `Tracers` が、`enable_profile=true` 下で `IllegalStateException` を引き起こしていました。[#74746](https://github.com/StarRocks/starrocks/pull/74746)
- `ChunksPartitioner` 内のパーティションコンシューマーのエラーが黙って破棄されており、パーティション化された TopN がエラーを表面化させずに部分的または誤った結果を返すことがありました。[#74693](https://github.com/StarRocks/starrocks/pull/74693)
- BE の vacuum タスクが、FE 呼び出し元のタイムアウト経過後もゾンビとして動作し続け、`RELEASE_SNAPSHOT` スレッドプールを枯渇させて vacuum のスループットを低下させていました。[#74694](https://github.com/StarRocks/starrocks/pull/74694)
- autovacuum の競合状態により、`minActiveTxnId` が一時的に実行中のトランザクションより 1 大きい値として計算されることがあり、BE がまだ必要な結合トランザクションログを削除してしまい、publish が永続的にスタックしていました。[#74906](https://github.com/StarRocks/starrocks/pull/74906)
- FE の EOS キャンセルと BE のステージ 2 デプロイの間の競合状態により、クエリが正常に完了した後でもキャンセルされたと誤って記録されることがありました。[#75009](https://github.com/StarRocks/starrocks/pull/75009)
- 集計 TopN ランタイムフィルタのビルドキーが `ConstColumn` の場合、`AggTopNRuntimeFilterUpdaterImpl` で `SIGSEGV` により BE がクラッシュしていました。[#74809](https://github.com/StarRocks/starrocks/pull/74809) [#74941](https://github.com/StarRocks/starrocks/pull/74941)
- all の非 null 配列が空の場合、`array_map` / `transform` が `NULL` 行を黙って削除し、誤った行数を返していました。[#75141](https://github.com/StarRocks/starrocks/pull/75141)
- JIT コンパイルされた式において、2^64 を超える `LARGEINT` / `DECIMAL128` リテラルが黙って 64 ビットに切り捨てられていました。[#75137](https://github.com/StarRocks/starrocks/pull/75137)
- UTF-8 文字列関数（`split`、`split_part`、`str_to_map`）は、最後の文字が切り詰められた、または不正なマルチバイトのリードバイトを持ち、デリミタが空の場合に、文字列の終端を超えて読み取っていました。[#75068](https://github.com/StarRocks/starrocks/pull/75068)
- `parse_json()` は、`ALLOW_THROW_EXCEPTION` SQL モードであってもクエリを失敗させる代わりに、不正な形式の JSON に対して黙って `NULL` を返していました。[#74976](https://github.com/StarRocks/starrocks/pull/74976)
- 厳格モードの数値縮小キャストが、スロットデータが未定義の `NULL` 行に対して誤ってオーバーフローエラーを発生させていました。[#74903](https://github.com/StarRocks/starrocks/pull/74903)
- サブ秒部分が非ゼロである 1970 年以前の Parquet `INT64` タイムスタンプが、負の切り捨て除算の余りが原因でデコード時に不正な値になっていました。[#75207](https://github.com/StarRocks/starrocks/pull/75207)
- 1970 年以前の ORC `TIMESTAMP` 値は、ロード時にサブ秒成分が欠落していました。[#75432](https://github.com/StarRocks/starrocks/pull/75432)
- ORC ストライプの min/max タイムスタンプ統計が、1970 年以前およびサブ秒の境界値に対して誤ってデコードされており、データファイルが誤ってプルーニングされる原因となっていました。[#75543](https://github.com/StarRocks/starrocks/pull/75543)
- `ARRAY`、`MAP`、または `STRUCT` 列内にネストされた `INT96` Parquet タイムスタンプは、ロード時にセッションのタイムゾーンオフセットが 1 つ失われていました。[#74868](https://github.com/StarRocks/starrocks/pull/74868)
- Parquet の `UINT_32` 値が `BIGINT` 列にロードされる際、ゼロ拡張ではなく符号拡張されており、上位ビットが立った符号なし整数に対して黙って負の値が格納されていました。[#75002](https://github.com/StarRocks/starrocks/pull/75002)
- `HiveDataSource` のデストラクタが、`_scanner_ctx`（それらのノードを参照する述語を保持する）より前に `_pool`（およびその `Expr` ノード）を破棄していたため、ヒープの use-after-free が発生していました。[#74818](https://github.com/StarRocks/starrocks/pull/74818)
- OpenX SerDe を使用して gzip 圧縮された JSON Hive 外部テーブルを読み取る際、マルチバイト UTF-8 文字が 8 MB の解凍バッファ境界をまたぐと `UTF8_ERROR` で失敗していました。[#74827](https://github.com/StarRocks/starrocks/pull/74827)
- ADLS2 `ListPaths` は、クライアントが無条件にアクセスする JSON フィールドが存在しない非 HNS アカウントで `SIGSEGV` によりクラッシュしていました。[#75166](https://github.com/StarRocks/starrocks/pull/75166)
- 複数の `UNNEST` オペレーターが同じ入力配列列を共有し、異なるサブフィールドを消費する場合、`unnest` がクラッシュするか誤った結果を返していました。[#75012](https://github.com/StarRocks/starrocks/pull/75012) [#75445](https://github.com/StarRocks/starrocks/pull/75445) [#76002](https://github.com/StarRocks/starrocks/pull/76002)
- `query_mem_limit` が `unnest` の実行中に強制されておらず、大きな配列に対する `unnest` がクエリを失敗させる代わりに BE を OOM キルさせることが可能になっていました。[#75179](https://github.com/StarRocks/starrocks/pull/75179)
- `RANK` 境界を伴う `TopN` は、ランク制限がチャンク境界にちょうど一致する場合に 1 行を欠落させていました。[#75045](https://github.com/StarRocks/starrocks/pull/75045)
- `PushDownDistinctAggregateRule` の後の列プルーニングが、空の分析（ウィンドウ）オペレーターを生成し、プランニングまたは実行エラーを引き起こす可能性がありました。[#74810](https://github.com/StarRocks/starrocks/pull/74810)
- `EliminateSortColumnWithEqualityPredicateRule` がグローバルな制限を設定せずにスキャンオペレーターのみに行数制限を設定していたため、制限付きサブクエリに対する `COUNT(*)` が同時実行下で予想以上の行数を返していました。[#74983](https://github.com/StarRocks/starrocks/pull/74983)
- Lake 主キー永続性インデックスの再構築が、セグメント範囲モードで誤ったセグメントイテレーター位置を使用しており、不正なキー範囲フィルタの適用を引き起こしていました。[#74887](https://github.com/StarRocks/starrocks/pull/74887) [#75206](https://github.com/StarRocks/starrocks/pull/75206)
- `DROP PERSISTENT INDEX` がテーブルロックなしで `rebuildPindexVersion` を変更していました。また `RestoreJob` はリストア後に DB READ ロックのみの状態で MV のベーステーブル情報を変更しており、`FinalizeCreateTableAction` は DB レベルのロックをイテレーター作成をまたいで渡していました。[#74968](https://github.com/StarRocks/starrocks/pull/74968)
- `dumpImage` は、データベースごとのロック取得がループの途中で例外を投げた場合、グローバルメタロックを無期限に保持したままにする可能性がありました。[#75488](https://github.com/StarRocks/starrocks/pull/75488)
- マルチステートメントの Stream Load は、トランザクションごとに `TxnStateCallbackFactory` エントリを 1 つリークしており、際限なく増加して最終的に FE のヒープを枯渇させていました。[#75188](https://github.com/StarRocks/starrocks/pull/75188)
- catalog、データベース、テーブル、またはパーティション名が長い場合、ヒストグラム統計の `information_schema.task_runs` 行数が `primary_key_limit_size`（128 バイト）をオーバーフローする可能性がありました。[#75735](https://github.com/StarRocks/starrocks/pull/75735)
- BE の JVM メトリクスが、不正な Prometheus `# TYPE` 行（メトリック名内にラベルセットが含まれる）を出力しており、Prometheus がスクレイプ全体を中断させる原因となっていました。[#75240](https://github.com/StarRocks/starrocks/pull/75240)
- `SHOW PARTITIONS` および `information_schema.partitions_meta` は、共有データテーブルにおいて、all 物理パーティションのバケット数を、各パーティションの実際のバケット数ではなくテーブルレベルのデフォルト値として報告していました。[#75734](https://github.com/StarRocks/starrocks/pull/75734)
- `SHOW PROC '.../index_schema/<id>'` は、共有データ（`CLOUD_NATIVE`）テーブル上の all ロールアップインデックスに対して、ベーステーブルのスキーマを返していました。[#76069](https://github.com/StarRocks/starrocks/pull/76069)
- `ALTER TABLE ... MODIFY COLUMN` の no-op 句が誤って軽量コメントパスにルーティングされており、バッチ `ALTER TABLE` ステートメントで `MODIFY COLUMN COMMENT can not be combined with other alter operations` エラーが発生していました。[#75736](https://github.com/StarRocks/starrocks/pull/75736)
- `isCommentOnlyModification` は、`isKey` / `aggregationType` の正規化が不正確であったため、キー列や集計列をコメントのみの変更と誤認識する可能性がありました。[#75545](https://github.com/StarRocks/starrocks/pull/75545)
- `ALTER VIEW` が循環参照するビュー定義をコミットしてしまう可能性があり、その後の `SELECT` が `StackOverflowError` をスローする原因となっていました。[#75033](https://github.com/StarRocks/starrocks/pull/75033)
- `accept()` が前のチャンクへのポインタをまだ保持している間に、後続のコンシューマーがそのチャンクを変更すると、`OrderedPartitionExchanger` によりヒープの use-after-free が発生していました。[#75279](https://github.com/StarRocks/starrocks/pull/75279)
- ビルド側のスロット記述子が非 nullable であるにもかかわらず、ランタイム状態が nullable である場合、NLJoin がクラッシュしていました。[#75343](https://github.com/StarRocks/starrocks/pull/75343) [#75788](https://github.com/StarRocks/starrocks/pull/75788)
- 構造体のフィールド名が解析可能な JSON パスでない場合、`CAST(json/variant AS struct)` がフラグメントの準備段階で BE をクラッシュさせていました。[#75355](https://github.com/StarRocks/starrocks/pull/75355)
- ネストされた辞書式のための辞書デコードが、プロデューサーフラグメントとコンシューマーフラグメントの間で互換性のない辞書変換を生成する可能性があり、実行時に `Dict Decode failed` エラーを引き起こしていました。[#75246](https://github.com/StarRocks/starrocks/pull/75246)
- `get_rowset_by_version` が `nullptr` を返し、`gtid` の比較が null チェックより前に配置されていた場合、スキーマ変更が null ポインタ参照でクラッシュしていました。[#74855](https://github.com/StarRocks/starrocks/pull/74855)
- スナップショット manager が、親 tablet メタデータを回収するかどうかを判断する際に reshard ジョブを考慮していなかったため、tablet の split/merge 後に共有データクラスタのスナップショットが復元不能になっていました。[#75638](https://github.com/StarRocks/starrocks/pull/75638)
- ファイルバンドリングの vacuum が、兄弟 tablet のゼロ行バンドルセグメントを非共有として誤ってフラグ付けしており、他の tablet がまだ参照している間にそのバンドルファイルが削除される原因となっていました。[#75689](https://github.com/StarRocks/starrocks/pull/75689)
- 共有データテーブルの Compaction パブリッシュが、Compaction トランザクション開始後に可視になったロールアップ/同期 MV インデックスをドロップしており、バンドルファイルにそれらのインデックスが含まれない状態になっていました。[#76105](https://github.com/StarRocks/starrocks/pull/76105)
- tablet の reshard 中に Compaction がドロップされた場合、共有データの永続性インデックス Compaction が、パススルーで再利用されている SSTable ファイルを誤って削除していました。[#75726](https://github.com/StarRocks/starrocks/pull/75726)
- `NOT NULL` から nullable へのフラット JSON 列のスキーマ変更が、Compaction の読み取りパスで `CHECK` クラッシュを引き起こしていました。[#75680](https://github.com/StarRocks/starrocks/pull/75680)
- Nullable 列に対する `count_combine` が、ストリーミング事前集計のパススルーパスで `SIGSEGV` によって BE をクラッシュさせていました。[#75298](https://github.com/StarRocks/starrocks/pull/75298)
- JDK 21 で削除されたリフレクティブな `DirectByteBuffer` コンストラクタ検索が原因で、Java UDF が JDK 21 以降でロードに失敗していました。[#75666](https://github.com/StarRocks/starrocks/pull/75666)
- パーサーが `CREATE TABLE AS SELECT` 内の `ENGINE` 句をサポートしていなかったため、Unified catalog（Hive メタストア）への `CTAS` が常に失敗していました。[#75771](https://github.com/StarRocks/starrocks/pull/75771)
- `JoinTuningGuide` のフィードバック駆動ジョイン再構築で `predicateCommonOperators` が失われ、共通部分式の再利用を含むプランで `InputDependenciesChecker` の検証エラーが発生していました。[#75773](https://github.com/StarRocks/starrocks/pull/75773)
- サブパーティションを持つテーブルで、バージョンリストが構築される前に空のサブパーティションがプルーニングされたため、クエリキャッシュの正規化処理が `Preconditions.checkState` でクラッシュしていました。[#75789](https://github.com/StarRocks/starrocks/pull/75789)
- `replayFromJson` がレガシーなエイリアスの下に保存されたセッション変数を無言でスキップしていたため、クエリダンプの再生がデフォルト値にフォールバックしていました。[#75813](https://github.com/StarRocks/starrocks/pull/75813)
- 行グループの開始オフセットが二重にカウントされていたため、複数の Parquet 行グループを持つデータファイルに対して Iceberg の `_row_id` 仮想列が不正な値を返していました。[#75758](https://github.com/StarRocks/starrocks/pull/75758)
- Iceberg の DELETE/UPDATE プランナーが、物理的なテーブルの識別情報ではなく合成テーブル ID によるマッチングを行っていたため、ターゲットのスキャンノードを特定できず、ベーススナップショット ID と競合検出フィルターが失われていました。[#76013](https://github.com/StarRocks/starrocks/pull/76013)
- `PipelineExecutorSet::start()` が呼び出される前に `cancel_plan_fragment` RPC が到着した場合に、`FragmentContext::set_final_status` が `SIGSEGV` でクラッシュしていました。[#75030](https://github.com/StarRocks/starrocks/pull/75030)
- `FragmentExecutor` がフラグメントの破棄処理をまだ行っている最中に `QueryContext` が回収される可能性があり、`ResGuard::reset()` で heap-use-after-free が発生していました。[#74978](https://github.com/StarRocks/starrocks/pull/74978)
- `StringSearch::_pattern` が初期化されていなかったため、デフォルト構築された `search()` が未初期化のポインタを参照できてしまっていました。[#75614](https://github.com/StarRocks/starrocks/pull/75614)
- `DATETIME` マイクロ秒が JVM のデフォルトロケールの数字セットを使用して描画されていたため、アラビア語やペルシア語などのロケールで非 ASCII 数字が発生し、tablet の事前分割における境界値のパースが壊れていました。[#75001](https://github.com/StarRocks/starrocks/pull/75001)
- tablet の統計情報が更新される前に `INSERT OVERWRITE` が実行されると、パーティションの行数が `_statistics_.column_statistics` にゼロとして書き込まれる場合があり、オプティマイザがパーティションのカーディナリティ推定を崩壊させていました。[#74801](https://github.com/StarRocks/starrocks/pull/74801)
- グローバル設定で無効化されている場合、`enable_statistic_collect_on_first_load` のテーブルレベルのオーバーライドで初回ロード時の統計情報収集を有効にできませんでした。[#74794](https://github.com/StarRocks/starrocks/pull/74794)
- UNION の一方の分岐に入力行がない場合、`PushDownNonGroupedAggregateBelowUnion` が非 Nullable として宣言された型に対して Nullable な出力を生成し、BE の `CHECK` エラーを引き起こしていました。[#76101](https://github.com/StarRocks/starrocks/pull/76101)

## 4.1.2 {/* #412 */}

リリース日：2026年6月18日

### 動作の変更 {/* #behavior-changes-5 */}

- ユーザーが権限を持たないデータベースへの接続時に、ERROR 2013 で接続を閉じる代わりに適切な MySQL エラーパケットが返されるようになりました。[#70072](https://github.com/StarRocks/starrocks/pull/70072)
- `SHOW FUNCTIONS` は、関数レベルの権限によって関数を参照できるものの、関数作成スコープの権限を持たないユーザーに対して、UDF ファイルおよびオブジェクトファイルのパスを `***` としてマスクするようになりました。[#73425](https://github.com/StarRocks/starrocks/pull/73425)
- 外部 catalog から Hive ビューをクエリする際に、Ranger の行フィルターおよび列マスキングポリシーが正しく適用されるようになりました。[#73265](https://github.com/StarRocks/starrocks/pull/73265)
- `ALTER TABLE ... ADD COLUMN ... DEFAULT current_timestamp` は `current_timestamp` のジェネレーター式を正しく保持するようになりました。`DESCRIBE` および `information_schema` は、バックフィル時点のリテラル値ではなく式そのものを反映するようになりました。[#73455](https://github.com/StarRocks/starrocks/pull/73455)
- `information_schema.loads` のロード時フィルタリングが、セッションタイムゾーンが UTC+8 と異なるクラスタでフィルター境界をずらすことがなくなりました。ロード時刻は FE–BE 間で UTC エポックミリ秒としてやり取りされるようになりました。[#73365](https://github.com/StarRocks/starrocks/pull/73365)
- `connector_max_split_size` セッション変数が、常にデフォルト値を使用する代わりに、Paimon スキャンの分割計算に正しく適用されるようになりました。[#71756](https://github.com/StarRocks/starrocks/pull/71756)
- `pipeline_enable_large_column_checker` がデフォルトで有効になりました。[#72798](https://github.com/StarRocks/starrocks/pull/72798)
- Hive のパーティション統計情報が、タイマーによってキーごとに自動更新されなくなりました。パーティション統計情報は、明示的な `refreshTable()` 呼び出し時にのみ更新されるようになり、大規模なパーティションテーブルにおける HMS の負荷が軽減されます。[#73563](https://github.com/StarRocks/starrocks/pull/73563)
- Iceberg または外部 catalog のベーステーブルにスキーマドリフト（列タイプの変更、列の削除、テーブルの削除）が発生した場合、依存するマテリアライズドビューは、NULL 行を無言で生成したり不明瞭なエラーを出したりする代わりに、次回のリフレッシュ時に非アクティブとしてマークされるようになりました。[#73770](https://github.com/StarRocks/starrocks/pull/73770)
- Iceberg コネクタは、`AND` の複合述語について、片方のみ変換可能な場合に述語全体を破棄するのではなく、変換可能な側の部分的なプッシュダウンを行うようになり、パーティションプルーニングとデータスキッピングが改善されました。[#70293](https://github.com/StarRocks/starrocks/pull/70293)
- 明示的トランザクションの `COMMIT` が、データベース書き込みロックを最大 `query_timeout` 秒（ミリ秒ではなく）正しく待機するようになり、短時間の同時書き込みアクティビティによる誤ったロックタイムアウトエラーが発生しなくなりました。[#73549](https://github.com/StarRocks/starrocks/pull/73549)
- IVM のリフレッシュ処理が、フィルタリングされた行を無言で破棄する代わりに、厳格ロードフィルターのエラーを呼び出し元に通知するようになりました。[#73938](https://github.com/StarRocks/starrocks/pull/73938)
- `count_combine(nullable_col)` は `COUNT(col)` のセマンティクスに合わせ、NULL 行を正しく除外するようになりました。`COUNT(<nullable column>)` を基にしたインクリメンタル MV は、以前は水増しされたカウントをマテリアライズしていました。[#74029](https://github.com/StarRocks/starrocks/pull/74029)
- `SHOW ALTER TABLE COLUMN` は、cloud ネイティブ（共有データ）テーブル上の `file_bundling` や `enable_persistent_index` などのプロパティに対して `ALTER TABLE ... SET (...)` によってトリガーされる非同期のメタデータのみの ALTER ジョブも表示するようになりました。[#74198](https://github.com/StarRocks/starrocks/pull/74198)
- 集計関数を参照する `HAVING` 句を使ってインクリメンタル MV を作成すると、初回リフレッシュ時に内部プランエラーを発生させる代わりに、`CREATE` 時点で明確なエラーとして失敗するようになりました。[#74054](https://github.com/StarRocks/starrocks/pull/74054)
- IVM は、インクリメンタルマテリアライズドビューにおいて `MIN`/`MAX(DECIMAL)` 集計関数をサポートするようになりました。[#73969](https://github.com/StarRocks/starrocks/pull/73969)
- IVM のアダプティブリフレッシュは、最初のデルタ特性がすでに `mv_max_rows_per_refresh` を超えている場合に、デルタウィンドウを正しく制限するようになり、1回のタスク実行でバックログ全体がリフレッシュされることを防ぎます。[#74464](https://github.com/StarRocks/starrocks/pull/74464)
- GROUP-BY のみのインクリメンタル MV（例えば `SELECT k FROM t GROUP BY k`）は、`__ROW_ID__` を VARCHAR として正しくエンコードするようになり、2回目のリフレッシュでのクラッシュが修正されました。[#74030](https://github.com/StarRocks/starrocks/pull/74030)

### 改善 {/* #improvements-6 */}

- `CREATE`/`REPLACE`/`DROP`、`SHOW`/`DESC` を含む Paimon ビュー、および外部 catalog からの Paimon ビューのクエリをサポートします。Paimon ビュー内のテーブル参照は、`default_catalog` ではなく Paimon の catalog に対して解決されるようになりました。[#56058](https://github.com/StarRocks/starrocks/pull/56058) [#70217](https://github.com/StarRocks/starrocks/pull/70217)
- スキーマドリフトや複雑なネスト型を持つファイルを読み取る際に安定したスキーマ制御を行うため、`FILES()` に明示的な `schema` パラメーターをサポートします。[#72033](https://github.com/StarRocks/starrocks/pull/72033)
- `get_query_profile()` は、接続中の FE だけでなく、all の FE ノード全体のクエリプロファイル情報を取得するようになりました。[#71123](https://github.com/StarRocks/starrocks/pull/71123)
- 現在実行中のクエリの UUID を返す組み込み関数 `query_id()` を追加しました。[#73621](https://github.com/StarRocks/starrocks/pull/73621)
- 共有データモードの `CREATE`/`ALTER STORAGE VOLUME` は、メタデータを永続化する前にストレージの場所のアクセス可能性（認証情報とエンドポイント）を検証するようになり、設定ミスを早期に検出できるようになりました。[#70053](https://github.com/StarRocks/starrocks/pull/70053)
- BE における AWS S3 認証情報用の `WebIdentity` トークンプロバイダーのサポートを追加し、`AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE` に対する既存の FE サポートと一致させました。[#69966](https://github.com/StarRocks/starrocks/pull/69966)
- 欠落した `txnlog`、セグメントの消失、またはリモート I/O の遅延によって publish が恒久的にブロックされている場合に、共有データテーブル上の `COMMITTED` トランザクションのスタックを解消する `ADMIN SKIP COMMITTED TRANSACTION` コマンドを追加しました。[#73553](https://github.com/StarRocks/starrocks/pull/73553)
- `information_schema.tables_config` は `table_name` 述語を FE にプッシュダウンするようになり、単一テーブルのルックアップにおけるオーバーヘッドを大幅に削減しました。[#73210](https://github.com/StarRocks/starrocks/pull/73210)
- MySQL 8 のスキーマを接続時の検査で参照する BI ツールや JDBC ドライバーとの互換性を向上させるため、`information_schema` テーブルに不足していた MySQL 8 の列を追加しました。[#73370](https://github.com/StarRocks/starrocks/pull/73370)
- `false` に設定した場合にセッション変数を上書きする、クラスター全体のキルスイッチとして機能する BE 設定項目 `enable_pipeline_event_scheduler` を追加しました。[#73264](https://github.com/StarRocks/starrocks/pull/73264)
- 複数のワイド文字列列を持つテーブルで統計情報を収集する際のクエリごとのメモリピークを削減するため、統計情報収集向けのオプトインのワイド文字列列分離を追加しました。[#73258](https://github.com/StarRocks/starrocks/pull/73258)
- ロックの競合が激しい状況下での JVM セーフポイントの停滞を防ぐため、スローロックのログ記録がイベント単位のレートリミットと設定可能なスタックキャプチャ制御をサポートするようになりました。[#73647](https://github.com/StarRocks/starrocks/pull/73647)
- マテリアライズドビューのリフレッシュログのエントリにデータベース名がプレフィックスとして含まれるようになり、同じマテリアライズドビュー名が複数のスキーマに存在するマルチテナント環境でもログ行を区別できるようになりました。[#73521](https://github.com/StarRocks/starrocks/pull/73521)
- `enable_profile_log` FE 設定項目が可変になり、FE を再起動することなく `ADMIN SET FRONTEND CONFIG` によって実行時に切り替えられるようになりました。[#73894](https://github.com/StarRocks/starrocks/pull/73894)
- ロードプロファイル（Stream Load、Routine Load、Broker Load、および merge-commit load）を `fe.profile.log` に書き込む FE 設定項目 `enable_print_load_profile_to_log`（デフォルト `false`）を追加しました。これにより、クエリプロファイルのバーストによってインメモリストアが退避された場合でもプロファイルが保持されます。[#74150](https://github.com/StarRocks/starrocks/pull/74150)
- `SHOW ROUTINE LOAD` は、Java のオブジェクト参照ではなく `JobProperties` 内の列マッピングを正しくレンダリングするようになりました。[#74199](https://github.com/StarRocks/starrocks/pull/74199)
- `CachingIcebergCatalog` は catalog レベルのロックではなくテーブルレベルのロックを使用するようになり、多数のテーブルが同時にアクティブな catalog でのリフレッシュのシリアライズ遅延を削減しました。[#73079](https://github.com/StarRocks/starrocks/pull/73079)
- メタスキャン（バックグラウンドの統計情報収集）は、`ADD COLUMN`、`DROP COLUMN`、`RENAME COLUMN`、`REORDER COLUMN` の schema change を適切に処理するようになり、変更後のセグメントファイルで not-found エラーが発生して失敗することがなくなりました。[#72901](https://github.com/StarRocks/starrocks/pull/72901)
- サンプルベースの tablet の事前分割が、複数パーティションのレンジ分散テーブルおよび Broker Load をカバーするようになり、既存のデータ階層のベースラインがなくても初回ロードの並列処理が可能になりました。[#73101](https://github.com/StarRocks/starrocks/pull/73101) [#73912](https://github.com/StarRocks/starrocks/pull/73912) [#74048](https://github.com/StarRocks/starrocks/pull/74048)
- MySQL の結果のシリアライズで、行単位の仮想ディスパッチを使用しなくなりました。タイプ付きの列ライターがチャンクごとに一度だけ構築されるようになり、幅広または大規模な結果セットのシリアライズオーバーヘッドが削減されました。[#66316](https://github.com/StarRocks/starrocks/pull/66316)
- `DATETIME`/`DATE` から文字列へのキャストが出力バッファへ直接書き込むようになり、行ごとのヒープ割り当てがなくなりました。[#73801](https://github.com/StarRocks/starrocks/pull/73801)
- クエリ統計のマージ処理で `SpinLock` の代わりにロックフリーの並列マップを使用するようになり、ワーカーが中間または最終的な統計情報を送信する際の大規模クラスターでの CPU 使用率が削減されました。[#73796](https://github.com/StarRocks/starrocks/pull/73796)
- 集計のハッシュマップとハッシュセットのプリフェッチが L2 キャッシュの常駐状況に基づいて制御されるようになり、バケット配列が L2 に収まる場合の 4〜9% の性能低下が回避されました。プリフェッチ距離は設定可能になりました。[#73943](https://github.com/StarRocks/starrocks/pull/73943)
- 共有データの主キーテーブルにおける軽量な Compaction の publish 向けに、パイプライン化されたセグメント単位の `.lcrm` 読み取りが導入され、逐次的なオブジェクトストレージへのラウンドトリップが削減されました。[#73992](https://github.com/StarRocks/starrocks/pull/73992)
- 共有データモードでのコールド PK インデックス再構築スキャンがセグメント間で並列化されるようになり、セグメント読み取りがリモート I/O に律速される場合の再構築時間が削減されました。[#74249](https://github.com/StarRocks/starrocks/pull/74249)
- 内部クエリ（統計情報収集、タスクの実行、マテリアライズドビューのリフレッシュ）が `SHOW PROC '/current_queries'` で可視化されるようになり、`KILL QUERY` で終了できるようになりました。[#74488](https://github.com/StarRocks/starrocks/pull/74488)
- S3 のスロットリングを監視し `lake_vacuum_min_batch_delete_size` をチューニングするための lake vacuum のバッチサイズおよびリトライ回数の bvar メトリクスを追加しました。[#74112](https://github.com/StarRocks/starrocks/pull/74112)
- ごみ箱の増加が FE のヒープを圧迫する前に把握できるよう、`CatalogRecycleBin` のサイズゲージメトリクスを追加しました。[#74440](https://github.com/StarRocks/starrocks/pull/74440)
- `LIST` でパーティション化されたテーブルは、レンジパーティション化されたテーブル向けに設計された最新 N 件のヒューリスティックを適用する代わりに、`OlapTableSink` で all パーティションをオープンするようになり、増分オープンの RPC オーバーヘッドが削減されました。[#74099](https://github.com/StarRocks/starrocks/pull/74099)
- `FILES()` または Broker Load を介して、`LARGE_LIST` および `FIXED_SIZE_LIST` の Arrow タイプを JSON 列にロードすることをサポートしました。[#73714](https://github.com/StarRocks/starrocks/pull/73714) [#73718](https://github.com/StarRocks/starrocks/pull/73718)
- 共有データテーブルにおける merge-commit（`FRONTEND_STREAMING`）ロードについて、トランザクションログとファイルのバンドルの組み合わせをサポートし、他のロードタイプと整合させました。[#74460](https://github.com/StarRocks/starrocks/pull/74460)
- FE を再起動することなく lake publish のフェーズ別内訳の警告しきい値を制御できる、可変 FE 設定項目 `slow_publish_partition_log_threshold_ms`（デフォルト 3000 ms）を追加しました。[#74043](https://github.com/StarRocks/starrocks/pull/74043)

### セキュリティ {/* #security-6 */}

- [CVE-2026-43869] 証明書ホスト検証の不備に対処するため、`libthrift` を 0.23.0 にバージョンアップしました。[#73243](https://github.com/StarRocks/starrocks/pull/73243)
- [CVE-2026-41293] HTTP/2 リクエストヘッダー検証に対処するため、Apache Tomcat を 9.0.118 にバージョンアップしました。[#73797](https://github.com/StarRocks/starrocks/pull/73797)
- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] SNI ハンドラーのヒープ枯渇（DoS）、IPv6 サブネットフィルターのバイパス、および DNS キャッシュポイズニングに対処するため、Netty を 4.1.135.Final にバージョンアップしました。[#74668](https://github.com/StarRocks/starrocks/pull/74668)
- Go 標準ライブラリのセキュリティ修正を含めるため、pprof のプリビルドバイナリを Go 1.25.11 にアップグレードしました。[#73545](https://github.com/StarRocks/starrocks/pull/73545) [#74669](https://github.com/StarRocks/starrocks/pull/74669)

### バグ修正 {/* #bug-fixes-6 */}

以下の問題が修正されました：

- URL に `host:port` パターン以外の場所に `:` が含まれている場合、`parse_url()` が誤ったホストを返していました。[#63542](https://github.com/StarRocks/starrocks/pull/63542)
- 辞書変換された式では、成り立たない式（例：`IF(col = '1', NULL, 'ok')`）に対しても `f(null) = null` が成り立つと誤って想定していました。[#69376](https://github.com/StarRocks/starrocks/pull/69376)
- トランザクション型の Stream Load がユーザー指定のタイムアウトではなくデフォルトの RPC タイムアウトを使用していたため、タイムアウトが早期に発生していました。[#67584](https://github.com/StarRocks/starrocks/pull/67584)
- アイデンティティ列に NULL 値を持つ Iceberg の equality delete ファイルで、ジョイン述語において `NULL = NULL` が UNKNOWN と評価されるため、一致する行を削除できていませんでした。[#67321](https://github.com/StarRocks/starrocks/pull/67321)
- `INJECTED` パーティションプロジェクション列を持つテーブルのエラーメッセージがより詳細になり、問題の原因となっている列が表示されるようになりました。[#68052](https://github.com/StarRocks/starrocks/pull/68052)
- insert-overwrite 操作が認識されなかったため、insert-only ACID Hive テーブルへのクエリが想定より多くの行を返していました。[#71460](https://github.com/StarRocks/starrocks/pull/71460)
- 同時読み取り中に Iceberg メタデータエントリがピン留めされた際、ディスクキャッシュが設定された容量を超えてオーバーフローしていました。[#71651](https://github.com/StarRocks/starrocks/pull/71651)
- external catalog へのクエリ時、Paimon の主キー列が誤って非 NULL 許容として扱われていました。[#71660](https://github.com/StarRocks/starrocks/pull/71660)
- 複数の `ARRAY_AGG(DISTINCT <const>)` 入力を持つクエリに対して `MultiDistinctByMultiFuncRewriter` が同じルールを繰り返し適用したため、オプティマイザがタイムアウトしていました。[#70605](https://github.com/StarRocks/starrocks/pull/70605)
- `DATE`/`TIMESTAMP` キーワードなしでプッシュダウンされた Oracle JDBC の日付述語が NLS フォーマットエラーを引き起こしていました。[#71412](https://github.com/StarRocks/starrocks/pull/71412)
- パーティション TopN で、子 Operator から必要な出力列が失われる場合がありました。[#72848](https://github.com/StarRocks/starrocks/pull/72848)
- パーティションエボリューションを持つ Iceberg テーブル上で、パーティション分割されていない MV を作成できませんでした。[#72285](https://github.com/StarRocks/starrocks/pull/72285)
- `information_schema.be_cloud_native_compactions` において、並列サブタスクの Compaction タスク統計が上書きされ失われていました。[#72331](https://github.com/StarRocks/starrocks/pull/72331)
- 同期 MV 向けの `SHOW CREATE MATERIALIZED VIEW` が「Table is not found」で失敗していました。[#73396](https://github.com/StarRocks/starrocks/pull/73396)
- ステートメントの `.log` ファイルが4セグメントパスに到達した場合、schema change 中に Lake publish のマルチステートメントトランザクションがデッドロックしていました。[#73423](https://github.com/StarRocks/starrocks/pull/73423)
- ソートマージプロバイダのエラーがフラグメントコンテキストに伝播せず、サイレントなクエリ失敗を引き起こしていました。[#73337](https://github.com/StarRocks/starrocks/pull/73337)
- 長時間稼働している Follower FE 上で `ConnectorTableId` が `int` から負の値にオーバーフローし、Iceberg および Hive クエリが誤解を招く「Invalid table type」エラーで失敗していました。[#73344](https://github.com/StarRocks/starrocks/pull/73344)
- 空の optimize 句（分散またはパーティション指定なし）を伴う `ALTER TABLE` が誤って解析され、FE リプレイ時にテーブルのデフォルト分散が破損する可能性がありました。[#73352](https://github.com/StarRocks/starrocks/pull/73352)
- `AZURE_PATH_KEY` が有効な `StorageVolumeMgr` パラメータとして認識されなかったため、ADLS2 共有データ災害復旧中に FE の起動が失敗していました。[#73509](https://github.com/StarRocks/starrocks/pull/73509)
- オプティマイザがネストされた型の一部を `UNKNOWN_TYPE` に刈り込んだ場合、または NULL 許容の array、map、struct スキーマが使用された場合に、Avro の複合型デコードが失敗していました。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- 2つの `NullableColumn` が同じ `NullColumn` オブジェクトを共有していたため、COW 列変更最適化が `map_apply` および類似の関数でクラッシュを引き起こしていました。[#73480](https://github.com/StarRocks/starrocks/pull/73480)
- プロバイダが FE 上で早期にインスタンス化されていたため、カスタム `LocationProvider` を持つ Iceberg テーブルが `ClassNotFoundException` を伴う `SELECT` クエリで失敗していました。[#73482](https://github.com/StarRocks/starrocks/pull/73482)
- JDBC `getTable()` がキャッシュミスのたびに余分な `getTableComment()` のラウンドトリップを実行し、負荷の高いプランニングフェーズのロック保持時間を延長させ、同時実行の DDL をブロックしていました。[#73488](https://github.com/StarRocks/starrocks/pull/73488)
- ネストされた MV が `FULL` または `UNKNOWN` のタイムリネスを返した場合、ネストされた MV のリフレッシュが `NullPointerException` をスローしていました。[#73644](https://github.com/StarRocks/starrocks/pull/73644)
- FE ワーカーが、速度の遅い MySQL クライアントへのクエリ結果送信で無期限にブロックされることがありました。結果送信パスには書き込みタイムアウトが適用されるようになりました。[#73646](https://github.com/StarRocks/starrocks/pull/73646)
- V1 エンコード（共有なし）から V2 エンコード（共有データ）のクラスタへ、または2つの共有データクラスタ間で主キーテーブルをレプリケートする際、PK `.del` ファイルがトランスコードされていませんでした。[#73649](https://github.com/StarRocks/starrocks/pull/73649) [#73958](https://github.com/StarRocks/starrocks/pull/73958)
- `VERSION_INCOMPLETE` の復旧中、古いレプリカ参照が新しいレプリカを追加する前に削除されなかったため、`TabletInvertedIndex` に重複したレプリカが蓄積していました。[#73661](https://github.com/StarRocks/starrocks/pull/73661)
- `REPLICATE_SNAPSHOT` タスクとファイルごとのコピーサブタスクが同じスレッドプールを共有していたため、共有データの Lake レプリケーションのファイルコピーが CN をクラッシュさせていました。[#73666](https://github.com/StarRocks/starrocks/pull/73666)
- BE によってユニットカウンタが `.000` 小数サフィックスでフォーマットされた場合、`RuntimeProfileParser` が `NumberFormatException` をスローしていました。[#73683](https://github.com/StarRocks/starrocks/pull/73683)
- 共有データ PK tablet 分割における共有セグメントの物理 rowid エンコーディングが不正で、誤った `rss_rowid` エントリを引き起こしていました。[#73686](https://github.com/StarRocks/starrocks/pull/73686)
- レンジコロケートとハッシュ分散が混在する `JOIN` クエリが、有効な結果の代わりに `Unknown error` を返していました。[#73702](https://github.com/StarRocks/starrocks/pull/73702)
- `TimeUtils.longToTimeString` は固定の UTC+8 フォーマッタを使用していましたが、出力はセッションの `time_zone` に従うようになりました。[#73619](https://github.com/StarRocks/starrocks/pull/73619)
- all の値が `NULL` であり、列が NULL 許容の単項関数パスを通過した場合、Decimal 型の列がスケールを失い、下流の結果タイプを破損させていました。[#73789](https://github.com/StarRocks/starrocks/pull/73789)
- ネストされた型に対する JSON の部分追加が ASAN クラッシュを引き起こしていました。[#73715](https://github.com/StarRocks/starrocks/pull/73715)
- `public` ロールの権限キャッシュが `GRANT`/`REVOKE` 時に無効化されず、期限切れまで古い権限が有効なままになっていました。[#73717](https://github.com/StarRocks/starrocks/pull/73717)
- サブライターに追加がない場合、`FlatJson` がクラッシュしていました。[#73730](https://github.com/StarRocks/starrocks/pull/73730)
- MV 自体に `HAVING` 述語がある場合、集計 MV の書き換えが誤って適用され、結果が不完全になる可能性がありました。[#73610](https://github.com/StarRocks/starrocks/pull/73610)
- 並列マージモードに入る際、スピルライターの `auto_flush` フラグでデータ競合が発生し、ARM 上で意図しないセグメントのフラッシュが引き起こされていました。[#73616](https://github.com/StarRocks/starrocks/pull/73616)
- Routine Load のスケジューラが、Kafka または Pulsar のパーティションメタデータを取得するためのブロッキング BE RPC を実行している間、ジョブごとの書き込みロックを保持しており、ロック保持時間が最大33.6秒に達していました。[#73591](https://github.com/StarRocks/starrocks/pull/73591)
- `tablet_sched_disable_colocate_balance` が有効な場合、停止中の BE 上の Colocate tablet が誤って `HEALTHY` として報告されていました。[#73550](https://github.com/StarRocks/starrocks/pull/73550)
- MISSING（ファントム）レプリカ行が存在する場合、`ADMIN SHOW REPLICA STATUS` が MySQL の結果ストリームを非同期化させ、クライアントのハングや切断を引き起こしていました。[#74393](https://github.com/StarRocks/starrocks/pull/74393)
- 共有データモードにおいて、パーティションごとのコーディネータ要求がすべての送信者の `open` RPC で再記録されず、一部の送信者がコーディネータ選出から漏れ、`combined_txn_log` ファイルが書き込まれないまま残っていました。[#73962](https://github.com/StarRocks/starrocks/pull/73962)
- `_statistics_` データベースまたはテーブルが削除された後、`_statistics_.pipe_file_list` 内部テーブルが再作成されていませんでした。[#73970](https://github.com/StarRocks/starrocks/pull/73970)
- `TaskCleaner` によって強制終了されたタスク実行がアーカイブされず、`information_schema.task_runs` から痕跡もなく消えてしまっていました。[#74146](https://github.com/StarRocks/starrocks/pull/74146)
- `RENAME TABLE` と `SWAP TABLE`/`SWAP MATERIALIZED VIEW` はデータベース書き込みロックではなく、集中的なテーブルロックのみを保持していたため、並行する読み取り処理が途中状態の名前からテーブルへのマッピングを観測できてしまっていました。[#74100](https://github.com/StarRocks/starrocks/pull/74100)
- PK インデックスの Compaction 出力 sstable が tablet メタデータなしで開かれていたため、`metadata is null when loading delvec` の失敗が恒久的に発生していました。[#74037](https://github.com/StarRocks/starrocks/pull/74037)
- 同一トランザクション内ですでに変更されたテーブルを対象とした、明示的トランザクション内の部分更新 `INSERT` が、`COMMIT` でサイレントにデータを破損させていました。[#74344](https://github.com/StarRocks/starrocks/pull/74344)
- レンジ分散と互換性のない（schema change、ソートキーの変更）`ALTER TABLE` 操作が、メタデータをサイレントに破損させるのではなく、適切なエラーで拒否されるようになりました。[#74020](https://github.com/StarRocks/starrocks/pull/74020)
- オプティマイザ内で子要素の型が一致しない集計関数が、誤ったクエリ結果を引き起こしていました。[#74159](https://github.com/StarRocks/starrocks/pull/74159)
- 予約語をテーブル名として使用した `ALTER ROUTINE LOAD` が、パース不能な `origStmt` を書き込んでいたため、FE 再起動後に列マッピングが失われていました。[#74188](https://github.com/StarRocks/starrocks/pull/74188)
- IVM の `state_union` 互換性チェックがネストされた型（例：`ARRAY<VARCHAR>`）に再帰しなかったため、`ARRAY_AGG` を使用する IMV に対して `CREATE MATERIALIZED VIEW` が失敗していました。[#73627](https://github.com/StarRocks/starrocks/pull/73627)
- スキャン範囲が完全にフィルタリングされた場合、Parquet の一時的な辞書コード列が上位層に漏れ出し、下流で型の不一致が発生していました。[#74452](https://github.com/StarRocks/starrocks/pull/74452)
- 浮動小数点と整数が混在した `WHEN` および結果型を持つ `CASE WHEN` が、無効な JIT IR を生成し、誤った結果やクラッシュを引き起こしていました。[#74382](https://github.com/StarRocks/starrocks/pull/74382)
- JIT コンパイルの失敗により `LLVMContext` の use-after-free が発生し、SIGSEGV を引き起こしていました。[#74396](https://github.com/StarRocks/starrocks/pull/74396)
- バックグラウンドの統計タスクがセッションの `WAREHOUSE` 設定を上書きしていたため、同一接続コンテキスト上の後続のユーザークエリに影響を与えていました。[#74385](https://github.com/StarRocks/starrocks/pull/74385)
- クラスタースナップショットが一度も正常に完了していない場合、`CatalogRecycleBin` がエントリの削除を停止していたため、`INSERT OVERWRITE` の負荷が高いワークロード下で FE メモリが無制限に増加していました。[#74379](https://github.com/StarRocks/starrocks/pull/74379)
- 非 PK レプリカのバージョンホールが FE によって検出されず、クエリが凍結した `max_version` を持つホール入りレプリカに恒久的にルーティングされていました。[#74408](https://github.com/StarRocks/starrocks/pull/74408)
- `MaterializedIndexMeta.updateSchemaBackendId`（共有読み取りロック下で変更される `HashSet`）に対するデータ競合により、エントリの消失やセットの破損が発生する可能性がありました。[#74412](https://github.com/StarRocks/starrocks/pull/74412)
- 保持境界のメタデータがすでに Vacuum 処理されている場合、Vacuum のウォーターマークが正しく報告されず、`file_bundling` のバージョン切り替えクリーンアップが停止していました。[#74429](https://github.com/StarRocks/starrocks/pull/74429)
- Lake vacuum の再試行は決定論的な指数バックオフを使用していましたが、S3 スロットリング下で CN 間に再試行を分散させるため、非相関ジッターが追加されるようになりました。[#74108](https://github.com/StarRocks/starrocks/pull/74108)
- クエリメモリプールから割り当てられた RPC リクエストがプロセスコンテキストで解放されていたため、`OlapTableSink` 内のメモリ計上が膨張していました。[#73807](https://github.com/StarRocks/starrocks/pull/73807)
- 自動パーティション作成が `_incremental_open_node_channel` を同時にトリガーした場合、`TabletSinkSender::_send_chunk_by_node` で競合状態が発生していました。[#73820](https://github.com/StarRocks/starrocks/pull/73820)
- 上流変更のバックポートによって作成された UDAF コンテキストが、`unique_ptr::release` 経由でメモリリークを引き起こしていました。[#74025](https://github.com/StarRocks/starrocks/pull/74025)
- `append_selective` におけるメモリ計上の不正確さに起因する、パーティション化ジョインのプローブ処理での範囲外アクセスの可能性。[#74315](https://github.com/StarRocks/starrocks/pull/74315)
- `azure_adls2_oauth2_client_endpoint` 設定フィールドの名前に誤記がありました。[#74581](https://github.com/StarRocks/starrocks/pull/74581)
- `StarMgrMetaSyncer` が、レンジコロケートの PACK シャードグループを誤って孤立していると判断し回収していたため、共有データモードでアクティブなシャードが恒久的に削除されていました。[#74117](https://github.com/StarRocks/starrocks/pull/74117)
- PRIMARY の KEY テーブルおよび明示的な `ORDER BY` を持たないテーブルでは、コロケート tablet 分割のソートキーの要素数がマテリアライズされたスキーマではなくベーススキーマから解決されていたため、分割ジョブが tablet サイズを削減することなく完了していました。[#74409](https://github.com/StarRocks/starrocks/pull/74409)
- 自動マージデーモンが、パーティションデータがマージ閾値を下回っている場合に事前分割された tablet を再びマージしてしまい、サンプルベースの事前分割による並行性のメリットを打ち消していました。[#74583](https://github.com/StarRocks/starrocks/pull/74583)
- 関数の `FunctionName.db` がソースデータベースを指したままになっていたため、`RESTORE ... AS <new_db>` 後に Follower FE で db レベルの UDF が欠落していました。[#74313](https://github.com/StarRocks/starrocks/pull/74313)
- ウェアハウスに複数の CN グループが存在する場合、共有データ `DISTRIBUTED BY RANDOM` の CTAS/INSERT において、不変パーティションの tablet ロケーションに対して誤った CN グループが割り当てられていました。[#74316](https://github.com/StarRocks/starrocks/pull/74316)
- 統計推定中にパーティションが同時に削除された場合、`StatisticsCalcUtils` で `NullPointerException` が発生していました。[#73711](https://github.com/StarRocks/starrocks/pull/73711)
- `InformationSchemaDataSource` と `FrontendServiceImpl` のメタデータ RPC ハンドラーがデータベース全体の READ ロックを保持していたため、関係のないテーブルへの DDL をブロックしていました。[#73936](https://github.com/StarRocks/starrocks/pull/73936) [#73913](https://github.com/StarRocks/starrocks/pull/73913)
- 終了状態を変更しても共有コンテキストのオブザーバーに通知しない Pipeline Operator が、イベントスケジューラー下でピアドライバーを停止させてしまう可能性がありました。[#74055](https://github.com/StarRocks/starrocks/pull/74055) [#74056](https://github.com/StarRocks/starrocks/pull/74056)
- 述語プッシュダウンにおける非ルートの複合述語が、スキャンレベルの EOF ではなく `NotPushDown` を生成していたため、UNION の下で到達不可能なネストされた AND 分岐が存在する場合に `OlapScanNode` が行を一切出力しない問題がありました。[#74218](https://github.com/StarRocks/starrocks/pull/74218)
- 単一の記憶媒体を持つ BE 上で `BackendLoadStatistic.init` がレプリカごとに高コストなスキャンを実行していましたが、ディスクが均質な BE については O(1) に変更されました。[#73555](https://github.com/StarRocks/starrocks/pull/73555)
- データディレクトリのロードスレッドにおけるスレッド名設定の競合により、BE 起動のたびにノイズの多い `failed to set thread name` 警告が発生していました。[#73862](https://github.com/StarRocks/starrocks/pull/73862)
- タスク manager が不正な `RUNNING→RUNNING` エディットログを書き込んでいたため、タスク実行が実行中マップに無期限に残ったままになる問題がありました。[#73882](https://github.com/StarRocks/starrocks/pull/73882)
- PK のマルチステートメントバッチトランザクションが複合ロウセット間で `num_rows`、`data_size`、`num_dels` を累積しなかったため、共有データの Primary Key テーブルで行数統計が不正確になっていました。[#74059](https://github.com/StarRocks/starrocks/pull/74059)
- Lake ロードのスピルクリーンアップが、トランザクション ID ベースの Vacuum 駆動型の回収を使用するようになり、BE のクラッシュや OOM の後に孤立したスピルファイルが残らなくなりました。[#73064](https://github.com/StarRocks/starrocks/pull/73064)
- `0000` 年以降の PostgreSQL JDBC の時刻値が、不正な型マッピング結果を引き起こしていました。[#70842](https://github.com/StarRocks/starrocks/pull/70842)
- schema change 中にロウセットから `gtid` を読み取る前の null チェックが欠落しており、NPE クラッシュを引き起こしていました。[#74855](https://github.com/StarRocks/starrocks/pull/74855)

## 4.1.1 {/* #411 */}

リリース日：2026 年 5 月 29 日

### 動作の変更 {/* #behavior-changes-6 */}

- Hive コネクタはデフォルトで、JNI Avro スキャナーの代わりにネイティブ C++ Avro スキャナーを使用するようになりました。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- INCREMENTAL/AUTO マテリアライズドビューに対するクエリの書き換えは無効化され、INCREMENTAL/AUTO マテリアライズドビューに対する FORCE リフレッシュおよびパーティションリフレッシュは拒否されるようになりました。[#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### 改善 {/* #improvements-7 */}

- Java UDF/UDAF/UDTF がより多くのタイプをサポートするようになりました：UDAF/UDTF の STRUCT 引数と戻り値、ネストされた ARRAY/MAP タイプ、DATE/DATETIME、DECIMAL、および可変長引数。[#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- スカラー UDF が STRUCT 引数をサポートするようになりました。[#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDF がネストされた ARRAY/MAP タイプをサポートするようになりました。[#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAF が一度だけロード・初期化され、クエリ間で再利用されるようになり、クエリごとのオーバーヘッドが削減されました。[#72038](https://github.com/StarRocks/starrocks/pull/72038)
- Hive コネクタの JNI Avro スキャナーをネイティブ C++ スキャナーに置き換え、直接バイナリデコードと `avro.schema.literal` および `avro.schema.url` のサポートを追加しました。[#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- CTAS 文で Trino の `WITH` 句をサポートします。[#71960](https://github.com/StarRocks/starrocks/pull/71960)
- シンクパスにおける Iceberg `timestamptz` パーティション変換のサポートを完成させました。[#73397](https://github.com/StarRocks/starrocks/pull/73397)
- Iceberg テーブルの集計に対して TopN ランタイムフィルターのプッシュダウンを有効にしました。[#72332](https://github.com/StarRocks/starrocks/pull/72332)
- Iceberg の日時の最小値/最大値最適化をサポートします。[#71870](https://github.com/StarRocks/starrocks/pull/71870)
- 複数の HDFS クラスタへのアクセスをサポートするため、Catalog と BE での HDFS HA 設定のパススルーを許可しました。[#71521](https://github.com/StarRocks/starrocks/pull/71521)
- 外部テーブルクエリにパーティションスキャン数の制限を追加しました。[#68480](https://github.com/StarRocks/starrocks/pull/68480)
- サポートされていない Iceberg V3 機能についてはフェイルファストするようになりました。[#70242](https://github.com/StarRocks/starrocks/pull/70242)
- INSERT INTO FILES 経由の CSV エクスポートで `csv.enclose` と `csv.escape` をサポートします。[#71589](https://github.com/StarRocks/starrocks/pull/71589)
- `files()` へのフルスキーマプッシュダウンのための `enable_push_down_schema` INSERT プロパティを追加しました。[#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load ジョブは、再試行不可能なエラー（例：主キーサイズ超過）が発生した場合に一時停止するようになりました。[#71161](https://github.com/StarRocks/starrocks/pull/71161)
- 2つの子ノードから生成される複雑な式のジョイン順序の並べ替えをサポートします。[#71615](https://github.com/StarRocks/starrocks/pull/71615)
- `date_trunc`、`array_map`、CASE WHEN、IS NULL、UNION、および定数に対する MCV/null 割合の伝播など、CBO 統計推定を改善しました。[#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- スキュージョインの検出を改善しました：all ジョインキーに偏りがある場合にのみスキューが検出されるようになり、スキューのルールを強制する `force_group_by_skew_eliminate_when_skewed` スイッチが追加されました。[#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- FE における `regexp_replace` の定数畳み込みをサポートします。[#70804](https://github.com/StarRocks/starrocks/pull/70804)
- 定数パーティション値を持つ日付パーティション列に対する MIN/MAX を最適化しました。[#69880](https://github.com/StarRocks/starrocks/pull/69880)
- マテリアライズドビューのリフレッシュにおいて、`ASYNC` の同義語として `SCHEDULE` キーワードを導入しました。[#72329](https://github.com/StarRocks/starrocks/pull/72329)
- 共有データモードにおける Lake テーブルの tablet 作成のリトライをサポートします。[#71068](https://github.com/StarRocks/starrocks/pull/71068)
- Lake 列モードの部分更新に対する条件付き更新をサポートします。[#71961](https://github.com/StarRocks/starrocks/pull/71961)
- 取り込みスループットを向上させるため、部分更新の公開、永続性インデックスの初期化、および SSTable のオープンを並列化しました。[#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- 共有なしから共有データへのレプリケーション中の DCG ファイル同期をサポートします。[#69339](https://github.com/StarRocks/starrocks/pull/69339)
- キー列・非キー列の両方における VARCHAR 長の拡張に対する schema change をサポートします。[#70747](https://github.com/StarRocks/starrocks/pull/70747)
- クラスタスナップショットの整合性チェックのために `snapshot_meta.json` マーカーを追加しました。[#71209](https://github.com/StarRocks/starrocks/pull/71209)
- DN パターンを介した LDAP ダイレクトバインド認証をサポートします。[#71559](https://github.com/StarRocks/starrocks/pull/71559)
- クエリのトラブルシューティングを容易にするため、`get_query_dump_from_query_id` メタ関数を追加しました。[#72875](https://github.com/StarRocks/starrocks/pull/72875)
- 監査ログでのクエリされたリレーションの監査をサポートします。[#71596](https://github.com/StarRocks/starrocks/pull/71596)
- MySQL バイナリ結果エンコーディング用のセッション変数を追加しました。[#71415](https://github.com/StarRocks/starrocks/pull/71415)
- 共有データクラスタ向けの `tablet_num`、`MemtableIOSpeed`、`staros_shard_count`、および Iceberg メタデータテーブルのクエリメトリクスなど、可観測性向上のためのメトリクスを追加しました。[#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- FE 設定項目 `deploy_serialization_min_thread_pool_size` を追加しました。[#72274](https://github.com/StarRocks/starrocks/pull/72274)
- MergeTabletJob の作成を無効化するための `tablet_reshard_enable_tablet_merge` 設定を追加しました。[#70906](https://github.com/StarRocks/starrocks/pull/70906)
- `SO_REUSEPORT` によって HTTP サーバーの accept における thundering-herd 問題を解消しました。[#72956](https://github.com/StarRocks/starrocks/pull/72956)
- `CREATE FUNCTION ... AS <sql_body>` 経由での SQL UDF 作成をサポートします。[#67558](https://github.com/StarRocks/starrocks/pull/67558)
- S3 からの UDF のロードをサポートします。[#64541](https://github.com/StarRocks/starrocks/pull/64541)
- 時系列順の UUID v7 値を生成する `uuid_v7` 関数を追加しました。[#67694](https://github.com/StarRocks/starrocks/pull/67694)
- external catalog の可観測性のために、catalog タイプごとのクエリメトリクスを追加しました。[#70533](https://github.com/StarRocks/starrocks/pull/70533)
- ウィンドウ関数に対する明示的なスキューヒントをサポートし、スキューしたパーティションキーを持つウィンドウ関数を UNION に分割することで自動的に最適化します。[#68739](https://github.com/StarRocks/starrocks/pull/68739)

### セキュリティ {/* #security-7 */}

- [CVE] Netty を 4.1.133.Final にアップグレードしました。[#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] pgjdbc を 42.7.11（無制限の SCRAM PBKDF2 反復回数によるクライアント側 DoS）に、BouncyCastle を 1.84（FrodoKEM 秘密鍵の漏洩）にバンプしました。[#72797](https://github.com/StarRocks/starrocks/pull/72797)
- Golang の CVE を解消するため、go1.25.9 で pprof をビルドしました。[#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- jetty-http を 9.4.58.v20250814 にアップグレードしました。[#71762](https://github.com/StarRocks/starrocks/pull/71762)
- Broker 依存関係の CVE をクリーンアップし、`wildfly-openssl` を削除しました。[#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- INSERT INTO FILES のエラーメッセージ内の認証情報をマスクしました。[#71245](https://github.com/StarRocks/starrocks/pull/71245)

### バグ修正 {/* #bug-fixes-7 */}

以下の問題を修正しました：

- `hash_util` の静的初期化順序によって起動時に発生する CN のセグメンテーション違反。[#71825](https://github.com/StarRocks/starrocks/pull/71825)
- 物理分割が有効な状態で空の tablet をスキャンした際の CN クラッシュ。[#70281](https://github.com/StarRocks/starrocks/pull/70281)
- `information_schema.warehouse_queries` をクエリした際の BE クラッシュ。[#72019](https://github.com/StarRocks/starrocks/pull/72019)
- rowset の `num_rows` がゼロの場合の Lake Compaction における SIGFPE。[#71742](https://github.com/StarRocks/starrocks/pull/71742)
- ExecutionDAG フラグメント接続におけるゼロ除算。[#67918](https://github.com/StarRocks/starrocks/pull/67918)
- SinkBuffer における正常終了時のクラッシュ。[#73202](https://github.com/StarRocks/starrocks/pull/73202)
- スピル可能なハッシュジョインのプローブ時のクラッシュ。[#72397](https://github.com/StarRocks/starrocks/pull/72397)
- 一時的な `std::string` へのフォーマット時に発生するスタックバッファオーバーフロー。[#72728](https://github.com/StarRocks/starrocks/pull/72728)
- `reverse(DecimalV3)` におけるクラッシュ。[#71834](https://github.com/StarRocks/starrocks/pull/71834)
- 一時的な `shared_ptr` の破棄によって引き起こされる `LoadChannel::get_load_replica_status` における解放済みメモリの使用（use-after-free）。[#71843](https://github.com/StarRocks/starrocks/pull/71843)
- スレッド作成に失敗した場合の `ThreadPool::do_submit` における解放済みメモリの使用（use-after-free）。[#71276](https://github.com/StarRocks/starrocks/pull/71276)
- フラグメントの解体処理全体にわたる Hive パーティション記述子の解放済みメモリの使用（use-after-free）。[#73176](https://github.com/StarRocks/starrocks/pull/73176)
- information schema シンクにおける解放済みメモリの使用（use-after-free）。[#71513](https://github.com/StarRocks/starrocks/pull/71513)
- HttpClient インスタンスを再利用することによる FE のファイルディスクリプタのリーク。[#73239](https://github.com/StarRocks/starrocks/pull/73239)
- `JDBCScanner::_init_jdbc_scanner` における JNI ローカル参照のリーク。[#72913](https://github.com/StarRocks/starrocks/pull/72913)
- MV プランコンテキストをキャッシュする際のメモリリーク。[#72300](https://github.com/StarRocks/starrocks/pull/72300)
- ローカル交換における予期しないメモリの過剰使用。[#72262](https://github.com/StarRocks/starrocks/pull/72262)
- Lake `publish_version` における `response->tablet_metas` のレース。[#73274](https://github.com/StarRocks/starrocks/pull/73274)
- `DeltaWriter::commit()` における同時 `SegmentFlushTask` のレース。[#73371](https://github.com/StarRocks/starrocks/pull/73371)
- シリアライズ時の `RuntimeProfile` の最小値/最大値のレース。[#72904](https://github.com/StarRocks/starrocks/pull/72904)
- クエリコンテキストの破棄中における `PipelineTimerTask` のレース条件。[#73082](https://github.com/StarRocks/starrocks/pull/73082)
- `_all_global_rf_ready_or_timeout` におけるレース条件。[#70920](https://github.com/StarRocks/starrocks/pull/70920)
- `map_apply` と `array_length` における共有 `NullColumn` の問題。[#71258](https://github.com/StarRocks/starrocks/pull/71258)
- パーティションバージョンのギャップによって引き起こされるバッチパブリッシュのデッドロック。[#71483](https://github.com/StarRocks/starrocks/pull/71483)
- 共有なしモードで rowset メタデータの LRU キャッシュをウォームアップする際のデッドロック。[#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `Locker` のロールバックが例外安全ではなく、ロック解除の順序が誤っている問題。[#72789](https://github.com/StarRocks/starrocks/pull/72789)
- 読み取り専用パスおよびメタデータパスにおける複数の DB ロックによって引き起こされる、DDL と StarOS RPC とのロック競合。[#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- プロジェクトノードの欠落による不正なシャッフル分散。[#71075](https://github.com/StarRocks/starrocks/pull/71075)
- AGG TopN ランタイムフィルター `exprOrder` の不一致によりクラッシュと誤った結果が発生する問題。[#71479](https://github.com/StarRocks/starrocks/pull/71479)
- dict-merge GROUP BY による誤った結果。[#70866](https://github.com/StarRocks/starrocks/pull/70866)
- Query Cache がローカルシャッフル集計と競合する問題。[#73194](https://github.com/StarRocks/starrocks/pull/73194)
- flat JSON におけるグローバル辞書生成の不整合。[#72953](https://github.com/StarRocks/starrocks/pull/72953)
- Flat JSON のマージ時の空データの不整合。[#72973](https://github.com/StarRocks/starrocks/pull/72973)
- 明示的なキー/値タイプが宣言されている場合の map リテラルにおけるタイプ不一致。[#71316](https://github.com/StarRocks/starrocks/pull/71316)
- JOIN USING トランスフォーマーにおいて COALESCE の子要素が共通タイプにキャストされない問題。[#72338](https://github.com/StarRocks/starrocks/pull/72338)
- グローバル変数を使用した reduce-cast 後に VARCHAR の長さが保持されない問題。[#70269](https://github.com/StarRocks/starrocks/pull/70269)
- MySQL 結果セット内のネストされたタイプで VARBINARY が誤ってエンコードされる問題。[#71346](https://github.com/StarRocks/starrocks/pull/71346)
- 小規模な LIMIT で集計スピルを無効にした場合の check-having-clause の問題。[#72705](https://github.com/StarRocks/starrocks/pull/72705)
- 日付解析前に引用符が取り除かれない問題、および PostgreSQL の日付/時刻のバグ。[#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)
- データファイルの shared フラグが失われ、vacuum が兄弟分割 tablet からまだ参照されているファイルを削除してしまう問題。[#71585](https://github.com/StarRocks/starrocks/pull/71585)
- split → Compaction → merge の一連の処理における tablet マージの正確性の問題。[#72350](https://github.com/StarRocks/starrocks/pull/72350)
- tablet の分割中にクロス公開トランザクションログの num_rows/data_size が膨張する問題。[#71144](https://github.com/StarRocks/starrocks/pull/71144)
- 同一公開バッチ内での Compaction 前書き込みによる delvec の孤立エントリの問題。[#71001](https://github.com/StarRocks/starrocks/pull/71001)
- StarMgr ジャーナルのリプレイを同期することで Follower FE における「no queryable replica」の問題を修正。[#71263](https://github.com/StarRocks/starrocks/pull/71263)
- 通常の rowset commit 適用時に `merge_condition` が保持されない問題。[#72542](https://github.com/StarRocks/starrocks/pull/72542)
- 誤ったスナップショット ID とフィルターを使用した Iceberg DELETE の競合検出の問題。[#73354](https://github.com/StarRocks/starrocks/pull/73354)
- 不正な Iceberg 変換引数による NPE。[#71917](https://github.com/StarRocks/starrocks/pull/71917)
- プランナーによって注入された余分な列により Iceberg の min/max 最適化がスキップされる問題。[#71863](https://github.com/StarRocks/starrocks/pull/71863)
- Iceberg ベーステーブルにおける aggregate-join-pushdown マテリアライズドビューの書き換えの問題。[#71856](https://github.com/StarRocks/starrocks/pull/71856)
- INSERT OVERWRITE のコミット前に Hive パーティションディレクトリが欠落する問題。[#71810](https://github.com/StarRocks/starrocks/pull/71810)
- JNI スキャナーに対して AWS の assume-role が適用されない問題。[#71422](https://github.com/StarRocks/starrocks/pull/71422)
- 枝刈りされた子要素とネストされた null 許容スキーマに対する Avro の複合タイプデコードの問題。[#73474](https://github.com/StarRocks/starrocks/pull/73474)
- Parquet の Broker Load エラーでファイル/列/行のコンテキストが欠落する問題。[#73236](https://github.com/StarRocks/starrocks/pull/73236)
- Parquet スキャナーにおいて Arrow ディクショナリ値のサポートが不足している問題。[#71855](https://github.com/StarRocks/starrocks/pull/71855)
- Paimon テーブルの主キーが SHOW CREATE に表示されず、DESC も返されない問題。[#70535](https://github.com/StarRocks/starrocks/pull/70535)
- PostgreSQL/Oracle の JDBC タイプ互換性と、末尾にスラッシュを含む JDBC URL の構築の問題。[#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)
- JDBC catalog 内の SQL Server テーブルにおけるマテリアライズドビューのリフレッシュの問題。[#72962](https://github.com/StarRocks/starrocks/pull/72962)
- 外部結合上のマテリアライズドビューにおける遅延実体化スロットの null 許容性の問題。[#72621](https://github.com/StarRocks/starrocks/pull/72621)
- AUTO と INCREMENTAL のマテリアライズドビューパーティションリフレッシュが拒否される問題。[#71355](https://github.com/StarRocks/starrocks/pull/71355)
- マテリアライズドビューが非アクティブになった後もマテリアライズドビュースケジューラーが停止しない問題。[#71265](https://github.com/StarRocks/starrocks/pull/71265)
- MySQL クライアント互換性のための `SHOW GRANTS FOR CURRENT_USER()` のサポートが不足している問題。[#71959](https://github.com/StarRocks/starrocks/pull/71959)
- 明示的なトランザクション内で SHOW 文が許可されない問題。[#72954](https://github.com/StarRocks/starrocks/pull/72954)
- 空の結果セットに対して Arrow Flight が列名 `r` を返す問題。[#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Java UDF コードで JNI 例外処理チェックが不足している問題。[#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `ai_query` 関数登録の問題。[#72103](https://github.com/StarRocks/starrocks/pull/72103)
- `enable_load_profile` 使用時の Stream Load プロファイル収集の問題。[#71952](https://github.com/StarRocks/starrocks/pull/71952)
- プロファイルの START_TIME/END_TIME がセッションのタイムゾーンで表示されない問題。[#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `star_mgr_meta_sync_interval_sec` がランタイムで変更可能ではない問題。[#71675](https://github.com/StarRocks/starrocks/pull/71675)
- `information_schema.tables` が等価述語の特殊文字をエスケープしない問題。[#71273](https://github.com/StarRocks/starrocks/pull/71273)
- エラーパスでの並列セグメント/ロウセットロード時の use-after-free。[#71083](https://github.com/StarRocks/starrocks/pull/71083)
- 集計スピル `set_finishing` におけるハッシュテーブルデータ損失の可能性。[#70851](https://github.com/StarRocks/starrocks/pull/70851)
- ディスク再移行（A→B→A）時の GC レースにより発生する PK tablet ロウセットメタ損失。[#70727](https://github.com/StarRocks/starrocks/pull/70727)
- `SharedDataStorageVolumeMgr` における DB 読み取りロックのリーク。[#70987](https://github.com/StarRocks/starrocks/pull/70987)
- IVM リフレッシュが不完全な PCT パーティションメタデータを記録する問題。[#71092](https://github.com/StarRocks/starrocks/pull/71092)
- 参照先の列が存在しない場合に Stream Load/Broker Load で生成列を解析すると発生する NPE。[#71116](https://github.com/StarRocks/starrocks/pull/71116)
- ショートサーキットポイントルックアップにおけるパーティション述語の欠落。[#71124](https://github.com/StarRocks/starrocks/pull/71124)

## 4.1.0 {/* #410 */}

リリース日：2026年4月13日

### 共有データアーキテクチャ {/* #shared-data-architecture */}

- **新しいマルチテナントデータ管理**

  共有データクラスタが、範囲ベースのデータ分散ならびに tablet の自動分割・マージをサポートするようになりました。tablet が肥大化したりホットスポット化したりした場合、スキーマ変更や SQL の変更、データの再取り込みを必要とせずに自動的に分割できます。この機能により、マルチテナントワークロードにおけるデータスキューとホットスポットの問題に直接対処し、ユーザビリティを大幅に向上できます。[#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **大容量 tablet サポート（フェーズ1）**

  共有データクラスタが tablet あたりでより多くのデータをホストできるようにし、長期的には tablet あたり100GBを目標としています。フェーズ1では、データ取り込み、主キー更新、Compaction のパイプライン全体にわたる tablet 内並行処理を導入しており、Lake tablet が肥大化しても単一スレッドのボトルネックにならないようにしています。改善点には、単一 tablet 内での並列 Compaction（セグメントレベルの分割を伴う）、Lake ロード（ロードスピル経路を含む）における並列 MemTable の確定・フラッシュ・マージ、主キーテーブル向けの tablet 内部の並列パブリッシュおよび並列条件更新、リモートストレージマッパーファイルをサポートした cloud ネイティブの主キーインデックス向けのレンジ分割・並列・サイズ階層型 Compaction が含まれます。これらの変更により、大規模な tablet ワークロードにおける取り込みメモリオーバーヘッド、Compaction の増幅、FE メタデータの負荷が大幅に削減されます。[#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **高速スキーマ進化 V2**

  共有データクラスタが高速スキーマ進化 V2 をサポートするようになりました。これにより、スキーマ操作の秒単位での DDL 実行が可能になり、さらにマテリアライズドビューにもサポートが拡張されています。[#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **【ベータ】共有データ上の転置インデックス**

  共有データクラスタ向けに組み込みの転置インデックスを有効化し、テキストフィルタリングおよび全文検索ワークロードを高速化します。[#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **キャッシュの可観測性**

  クエリレベルのキャッシュヒット率が監査ログおよび監視システムに公開されるようになり、キャッシュの透明性とレイテンシ診断が向上しました。追加の Data Cache メトリクスには、メモリおよびディスクのクォータ使用状況、ページキャッシュの統計情報が含まれます。[#63964](https://github.com/StarRocks/starrocks/pull/63964)

- Lake テーブル向けにセグメントメタデータフィルターを追加し、スキャン時にソートキーの範囲に基づいて無関係なセグメントをスキップすることで、範囲述語クエリの I/O を削減します。[#68124](https://github.com/StarRocks/starrocks/pull/68124)

- Lake DeltaWriter の高速キャンセルをサポートし、共有データクラスタでキャンセルされた取り込みジョブのレイテンシを削減します。[#68877](https://github.com/StarRocks/starrocks/pull/68877)

- 自動クラスタスナップショットの間隔ベースのスケジューリングサポートを追加しました。[#67525](https://github.com/StarRocks/starrocks/pull/67525)

- MemTable のフラッシュおよびマージのパイプライン実行をサポートし、共有データクラスタにおける cloud ネイティブテーブルの取り込みスループットを改善します。[#67878](https://github.com/StarRocks/starrocks/pull/67878)

- cloud ネイティブテーブルの修復用に `dry_run` モードをサポートし、実行前に修復アクションをプレビューできるようにしました。[#68494](https://github.com/StarRocks/starrocks/pull/68494)

- 共有なしクラスタにパブリッシュトランザクション用のスレッドプールを追加し、パブリッシュスループットを改善しました。[#67797](https://github.com/StarRocks/starrocks/pull/67797)

### データレイク分析 {/* #data-lake-analytics */}

- **Iceberg DELETE サポート**

  Iceberg テーブル向けの position delete ファイルの書き込みをサポートし、StarRocks から直接 Iceberg テーブルに対する DELETE 操作を可能にします。このサポートは Plan、Sink、Commit、Audit の全パイプラインをカバーしています。[#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **Hive および Iceberg テーブル向けの TRUNCATE**

  外部 Hive テーブルおよび Iceberg テーブルに対する TRUNCATE TABLE をサポートします。[#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Iceberg 上のインクリメンタルマテリアライズドビュー**

  インクリメンタルマテリアライズドビューリフレッシュのサポートを Iceberg の追記専用（append-only）テーブルに拡張し、テーブル全体のリフレッシュを行うことなくクエリアクセラレーションを可能にします。[#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **Iceberg における半構造化データ向けの VARIANT タイプ**

  Iceberg Catalog における VARIANT データタイプをサポートし、半構造化データの柔軟な schema-on-read ストレージとクエリを実現します。読み取り、書き込み、タイプキャスト、Parquet 連携をサポートします。[#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3 サポート**

  Iceberg v3 のデフォルト値機能と行系譜（row lineage）のサポートを追加しました。[#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Iceberg テーブルメンテナンス手順**

  `rewrite_manifests` 手順のサポートを追加し、`expire_snapshots` および `remove_orphan_files` 手順をより細かなテーブルメンテナンスのための追加引数で拡張しました。[#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- Iceberg テーブルからファイルパスと行位置のメタデータ列を読み取ることをサポートします。[#67003](https://github.com/StarRocks/starrocks/pull/67003)

- Iceberg v3 テーブルから `_row_id` を読み取ることをサポートし、Iceberg v3 のグローバル後期実体化をサポートします。[#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)

- カスタムプロパティを使用した Iceberg ビューの作成をサポートし、SHOW CREATE VIEW の出力にプロパティを表示します。[#65938](https://github.com/StarRocks/starrocks/pull/65938)

- 特定のブランチ、タグ、バージョン、またはタイムスタンプを指定して Paimon テーブルをクエリすることをサポートします。[#63316](https://github.com/StarRocks/starrocks/pull/63316)

- Paimon テーブルの複合型（ARRAY、MAP、STRUCT）をサポートします。[#66784](https://github.com/StarRocks/starrocks/pull/66784)

- Iceberg テーブルの作成時に、括弧構文によるパーティション変換（Partition Transforms）をサポートします。[#68945](https://github.com/StarRocks/starrocks/pull/68945)

- データ構成の改善のため、Transform Partition に基づく Iceberg のグローバルシャッフルをサポートします。[#70009](https://github.com/StarRocks/starrocks/pull/70009)

- Iceberg テーブルシンクのグローバルシャッフルを動的に有効化することをサポートします。[#67442](https://github.com/StarRocks/starrocks/pull/67442)

- 同時コミットの競合を回避するため、Iceberg テーブルシンクにコミットキューを導入しました。[#68084](https://github.com/StarRocks/starrocks/pull/68084)

- データ構成と読み取りパフォーマンスを改善するため、Iceberg テーブルシンクにホストレベルのソートを追加しました。[#68121](https://github.com/StarRocks/starrocks/pull/68121)

- ETL 実行モードで追加の最適化をデフォルトで有効化し、明示的な設定なしで INSERT INTO SELECT、CREATE TABLE AS SELECT、および類似のバッチ操作のパフォーマンスを向上させました。[#66841](https://github.com/StarRocks/starrocks/pull/66841)

- Iceberg テーブルに対する INSERT および DELETE 操作のコミット監査情報を追加しました。[#69198](https://github.com/StarRocks/starrocks/pull/69198)

- Iceberg REST Catalog でのビューエンドポイント操作の有効化または無効化をサポートします。[#66083](https://github.com/StarRocks/starrocks/pull/66083)

- CachingIcebergCatalog でのキャッシュ検索効率を最適化しました。[#66388](https://github.com/StarRocks/starrocks/pull/66388)

- さまざまな Iceberg catalog タイプに対する EXPLAIN をサポートします。[#66563](https://github.com/StarRocks/starrocks/pull/66563)

- AWS Glue Catalog テーブル内のテーブルに対するパーティションプロジェクションをサポートします。[#67601](https://github.com/StarRocks/starrocks/pull/67601)

- AWS Glue の `GetDatabases` API にリソース共有タイプのサポートを追加しました。[#69056](https://github.com/StarRocks/starrocks/pull/69056)

- エンドポイント注入（`azblob`/`adls2`）を使用した Azure ABFS/WASB のパスマッピングをサポートします。[#67847](https://github.com/StarRocks/starrocks/pull/67847)

- リモート RPC のオーバーヘッドと外部システム障害の影響を軽減するため、JDBC catalog にデータベースメタデータキャッシュを追加しました。[#68256](https://github.com/StarRocks/starrocks/pull/68256)

- `information_schema` における PostgreSQL テーブルの列コメントをサポートします。[#70520](https://github.com/StarRocks/starrocks/pull/70520)

- Oracle および PostgreSQL の JDBC タイプマッピングを改善しました。[#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### クエリエンジン {/* #query-engine */}

- **再帰 CTE**

  階層的なトラバーサル、グラフクエリ、反復的な SQL 計算のための再帰共通テーブル式（CTE）をサポートします。[#65932](https://github.com/StarRocks/starrocks/pull/65932)

- 統計ベースのスキュー検出、ヒストグラムのサポート、NULL スキュー認識により、Skew Join v2 の書き換えを改善しました。[#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)

- ウィンドウにわたる COUNT DISTINCT を改善し、融合された複数重複排除集計のサポートを追加しました。[#67453](https://github.com/StarRocks/starrocks/pull/67453)

- ウィンドウ関数に対する明示的なスキューヒントをサポートし、スキューしたパーティションキーを持つウィンドウ関数を UNION に分割することで自動的に最適化します。[#67944](https://github.com/StarRocks/starrocks/pull/67944)

- Trino Parser の INSERT ステートメントに対する EXPLAIN および EXPLAIN ANALYZE をサポートします。[#70174](https://github.com/StarRocks/starrocks/pull/70174)

- クエリキューの可視性のための EXPLAIN をサポートします。[#69933](https://github.com/StarRocks/starrocks/pull/69933)

### 関数と SQL 構文 {/* #functions-and-sql-syntax */}

- 以下の関数を追加しました：
  - `array_top_n`：配列から値でランク付けされた上位 N 個の要素を返します。[#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`：複数の配列を要素ごとに結合して構造体の配列にします。[#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`：JSON 文字列をインデント付きでフォーマットします。[#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`：JSON 文字列内の指定されたパスに値を設定します。[#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`：各単語の最初の文字を大文字に変換します。[#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`：同じキーを持つ行にわたって MAP 値を合計します。[#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`：現在のセッションのタイムゾーンを返します。[#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`：現在のウェアハウスの名前を返します。[#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`：秒数を TIME 値に変換します。[#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`：推論ワークロードのために SQL から外部 AI モデルを呼び出します。[#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `raise_error`：SQL 式でユーザー定義エラーを発生させます。[#69661](https://github.com/StarRocks/starrocks/pull/69661)
- 以下の関数または構文の拡張を提供します：
  - カスタムソート順のために `array_sort` でラムダ比較演算子をサポートします。[#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - SQL 標準のセマンティクスを持つ FULL OUTER JOIN 向けの USING 句をサポートします。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - ORDER BY/PARTITION BY を使用したフレーム付きウィンドウ関数での DISTINCT 集計をサポートします。[#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - `lead`/`lag`/`first_value`/`last_value` ウィンドウ関数で ARRAY タイプをサポートします。[#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - count distinct 系の集計関数に対して VARBINARY をサポートします。[#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - IN 式での日付型と文字列型のキャストをサポートします。[#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - BEGIN/START TRANSACTION 向けの WITH LABEL 構文をサポートします。[#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - SHOW 文での WHERE/ORDER/LIMIT 句をサポートします。[#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - タスク管理のための `ALTER TASK` 文をサポートします。[#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - CSV ファイルのエクスポートにおいて複数の圧縮フォーマット（GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2）をサポートします。[#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - 名前ベースの構造体フィールドマッチングのために `STRUCT_CAST_BY_NAME` SQL モードをサポートします。[#69845](https://github.com/StarRocks/starrocks/pull/69845)

### 管理と可観測性 {/* #management--observability */}

- マルチウェアハウスの CPU リソース分離を改善するために、リソースグループ向けの `warehouses`、`cpu_weight_percent`、`exclusive_cpu_weight` 属性をサポートします。[#66947](https://github.com/StarRocks/starrocks/pull/66947)
- FE スレッドの状態を調査するための `information_schema.fe_threads` システムビューを導入します。[#65431](https://github.com/StarRocks/starrocks/pull/65431)
- クラスタレベルで特定のクエリパターンをブロックするために SQL Digest Blacklist をサポートします。[#66499](https://github.com/StarRocks/starrocks/pull/66499)
- ネットワークトポロジの制約によりアクセスできないノードからの Arrow Flight Data Retrieval をサポートします。[#66348](https://github.com/StarRocks/starrocks/pull/66348)
- 再接続せずに既存の接続へグローバル変数の変更を反映させる REFRESH CONNECTIONS コマンドを導入します。[#64964](https://github.com/StarRocks/starrocks/pull/64964)
- クエリプロファイルを分析し、フォーマット済みの SQL を表示するための組み込み UI 機能を追加し、クエリチューニングをより利用しやすくしました。[#63867](https://github.com/StarRocks/starrocks/pull/63867)
- 構造化されたクラスタ概要を提供するために `ClusterSummaryActionV2` API エンドポイントを実装しました。[#68836](https://github.com/StarRocks/starrocks/pull/68836)
- 現在のクラスタ実行モード（共有データまたは共有なし）を照会するためのグローバルな読み取り専用システム変数 `@@run_mode` を追加しました。[#69247](https://github.com/StarRocks/starrocks/pull/69247)
- クエリキューの管理を改善するために、`query_queue_v2` をデフォルトで有効化しました。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- Stream Load および Merge Commit 操作に対して、ユーザーレベルのデフォルトウェアハウスをサポートします。[#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- 必要に応じてバックエンドブラックリスト検証をバイパスするための `skip_black_list` セッション変数を追加しました。[#67467](https://github.com/StarRocks/starrocks/pull/67467)
- メトリクス API 向けに `enable_table_metrics_collect` オプションを追加しました。[#68691](https://github.com/StarRocks/starrocks/pull/68691)
- クエリ詳細 HTTP API に対するユーザーのなりすましサポートを追加しました。[#68674](https://github.com/StarRocks/starrocks/pull/68674)
- テーブルレベルのプロパティとして `table_query_timeout` を追加しました。[#67547](https://github.com/StarRocks/starrocks/pull/67547)
- FE オブザーバーノードの追加をサポートします。[#67778](https://github.com/StarRocks/starrocks/pull/67778)
- ロードジョブの可視性を向上させるために、`information_schema.loads` における Merge Commit 情報をサポートします。[#67879](https://github.com/StarRocks/starrocks/pull/67879)
- トラブルシューティングを向上させるために、cloud ネイティブテーブルでの tablet ステータスの表示をサポートします。[#69616](https://github.com/StarRocks/starrocks/pull/69616)

### セキュリティ {/* #security-8 */}

- [CVE-2026-33870] [CVE-2026-33871] AWS バンドルを置き換え、Netty を 4.1.132.Final にアップグレードしました。[#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] Hadoop を v3.4.2 にアップグレードしました。[#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] `spark-core_2.12` を 3.5.7 にアップグレードしました。[#70862](https://github.com/StarRocks/starrocks/pull/70862)

### バグ修正 {/* #bug-fixes-8 */}

以下の問題が修正されました：

- レンジ分散 tablet に対するデータファイル削除をスキップすることで、tablet 分割後のデータ損失を修正しました。[#71135](https://github.com/StarRocks/starrocks/pull/71135)
- 複合型に対する `DefaultValueColumnIterator` でのメモリリークを修正しました。[#71142](https://github.com/StarRocks/starrocks/pull/71142)
- `shared_ptr` サイクルが `BatchUnit` と `FetchTaskContext` の間で発生することによって引き起こされるメモリリークを修正しました。[#71126](https://github.com/StarRocks/starrocks/pull/71126)
- 同時getlineアクセスによる SystemMetrics での double-free クラッシュを修正しました。[#71040](https://github.com/StarRocks/starrocks/pull/71040)
- eager merge が all ブロックを消費する際に SpillMemTableSink でクラッシュする問題を修正しました。[#69046](https://github.com/StarRocks/starrocks/pull/69046)
- 自動作成されたパーティションが TTL クリーナーによって削除された際の NPE を修正しました。[#68257](https://github.com/StarRocks/starrocks/pull/68257)
- スナップショットが期限切れの場合に `IcebergCatalog.getPartitionLastUpdatedTime` で発生する NPE を修正しました。[#68925](https://github.com/StarRocks/starrocks/pull/68925)
- 定数側の列参照を持つ outer join における不正な述語書き換えを修正しました。[#67072](https://github.com/StarRocks/starrocks/pull/67072)
- 共有データモードで CHAR 列の長さを変更した後にクエリ結果が誤りとなる問題を修正しました。[#68808](https://github.com/StarRocks/starrocks/pull/68808)
- 複数テーブルの場合における MV リフレッシュのバグを修正しました。[#61763](https://github.com/StarRocks/starrocks/pull/61763)
- 強制リフレッシュした場合に MV のリサイクル時間が不正になる問題を修正しました。[#68673](https://github.com/StarRocks/starrocks/pull/68673)
- 同期 MV における all-null 値処理のバグを修正しました。[#69136](https://github.com/StarRocks/starrocks/pull/69136)
- fast schema change ADD COLUMN の後に MV をクエリすると重複した列 ID エラーが発生する問題を修正しました。[#71072](https://github.com/StarRocks/starrocks/pull/71072)
- 共有された DecodeInfo によって引き起こされる低基数書き換えの NPE を修正しました。[#68799](https://github.com/StarRocks/starrocks/pull/68799)
- 低基数ジョイン述語のタイプ不一致を修正しました。[#68568](https://github.com/StarRocks/starrocks/pull/68568)
- `null_counts` が空の場合に Parquet Page Index Filter で発生する Segfault を修正しました。[#68463](https://github.com/StarRocks/starrocks/pull/68463)
- 同一パス上で配列とオブジェクトを JSON 平坦化する際の競合を修正しました。[#68804](https://github.com/StarRocks/starrocks/pull/68804)
- Iceberg キャッシュ weigher の不正確さを修正しました。[#69058](https://github.com/StarRocks/starrocks/pull/69058)
- Iceberg テーブルキャッシュのメモリ制限を修正しました。[#67769](https://github.com/StarRocks/starrocks/pull/67769)
- Iceberg の delete 列の nullability の問題を修正しました。[#68649](https://github.com/StarRocks/starrocks/pull/68649)
- Azure ABFS/WASB FileSystem のキャッシュキーにコンテナを含めるよう修正しました。[#68901](https://github.com/StarRocks/starrocks/pull/68901)
- HMS 接続プールが満杯の場合のデッドロックを修正しました。[#68033](https://github.com/StarRocks/starrocks/pull/68033)
- Paimon Catalog における VARCHAR フィールドタイプの不正な長さを修正しました。[#68383](https://github.com/StarRocks/starrocks/pull/68383)
- Paimon catalog のリフレッシュ時に ObjectTable で ClassCastException によりクラッシュする問題を修正しました。[#70224](https://github.com/StarRocks/starrocks/pull/70224)
- 定数サブクエリを伴う FULL OUTER JOIN USING を修正しました。[#69028](https://github.com/StarRocks/starrocks/pull/69028)
- CTE スコープを伴う join の on 句のバグを修正しました。[#68809](https://github.com/StarRocks/starrocks/pull/68809)
- bindScope() パターンを使用することで ConnectContext のメモリリークを修正しました。[#68215](https://github.com/StarRocks/starrocks/pull/68215)
- 共有なしクラスタ向けの `CatalogRecycleBin.asyncDeleteForTables` におけるメモリリークを修正しました。[#68275](https://github.com/StarRocks/starrocks/pull/68275)
- 例外が発生した際に Thrift の accept スレッドが終了してしまう問題を修正しました。[#68644](https://github.com/StarRocks/starrocks/pull/68644)
- routine load の列マッピングにおける UDF 解決を修正しました。[#68201](https://github.com/StarRocks/starrocks/pull/68201)
- `DROP FUNCTION IF EXISTS` が `ifExists` フラグを無視する問題を修正しました。[#69216](https://github.com/StarRocks/starrocks/pull/69216)
- dict ページが大きすぎる場合のスキャン結果エラーを修正しました。[#68258](https://github.com/StarRocks/starrocks/pull/68258)
- レンジパーティション化の重複を修正しました。[#68255](https://github.com/StarRocks/starrocks/pull/68255)
- クエリキューの割り当て時間と保留タイムアウトを修正しました。[#65802](https://github.com/StarRocks/starrocks/pull/65802)
- null リテラル配列を処理する際の `array_map` のクラッシュを修正しました。[#70629](https://github.com/StarRocks/starrocks/pull/70629)
- `to_base64` のスタックオーバーフローを修正しました。[#70623](https://github.com/StarRocks/starrocks/pull/70623)
- LDAP 認証における大文字小文字を区別しないユーザー名の正規化を修正しました。[#67966](https://github.com/StarRocks/starrocks/pull/67966)
- API `proc_file` の SSRF リスクを軽減しました。[#68997](https://github.com/StarRocks/starrocks/pull/68997)
- 監査ログおよび SQL のリダクションにおいてユーザー認証文字列をマスクしました。[#70360](https://github.com/StarRocks/starrocks/pull/70360)

### 動作の変更 {/* #behavior-changes-7 */}

- ETL 実行モードの最適化がデフォルトで有効になりました。これにより、INSERT INTO SELECT、CREATE TABLE AS SELECT、および同様のバッチワークロードが、明示的な設定変更なしに恩恵を受けられます。[#66841](https://github.com/StarRocks/starrocks/pull/66841)
- `lag`/`lead` ウィンドウ関数の3番目の引数が、定数値に加えて列参照もサポートするようになりました。[#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING は現在 SQL 標準のセマンティクスに従うようになりました：出力に USING 列が2回ではなく1回だけ表示されます。[#65122](https://github.com/StarRocks/starrocks/pull/65122)
- `query_queue_v2` はデフォルトで有効になりました。[#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQL トランザクションは、デフォルトでセッション変数 `enable_sql_transaction` の背後でゲートされています。[#63535](https://github.com/StarRocks/starrocks/pull/63535)
- 以前は、FE 設定項目 `max_scalar_operator_flat_children`（デフォルト `10000`）は `CASE WHEN` によって構築される式のノード数のみを制限していました。現在は、オプティマイザが書き換えるすべてのスカラー式を制限するようになり、ノード数はキャッシュされた値を再利用するのではなく、書き換えのたびに再計算されます。その結果、アップグレード後は、以前は実行できていたクエリが、ビュー、CTE、またはサブクエリのインライン化によって式ツリーが拡張される場合に、解析フェーズで `Expression too complex. Current nodes: N, Limit: M.` として拒否されることがあります。式を簡略化するか、動的に変更可能な `max_scalar_operator_flat_children` を増やしてください。[#66324](https://github.com/StarRocks/starrocks/pull/66324)
