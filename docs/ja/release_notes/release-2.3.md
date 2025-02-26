---
displayed_sidebar: docs
---

# StarRocks version 2.3

## 2.3.18

リリース日: 2023年10月11日

### バグ修正

以下の問題を修正しました:

- サードパーティライブラリ librdkafka のバグにより、Routine Load ジョブのロードタスクがデータロード中にスタックし、新しく作成されたロードタスクも実行に失敗します。 [#28301](https://github.com/StarRocks/starrocks/pull/28301)
- Spark または Flink コネクタがメモリ統計の不正確さのためにデータのエクスポートに失敗する可能性があります。 [#31200](https://github.com/StarRocks/starrocks/pull/31200) [#30751](https://github.com/StarRocks/starrocks/pull/30751)
- Stream Load ジョブがキーワード `if` を使用すると、BEs がクラッシュします。 [#31926](https://github.com/StarRocks/starrocks/pull/31926)
- パーティション化された StarRocks 外部テーブルにデータをロードする際に、エラー `"get TableMeta failed from TNetworkAddress"` が報告されます。 [#30466](https://github.com/StarRocks/starrocks/pull/30466)

## 2.3.17

リリース日: 2023年9月4日

### バグ修正

以下の問題を修正しました:

- Routine Load ジョブがデータの消費に失敗します。 [#29883](https://github.com/StarRocks/starrocks/issues/29883) [#18550](https://github.com/StarRocks/starrocks/pull/18550)

## 2.3.16

リリース日: 2023年8月4日

### バグ修正

LabelCleaner スレッドがブロックされたことによる FE メモリリーク。 [#28311](https://github.com/StarRocks/starrocks/pull/28311) [#28636](https://github.com/StarRocks/starrocks/pull/28636)

## 2.3.15

リリース日: 2023年7月31日

### 改善

- タブレットが長期間保留状態になったり、特定の状況で FE がクラッシュしたりしないように、タブレットスケジューリングロジックを最適化しました。 [#21647](https://github.com/StarRocks/starrocks/pull/21647) [#23062](https://github.com/StarRocks/starrocks/pull/23062) [#25785](https://github.com/StarRocks/starrocks/pull/25785)
- TabletChecker のスケジューリングロジックを最適化し、修復されていないタブレットを繰り返しスケジューリングしないようにしました。 [#27648](https://github.com/StarRocks/starrocks/pull/27648)
- パーティションメタデータが visibleTxnId を記録し、これはタブレットレプリカの可視バージョンに対応します。レプリカのバージョンが他と一致しない場合、このバージョンを作成したトランザクションを追跡しやすくなります。 [#27924](https://github.com/StarRocks/starrocks/pull/27924)

### バグ修正

以下の問題を修正しました:

- FEs のテーブルレベルのスキャン統計が不正確なため、テーブルクエリとロードに関連するメトリクスが不正確になります。 [#28022](https://github.com/StarRocks/starrocks/pull/28022)
- Join キーが大きな BINARY 列の場合、BEs がクラッシュする可能性があります。 [#25084](https://github.com/StarRocks/starrocks/pull/25084)
- 集計オペレーターが特定のシナリオでスレッドセーフティの問題を引き起こし、BEs がクラッシュする可能性があります。 [#26092](https://github.com/StarRocks/starrocks/pull/26092)
- データが [RESTORE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/RESTORE/) を使用して復元された後、タブレットのバージョン番号が BE と FE の間で一致しません。 [#26518](https://github.com/StarRocks/starrocks/pull/26518/files)
- [RECOVER](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/RECOVER/) を使用してテーブルを復元した後、パーティションが自動的に作成されません。 [#26813](https://github.com/StarRocks/starrocks/pull/26813)
- INSERT INTO を使用してロードされるデータが品質要件を満たさず、データロードの厳密モードが有効になっている場合、ロードトランザクションが保留状態でスタックし、DDL ステートメントがハングします。 [#27140](https://github.com/StarRocks/starrocks/pull/27140)
- 低基数最適化が有効になっている場合、一部の INSERT ジョブが `[42000][1064] Dict Decode failed, Dict can't take cover all key :0` を返します。 [#27395](https://github.com/StarRocks/starrocks/pull/27395)
- パイプラインが有効になっていない場合、INSERT INTO SELECT 操作がタイムアウトすることがあります。 [#26594](https://github.com/StarRocks/starrocks/pull/26594)
- クエリ条件が `WHERE partition_column < xxx` で、`xxx` の値が時間までしか正確でない場合、クエリがデータを返しません。例えば、`2023-7-21 22`。 [#27780](https://github.com/StarRocks/starrocks/pull/27780)

## 2.3.14

リリース日: 2023年6月28日

### 改善

- CREATE TABLE がタイムアウトした際に返されるエラーメッセージを最適化し、パラメータ調整のヒントを追加しました。 [#24510](https://github.com/StarRocks/starrocks/pull/24510)
- 多数の累積タブレットバージョンを持つ主キーテーブルのメモリ使用量を最適化しました。 [#20760](https://github.com/StarRocks/starrocks/pull/20760)
- StarRocks 外部テーブルメタデータの同期がデータロード中に行われるように変更されました。 [#24739](https://github.com/StarRocks/starrocks/pull/24739)
- サーバー間でシステムクロックが不一致であるために発生する不正確な NetworkTime を修正するために、NetworkTime のシステムクロックへの依存を削除しました。 [#24858](https://github.com/StarRocks/starrocks/pull/24858)

### バグ修正

以下の問題を修正しました:

- 小さなテーブルに対して低基数辞書最適化が適用され、頻繁に TRUNCATE 操作が行われる場合、クエリがエラーに遭遇することがあります。 [#23185](https://github.com/StarRocks/starrocks/pull/23185)
- ビューが UNION を含み、その最初の子が定数 NULL である場合、クエリ時に BEs がクラッシュすることがあります。 [#13792](https://github.com/StarRocks/starrocks/pull/13792)
- 一部のケースでは、ビットマップインデックスに基づくクエリがエラーを返すことがあります。 [#23484](https://github.com/StarRocks/starrocks/pull/23484)
- DOUBLE または FLOAT 値を DECIMAL 値に丸めた結果が、BEs と FEs で一致しません。 [#23152](https://github.com/StarRocks/starrocks/pull/23152)
- スキーマ変更がデータロードと同時に発生する場合、スキーマ変更がハングすることがあります。 [#23456](https://github.com/StarRocks/starrocks/pull/23456)
- Broker Load、Spark コネクタ、または Flink コネクタを使用して Parquet ファイルを StarRocks にロードする際、BE OOM の問題が発生することがあります。 [#25254](https://github.com/StarRocks/starrocks/pull/25254)
- ORDER BY 句に定数が指定され、クエリに LIMIT 句が含まれている場合、エラーメッセージ `unknown error` が返されます。 [#25538](https://github.com/StarRocks/starrocks/pull/25538)

## 2.3.13

リリース日: 2023年6月1日

### 改善

- INSERT INTO ... SELECT が小さな `thrift_server_max_worker_thread` 値のために期限切れになる際に報告されるエラーメッセージを最適化しました。 [#21964](https://github.com/StarRocks/starrocks/pull/21964)
- `bitmap_contains` 関数を使用するマルチテーブルジョインのメモリ消費を削減し、パフォーマンスを最適化しました。 [#20617](https://github.com/StarRocks/starrocks/pull/20617) [#20653](https://github.com/StarRocks/starrocks/pull/20653)

### バグ修正

以下のバグを修正しました:

- パーティション名に対して TRUNCATE 操作が大文字小文字を区別するため、パーティションの切り捨てが失敗します。 [#21809](https://github.com/StarRocks/starrocks/pull/21809)
- Parquet ファイルから int96 タイムスタンプデータをロードするとデータオーバーフローが発生します。 [#22355](https://github.com/StarRocks/starrocks/issues/22355)
- マテリアライズドビューが削除された後、BE の退役が失敗します。 [#22743](https://github.com/StarRocks/starrocks/issues/22743)
- クエリの実行計画に Broadcast Join の後に Bucket Shuffle Join が含まれている場合、例えば `SELECT * FROM t1 JOIN [Broadcast] t2 ON t1.a = t2.b JOIN [Bucket] t3 ON t2.b = t3.c` のように、Broadcast Join の左テーブルの等値結合キーのデータが Bucket Shuffle Join に送信される前に削除されると、BE がクラッシュすることがあります。 [#23227](https://github.com/StarRocks/starrocks/pull/23227)
- クエリの実行計画に Cross Join の後に Hash Join が含まれており、フラグメントインスタンス内の Hash Join の右テーブルが空の場合、返される結果が不正確になることがあります。 [#23877](https://github.com/StarRocks/starrocks/pull/23877)
- マテリアライズドビューの一時パーティションの作成に失敗したため、BE の退役が失敗します。 [#22745](https://github.com/StarRocks/starrocks/pull/22745)
- SQL ステートメントに複数のエスケープ文字を含む STRING 値が含まれている場合、SQL ステートメントを解析できません。 [#23119](https://github.com/StarRocks/starrocks/issues/23119)
- パーティション列の最大値を持つデータをクエリする際に失敗します。 [#23153](https://github.com/StarRocks/starrocks/issues/23153)
- StarRocks が v2.4 から v2.3 にロールバックされた後、ロードジョブが失敗します。 [#23642](https://github.com/StarRocks/starrocks/pull/23642)
- 列のプルーニングと再利用に関連する問題。 [#16624](https://github.com/StarRocks/starrocks/issues/16624)

## 2.3.12

リリース日: 2023年4月25日

### 改善

式の返される値が有効なブール値に変換できる場合、暗黙の変換をサポートします。 [# 21792](https://github.com/StarRocks/starrocks/pull/21792)

### バグ修正

以下のバグを修正しました:

- ユーザーの LOAD_PRIV がテーブルレベルで付与されている場合、ロードジョブが失敗した際にトランザクションがロールバックされると、エラーメッセージ `Access denied; you need (at least one of) the LOAD privilege(s) for this operation` が返されます。 [# 21129](https://github.com/StarRocks/starrocks/issues/21129)
- ALTER SYSTEM DROP BACKEND が実行されて BE が削除された後、その BE 上でレプリケーション数が 2 に設定されているテーブルのレプリカを修復できません。この状況では、これらのテーブルへのデータロードが失敗します。 [# 20681](https://github.com/StarRocks/starrocks/pull/20681)
- CREATE TABLE でサポートされていないデータ型が使用されると NPE が返されます。 [# 20999](https://github.com/StarRocks/starrocks/issues/20999)
- Broadcast Join のショートサーキットロジックが異常で、クエリ結果が不正確になります。 [# 20952](https://github.com/StarRocks/starrocks/issues/20952)
- マテリアライズドビューが使用された後、ディスク使用量が大幅に増加することがあります。 [# 20590](https://github.com/StarRocks/starrocks/pull/20590)
- Audit Loader プラグインを完全にアンインストールできません。 [# 20468](https://github.com/StarRocks/starrocks/issues/20468)
- `INSERT INTO XXX SELECT` の結果に表示される行数が `SELECT COUNT(*) FROM XXX` の結果と一致しないことがあります。 [# 20084](https://github.com/StarRocks/starrocks/issues/20084)
- サブクエリがウィンドウ関数を使用し、その親クエリが GROUP BY 句を使用する場合、クエリ結果を集計できません。 [# 19725](https://github.com/StarRocks/starrocks/issues/19725)
- BE が起動されると、BE プロセスは存在しますが、すべての BE のポートを開くことができません。 [# 19347](https://github.com/StarRocks/starrocks/pull/19347)
- ディスク I/O が非常に高い場合、主キーテーブルのトランザクションが遅くコミットされ、その結果、これらのテーブルに対するクエリが "backend not found" エラーを返すことがあります。 [# 18835](https://github.com/StarRocks/starrocks/issues/18835)

## 2.3.11

リリース日: 2023年3月28日

### 改善

- 多くの式を含む複雑なクエリを実行すると、通常、多数の `ColumnRefOperators` が生成されます。元々、StarRocks は `BitSet` を使用して `ColumnRefOperator::id` を格納していましたが、これは大量のメモリを消費します。メモリ使用量を削減するために、StarRocks は現在 `RoaringBitMap` を使用して `ColumnRefOperator::id` を格納しています。 [#16499](https://github.com/StarRocks/starrocks/pull/16499)
- 大規模なクエリが小規模なクエリに与えるパフォーマンスへの影響を軽減するために、新しい I/O スケジューリング戦略が導入されました。新しい I/O スケジューリング戦略を有効にするには、BE 静的パラメータ `pipeline_scan_queue_mode=1` を **be.conf** に設定し、その後 BEs を再起動します。 [#19009](https://github.com/StarRocks/starrocks/pull/19009)

### バグ修正

以下のバグを修正しました:

- 期限切れデータが適切にリサイクルされないテーブルが、比較的大きなディスクスペースを占有します。 [#19796](https://github.com/StarRocks/starrocks/pull/19796)
- 次のシナリオで表示されるエラーメッセージが情報不足です: Broker Load ジョブが Parquet ファイルを StarRocks にロードし、`NULL` 値が NOT NULL 列にロードされる場合。 [#19885](https://github.com/StarRocks/starrocks/pull/19885)
- 多数の一時パーティションを頻繁に作成して既存のパーティションを置き換えると、メモリリークと FE ノードでの Full GC が発生します。 [#19283](https://github.com/StarRocks/starrocks/pull/19283)
- コロケーションテーブルでは、`ADMIN SET REPLICA STATUS PROPERTIES ("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");` のようなステートメントを使用してレプリカのステータスを手動で `bad` に指定できます。BEs の数がレプリカの数以下の場合、破損したレプリカを修復できません。 [#19443](https://github.com/StarRocks/starrocks/pull/19443)
- リクエスト `INSERT INTO SELECT` が Follower FE に送信されると、パラメータ `parallel_fragment_exec_instance_num` が効果を発揮しません。 [#18841](https://github.com/StarRocks/starrocks/pull/18841)
- 演算子 `<=>` を使用して値を `NULL` 値と比較すると、比較結果が不正確になります。 [#19210](https://github.com/StarRocks/starrocks/pull/19210)
- クエリの同時実行メトリックが、リソースグループの同時実行制限に継続的に達した場合にゆっくりと減少します。 [#19363](https://github.com/StarRocks/starrocks/pull/19363)
- 高度に同時実行されるデータロードジョブが、エラー `"get database read lock timeout, database=xxx"` を引き起こす可能性があります。 [#16748](https://github.com/StarRocks/starrocks/pull/16748) [#18992](https://github.com/StarRocks/starrocks/pull/18992)

## 2.3.10

リリース日: 2023年3月9日

### 改善

`storage_medium` の推論を最適化しました。BEs が SSD と HDD の両方を記憶媒体として使用する場合、プロパティ `storage_cooldown_time` が指定されている場合、StarRocks は `storage_medium` を `SSD` に設定します。それ以外の場合、StarRocks は `storage_medium` を `HDD` に設定します。 [#18649](https://github.com/StarRocks/starrocks/pull/18649)

### バグ修正

以下のバグを修正しました:

- データレイクの Parquet ファイルから ARRAY データをクエリすると、クエリが失敗することがあります。 [#17626](https://github.com/StarRocks/starrocks/pull/17626) [#17788](https://github.com/StarRocks/starrocks/pull/17788) [#18051](https://github.com/StarRocks/starrocks/pull/18051)
- プログラムによって開始された Stream Load ジョブがハングし、FE がプログラムから送信された HTTP リクエストを受信しません。 [#18559](https://github.com/StarRocks/starrocks/pull/18559)
- Elasticsearch 外部テーブルをクエリするとエラーが発生することがあります。 [#13727](https://github.com/StarRocks/starrocks/pull/13727)
- 式が初期化中にエラーに遭遇すると、BEs がクラッシュすることがあります。 [#11396](https://github.com/StarRocks/starrocks/pull/11396)
- SQL ステートメントが空の配列リテラル `[]` を使用すると、クエリが失敗することがあります。 [#18550](https://github.com/StarRocks/starrocks/pull/18550)
- StarRocks がバージョン 2.2 以降からバージョン 2.3.9 以降にアップグレードされた後、Routine Load ジョブが `COLUMN` パラメータに計算式を指定して作成されると、エラー `No match for <expr> with operand types xxx and xxx` が発生することがあります。 [#17856](https://github.com/StarRocks/starrocks/pull/17856)
- BE が再起動した後、ロードジョブがハングします。 [#18488](https://github.com/StarRocks/starrocks/pull/18488)
- SELECT ステートメントが WHERE 句で OR 演算子を使用すると、余分なパーティションがスキャンされます。 [#18610](https://github.com/StarRocks/starrocks/pull/18610)

## 2.3.9

リリース日: 2023年2月20日

### バグ修正

- スキーマ変更中にタブレットクローンがトリガーされ、タブレットレプリカが存在する BE ノードが変更されると、スキーマ変更が失敗します。 [#16948](https://github.com/StarRocks/starrocks/pull/16948)
- group_concat() 関数が返す文字列が切り捨てられます。 [#16948](https://github.com/StarRocks/starrocks/pull/16948)
- Tencent Big Data Suite (TBDS) を通じて HDFS からデータをロードするために Broker Load を使用すると、エラー `invalid hadoop.security.authentication.tbds.securekey` が発生し、StarRocks が TBDS によって提供された認証情報を使用して HDFS にアクセスできないことを示します。 [#14125](https://github.com/StarRocks/starrocks/pull/14125) [#15693](https://github.com/StarRocks/starrocks/pull/15693)
- 一部のケースでは、CBO が 2 つのオペレーターが等価であるかどうかを比較するために不正確なロジックを使用することがあります。 [#17227](https://github.com/StarRocks/starrocks/pull/17227) [#17199](https://github.com/StarRocks/starrocks/pull/17199)
- 非 Leader FE ノードに接続して SQL ステートメント `USE <catalog_name>.<database_name>` を送信すると、非 Leader FE ノードが `<catalog_name>` を除外して SQL ステートメントを Leader FE ノードに転送します。その結果、Leader FE ノードは `default_catalog` を使用することを選択し、最終的に指定されたデータベースを見つけることができません。 [#17302](https://github.com/StarRocks/starrocks/pull/17302)

## 2.3.8

リリース日: 2023年2月2日

### バグ修正

以下のバグを修正しました:

- 大規模なクエリが終了した後にリソースが解放される際、他のクエリが遅くなる可能性が低い確率であります。この問題は、リソースグループが有効になっている場合や、大規模なクエリが予期せず終了した場合に発生しやすくなります。 [#16454](https://github.com/StarRocks/starrocks/pull/16454) [#16602](https://github.com/StarRocks/starrocks/pull/16602)
- 主キーテーブルの場合、レプリカのメタデータバージョンが遅れていると、StarRocks は他のレプリカからこのレプリカに不足しているメタデータをインクリメンタルにクローンします。このプロセスで、StarRocks は大量のメタデータバージョンをプルし、メタデータのバージョンがタイムリーに GC されないと、過剰なメモリが消費され、結果として BEs が OOM 例外に遭遇する可能性があります。 [#15935](https://github.com/StarRocks/starrocks/pull/15935)
- FE が BE に偶発的なハートビートを送信し、ハートビート接続がタイムアウトすると、FE は BE を利用できないと見なし、BE 上のトランザクションが失敗します。 [# 16386](https://github.com/StarRocks/starrocks/pull/16386)
- StarRocks クラスター間でデータをロードするために StarRocks 外部テーブルを使用する場合、ソース StarRocks クラスターが以前のバージョンであり、ターゲット StarRocks クラスターが後のバージョン（2.2.8 ~ 2.2.11、2.3.4 ~ 2.3.7、2.4.1 または 2.4.2）である場合、データロードが失敗します。 [#16173](https://github.com/StarRocks/starrocks/pull/16173)
- 複数のクエリが同時に実行され、メモリ使用量が比較的高い場合、BEs がクラッシュします。 [#16047](https://github.com/StarRocks/starrocks/pull/16047)
- テーブルに対して動的パーティション化が有効になっており、一部のパーティションが動的に削除される場合、TRUNCATE TABLE を実行すると、エラー `NullPointerException` が返されます。同時に、テーブルにデータをロードすると、FEs がクラッシュして再起動できません。 [#16822](https://github.com/StarRocks/starrocks/pull/16822)

## 2.3.7

リリース日: 2022年12月30日

### バグ修正

以下のバグを修正しました:

- StarRocks テーブルで NULL が許可されている列が、そのテーブルから作成されたビューで誤って NOT NULL に設定されます。 [#15749](https://github.com/StarRocks/starrocks/pull/15749)
- StarRocks にデータがロードされると、新しいタブレットバージョンが生成されます。ただし、FE はまだ新しいタブレットバージョンを検出しておらず、BEs に履歴バージョンのタブレットを読み取るように要求します。ガーベジコレクションメカニズムが履歴バージョンを削除すると、クエリは履歴バージョンを見つけることができず、エラー "Not found: get_applied_rowsets(version xxxx) failed tablet:xxx #version:x [xxxxxxx]" が返されます。 [#15726](https://github.com/StarRocks/starrocks/pull/15726)
- データが頻繁にロードされると、FE が過剰なメモリを消費します。 [#15377](https://github.com/StarRocks/starrocks/pull/15377)
- 集計クエリとマルチテーブル JOIN クエリでは、統計が正確に収集されず、実行計画に CROSS JOIN が発生し、クエリの待ち時間が長くなります。 [#12067](https://github.com/StarRocks/starrocks/pull/12067)  [#14780](https://github.com/StarRocks/starrocks/pull/14780)

## 2.3.6

リリース日: 2022年12月22日

### 改善

- パイプライン実行エンジンが INSERT INTO ステートメントをサポートします。有効にするには、FE 設定項目 `enable_pipeline_load_for_insert` を `true` に設定します。  [#14723](https://github.com/StarRocks/starrocks/pull/14723)
- 主キーテーブルの Compaction に使用されるメモリを削減しました。 [#13861](https://github.com/StarRocks/starrocks/pull/13861)  [#13862](https://github.com/StarRocks/starrocks/pull/13862)

### 動作変更

- FE パラメータ `default_storage_medium` を廃止しました。テーブルの記憶媒体はシステムによって自動的に推論されます。 [#14394](https://github.com/StarRocks/starrocks/pull/14394)

### バグ修正

以下のバグを修正しました:

- リソースグループ機能が有効になっており、複数のリソースグループが同時にクエリを実行すると、BEs がハングアップすることがあります。 [#14905](https://github.com/StarRocks/starrocks/pull/14905)
- CREATE MATERIALIZED VIEW AS SELECT を使用してマテリアライズドビューを作成する際、SELECT 句が集計関数を使用せず、GROUP BY を使用する場合、例えば `CREATE MATERIALIZED VIEW test_view AS SELECT a,b from test group by b,a order by a;` のように、BE ノードがすべてクラッシュします。 [#13743](https://github.com/StarRocks/starrocks/pull/13743)
- 主キーテーブルに頻繁にデータをロードしてデータ変更を行った直後に BE を再起動すると、BE の再起動が非常に遅くなることがあります。 [#15128](https://github.com/StarRocks/starrocks/pull/15128)
- 環境に JRE のみがインストールされており、JDK がインストールされていない場合、FE の再起動後にクエリが失敗します。バグが修正された後、その環境で FE を再起動できず、エラー `JAVA_HOME can not be jre` が返されます。FE を正常に再起動するには、環境に JDK をインストールする必要があります。 [#14332](https://github.com/StarRocks/starrocks/pull/14332)
- クエリが BE のクラッシュを引き起こします。 [#14221](https://github.com/StarRocks/starrocks/pull/14221)
- `exec_mem_limit` を式に設定できません。 [#13647](https://github.com/StarRocks/starrocks/pull/13647)
- サブクエリ結果に基づいて同期更新マテリアライズドビューを作成できません。 [#13507](https://github.com/StarRocks/starrocks/pull/13507)
- Hive 外部テーブルを更新した後、列のコメントが削除されます。 [#13742](https://github.com/StarRocks/starrocks/pull/13742)
- 相関 JOIN 中に、右テーブルが左テーブルの前に処理され、右テーブルが非常に大きい場合、左テーブルに対して Compaction が実行されると、BE ノードがクラッシュします。 [#14070](https://github.com/StarRocks/starrocks/pull/14070)
- Parquet ファイルの列名が大文字小文字を区別し、クエリ条件が Parquet ファイルの大文字の列名を使用する場合、クエリは結果を返しません。 [#13860](https://github.com/StarRocks/starrocks/pull/13860) [#14773](https://github.com/StarRocks/starrocks/pull/14773)
- バルクローディング中に、Broker への接続数がデフォルトの最大接続数を超えると、Broker が切断され、ロードジョブがエラーメッセージ `list path error` と共に失敗します。 [#13911](https://github.com/StarRocks/starrocks/pull/13911)
- BEs が高負荷の場合、リソースグループのメトリック `starrocks_be_resource_group_running_queries` が不正確になることがあります。 [#14043](https://github.com/StarRocks/starrocks/pull/14043)
- クエリステートメントが OUTER JOIN を使用すると、BE ノードがクラッシュすることがあります。 [#14840](https://github.com/StarRocks/starrocks/pull/14840)
- StarRocks 2.4 を使用して非同期マテリアライズドビューを作成し、2.3 にロールバックすると、FE が起動に失敗することがあります。 [#14400](https://github.com/StarRocks/starrocks/pull/14400)
- 主キーテーブルが delete_range を使用し、パフォーマンスが良くない場合、RocksDB からのデータ読み取りが遅くなり、CPU 使用率が高くなることがあります。 [#15130](https://github.com/StarRocks/starrocks/pull/15130)

## 2.3.5

リリース日: 2022年11月30日

### 改善

- Colocate Join が Equi Join をサポートします。 [#13546](https://github.com/StarRocks/starrocks/pull/13546)
- データが頻繁にロードされると、WAL レコードが継続的に追加されるため、主キーインデックスファイルが大きくなりすぎる問題を修正しました。 [#12862](https://github.com/StarRocks/starrocks/pull/12862)
- FE はすべてのタブレットをバッチでスキャンし、db.readLock を長時間保持しないようにスキャン間隔で db.readLock を解放します。 [#13070](https://github.com/StarRocks/starrocks/pull/13070)

### バグ修正

以下のバグを修正しました:

- UNION ALL の結果に直接基づいてビューが作成され、UNION ALL オペレーターの入力列に NULL 値が含まれている場合、ビューのスキーマが不正確で、列のデータ型が NULL_TYPE ではなく UNION ALL の入力列になります。 [#13917](https://github.com/StarRocks/starrocks/pull/13917)
- `SELECT * FROM ...` と `SELECT * FROM ... LIMIT ...` のクエリ結果が一致しません。 [#13585](https://github.com/StarRocks/starrocks/pull/13585)
- 外部タブレットメタデータが FE に同期されると、ローカルタブレットメタデータを上書きする可能性があり、Flink からのデータロードが失敗します。 [#12579](https://github.com/StarRocks/starrocks/pull/12579)
- ランタイムフィルターでリテラル定数を処理する際に null フィルターが原因で BE ノードがクラッシュします。 [#13526](https://github.com/StarRocks/starrocks/pull/13526)
- CTAS を実行するとエラーが返されます。 [#12388](https://github.com/StarRocks/starrocks/pull/12388)
- パイプラインエンジンで監査ログに収集されたメトリック `ScanRows` が間違っている可能性があります。 [#12185](https://github.com/StarRocks/starrocks/pull/12185)
- 圧縮された HIVE データをクエリすると、クエリ結果が不正確です。 [#11546](https://github.com/StarRocks/starrocks/pull/11546)
- BE ノードがクラッシュした後、クエリがタイムアウトし、StarRocks の応答が遅くなります。 [#12955](https://github.com/StarRocks/starrocks/pull/12955)
- Broker Load を使用してデータをロードする際に、Kerberos 認証の失敗エラーが発生します。 [#13355](https://github.com/StarRocks/starrocks/pull/13355)
- OR 述語が多すぎると、統計推定に時間がかかりすぎます。 [#13086](https://github.com/StarRocks/starrocks/pull/13086)
- Broker Load が ORC ファイル（Snappy 圧縮）をロードし、大文字の列名を含む場合、BE ノードがクラッシュします。 [#12724](https://github.com/StarRocks/starrocks/pull/12724)
- プライマリキーテーブルのアンロードまたはクエリに 30 分以上かかるとエラーが返されます。 [#13403](https://github.com/StarRocks/starrocks/pull/13403)
- Broker を使用して HDFS に大容量データをバックアップする際に、バックアップタスクが失敗します。 [#12836](https://github.com/StarRocks/starrocks/pull/12836)
- Iceberg から読み取るデータが不正確になる可能性があり、これは `parquet_late_materialization_enable` パラメータが原因です。 [#13132](https://github.com/StarRocks/starrocks/pull/13132)
- ビューを作成すると、エラー `failed to init view stmt` が返されます。 [#13102](https://github.com/StarRocks/starrocks/pull/13102)
- JDBC を使用して StarRock に接続し、SQL ステートメントを実行するとエラーが返されます。 [#13526](https://github.com/StarRocks/starrocks/pull/13526)
- クエリが多くのバケットを含み、タブレットヒントを使用する場合、クエリがタイムアウトします。 [#13272](https://github.com/StarRocks/starrocks/pull/13272)
- BE ノードがクラッシュして再起動できず、その間に新しく構築されたテーブルへのロードジョブがエラーを報告します。 [#13701](https://github.com/StarRocks/starrocks/pull/13701)
- マテリアライズドビューが作成されると、すべての BE ノードがクラッシュします。 [#13184](https://github.com/StarRocks/starrocks/pull/13184)
- ALTER ROUTINE LOAD を実行して消費されたパーティションのオフセットを更新すると、エラー `The specified partition 1 is not in the consumed partitions` が返されることがあり、フォロワーが最終的にクラッシュします。 [#12227](https://github.com/StarRocks/starrocks/pull/12227)

## 2.3.4

リリース日: 2022年11月10日

### 改善

- 実行中の Routine Load ジョブの数が制限を超えたために StarRocks が Routine Load ジョブの作成に失敗した場合、エラーメッセージが解決策を提供します。 [#12204](https://github.com/StarRocks/starrocks/pull/12204)
- StarRocks が Hive からデータをクエリし、CSV ファイルの解析に失敗した場合、クエリが失敗します。 [#13013](https://github.com/StarRocks/starrocks/pull/13013)

### バグ修正

以下のバグを修正しました:

- HDFS ファイルパスに `()` が含まれている場合、クエリが失敗することがあります。 [#12660](https://github.com/StarRocks/starrocks/pull/12660)
- サブクエリに LIMIT が含まれている場合、ORDER BY ... LIMIT ... OFFSET の結果が不正確です。 [#9698](https://github.com/StarRocks/starrocks/issues/9698)
- StarRocks は ORC ファイルをクエリする際に大文字小文字を区別しません。 [#12724](https://github.com/StarRocks/starrocks/pull/12724)
- RuntimeFilter が prepare メソッドを呼び出さずに閉じられると、BE がクラッシュすることがあります。 [#12906](https://github.com/StarRocks/starrocks/issues/12906)
- メモリリークのために BE がクラッシュすることがあります。 [#12906](https://github.com/StarRocks/starrocks/issues/12906)
- 新しい列を追加してすぐにデータを削除した後、クエリ結果が不正確になることがあります。 [#12907](https://github.com/StarRocks/starrocks/pull/12907)
- データのソートのために BE がクラッシュすることがあります。 [#11185](https://github.com/StarRocks/starrocks/pull/11185)
- StarRocks と MySQL クライアントが同じ LAN にない場合、INSERT INTO SELECT を使用して作成されたロードジョブを KILL を 1 回だけ実行して正常に終了できません。 [#11879](https://github.com/StarRocks/starrocks/pull/11897)
- パイプラインエンジンで監査ログに収集されたメトリック `ScanRows` が間違っている可能性があります。 [#12185](https://github.com/StarRocks/starrocks/pull/12185)

## 2.3.3

リリース日: 2022年9月27日

### バグ修正

以下のバグを修正しました:

- テキストファイルとして保存された Hive 外部テーブルをクエリすると、クエリ結果が不正確になることがあります。 [#11546](https://github.com/StarRocks/starrocks/pull/11546)
- Parquet ファイルをクエリする際にネストされた配列がサポートされていません。 [#10983](https://github.com/StarRocks/starrocks/pull/10983)
- StarRocks と外部データソースからデータを読み取る同時クエリが同じリソースグループにルーティングされる場合、またはクエリが StarRocks と外部データソースからデータを読み取る場合、クエリがタイムアウトすることがあります。 [#10983](https://github.com/StarRocks/starrocks/pull/10983)
- パイプライン実行エンジンがデフォルトで有効になっている場合、パラメータ parallel_fragment_exec_instance_num が 1 に変更されます。これにより、INSERT INTO を使用したデータロードが遅くなります。 [#11462](https://github.com/StarRocks/starrocks/pull/11462)
- 式の初期化時にミスがある場合、BE がクラッシュすることがあります。 [#11396](https://github.com/StarRocks/starrocks/pull/11396)
- ORDER BY LIMIT を実行すると、エラー heap-buffer-overflow が発生することがあります。 [#11185](https://github.com/StarRocks/starrocks/pull/11185)
- Leader FE を再起動する間にスキーマ変更が失敗します。 [#11561](https://github.com/StarRocks/starrocks/pull/11561)

## 2.3.2

リリース日: 2022年9月7日

### 新機能

- Parquet 形式の外部テーブルに対する範囲フィルタベースのクエリを加速するために、後期実体化がサポートされます。 [#9738](https://github.com/StarRocks/starrocks/pull/9738)
- ユーザー認証関連情報を表示するために SHOW AUTHENTICATION ステートメントが追加されました。 [#9996](https://github.com/StarRocks/starrocks/pull/9996)

### 改善

- StarRocks がデータをクエリする Hive テーブルのすべてのデータファイルを再帰的にトラバースするかどうかを制御するための設定項目が提供されます。 [#10239](https://github.com/StarRocks/starrocks/pull/10239)
- リソースグループタイプ `realtime` が `short_query` に名前が変更されました。 [#10247](https://github.com/StarRocks/starrocks/pull/10247)
- StarRocks はデフォルトで Hive 外部テーブルで大文字と小文字を区別しません。 [#10187](https://github.com/StarRocks/starrocks/pull/10187)

### バグ修正

以下のバグを修正しました:

- Elasticsearch 外部テーブルに対するクエリが、テーブルが複数のシャードに分割されている場合に予期せず終了することがあります。 [#10369](https://github.com/StarRocks/starrocks/pull/10369)
- サブクエリが共通テーブル式 (CTE) として書き換えられると、StarRocks がエラーをスローします。 [#10397](https://github.com/StarRocks/starrocks/pull/10397)
- 大量のデータがロードされると、StarRocks がエラーをスローします。 [#10370](https://github.com/StarRocks/starrocks/issues/10370) [#10380](https://github.com/StarRocks/starrocks/issues/10380)
- 同じ Thrift サービス IP アドレスが複数のカタログに設定されている場合、1 つのカタログを削除すると、他のカタログの増分メタデータ更新が無効になります。 [#10511](https://github.com/StarRocks/starrocks/pull/10511)
- BEs からのメモリ消費の統計が不正確です。 [#9837](https://github.com/StarRocks/starrocks/pull/9837)
- 主キーテーブルに対するクエリで StarRocks がエラーをスローします。 [#10811](https://github.com/StarRocks/starrocks/pull/10811)
- ビューに対するクエリが、これらのビューに対する SELECT 権限を持っている場合でも許可されません。 [#10563](https://github.com/StarRocks/starrocks/pull/10563)
- StarRocks はビューの命名に制限を課していません。現在、ビューはテーブルと同じ命名規則に従う必要があります。 [#10558](https://github.com/StarRocks/starrocks/pull/10558)

### 動作変更

- ビットマップ関数のデフォルト値 1000000 で BE 設定 `max_length_for_bitmap_function` を追加し、クラッシュを防ぐために base64 のデフォルト値 200000 で `max_length_for_to_base64` を追加しました。 [#10851](https://github.com/StarRocks/starrocks/pull/10851)

## 2.3.1

リリース日: 2022年8月22日

### 改善

- Broker Load は Parquet ファイル内の List 型を非ネスト ARRAY データ型に変換することをサポートします。 [#9150](https://github.com/StarRocks/starrocks/pull/9150)
- JSON 関連関数（json_query、get_json_string、get_json_int）のパフォーマンスを最適化しました。 [#9623](https://github.com/StarRocks/starrocks/pull/9623)
- エラーメッセージを最適化しました: Hive、Iceberg、または Hudi に対するクエリ中に、クエリする列のデータ型が StarRocks によってサポートされていない場合、システムはその列に例外をスローします。 [#10139](https://github.com/StarRocks/starrocks/pull/10139)
- リソースグループのスケジューリング待ち時間を短縮し、リソース分離パフォーマンスを最適化しました。 [#10122](https://github.com/StarRocks/starrocks/pull/10122)

### バグ修正

以下のバグを修正しました:

- `limit` 演算子のプッシュダウンが不正確なため、Elasticsearch 外部テーブルに対するクエリの結果が誤って返されます。 [#9952](https://github.com/StarRocks/starrocks/pull/9952)
- `limit` 演算子が使用されると、Oracle 外部テーブルに対するクエリが失敗します。 [#9542](https://github.com/StarRocks/starrocks/pull/9542)
- すべての Kafka Broker が Routine Load 中に停止すると、BE がブロックされます。 [#9935](https://github.com/StarRocks/starrocks/pull/9935)
- Parquet ファイルのデータ型が対応する外部テーブルのデータ型と一致しない場合、クエリ中に BE がクラッシュします。 [#10107](https://github.com/StarRocks/starrocks/pull/10107)
- 外部テーブルのスキャン範囲が空であるため、クエリがタイムアウトします。 [#10091](https://github.com/StarRocks/starrocks/pull/10091)
- サブクエリに ORDER BY 句が含まれている場合、システムは例外をスローします。 [#10180](https://github.com/StarRocks/starrocks/pull/10180)
- Hive メタデータが非同期でリロードされると、Hive Metastore がハングします。 [#10132](https://github.com/StarRocks/starrocks/pull/10132)

## 2.3.0

リリース日: 2022年7月29日

### 新機能

- 主キーテーブルは完全な DELETE WHERE 構文をサポートします。詳細については、[DELETE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/DELETE#delete-data-by-primary-key) を参照してください。
- 主キーテーブルは永続的な主キーインデックスをサポートします。主キーインデックスをメモリではなくディスクに永続化することを選択でき、メモリ使用量を大幅に削減できます。詳細については、[Primary Key table](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/) を参照してください。
- グローバル辞書はリアルタイムデータ取り込み中に更新可能で、クエリパフォーマンスを最適化し、文字列データのクエリパフォーマンスを2倍に向上させます。
- CREATE TABLE AS SELECT ステートメントは非同期で実行できます。詳細については、[CREATE TABLE AS SELECT](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE_AS_SELECT/) を参照してください。
- 次のリソースグループ関連機能をサポートします:
  - リソースグループの監視: 監査ログでクエリのリソースグループを表示し、API を呼び出してリソースグループのメトリックを取得できます。詳細については、[Monitor and Alerting](https://docs.starrocks.io/docs/administration/Monitor_and_Alert#monitor-and-alerting) を参照してください。
  - 大規模なクエリによる CPU、メモリ、I/O リソースの消費を制限: クラシファイアを基に、またはセッション変数を設定して、クエリを特定のリソースグループにルーティングできます。詳細については、[Resource group](https://docs.starrocks.io/docs/administration/resource_group/) を参照してください。
- JDBC 外部テーブルを使用して、Oracle、PostgreSQL、MySQL、SQLServer、ClickHouse などのデータベースのデータを便利にクエリできます。StarRocks はまた、述語プッシュダウンをサポートし、クエリパフォーマンスを向上させます。詳細については、[External table for a JDBC-compatible database](https://docs.starrocks.io/docs/data_source/External_table#external-table-for-a-JDBC-compatible-database) を参照してください。
- [プレビュー] 新しいデータソースコネクタフレームワークがリリースされ、外部カタログをサポートします。外部カタログを使用して、外部テーブルを作成せずに Hive データに直接アクセスしてクエリできます。詳細については、[Use catalogs to manage internal and external data](https://docs.starrocks.io/docs/data_source/catalog/query_external_data/) を参照してください。
- 次の関数を追加しました:
  - [window_funnel](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/window_funnel/)
  - [ntile](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/)
  - [bitmap_union_count](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_union_count/), [base64_to_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/base64_to_bitmap/), [array_to_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_to_bitmap/)
  - [week](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/week/), [time_slice](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/time_slice/)

### 改善

- Compaction メカニズムは、大量のメタデータをより迅速にマージできます。これにより、頻繁なデータ更新直後に発生する可能性のあるメタデータの圧迫と過剰なディスク使用を防ぎます。
- Parquet ファイルと圧縮ファイルのロードパフォーマンスを最適化しました。
- マテリアライズドビューの作成メカニズムを最適化しました。最適化後、マテリアライズドビューは以前より最大10倍速く作成できます。
- 次のオペレーターのパフォーマンスを最適化しました:
  - TopN とソートオペレーター
  - 関数を含む等価比較オペレーターは、これらのオペレーターがスキャンオペレーターにプッシュダウンされるときに Zone Map インデックスを使用できます。
- Apache Hive™ 外部テーブルを最適化しました。
  - Apache Hive™ テーブルが Parquet、ORC、または CSV 形式で保存されている場合、Hive で ADD COLUMN または REPLACE COLUMN によって引き起こされたスキーマ変更は、対応する Hive 外部テーブルで REFRESH ステートメントを実行すると StarRocks に同期されます。詳細については、[Hive external table](https://docs.starrocks.io/docs/2.3/data_source/External_table/#hive-external-table) を参照してください。
  - Hive リソースに対して `hive.metastore.uris` を変更できます。詳細については、[ALTER RESOURCE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_RESOURCE/) を参照してください。
- Apache Iceberg 外部テーブルのパフォーマンスを最適化しました。カスタムカタログを使用して Iceberg リソースを作成できます。詳細については、[Apache Iceberg external table](https://docs.starrocks.io/docs/2.3/data_source/External_table/#deprecated-iceberg-external-table) を参照してください。
- Elasticsearch 外部テーブルのパフォーマンスを最適化しました。Elasticsearch クラスター内のデータノードのアドレスのスニッフィングを無効にできます。詳細については、[Elasticsearch external table](https://docs.starrocks.io/docs/2.3/data_source/External_table/#elasticsearch-external-table) を参照してください。
- sum() 関数が数値文字列を受け入れると、数値文字列を暗黙的に変換します。
- year()、month()、day() 関数は DATE データ型をサポートします。

### バグ修正

以下のバグを修正しました:

- タブレットの過剰な数による CPU 使用率の急上昇。
- "fail to prepare tablet reader" が発生する原因となる問題。
- FEs が再起動に失敗します。[#5642](https://github.com/StarRocks/starrocks/issues/5642 )  [#4969](https://github.com/StarRocks/starrocks/issues/4969 )  [#5580](https://github.com/StarRocks/starrocks/issues/5580)
- CTAS ステートメントが JSON 関数を含むと正常に実行できません。 [#6498](https://github.com/StarRocks/starrocks/issues/6498)

### その他

- StarGo、クラスター管理ツールは、クラスターのデプロイ、開始、アップグレード、ロールバック、および複数のクラスターの管理を行うことができます。詳細については、[Deploy StarRocks with StarGo](https://docs.starrocks.io/docs/administration/stargo/) を参照してください。
- StarRocks をバージョン 2.3 にアップグレードするか、StarRocks をデプロイすると、パイプラインエンジンがデフォルトで有効になります。パイプラインエンジンは、高い同時実行シナリオでの単純なクエリのパフォーマンスと複雑なクエリのパフォーマンスを向上させます。StarRocks 2.3 を使用しているときにパフォーマンスの大幅な低下を検出した場合、`SET GLOBAL` ステートメントを実行して `enable_pipeline_engine` を `false` に設定することでパイプラインエンジンを無効にできます。
- [SHOW GRANTS](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/SHOW_GRANTS/) ステートメントは MySQL 構文と互換性があり、ユーザーに割り当てられた権限を GRANT ステートメントの形式で表示します。
- スキーマ変更のための memory_limitation_per_thread_for_schema_change (BE 設定項目) はデフォルト値 2 GB を使用することをお勧めします。この制限を超えるデータ量がある場合、データはディスクに書き込まれます。したがって、このパラメータを以前に大きな値に設定していた場合、2 GB に設定することをお勧めします。そうしないと、スキーマ変更タスクが大量のメモリを消費する可能性があります。

### アップグレードノート

アップグレード前のバージョンにロールバックするには、各 FE の **fe.conf** ファイルに `ignore_unknown_log_id` パラメータを追加し、このパラメータを `true` に設定します。このパラメータは、StarRocks v2.2.0 で新しいタイプのログが追加されたために必要です。このパラメータを追加しない場合、以前のバージョンにロールバックできません。チェックポイントが作成された後、各 FE の **fe.conf** ファイルで `ignore_unknown_log_id` パラメータを `false` に設定することをお勧めします。その後、FEs を再起動して、FEs を以前の設定に戻します。