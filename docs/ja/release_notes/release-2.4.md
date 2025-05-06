---
displayed_sidebar: docs
---

# StarRocks version 2.4

## 2.4.5

リリース日: 2023年4月21日

### 改善点

- メタデータのアップグレード時にエラーを引き起こす可能性があるため、リストパーティション構文を禁止しました。 [#15401](https://github.com/StarRocks/starrocks/pull/15401)
- マテリアライズドビューでBITMAP、HLL、およびPERCENTILEタイプをサポートしました。 [#15731](https://github.com/StarRocks/starrocks/pull/15731)
- `storage_medium` の推論を最適化しました。BEがSSDとHDDの両方を記憶装置として使用する場合、`storage_cooldown_time` プロパティが指定されていると、StarRocksは `storage_medium` を `SSD` に設定します。それ以外の場合、StarRocksは `storage_medium` を `HDD` に設定します。 [#18649](https://github.com/StarRocks/starrocks/pull/18649)
- スレッドダンプの精度を最適化しました。 [#16748](https://github.com/StarRocks/starrocks/pull/16748)
- メタデータのコンパクションをロード前にトリガーすることで、ロード効率を最適化しました。 [#19347](https://github.com/StarRocks/starrocks/pull/19347)
- Stream Load プランナーのタイムアウトを最適化しました。 [#18992](https://github.com/StarRocks/starrocks/pull/18992/files)
- ユニークキーテーブルのパフォーマンスを、値カラムからの統計収集を禁止することで最適化しました。 [#19563](https://github.com/StarRocks/starrocks/pull/19563)

### バグ修正

以下のバグが修正されました:

- CREATE TABLEでサポートされていないデータタイプを使用するとNPEが返されます。 [# 20999](https://github.com/StarRocks/starrocks/issues/20999)
- ショートサーキットを使用したBroadcast Joinのクエリに対して誤った結果が返されます。 [#20952](https://github.com/StarRocks/starrocks/issues/20952)
- データ切り捨てロジックの誤りによるディスク占有問題。 [#20590](https://github.com/StarRocks/starrocks/pull/20590)
- AuditLoaderプラグインがインストールも削除もできません。 [#20468](https://github.com/StarRocks/starrocks/issues/20468)
- tabletがスケジュールされているときに例外が発生すると、同じバッチ内の他のtabletはスケジュールされません。 [#20681](https://github.com/StarRocks/starrocks/pull/20681)
- 同期マテリアライズドビューの作成時にサポートされていないSQL関数を使用するとUnknown Errorが返されます。 [#20348](https://github.com/StarRocks/starrocks/issues/20348)
- 複数のCOUNT DISTINCT計算が誤って書き換えられます。 [#19714](https://github.com/StarRocks/starrocks/pull/19714)
- コンパクション中のtabletに対するクエリに誤った結果が返されます。 [#20084](https://github.com/StarRocks/starrocks/issues/20084)
- 集約を伴うクエリに誤った結果が返されます。 [#19725](https://github.com/StarRocks/starrocks/issues/19725)
- NOT NULLカラムにNULLパーケットデータをロードしてもエラーメッセージが返されません。 [#19885](https://github.com/StarRocks/starrocks/pull/19885)
- リソースグループの同時実行制限に連続して達すると、クエリの同時実行メトリックがゆっくりと減少します。 [#19363](https://github.com/StarRocks/starrocks/pull/19363)
- `InsertOverwriteJob` の状態変更ログを再生するときにFEが起動に失敗します。 [#19061](https://github.com/StarRocks/starrocks/issues/19061)
- 主キーテーブルのデッドロック。 [#18488](https://github.com/StarRocks/starrocks/pull/18488)
- Colocationテーブルでは、`ADMIN SET REPLICA STATUS PROPERTIES ("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");` のようなステートメントを使用してレプリカの状態を手動で `bad` に指定できます。BEの数がレプリカの数以下の場合、破損したレプリカは修復できません。 [#17876](https://github.com/StarRocks/starrocks/issues/17876)
- ARRAY関連の関数によって引き起こされる問題。 [#18556](https://github.com/StarRocks/starrocks/pull/18556)

## 2.4.4

リリース日: 2023年2月22日

### 改善点

- ロードの高速キャンセルをサポートしました。 [#15514](https://github.com/StarRocks/starrocks/pull/15514) [#15398](https://github.com/StarRocks/starrocks/pull/15398) [#15969](https://github.com/StarRocks/starrocks/pull/15969)
- コンパクションフレームワークのCPU使用率を最適化しました。 [#11747](https://github.com/StarRocks/starrocks/pull/11747)
- バージョンが欠落しているtabletでの累積コンパクションをサポートしました。 [#17030](https://github.com/StarRocks/starrocks/pull/17030)

### バグ修正

以下のバグが修正されました:

- 無効なDATE値が指定されたときに過剰な動的パーティションが作成されます。 [#17966](https://github.com/StarRocks/starrocks/pull/17966)
- デフォルトのHTTPSポートでElasticsearch外部テーブルに接続できません。 [#13726](https://github.com/StarRocks/starrocks/pull/13726)
- トランザクションがタイムアウトした後、BEがStream Loadトランザクションをキャンセルできませんでした。 [#15738](https://github.com/StarRocks/starrocks/pull/15738)
- 単一のBEでのローカルシャッフル集約から誤ったクエリ結果が返されます。 [#17845](https://github.com/StarRocks/starrocks/pull/17845)
- "wait_for_version version: failed: apply stopped" というエラーメッセージでクエリが失敗することがあります。 [#17848](https://github.com/StarRocks/starrocks/pull/17850)
- OLAPスキャンバケット式が正しくクリアされないため、誤ったクエリ結果が返されます。 [#17666](https://github.com/StarRocks/starrocks/pull/17666)
- Colocate Group内の動的パーティションテーブルのバケット数を変更できず、エラーメッセージが返されます。 [#17418](https://github.com/StarRocks/starrocks/pull/17418)
- 非Leader FEノードに接続してSQL文 `USE <catalog_name>.<database_name>` を送信すると、非Leader FEノードはSQL文を `<catalog_name>` を除いてLeader FEノードに転送します。その結果、Leader FEノードは `default_catalog` を使用することを選び、最終的に指定されたデータベースを見つけることができません。 [#17302](https://github.com/StarRocks/starrocks/pull/17302)
- 書き換え前のdictmappingチェックのロジックが誤っています。 [#17405](https://github.com/StarRocks/starrocks/pull/17405)
- FEがBEに偶発的なハートビートを送信し、ハートビート接続がタイムアウトすると、FEはBEを利用不可と見なし、BEでのトランザクションが失敗します。 [#16386](https://github.com/StarRocks/starrocks/pull/16386)
- フォロワーFEで新しくクローンされたtabletに対するクエリで `get_applied_rowsets` が失敗しました。 [#17192](https://github.com/StarRocks/starrocks/pull/17192)
- フォロワーFEで `SET variable = default` を実行するとNPEが発生します。 [#17549](https://github.com/StarRocks/starrocks/pull/17549)
- プロジェクション内の式が辞書によって書き換えられません。 [#17558](https://github.com/StarRocks/starrocks/pull/17558)
- 週単位での動的パーティション作成のロジックが誤っています。 [#17163](https://github.com/StarRocks/starrocks/pull/17163)
- ローカルシャッフルから誤ったクエリ結果が返されます。 [#17130](https://github.com/StarRocks/starrocks/pull/17130)
- インクリメンタルクローンが失敗することがあります。 [#16930](https://github.com/StarRocks/starrocks/pull/16930)
- CBOが二つのオペレーターが等価かどうかを比較する際に誤ったロジックを使用することがあります。 [#17199](https://github.com/StarRocks/starrocks/pull/17199) [#17227](https://github.com/StarRocks/starrocks/pull/17227)
- JuiceFSスキーマが正しくチェックされ解決されないため、JuiceFSへのアクセスに失敗しました。 [#16940](https://github.com/StarRocks/starrocks/pull/16940)

## 2.4.3

リリース日: 2023年1月19日

### 改善点

- StarRocksは、NPEを防ぐためにAnalyze中に対応するデータベースとテーブルが存在するかどうかを確認します。 [#14467](https://github.com/StarRocks/starrocks/pull/14467)
- 外部テーブルのクエリに対してサポートされていないデータタイプを持つカラムは実体化されません。 [#13305](https://github.com/StarRocks/starrocks/pull/13305)
- FE開始スクリプト **start_fe.sh** にJavaバージョンチェックを追加しました。 [#14333](https://github.com/StarRocks/starrocks/pull/14333)

### バグ修正

以下のバグが修正されました:

- タイムアウトが設定されていない場合、Stream Loadが失敗することがあります。 [#16241](https://github.com/StarRocks/starrocks/pull/16241)
- メモリ使用量が高いときにbRPC Sendがクラッシュします。 [#16046](https://github.com/StarRocks/starrocks/issues/16046)
- StarRocksは、初期バージョンのStarRocksインスタンスから外部テーブルのデータをロードできません。 [#16130](https://github.com/StarRocks/starrocks/pull/16130)
- マテリアライズドビューのリフレッシュ失敗がメモリリークを引き起こすことがあります。 [#16041](https://github.com/StarRocks/starrocks/pull/16041)
- スキーマ変更がPublishステージでハングします。 [#14148](https://github.com/StarRocks/starrocks/issues/14148)
- マテリアライズドビューのQeProcessorImplの問題によるメモリリーク。 [#15699](https://github.com/StarRocks/starrocks/pull/15699)
- `limit` を伴うクエリの結果が一貫しません。 [#13574](https://github.com/StarRocks/starrocks/pull/13574)
- INSERTによるメモリリーク。 [#14718](https://github.com/StarRocks/starrocks/pull/14718)
- 主キーテーブルがTablet Migrationを実行します。 [#13720](https://github.com/StarRocks/starrocks/pull/13720)
- Broker Load中にBroker Kerberosチケットがタイムアウトします。 [#16149](https://github.com/StarRocks/starrocks/pull/16149)
- テーブルのビューで `nullable` 情報が誤って推論されます。 [#15744](https://github.com/StarRocks/starrocks/pull/15744)

### 動作変更

- Thrift Listenのデフォルトバックログを `1024` に変更しました。 [#13911](https://github.com/StarRocks/starrocks/pull/13911)
- SQLモード `FORBID_INVALID_DATES` を追加しました。このSQLモードはデフォルトで無効です。有効にすると、StarRocksはDATEタイプの入力を検証し、無効な入力があるとエラーを返します。 [#14143](https://github.com/StarRocks/starrocks/pull/14143)

## 2.4.2

リリース日: 2022年12月14日

### 改善点

- 多数のバケットが存在する場合のBucket Hintのパフォーマンスを最適化しました。 [#13142](https://github.com/StarRocks/starrocks/pull/13142)

### バグ修正

以下のバグが修正されました:

- 主キーインデックスのフラッシュがBEをクラッシュさせることがあります。 [#14857](https://github.com/StarRocks/starrocks/pull/14857) [#14819](https://github.com/StarRocks/starrocks/pull/14819)
- マテリアライズドビュータイプが `SHOW FULL TABLES` によって正しく識別されません。 [#13954](https://github.com/StarRocks/starrocks/pull/13954)
- StarRocks v2.2からv2.4へのアップグレードがBEをクラッシュさせることがあります。 [#13795](https://github.com/StarRocks/starrocks/pull/13795)
- Broker LoadがBEをクラッシュさせることがあります。 [#13973](https://github.com/StarRocks/starrocks/pull/13973)
- セッション変数 `statistic_collect_parallel` が効果を発揮しません。 [#14352](https://github.com/StarRocks/starrocks/pull/14352)
- INSERT INTOがBEをクラッシュさせることがあります。 [#14818](https://github.com/StarRocks/starrocks/pull/14818)
- JAVA UDFがBEをクラッシュさせることがあります。 [#13947](https://github.com/StarRocks/starrocks/pull/13947)
- 部分更新中のレプリカのクローンがBEをクラッシュさせ、再起動に失敗することがあります。 [#13683](https://github.com/StarRocks/starrocks/pull/13683)
- Colocated Joinが効果を発揮しないことがあります。 [#13561](https://github.com/StarRocks/starrocks/pull/13561)

### 動作変更

- セッション変数 `query_timeout` を上限 `259200`、下限 `1` で制約しました。
- FEパラメータ `default_storage_medium` を廃止しました。テーブルの記憶媒体はシステムによって自動的に推論されます。 [#14394](https://github.com/StarRocks/starrocks/pull/14394)

## 2.4.1

リリース日: 2022年11月14日

### 新機能

- 非等値ジョイン - LEFT SEMI JOINとANTI JOINをサポートしました。JOIN機能を最適化しました。 [#13019](https://github.com/StarRocks/starrocks/pull/13019)

### 改善点

- `HeartbeatResponse` にプロパティ `aliveStatus` をサポートしました。`aliveStatus` はクラスタ内のノードが生存しているかどうかを示します。`aliveStatus` を判断するメカニズムがさらに最適化されました。 [#12713](https://github.com/StarRocks/starrocks/pull/12713)

- Routine Loadのエラーメッセージを最適化しました。 [#12155](https://github.com/StarRocks/starrocks/pull/12155)

### バグ修正

- v2.4.0RCからv2.4.0にアップグレードした後、BEがクラッシュします。 [#13128](https://github.com/StarRocks/starrocks/pull/13128)

- 後期実体化がデータレイクのクエリに誤った結果を引き起こします。 [#13133](https://github.com/StarRocks/starrocks/pull/13133)

- get_json_int関数が例外を投げます。 [#12997](https://github.com/StarRocks/starrocks/pull/12997)

- 主キーテーブルから削除された後、データが一貫しない可能性があります。 [#12719](https://github.com/StarRocks/starrocks/pull/12719)

- 主キーテーブルでのコンパクション中にBEがクラッシュする可能性があります。 [#12914](https://github.com/StarRocks/starrocks/pull/12914)

- json_object関数が入力に空の文字列を含むと誤った結果を返します。 [#13030](https://github.com/StarRocks/starrocks/issues/13030)

- `RuntimeFilter` によりBEがクラッシュします。 [#12807](https://github.com/StarRocks/starrocks/pull/12807)

- CBOでの過剰な再帰計算によりFEがハングします。 [#12788](https://github.com/StarRocks/starrocks/pull/12788)

- BEが正常に終了する際にクラッシュまたはエラーを報告する可能性があります。 [#12852](https://github.com/StarRocks/starrocks/pull/12852)

- 新しいカラムが追加されたテーブルからデータが削除された後、コンパクションがクラッシュします。 [#12907](https://github.com/StarRocks/starrocks/pull/12907)

- OLAP外部テーブルのメタデータ同期の不正なメカニズムによりデータが一貫しない可能性があります。 [#12368](https://github.com/StarRocks/starrocks/pull/12368)

- 1つのBEがクラッシュすると、他のBEが関連するクエリをタイムアウトまで実行することがあります。 [#12954](https://github.com/StarRocks/starrocks/pull/12954)

### 動作変更

- Hive外部テーブルの解析に失敗した場合、StarRocksは関連するカラムをNULLカラムに変換する代わりにエラーメッセージをスローします。 [#12382](https://github.com/StarRocks/starrocks/pull/12382)

## 2.4.0

リリース日: 2022年10月20日

### 新機能

- 複数のベーステーブルに基づく非同期マテリアライズドビューの作成をサポートし、JOIN操作を伴うクエリを高速化します。非同期マテリアライズドビューはすべての [テーブルタイプ](../table_design/table_types/table_types.md) をサポートします。詳細は [Materialized View](../using_starrocks/async_mv/Materialized_view.md) を参照してください。

- INSERT OVERWRITEを使用したデータの上書きをサポートします。詳細は [Load data using INSERT](../loading/InsertInto.md) を参照してください。

- [プレビュー] ステートレスなCompute Nodes (CN) を提供し、水平スケーリングが可能です。StarRocks Operatorを使用してCNをKubernetes (K8s) クラスターにデプロイし、自動水平スケーリングを実現できます。詳細は [Deploy and manage CN on Kubernetes with StarRocks Operator](../deployment/sr_operator.md) を参照してください。

- 外部結合は、`<`, `<=`, `>`, `>=`, `<>` などの比較演算子によって関連付けられた非等値ジョインをサポートします。詳細は [SELECT](../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を参照してください。

- IcebergカタログとHudiカタログの作成をサポートし、Apache IcebergとApache Hudiからのデータに直接クエリを実行できます。詳細は [Iceberg catalog](../data_source/catalog/iceberg_catalog.md) と [Hudi catalog](../data_source/catalog/hudi_catalog.md) を参照してください。

- Apache Hive™ テーブルのCSV形式からARRAYタイプのカラムへのクエリをサポートします。詳細は [External table](../data_source/External_table.md) を参照してください。

- DESCを使用して外部データのスキーマを表示することをサポートします。詳細は [DESC](../sql-reference/sql-statements/table_bucket_part_index/DESCRIBE.md) を参照してください。

- GRANTを使用して特定のロールまたはIMPERSONATE権限をユーザーに付与し、REVOKEを使用してそれらを取り消すことをサポートし、IMPERSONATE権限を使用してSQL文を実行することをサポートします。詳細は [GRANT](../sql-reference/sql-statements/account-management/GRANT.md)、[REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md)、および [EXECUTE AS](../sql-reference/sql-statements/account-management/EXECUTE_AS.md) を参照してください。

- FDQNアクセスをサポートします: BEまたはFEノードの一意の識別としてドメイン名またはホスト名とポートの組み合わせを使用できます。これにより、IPアドレスの変更によるアクセス障害を防ぎます。詳細は [Enable FQDN Access](../administration/management/enable_fqdn.md) を参照してください。

- flink-connector-starrocksは主キーテーブルの部分更新をサポートします。詳細は [Load data by using flink-connector-starrocks](../loading/Flink-connector-starrocks.md) を参照してください。

- 以下の新しい関数を提供します:

  - array_contains_all: 特定の配列が別の配列の部分集合であるかどうかを確認します。詳細は [array_contains_all](../sql-reference/sql-functions/array-functions/array_contains_all.md) を参照してください。
  - percentile_cont: 線形補間を使用してパーセンタイル値を計算します。詳細は [percentile_cont](../sql-reference/sql-functions/aggregate-functions/percentile_cont.md) を参照してください。

### 改善点

- 主キーテーブルは、VARCHARタイプの主キーインデックスをディスクにフラッシュすることをサポートします。バージョン2.4.0から、主キーテーブルは永続性インデックスがオンになっているかどうかに関係なく、主キーインデックスに同じデータタイプをサポートします。

- 外部テーブルのクエリパフォーマンスを最適化しました。

  - Parquet形式の外部テーブルのクエリ中に後期実体化をサポートし、小規模なフィルタリングを伴うデータレイクのクエリパフォーマンスを最適化します。
  - 小規模なI/O操作をマージしてデータレイクのクエリ遅延を削減し、外部テーブルのクエリパフォーマンスを向上させます。

- ウィンドウ関数のパフォーマンスを最適化しました。

- Cross Joinのパフォーマンスを最適化し、述語プッシュダウンをサポートしました。

- ヒストグラムをCBO統計に追加しました。完全な統計収集がさらに最適化されました。詳細は [Gather CBO statistics](../using_starrocks/Cost_based_optimizer.md) を参照してください。

- タブレットスキャンのための適応型マルチスレッドを有効にし、スキャンパフォーマンスのタブレット数への依存を減らしました。その結果、バケット数をより簡単に設定できます。詳細は [Determine the number of buckets](../table_design/data_distribution/Data_distribution.md#how-to-determine-the-number-of-buckets) を参照してください。

- Apache Hiveで圧縮されたTXTファイルのクエリをサポートします。

- デフォルトのPageCacheサイズ計算とメモリ整合性チェックのメカニズムを調整し、マルチインスタンスデプロイメント中のOOM問題を回避しました。

- 主キーテーブルでの大規模バッチロードのパフォーマンスを最終マージ操作を削除することで最大2倍向上させました。

- Stream Loadトランザクションインターフェースをサポートし、Apache Flink®やApache Kafka®などの外部システムからデータをロードするトランザクションの2フェーズコミット (2PC) を実装し、高度に同時実行されるストリームロードのパフォーマンスを向上させます。

- 関数:

  - 1つのステートメントで複数のCOUNT(DISTINCT)を使用できます。詳細は [count](../sql-reference/sql-functions/aggregate-functions/count.md) を参照してください。
  - ウィンドウ関数min()とmax()はスライディングウィンドウをサポートします。詳細は [Window functions](../sql-reference/sql-functions/Window_function.md) を参照してください。
  - window_funnel関数のパフォーマンスを最適化しました。詳細は [window_funnel](../sql-reference/sql-functions/aggregate-functions/window_funnel.md) を参照してください。

### バグ修正

以下のバグが修正されました:

- DESCによって返されるDECIMALデータタイプがCREATE TABLE文で指定されたものと異なります。 [#7309](https://github.com/StarRocks/starrocks/pull/7309)

- FEメタデータ管理の問題がFEの安定性に影響を与えます。 [#6685](https://github.com/StarRocks/starrocks/pull/6685) [#9445](https://github.com/StarRocks/starrocks/pull/9445) [#7974](https://github.com/StarRocks/starrocks/pull/7974) [#7455](https://github.com/StarRocks/starrocks/pull/7455)

- データロード関連の問題:

  - ARRAYカラムが指定されている場合、Broker Loadが失敗します。 [#9158](https://github.com/StarRocks/starrocks/pull/9158)
  - Broker Loadを介して非重複キーテーブルにデータがロードされた後、レプリカが一貫しません。 [#8714](https://github.com/StarRocks/starrocks/pull/8714)
  - ALTER ROUTINE LOADを実行するとNPEが発生します。 [#7804](https://github.com/StarRocks/starrocks/pull/7804)

- データレイク分析関連の問題:

  - Hive外部テーブルのParquetデータに対するクエリが失敗します。 [#7413](https://github.com/StarRocks/starrocks/pull/7413) [#7482](https://github.com/StarRocks/starrocks/pull/7482) [#7624](https://github.com/StarRocks/starrocks/pull/7624)
  - Elasticsearch外部テーブルで `limit` 句を伴うクエリに対して誤った結果が返されます。 [#9226](https://github.com/StarRocks/starrocks/pull/9226)
  - 複雑なデータタイプを持つApache Icebergテーブルに対するクエリ中に不明なエラーが発生します。 [#11298](https://github.com/StarRocks/starrocks/pull/11298)

- Leader FEとFollower FEノード間でメタデータが一貫しません。 [#11215](https://github.com/StarRocks/starrocks/pull/11215)

- BITMAPデータのサイズが2GBを超えるとBEがクラッシュします。 [#11178](https://github.com/StarRocks/starrocks/pull/11178)

### 動作変更

- Page Cacheはデフォルトで有効です ("disable_storage_page_cache" = "false")。デフォルトのキャッシュサイズ (`storage_page_cache_limit`) はシステムメモリの20%です。
- CBOはデフォルトで有効です。セッション変数 `enable_cbo` を廃止しました。
- ベクトル化エンジンはデフォルトで有効です。セッション変数 `vectorized_engine_enable` を廃止しました。

### その他

- リソースグループの安定したリリースを発表します。
- JSONデータタイプとその関連機能の安定したリリースを発表します。