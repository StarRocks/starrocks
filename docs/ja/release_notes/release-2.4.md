---
displayed_sidebar: docs
---

# StarRocks バージョン 2.4

## 2.4.5

リリース日: 2023年4月21日

### 改善点

- メタデータのアップグレードでエラーを引き起こす可能性があるため、List Partition 構文を禁止しました。[#15401](https://github.com/StarRocks/starrocks/pull/15401)
- マテリアライズドビューで BITMAP、HLL、および PERCENTILE 型をサポートしました。[#15731](https://github.com/StarRocks/starrocks/pull/15731)
- `storage_medium` の推論を最適化しました。BE が SSD と HDD の両方を記憶装置として使用する場合、`storage_cooldown_time` プロパティが指定されていれば、StarRocks は `storage_medium` を `SSD` に設定します。そうでなければ、`HDD` に設定します。[#18649](https://github.com/StarRocks/starrocks/pull/18649)
- スレッドダンプの精度を最適化しました。[#16748](https://github.com/StarRocks/starrocks/pull/16748)
- メタデータのコンパクションをロード前にトリガーすることで、ロード効率を最適化しました。[#19347](https://github.com/StarRocks/starrocks/pull/19347)
- Stream Load プランナーのタイムアウトを最適化しました。[#18992](https://github.com/StarRocks/starrocks/pull/18992/files)
- ユニークキーテーブルのパフォーマンスを、値カラムからの統計収集を禁止することで最適化しました。[#19563](https://github.com/StarRocks/starrocks/pull/19563)

### バグ修正

以下のバグが修正されました:

- CREATE TABLE でサポートされていないデータ型を使用すると NPE が返されます。[# 20999](https://github.com/StarRocks/starrocks/issues/20999)
- ショートサーキットを使用した Broadcast Join を使用するクエリに誤った結果が返されます。[#20952](https://github.com/StarRocks/starrocks/issues/20952)
- データ切り捨てロジックの誤りによるディスク占有問題。[#20590](https://github.com/StarRocks/starrocks/pull/20590)
- AuditLoader プラグインがインストールも削除もできません。[#20468](https://github.com/StarRocks/starrocks/issues/20468)
- tablet がスケジュールされているときに例外がスローされると、同じバッチ内の他の tablet はスケジュールされません。[#20681](https://github.com/StarRocks/starrocks/pull/20681)
- 同期マテリアライズドビューの作成でサポートされていない SQL 関数を使用すると Unknown Error が返されます。[#20348](https://github.com/StarRocks/starrocks/issues/20348)
- 複数の COUNT DISTINCT 計算が誤って書き換えられます。[#19714](https://github.com/StarRocks/starrocks/pull/19714)
- コンパクション中の tablet に対するクエリに誤った結果が返されます。[#20084](https://github.com/StarRocks/starrocks/issues/20084)
- 集約を含むクエリに誤った結果が返されます。[#19725](https://github.com/StarRocks/starrocks/issues/19725)
- NOT NULL カラムに NULL パーケットデータをロードしてもエラーメッセージが返されません。[#19885](https://github.com/StarRocks/starrocks/pull/19885)
- リソースグループの同時実行制限に継続的に達すると、クエリの同時実行メトリックがゆっくりと減少します。[#19363](https://github.com/StarRocks/starrocks/pull/19363)
- `InsertOverwriteJob` の状態変更ログを再生するときに FE が起動に失敗します。[#19061](https://github.com/StarRocks/starrocks/issues/19061)
- 主キーテーブルのデッドロック。[#18488](https://github.com/StarRocks/starrocks/pull/18488)
- Colocation テーブルでは、`ADMIN SET REPLICA STATUS PROPERTIES ("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");` のようなステートメントを使用してレプリカのステータスを手動で `bad` に指定できます。BE の数がレプリカの数以下の場合、破損したレプリカは修復できません。[#17876](https://github.com/StarRocks/starrocks/issues/17876)
- ARRAY 関連の関数によって引き起こされる問題。[#18556](https://github.com/StarRocks/starrocks/pull/18556)

## 2.4.4

リリース日: 2023年2月22日

### 改善点

- ロードの高速キャンセルをサポートしました。[#15514](https://github.com/StarRocks/starrocks/pull/15514) [#15398](https://github.com/StarRocks/starrocks/pull/15398) [#15969](https://github.com/StarRocks/starrocks/pull/15969)
- コンパクションフレームワークの CPU 使用率を最適化しました。[#11747](https://github.com/StarRocks/starrocks/pull/11747)
- バージョンが欠落している tablet に対する累積コンパクションをサポートしました。[#17030](https://github.com/StarRocks/starrocks/pull/17030)

### バグ修正

以下のバグが修正されました:

- 無効な DATE 値が指定されたときに過剰な動的パーティションが作成されます。[#17966](https://github.com/StarRocks/starrocks/pull/17966)
- デフォルトの HTTPS ポートで Elasticsearch 外部テーブルに接続できません。[#13726](https://github.com/StarRocks/starrocks/pull/13726)
- トランザクションがタイムアウトした後、BE が Stream Load トランザクションをキャンセルできませんでした。[#15738](https://github.com/StarRocks/starrocks/pull/15738)
- 単一の BE でのローカルシャッフル集約から誤ったクエリ結果が返されます。[#17845](https://github.com/StarRocks/starrocks/pull/17845)
- エラーメッセージ "wait_for_version version: failed: apply stopped" でクエリが失敗することがあります。[#17848](https://github.com/StarRocks/starrocks/pull/17850)
- OLAP スキャンバケット式が正しくクリアされないために誤ったクエリ結果が返されます。[#17666](https://github.com/StarRocks/starrocks/pull/17666)
- Colocate Group 内の動的パーティションテーブルのバケット数を変更できず、エラーメッセージが返されます。[#17418](https://github.com/StarRocks/starrocks/pull/17418)
- 非 Leader FE ノードに接続して SQL ステートメント `USE <catalog_name>.<database_name>` を送信すると、非 Leader FE ノードは `<catalog_name>` を除外して SQL ステートメントを Leader FE ノードに転送します。その結果、Leader FE ノードは `default_catalog` を使用することを選択し、最終的に指定されたデータベースを見つけることができません。[#17302](https://github.com/StarRocks/starrocks/pull/17302)
- 書き換え前の dictmapping チェックのロジックが誤っています。[#17405](https://github.com/StarRocks/starrocks/pull/17405)
- FE が BE に偶発的なハートビートを送信し、ハートビート接続がタイムアウトすると、FE は BE を利用できないと見なし、BE でのトランザクションが失敗します。[#16386](https://github.com/StarRocks/starrocks/pull/16386)
- フォロワー FE 上で新たにクローンされた tablet に対するクエリで `get_applied_rowsets` が失敗しました。[#17192](https://github.com/StarRocks/starrocks/pull/17192)
- フォロワー FE 上で `SET variable = default` を実行すると NPE が発生します。[#17549](https://github.com/StarRocks/starrocks/pull/17549)
- プロジェクション内の式が辞書によって書き換えられません。[#17558](https://github.com/StarRocks/starrocks/pull/17558)
- 週単位で動的パーティションを作成する際のロジックが誤っています。[#17163](https://github.com/StarRocks/starrocks/pull/17163)
- ローカルシャッフルから誤ったクエリ結果が返されます。[#17130](https://github.com/StarRocks/starrocks/pull/17130)
- インクリメンタルクローンが失敗することがあります。[#16930](https://github.com/StarRocks/starrocks/pull/16930)
- 場合によっては、CBO が 2 つのオペレーターが等価であるかどうかを比較する際に誤ったロジックを使用することがあります。[#17199](https://github.com/StarRocks/starrocks/pull/17199) [#17227](https://github.com/StarRocks/starrocks/pull/17227)
- JuiceFS スキーマが正しくチェックおよび解決されないため、JuiceFS にアクセスできませんでした。[#16940](https://github.com/StarRocks/starrocks/pull/16940)

## 2.4.3

リリース日: 2023年1月19日

### 改善点

- NPE を防ぐために、Analyze 中に対応するデータベースとテーブルが存在するかどうかを StarRocks がチェックします。[#14467](https://github.com/StarRocks/starrocks/pull/14467)
- 外部テーブルに対するクエリでは、サポートされていないデータ型のカラムはマテリアライズされません。[#13305](https://github.com/StarRocks/starrocks/pull/13305)
- FE 起動スクリプト **start_fe.sh** に Java バージョンチェックを追加しました。[#14333](https://github.com/StarRocks/starrocks/pull/14333)

### バグ修正

以下のバグが修正されました:

- タイムアウトが設定されていない場合、Stream Load が失敗することがあります。[#16241](https://github.com/StarRocks/starrocks/pull/16241)
- メモリ使用量が高いときに bRPC Send がクラッシュします。[#16046](https://github.com/StarRocks/starrocks/issues/16046)
- 以前のバージョンの StarRocks インスタンスから外部テーブルにデータをロードできません。[#16130](https://github.com/StarRocks/starrocks/pull/16130)
- マテリアライズドビューのリフレッシュ失敗がメモリリークを引き起こすことがあります。[#16041](https://github.com/StarRocks/starrocks/pull/16041)
- スキーマ変更が公開段階で停止します。[#14148](https://github.com/StarRocks/starrocks/issues/14148)
- マテリアライズドビューの QeProcessorImpl 問題によるメモリリーク。[#15699](https://github.com/StarRocks/starrocks/pull/15699)
- `limit` を含むクエリの結果が一貫しません。[#13574](https://github.com/StarRocks/starrocks/pull/13574)
- INSERT によるメモリリーク。[#14718](https://github.com/StarRocks/starrocks/pull/14718)
- 主キーテーブルが Tablet Migration を実行します。[#13720](https://github.com/StarRocks/starrocks/pull/13720)
- Broker Load 中に Broker Kerberos チケットがタイムアウトします。[#16149](https://github.com/StarRocks/starrocks/pull/16149)
- テーブルのビューで `nullable` 情報が誤って推論されます。[#15744](https://github.com/StarRocks/starrocks/pull/15744)

### 動作の変更

- Thrift Listen のデフォルトバックログを `1024` に変更しました。[#13911](https://github.com/StarRocks/starrocks/pull/13911)
- SQL モード `FORBID_INVALID_DATES` を追加しました。この SQL モードはデフォルトでは無効です。有効にすると、StarRocks は DATE 型の入力を検証し、入力が無効な場合にエラーを返します。[#14143](https://github.com/StarRocks/starrocks/pull/14143)

## 2.4.2

リリース日: 2022年12月14日

### 改善点

- 多数のバケットが存在する場合の Bucket Hint のパフォーマンスを最適化しました。[#13142](https://github.com/StarRocks/starrocks/pull/13142)

### バグ修正

以下のバグが修正されました:

- 主キーインデックスのフラッシュが BE をクラッシュさせることがあります。[#14857](https://github.com/StarRocks/starrocks/pull/14857) [#14819](https://github.com/StarRocks/starrocks/pull/14819)
- マテリアライズドビューの型が `SHOW FULL TABLES` によって正しく識別されません。[#13954](https://github.com/StarRocks/starrocks/pull/13954)
- StarRocks v2.2 から v2.4 へのアップグレードが BE をクラッシュさせることがあります。[#13795](https://github.com/StarRocks/starrocks/pull/13795)
- Broker Load が BE をクラッシュさせることがあります。[#13973](https://github.com/StarRocks/starrocks/pull/13973)
- セッション変数 `statistic_collect_parallel` が効果を発揮しません。[#14352](https://github.com/StarRocks/starrocks/pull/14352)
- INSERT INTO が BE をクラッシュさせることがあります。[#14818](https://github.com/StarRocks/starrocks/pull/14818)
- JAVA UDF が BE をクラッシュさせることがあります。[#13947](https://github.com/StarRocks/starrocks/pull/13947)
- 部分更新中のレプリカのクローンが BE をクラッシュさせ、再起動に失敗することがあります。[#13683](https://github.com/StarRocks/starrocks/pull/13683)
- Colocated Join が効果を発揮しないことがあります。[#13561](https://github.com/StarRocks/starrocks/pull/13561)

### 動作の変更

- セッション変数 `query_timeout` を上限 `259200`、下限 `1` で制約しました。
- FE パラメータ `default_storage_medium` を廃止しました。テーブルの記憶媒体はシステムによって自動的に推論されます。[#14394](https://github.com/StarRocks/starrocks/pull/14394)

## 2.4.1

リリース日: 2022年11月14日

### 新機能

- 非等値ジョイン - LEFT SEMI JOIN と ANTI JOIN をサポートしました。JOIN 機能を最適化しました。[#13019](https://github.com/StarRocks/starrocks/pull/13019)

### 改善点

- `HeartbeatResponse` にプロパティ `aliveStatus` をサポートしました。`aliveStatus` はクラスタ内でノードが生存しているかどうかを示します。`aliveStatus` を判断するメカニズムがさらに最適化されました。[#12713](https://github.com/StarRocks/starrocks/pull/12713)

- Routine Load のエラーメッセージを最適化しました。[#12155](https://github.com/StarRocks/starrocks/pull/12155)

### バグ修正

- v2.4.0RC から v2.4.0 にアップグレード後、BE がクラッシュします。[#13128](https://github.com/StarRocks/starrocks/pull/13128)

- 後期実体化がデータレイクに対するクエリに誤った結果を引き起こします。[#13133](https://github.com/StarRocks/starrocks/pull/13133)

- get_json_int 関数が例外をスローします。[#12997](https://github.com/StarRocks/starrocks/pull/12997)

- 永続性インデックスを持つ主キーテーブルから削除後、データが一貫しないことがあります。[#12719](https://github.com/StarRocks/starrocks/pull/12719)

- 主キーテーブルでのコンパクション中に BE がクラッシュすることがあります。[#12914](https://github.com/StarRocks/starrocks/pull/12914)

- json_object 関数が入力に空の文字列を含むと誤った結果を返します。[#13030](https://github.com/StarRocks/starrocks/issues/13030)

- `RuntimeFilter` により BE がクラッシュします。[#12807](https://github.com/StarRocks/starrocks/pull/12807)

- CBO で過剰な再帰計算により FE がハングします。[#12788](https://github.com/StarRocks/starrocks/pull/12788)

- BE が正常に終了する際にクラッシュまたはエラーを報告することがあります。[#12852](https://github.com/StarRocks/starrocks/pull/12852)

- 新しいカラムが追加されたテーブルからデータが削除された後、コンパクションがクラッシュします。[#12907](https://github.com/StarRocks/starrocks/pull/12907)

- OLAP 外部テーブルのメタデータ同期の不正なメカニズムによりデータが一貫しないことがあります。[#12368](https://github.com/StarRocks/starrocks/pull/12368)

- BE の一つがクラッシュすると、他の BE が関連するクエリをタイムアウトまで実行することがあります。[#12954](https://github.com/StarRocks/starrocks/pull/12954)

### 動作の変更

- Hive 外部テーブルの解析に失敗した場合、StarRocks は関連するカラムを NULL カラムに変換する代わりにエラーメッセージをスローします。[#12382](https://github.com/StarRocks/starrocks/pull/12382)

## 2.4.0

リリース日: 2022年10月20日

### 新機能

- 複数のベーステーブルに基づく非同期マテリアライズドビューの作成をサポートし、JOIN 操作を伴うクエリを高速化します。非同期マテリアライズドビューはすべての [テーブルタイプ](https://docs.starrocks.io/docs/table_design/table_types/table_types/) をサポートします。詳細は [Materialized View](https://docs.starrocks.io/docs/using_starrocks/Materialized_view/) を参照してください。

- INSERT OVERWRITE を介したデータの上書きをサポートします。詳細は [Load data using INSERT](https://docs.starrocks.io/docs/loading/InsertInto/) を参照してください。

- [プレビュー] ステートレスな Compute Nodes (CN) を提供し、水平スケーリングが可能です。StarRocks Operator を使用して CN を Kubernetes (K8s) クラスタにデプロイし、自動水平スケーリングを実現できます。詳細は [Deploy and manage CN on Kubernetes with StarRocks Operator](https://docs.starrocks.io/docs/deployment/sr_operator/) を参照してください。

- 外部結合は、`<`、`<=`、`>`、`>=`、`<>` を含む比較演算子で関連付けられた非等値ジョインをサポートします。詳細は [SELECT](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SELECT/) を参照してください。

- Iceberg カタログと Hudi カタログの作成をサポートし、Apache Iceberg と Apache Hudi からのデータに直接クエリを実行できます。詳細は [Iceberg catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/) および [Hudi catalog](https://docs.starrocks.io/docs/data_source/catalog/hudi_catalog/) を参照してください。

- Apache Hive™ テーブルの ARRAY 型カラムを CSV 形式でクエリすることをサポートします。詳細は [External table](https://docs.starrocks.io/docs/data_source/External_table/) を参照してください。

- DESC を介して外部データのスキーマを表示することをサポートします。詳細は [DESC](https://docs.starrocks.io/docs/sql-reference/sql-statements/Utility/DESCRIBE/) を参照してください。

- GRANT を介して特定のロールまたは IMPERSONATE 権限をユーザーに付与し、REVOKE を介してそれらを取り消すことをサポートし、IMPERSONATE 権限を使用して SQL ステートメントを実行することをサポートします。詳細は [GRANT](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/GRANT/)、[REVOKE](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/REVOKE/)、および [EXECUTE AS](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/EXECUTE_AS/) を参照してください。

- FDQN アクセスをサポートします: 今後は、ドメイン名またはホスト名とポートの組み合わせを BE または FE ノードの一意の識別として使用できます。これにより、IP アドレスの変更によるアクセス障害を防ぎます。詳細は [Enable FQDN Access](https://docs.starrocks.io/docs/administration/enable_fqdn/) を参照してください。

- flink-connector-starrocks は主キーテーブルの部分更新をサポートします。詳細は [Load data by using flink-connector-starrocks](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/) を参照してください。

- 以下の新しい関数を提供します:

  - array_contains_all: 特定の配列が他の配列の部分集合であるかどうかを確認します。詳細は [array_contains_all](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_contains_all/) を参照してください。
  - percentile_cont: 線形補間を使用してパーセンタイル値を計算します。詳細は [percentile_cont](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/percentile_cont/) を参照してください。

### 改善点

- 主キーテーブルは、VARCHAR 型の主キーインデックスをディスクにフラッシュすることをサポートします。バージョン 2.4.0 から、主キーテーブルは永続性主キーインデックスがオンかオフかに関係なく、主キーインデックスに同じデータ型をサポートします。

- 外部テーブルに対するクエリパフォーマンスを最適化しました。

  - Parquet 形式の外部テーブルに対するクエリ中に後期実体化をサポートし、小規模なフィルタリングを伴うデータレイクでのクエリパフォーマンスを最適化します。
  - 小規模な I/O 操作をマージしてデータレイクのクエリ遅延を減らし、外部テーブルに対するクエリパフォーマンスを向上させます。

- ウィンドウ関数のパフォーマンスを最適化しました。

- Cross Join のパフォーマンスを、述語プッシュダウンをサポートすることで最適化しました。

- CBO 統計にヒストグラムを追加しました。完全な統計収集がさらに最適化されました。詳細は [Gather CBO statistics](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/) を参照してください。

- タブレットスキャンのための適応型マルチスレッドを有効にし、スキャンパフォーマンスのタブレット数への依存を減らしました。その結果、バケット数をより簡単に設定できます。詳細は [Determine the number of buckets](https://docs.starrocks.io/docs/2.4/table_design/Data_distribution/#determine-the-number-of-tablets) を参照してください。

- Apache Hive で圧縮された TXT ファイルのクエリをサポートします。

- デフォルトの PageCache サイズ計算とメモリ一貫性チェックのメカニズムを調整し、マルチインスタンスデプロイメント中の OOM 問題を回避しました。

- 主キーテーブルでの大規模バッチロードのパフォーマンスを、final_merge 操作を削除することで最大 2 倍向上させました。

- Stream Load トランザクションインターフェースをサポートし、Apache Flink® や Apache Kafka® などの外部システムからデータをロードするトランザクションに対して 2 フェーズコミット (2PC) を実装し、高度に並行したストリームロードのパフォーマンスを向上させます。

- 関数:

  - 一つのステートメントで複数の COUNT(DISTINCT) を使用できます。詳細は [count](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/count/) を参照してください。
  - ウィンドウ関数 min() と max() はスライディングウィンドウをサポートします。詳細は [Window functions](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/) を参照してください。
  - window_funnel 関数のパフォーマンスを最適化しました。詳細は [window_funnel](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/window_funnel/) を参照してください。

### バグ修正

以下のバグが修正されました:

- DESC によって返される DECIMAL データ型が CREATE TABLE ステートメントで指定されたものと異なります。[#7309](https://github.com/StarRocks/starrocks/pull/7309)

- FE メタデータ管理の問題が FE の安定性に影響します。[#6685](https://github.com/StarRocks/starrocks/pull/6685) [#9445](https://github.com/StarRocks/starrocks/pull/9445) [#7974](https://github.com/StarRocks/starrocks/pull/7974) [#7455](https://github.com/StarRocks/starrocks/pull/7455)

- データロード関連の問題:

  - ARRAY カラムが指定されている場合、Broker Load が失敗します。[#9158](https://github.com/StarRocks/starrocks/pull/9158)
  - Broker Load を介して非重複キーテーブルにデータがロードされた後、レプリカが一貫しません。[#8714](https://github.com/StarRocks/starrocks/pull/8714)
  - ALTER ROUTINE LOAD の実行で NPE が発生します。[#7804](https://github.com/StarRocks/starrocks/pull/7804)

- データレイク分析関連の問題:

  - Hive 外部テーブルの Parquet データに対するクエリが失敗します。[#7413](https://github.com/StarRocks/starrocks/pull/7413) [#7482](https://github.com/StarRocks/starrocks/pull/7482) [#7624](https://github.com/StarRocks/starrocks/pull/7624)
  - Elasticsearch 外部テーブルで `limit` 句を含むクエリに誤った結果が返されます。[#9226](https://github.com/StarRocks/starrocks/pull/9226)
  - 複雑なデータ型を持つ Apache Iceberg テーブルに対するクエリ中に不明なエラーが発生します。[#11298](https://github.com/StarRocks/starrocks/pull/11298)

- Leader FE と Follower FE ノード間でメタデータが一貫しません。[#11215](https://github.com/StarRocks/starrocks/pull/11215)

- BITMAP データのサイズが 2 GB を超えると BE がクラッシュします。[#11178](https://github.com/StarRocks/starrocks/pull/11178)

### 動作の変更

- Page Cache がデフォルトで有効になっています ("disable_storage_page_cache" = "false")。デフォルトのキャッシュサイズ (`storage_page_cache_limit`) はシステムメモリの 20% です。
- CBO がデフォルトで有効になっています。セッション変数 `enable_cbo` を廃止しました。
- ベクトル化エンジンがデフォルトで有効になっています。セッション変数 `vectorized_engine_enable` を廃止しました。

### その他

- リソースグループの安定版リリースを発表します。
- JSON データ型とその関連関数の安定版リリースを発表します。