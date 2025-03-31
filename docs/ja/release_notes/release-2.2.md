---
displayed_sidebar: docs
---

# StarRocks version 2.2

## 2.2.13

リリース日: 2023年4月6日

### 改善点

- 一部のシナリオでメモリ消費を削減し、パフォーマンスを向上させるために、bitmap_contains() 関数を最適化しました。[#20616](https://github.com/StarRocks/starrocks/issues/20616)
- Compaction フレームワークを最適化し、CPUリソースの消費を削減しました。[#11746](https://github.com/StarRocks/starrocks/issues/11746)

### バグ修正

以下のバグが修正されました:

- Stream Load ジョブで要求されたURLが正しくない場合、責任を持つ FE がハングし、HTTPリクエストを処理できません。[#18468](https://github.com/StarRocks/starrocks/issues/18468)
- 責任を持つ FE が統計を収集する際に、異常に大量のメモリを消費し、OOMを引き起こす可能性があります。[#16331](https://github.com/StarRocks/starrocks/issues/16331)
- 一部のクエリでメモリ解放が適切に処理されない場合、BEs がクラッシュします。[#11395](https://github.com/StarRocks/starrocks/issues/11395)
- TRUNCATE TABLE コマンドを実行した後、NullPointerException が発生し、責任を持つ FE が再起動に失敗する可能性があります。[#16773](https://github.com/StarRocks/starrocks/issues/16773)

## 2.2.10

リリース日: 2022年12月2日

### 改善点

- Routine Load ジョブに対して返されるエラーメッセージを最適化しました。[#12203](https://github.com/StarRocks/starrocks/pull/12203)

- 論理演算子 `&&` をサポートしました。[#11819](https://github.com/StarRocks/starrocks/issues/11819)

- BE がクラッシュした際にクエリが即座にキャンセルされ、期限切れのクエリによって引き起こされるシステムのスタック問題を防ぎます。[#12954](https://github.com/StarRocks/starrocks/pull/12954)

- FE の起動スクリプトを最適化しました。FE 起動時に Java バージョンがチェックされます。[#14094](https://github.com/StarRocks/starrocks/pull/14094)

- 主キーテーブルから大量のデータを削除することをサポートしました。[#4772](https://github.com/StarRocks/starrocks/issues/4772)

### バグ修正

以下のバグが修正されました:

- 複数のテーブル (UNION) からビューを作成する際、UNION 操作の最左の子が NULL 定数を使用すると BEs がクラッシュします。[#13792](https://github.com/StarRocks/starrocks/pull/13792)

- クエリする Parquet ファイルが Hive テーブルスキーマと一致しない列タイプを持つ場合、BEs がクラッシュします。[#8848](https://github.com/StarRocks/starrocks/issues/8848)

- クエリに多数の OR 演算子が含まれる場合、プランナーが過剰な再帰計算を行う必要があり、クエリがタイムアウトします。[#12788](https://github.com/StarRocks/starrocks/pull/12788)

- サブクエリに LIMIT 句が含まれる場合、クエリ結果が正しくありません。[#12466](https://github.com/StarRocks/starrocks/pull/12466)

- SELECT 句で二重引用符と単一引用符が混在している場合、CREATE VIEW ステートメントが失敗します。[#13102](https://github.com/StarRocks/starrocks/pull/13102)

## 2.2.9

リリース日: 2022年11月15日

### 改善点

- Hive パーティションから統計を収集する数を制御するためのセッション変数 `hive_partition_stats_sample_size` を追加しました。過剰な数のパーティションは、Hive メタデータの取得エラーを引き起こします。[#12700](https://github.com/StarRocks/starrocks/pull/12700)

- Elasticsearch 外部テーブルがカスタムタイムゾーンをサポートします。[#12662](https://github.com/StarRocks/starrocks/pull/12662)

### バグ修正

以下のバグが修正されました:

- 外部テーブルのメタデータ同期中にエラーが発生すると、DECOMMISSION 操作がスタックします。[#12369](https://github.com/StarRocks/starrocks/pull/12368)

- 新しく追加された列が削除されると、Compaction がクラッシュします。[#12907](https://github.com/StarRocks/starrocks/pull/12907)

- SHOW CREATE VIEW がビュー作成時に追加されたコメントを表示しません。[#4163](https://github.com/StarRocks/starrocks/issues/4163)

- Java UDF のメモリリークが OOM を引き起こす可能性があります。[#12418](https://github.com/StarRocks/starrocks/pull/12418)

- Follower FEs に保存されているノードの生存状態が、一部のシナリオで正確でない場合があります。この状態は `heartbeatRetryTimes` に依存しているため、この問題を修正するために、ノードの生存状態を示すプロパティ `aliveStatus` が `HeartbeatResponse` に追加されました。[#12481](https://github.com/StarRocks/starrocks/pull/12481)

### 動作の変更

StarRocks がクエリできる Hive STRING 列の長さを 64 KB から 1 MB に拡張しました。STRING 列が 1 MB を超える場合、クエリ中に null 列として処理されます。[#12986](https://github.com/StarRocks/starrocks/pull/12986)

## 2.2.8

リリース日: 2022年10月17日

### バグ修正

以下のバグが修正されました:

- 初期化段階で式がエラーに遭遇すると、BEs がクラッシュする可能性があります。[#11395](https://github.com/StarRocks/starrocks/pull/11395)

- 無効な JSON データがロードされると、BEs がクラッシュする可能性があります。[#10804](https://github.com/StarRocks/starrocks/issues/10804)

- パイプラインエンジンが有効な場合、並列書き込みがエラーに遭遇します。[#11451](https://github.com/StarRocks/starrocks/issues/11451)

- ORDER BY NULL LIMIT 句が使用されると、BEs がクラッシュします。[#11648](https://github.com/StarRocks/starrocks/issues/11648)

- クエリする Parquet ファイルが Hive テーブルスキーマと一致しない列タイプを持つ場合、BEs がクラッシュします。[#11839](https://github.com/StarRocks/starrocks/issues/11839)

## 2.2.7

リリース日: 2022年9月23日

### バグ修正

以下のバグが修正されました:

- JSON データを StarRocks にロードする際にデータが失われる可能性があります。[#11054](https://github.com/StarRocks/starrocks/issues/11054)

- SHOW FULL TABLES の出力が正しくありません。[#11126](https://github.com/StarRocks/starrocks/issues/11126)

- 以前のバージョンでは、ビュー内のデータにアクセスするには、ユーザーがベーステーブルとビューの両方に対する権限を持っている必要がありました。現在のバージョンでは、ユーザーはビューに対する権限のみを持っていれば十分です。[#11290](https://github.com/StarRocks/starrocks/pull/11290)

- EXISTS または IN でネストされた複雑なクエリの結果が正しくありません。[#11415](https://github.com/StarRocks/starrocks/pull/11415)

- 対応する Hive テーブルのスキーマが変更されると、REFRESH EXTERNAL TABLE が失敗します。[#11406](https://github.com/StarRocks/starrocks/pull/11406)

- 非リーダー FE がビットマップインデックス作成操作を再生する際にエラーが発生する可能性があります。[#11261](

## 2.2.6

リリース日: 2022年9月14日

### バグ修正

以下のバグが修正されました:

- サブクエリに LIMIT が含まれる場合、`order by... limit...offset` の結果が正しくありません。[#9698](https://github.com/StarRocks/starrocks/issues/9698)

- 大量のデータを持つテーブルに部分更新を行うと、BE がクラッシュします。[#9809](https://github.com/StarRocks/starrocks/issues/9809)

- Compaction が 2 GB を超える BITMAP データのコンパクト化を引き起こすと、BEs がクラッシュします。[#11159](https://github.com/StarRocks/starrocks/pull/11159)

- パターンの長さが 16 KB を超える場合、like() および regexp() 関数が機能しません。[#10364](https://github.com/StarRocks/starrocks/issues/10364)

### 動作の変更

配列内の JSON 値を表すために使用される形式が変更されました。返される JSON 値にはエスケープ文字が使用されなくなりました。[#10790](https://github.com/StarRocks/starrocks/issues/10790)

## 2.2.5

リリース日: 2022年8月18日

### 改善点

- パイプラインエンジンが有効な場合のシステムパフォーマンスを向上させました。[#9580](https://github.com/StarRocks/starrocks/pull/9580)

- インデックスメタデータのメモリ統計の精度を向上させました。[#9837](https://github.com/StarRocks/starrocks/pull/9837)

### バグ修正

以下のバグが修正されました:

- Routine Load 中に Kafka パーティションオフセット (`get_partition_offset`) をクエリする際に BEs がスタックする可能性があります。[#9937](https://github.com/StarRocks/starrocks/pull/9937)

- 複数の Broker Load スレッドが同じ HDFS ファイルをロードしようとするとエラーが発生します。[#9507](https://github.com/StarRocks/starrocks/pull/9507)

## 2.2.4

リリース日: 2022年8月3日

### 改善点

- Hive テーブルのスキーマ変更を対応する外部テーブルに同期することをサポートしました。[#9010](https://github.com/StarRocks/starrocks/pull/9010)

- Parquet ファイル内の ARRAY データを Broker Load 経由でロードすることをサポートしました。[#9131](https://github.com/StarRocks/starrocks/pull/9131)

### バグ修正

以下のバグが修正されました:

- Broker Load が複数の keytab ファイルを使用した Kerberos ログインを処理できません。[#8820](https://github.com/StarRocks/starrocks/pull/8820) [#8837](https://github.com/StarRocks/starrocks/pull/8837)

- **stop_be.sh** が実行直後に終了すると、スーパーバイザーがサービスの再起動に失敗する可能性があります。[#9175](https://github.com/StarRocks/starrocks/pull/9175)

- 不正な Join Reorder の優先順位が「列を解決できません」というエラーを引き起こします。[#9063](https://github.com/StarRocks/starrocks/pull/9063) [#9487](https://github.com/StarRocks/starrocks/pull/9487)

## 2.2.3

リリース日: 2022年7月24日

### バグ修正

以下のバグが修正されました:

- ユーザーがリソースグループを削除する際にエラーが発生します。[#8036](https://github.com/StarRocks/starrocks/pull/8036)

- スレッド数が不足している場合、Thrift サーバーが終了します。[#7974](https://github.com/StarRocks/starrocks/pull/7974)

- 一部のシナリオで、CBO の join reorder が結果を返しません。[#7099](https://github.com/StarRocks/starrocks/pull/7099) [#7831](https://github.com/StarRocks/starrocks/pull/7831) [#6866](https://github.com/StarRocks/starrocks/pull/6866)

## 2.2.2

リリース日: 2022年6月29日

### 改善点

- UDF をデータベース間で使用できるようになりました。[#6865](https://github.com/StarRocks/starrocks/pull/6865) [#7211](https://github.com/StarRocks/starrocks/pull/7211)

- スキーマ変更などの内部処理の並行制御を最適化しました。これにより、FE メタデータ管理への負荷が軽減されます。また、大量のデータを高い並行性でロードするシナリオで、ロードジョブが積み重なったり遅くなったりする可能性が減少します。[#6838](https://github.com/StarRocks/starrocks/pull/6838)

### バグ修正

以下のバグが修正されました:

- CTAS を使用して作成されたレプリカ (`replication_num`) の数が正しくありません。[#7036](https://github.com/StarRocks/starrocks/pull/7036)

- ALTER ROUTINE LOAD を実行した後、メタデータが失われる可能性があります。[#7068](https://github.com/StarRocks/starrocks/pull/7068)

- ランタイムフィルタがプッシュダウンに失敗します。[#7206](https://github.com/StarRocks/starrocks/pull/7206) [#7258](https://github.com/StarRocks/starrocks/pull/7258)

- メモリリークを引き起こす可能性のあるパイプラインの問題。[#7295](https://github.com/StarRocks/starrocks/pull/7295)

- Routine Load ジョブが中止されるとデッドロックが発生する可能性があります。[#6849](https://github.com/StarRocks/starrocks/pull/6849)

- 一部のプロファイル統計情報が不正確です。[#7074](https://github.com/StarRocks/starrocks/pull/7074) [#6789](https://github.com/StarRocks/starrocks/pull/6789)

- get_json_string 関数が JSON 配列を誤って処理します。[#7671](https://github.com/StarRocks/starrocks/pull/7671)

## 2.2.1

リリース日: 2022年6月2日

### 改善点

- ホットスポットコードの一部を再構築し、ロックの粒度を減らすことで、データロードのパフォーマンスを最適化し、ロングテールのレイテンシーを削減しました。[#6641](https://github.com/StarRocks/starrocks/pull/6641)

- 各クエリに対して、BEs が展開されているマシンの CPU とメモリ使用情報を FE 監査ログに追加しました。[#6208](https://github.com/StarRocks/starrocks/pull/6208) [#6209](https://github.com/StarRocks/starrocks/pull/6209)

- 主キーテーブルとユニークキーテーブルで JSON データ型をサポートしました。[#6544](https://github.com/StarRocks/starrocks/pull/6544)

- ロックの粒度を減らし、BE レポート要求を重複排除することで、FEs の負荷を軽減しました。多数の BEs が展開されている場合のレポートパフォーマンスを最適化し、大規模クラスターで Routine Load タスクがスタックする問題を解決しました。[#6293](https://github.com/StarRocks/starrocks/pull/6293)

### バグ修正

以下のバグが修正されました:

- StarRocks が `SHOW FULL TABLES FROM DatabaseName` ステートメントで指定されたエスケープ文字を解析する際にエラーが発生します。[#6559](https://github.com/StarRocks/starrocks/issues/6559)

- FE のディスクスペース使用量が急激に増加します (このバグは BDBJE バージョンをロールバックすることで修正されます)。[#6708](https://github.com/StarRocks/starrocks/pull/6708)

- カラムスキャンが有効 (`enable_docvalue_scan=true`) の場合、返されたデータに関連するフィールドが見つからないため、BEs が故障します。[#6600](https://github.com/StarRocks/starrocks/pull/6600)

## 2.2.0

リリース日: 2022年5月22日

### 新機能

- [プレビュー] リソースグループ管理機能がリリースされました。この機能により、StarRocks は同じクラスター内の異なるテナントからの複雑なクエリと単純なクエリを処理する際に、CPU とメモリリソースを分離して効率的に使用できます。

- [プレビュー] Java ベースのユーザー定義関数 (UDF) フレームワークが実装されました。このフレームワークは、Java の構文に準拠してコンパイルされた UDF をサポートし、StarRocks の機能を拡張します。

- [プレビュー] 主キーテーブルは、注文の更新やマルチストリームジョインなどのリアルタイムデータ更新シナリオで、データがロードされる際に特定の列のみを更新することをサポートします。

- [プレビュー] JSON データ型と JSON 関数がサポートされます。

- 外部テーブルを使用して Apache Hudi からデータをクエリできます。これにより、StarRocks でのデータレイク分析の体験がさらに向上します。詳細については、[外部テーブル](https://docs.starrocks.io/docs/data_source/External_table/) を参照してください。

- 次の関数が追加されました:
  - ARRAY 関数: [array_agg](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_agg/)、array_sort、array_distinct、array_join、reverse、array_slice、array_concat、array_difference、arrays_overlap、array_intersect
  - BITMAP 関数: bitmap_max、bitmap_min
  - その他の関数: retention、square

### 改善点

- コストベースオプティマイザ (CBO) のパーサーとアナライザーが再構築され、コード構造が最適化され、INSERT with Common Table Expression (CTE) などの構文がサポートされました。これらの改善により、CTE の再利用を含む複雑なクエリのパフォーマンスが向上します。

- AWS Simple Storage Service (S3)、Alibaba Cloud Object Storage Service (OSS)、Tencent Cloud Object Storage (COS) などのクラウドオブジェクトストレージサービスに保存されている Apache Hive™ 外部テーブルのクエリパフォーマンスが最適化されました。最適化後、オブジェクトストレージベースのクエリのパフォーマンスは HDFS ベースのクエリと同等です。さらに、ORC ファイルの後期実体化がサポートされ、小さなファイルのクエリが加速されます。詳細については、[Apache Hive™ 外部テーブル](https://docs.starrocks.io/docs/data_source/External_table/) を参照してください。

- Apache Hive™ からのクエリが外部テーブルを使用して実行される場合、StarRocks はデータ変更やパーティション変更などの Hive メタストアイベントを消費することで、キャッシュされたメタデータの増分更新を自動的に行います。StarRocks はまた、Apache Hive™ からの DECIMAL 型および ARRAY 型のデータのクエリをサポートします。詳細については、[Apache Hive™ 外部テーブル](https://docs.starrocks.io/docs/data_source/External_table/) を参照してください。

- UNION ALL 演算子が以前よりも 2 倍から 25 倍速く実行されるように最適化されました。

- 適応並行性をサポートし、最適化されたプロファイルを提供するパイプラインエンジンがリリースされ、高い並行性シナリオでの単純なクエリのパフォーマンスを向上させます。

- 複数の文字を組み合わせて、インポートされる CSV ファイルの単一行区切り文字として使用できます。

### バグ修正

- 主キーテーブルに基づくテーブルにデータがロードされたり変更がコミットされたりすると、デッドロックが発生します。[#4998](https://github.com/StarRocks/starrocks/pull/4998)

- Oracle Berkeley DB Java Edition (BDB JE) を実行する FEs を含むフロントエンド (FEs) が不安定です。[#4428](https://github.com/StarRocks/starrocks/pull/4428)、[#4666](https://github.com/StarRocks/starrocks/pull/4666)、[#2](https://github.com/StarRocks/bdb-je/pull/2)

- SUM 関数が大量のデータに対して呼び出されると、算術オーバーフローが発生します。[#3944](https://github.com/StarRocks/starrocks/pull/3944)

- ROUND および TRUNCATE 関数が返す結果の精度が不満足です。[#4256](https://github.com/StarRocks/starrocks/pull/4256)

- Synthesized Query Lancer (SQLancer) によって検出されたいくつかのバグがあります。詳細については、[SQLancer 関連の問題](https://github.com/StarRocks/starrocks/issues?q=is:issue++label:sqlancer++milestone:2.2) を参照してください。

### その他

Flink-connector-starrocks は Apache Flink® v1.14 をサポートします。

### アップグレードノート

- StarRocks バージョン 2.0.4 以降または StarRocks バージョン 2.1.x の 2.1.6 以降を使用している場合、アップグレード前にタブレットクローン機能を無効にすることができます (`ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets" = "0");` および `ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "0");`)。アップグレード後、この機能を有効にすることができます (`ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets" = "2000");` および `ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "100");`)。

- アップグレード前のバージョンにロールバックするには、各 FE の **fe.conf** ファイルに `ignore_unknown_log_id` パラメータを追加し、このパラメータを `true` に設定します。このパラメータは、StarRocks v2.2.0 で新しいタイプのログが追加されたため必要です。このパラメータを追加しない場合、前のバージョンにロールバックできません。チェックポイントが作成された後、各 FE の **fe.conf** ファイルで `ignore_unknown_log_id` パラメータを `false` に設定することをお勧めします。その後、FEs を再起動して、FEs を以前の構成に復元します。