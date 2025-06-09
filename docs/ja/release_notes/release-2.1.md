---
displayed_sidebar: docs
---

# StarRocks version 2.1

## 2.1.13

リリース日: 2022年9月6日

### 改善点

- ロードされたデータの長さをチェックするための BE 設定項目 `enable_check_string_lengths` を追加しました。このメカニズムは、VARCHAR データサイズが範囲外であることによる Compaction の失敗を防ぐのに役立ちます。[#10380](https://github.com/StarRocks/starrocks/issues/10380)
- クエリに 1000 を超える OR 演算子が含まれる場合のクエリパフォーマンスを最適化しました。[#9332](https://github.com/StarRocks/starrocks/pull/9332)

### バグ修正

以下のバグが修正されました:

- Aggregate table を使用してテーブルから ARRAY 列（REPLACE_IF_NOT_NULL 関数を使用して計算された）をクエリすると、エラーが発生し、BEs がクラッシュすることがあります。[#10144](https://github.com/StarRocks/starrocks/issues/10144)
- クエリ内に複数の IFNULL() 関数がネストされている場合、クエリ結果が正しくありません。[#5028](https://github.com/StarRocks/starrocks/issues/5028) [#10486](https://github.com/StarRocks/starrocks/pull/10486)
- 動的パーティションが切り捨てられた後、パーティション内のタブレットの数が動的パーティション化で設定された値からデフォルト値に変わります。[#10435](https://github.com/StarRocks/starrocks/issues/10435)
- Kafka クラスターが停止しているときに Routine Load を使用して StarRocks にデータをロードすると、デッドロックが発生し、クエリパフォーマンスに影響を与えることがあります。[#8947](https://github.com/StarRocks/starrocks/issues/8947)
- クエリにサブクエリと ORDER BY 句の両方が含まれている場合、エラーが発生します。[#10066](https://github.com/StarRocks/starrocks/pull/10066)

## 2.1.12

リリース日: 2022年8月9日

### 改善点

BDB JE のメタデータクリーンアップを高速化するために、`bdbje_cleaner_threads` と `bdbje_replay_cost_percent` の2つのパラメータを追加しました。[#8371](https://github.com/StarRocks/starrocks/pull/8371)

### バグ修正

以下のバグが修正されました:

- 一部のクエリが Leader FE に転送され、SHOW FRONTENDS などの SQL ステートメントに関する実行情報が正しく返されないことがあります。[#9185](https://github.com/StarRocks/starrocks/issues/9185)
- BE が終了した後、現在のプロセスが完全に終了せず、BE の再起動に失敗します。[#9175](https://github.com/StarRocks/starrocks/pull/9267)
- 同じ HDFS データファイルをロードするために複数の Broker Load ジョブが作成された場合、1つのジョブが例外に遭遇すると、他のジョブがデータを正しく読み取れず、失敗することがあります。[#9506](https://github.com/StarRocks/starrocks/issues/9506)
- テーブルのスキーマが変更されたときに関連する変数がリセットされず、テーブルをクエリするとエラー (`no delete vector found tablet`) が発生します。[#9192](https://github.com/StarRocks/starrocks/issues/9192)

## 2.1.11

リリース日: 2022年7月9日

### バグ修正

以下のバグが修正されました:

- 主キーテーブルのテーブルにデータをロードする際に、頻繁にデータロードが行われるとデータロードが中断されます。[#7763](https://github.com/StarRocks/starrocks/issues/7763)
- 低基数最適化中に集計式が誤った順序で処理され、`count distinct` 関数が予期しない結果を返します。[#7659](https://github.com/StarRocks/starrocks/issues/7659)
- LIMIT 句に対して結果が返されないのは、句内のプルーニングルールが正しく処理されないためです。[#7894](https://github.com/StarRocks/starrocks/pull/7894)
- 低基数最適化のためのグローバル辞書がクエリのジョイン条件として定義された列に適用されると、クエリが予期しない結果を返します。[#8302](https://github.com/StarRocks/starrocks/issues/8302)

## 2.1.10

リリース日: 2022年6月24日

### バグ修正

以下のバグが修正されました:

- Leader FE ノードを繰り返し切り替えると、すべてのロードジョブが停止して失敗することがあります。[#7350](https://github.com/StarRocks/starrocks/issues/7350)
- `DESC` SQL でテーブルスキーマをチェックするときに、DECIMAL(18,2) 型のフィールドが DECIMAL64(18,2) と表示されます。[#7309](https://github.com/StarRocks/starrocks/pull/7309)
- メモリ使用量の推定が 4GB を超えると、MemTable のロード中にデータスキューが発生し、一部のフィールドが大量のメモリリソースを占有するため、BE がクラッシュします。[#7161](https://github.com/StarRocks/starrocks/issues/7161)
- Compaction 中に多くの入力行がある場合、max_rows_per_segment の計算でオーバーフローが発生し、多数の小さなセグメントファイルが作成されます。[#5610](https://github.com/StarRocks/starrocks/issues/5610)

## 2.1.8

リリース日: 2022年6月9日

### 改善点

- スキーマ変更などの内部処理ワークロードに使用される並行制御メカニズムが最適化され、フロントエンド (FE) メタデータへの負荷が軽減されました。そのため、大量のデータをロードするためにこれらのロードジョブが同時に実行される場合、ロードジョブが積み重なって遅くなる可能性が低くなります。[#6560](https://github.com/StarRocks/starrocks/pull/6560) [#6804](https://github.com/StarRocks/starrocks/pull/6804)
- 高頻度でデータをロードする際の StarRocks のパフォーマンスが向上しました。[#6532](https://github.com/StarRocks/starrocks/pull/6532) [#6533](https://github.com/StarRocks/starrocks/pull/6533)

### バグ修正

以下のバグが修正されました:

- ALTER 操作ログが LOAD ステートメントに関するすべての情報を記録しないため、ルーチンロードジョブに対して ALTER 操作を実行した後、チェックポイントが作成されるとジョブのメタデータが失われます。[#6936](https://github.com/StarRocks/starrocks/issues/6936)
- ルーチンロードジョブを停止するとデッドロックが発生する可能性があります。[#6450](https://github.com/StarRocks/starrocks/issues/6450)
- デフォルトでは、バックエンド (BE) はロードジョブに対してデフォルトの UTC+8 タイムゾーンを使用します。サーバーが UTC タイムゾーンを使用している場合、Spark load ジョブを使用してロードされたテーブルの DateTime 列のタイムスタンプに 8 時間が追加されます。[#6592](https://github.com/StarRocks/starrocks/issues/6592)
- GET_JSON_STRING 関数は非 JSON 文字列を処理できません。JSON オブジェクトまたは配列から JSON 値を抽出すると、関数は NULL を返します。関数は、JSON オブジェクトまたは配列に対して同等の JSON 形式の STRING 値を返すように最適化されています。[#6426](https://github.com/StarRocks/starrocks/issues/6426)
- データ量が多い場合、スキーマ変更が過剰なメモリ消費のために失敗することがあります。スキーマ変更のすべての段階でメモリ消費制限を指定できるように最適化が行われました。[#6705](https://github.com/StarRocks/starrocks/pull/6705)
- Compact 中のテーブルの列に重複する値の数が 0x40000000 を超えると、Compaction が中断されます。[#6513](https://github.com/StarRocks/starrocks/issues/6513)
- FE が再起動した後、BDB JE v7.3.8 のいくつかの問題により高い I/O と異常なディスク使用量の増加に直面し、正常に戻る兆しがありません。FE は BDB JE v7.3.7 にロールバックした後に正常に戻ります。[#6634](https://github.com/StarRocks/starrocks/issues/6634)

## 2.1.7

リリース日: 2022年5月26日

### 改善点

計算に関与するパーティションが大きい場合、フレームが ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW に設定されたウィンドウ関数に対して、StarRocks は計算を実行する前にパーティションのすべてのデータをキャッシュします。この状況では、大量のメモリリソースが消費されます。StarRocks はこの状況でパーティションのすべてのデータをキャッシュしないように最適化されています。[5829](https://github.com/StarRocks/starrocks/issues/5829)

### バグ修正

以下のバグが修正されました:

- 主キーテーブルを使用するテーブルにデータがロードされるとき、システムに保存されている各データバージョンの作成時間が後方に移動したシステム時間や関連する不明なバグなどの理由で単調に増加しない場合、データ処理エラーが発生することがあります。このようなデータ処理エラーはバックエンド (BEs) を停止させます。[#6046](https://github.com/StarRocks/starrocks/issues/6046)
- 一部のグラフィカルユーザーインターフェース (GUI) ツールは set_sql_limit 変数を自動的に設定します。その結果、SQL ステートメント ORDER BY LIMIT が無視され、クエリに対して誤った行数が返されます。[#5966](https://github.com/StarRocks/starrocks/issues/5966)
- DROP SCHEMA ステートメントがデータベースで実行されると、データベースが強制的に削除され、復元できなくなります。[#6201](https://github.com/StarRocks/starrocks/issues/6201)
- JSON 形式のデータがロードされるとき、データに JSON 形式のエラーが含まれている場合、BEs が停止します。たとえば、キーと値のペアがカンマ (,) で区切られていない場合です。[#6098](https://github.com/StarRocks/starrocks/issues/6098)
- 高い同時実行性で大量のデータがロードされているとき、ディスクにデータを書き込むために実行されるタスクが BEs に積み重なります。この状況では、BEs が停止することがあります。[#3877](https://github.com/StarRocks/starrocks/issues/3877)
- StarRocks はテーブルに対してスキーマ変更を実行する前に必要なメモリ量を推定します。テーブルに大量の STRING フィールドが含まれている場合、メモリ推定結果が不正確になることがあります。この状況では、推定された必要メモリ量が単一のスキーマ変更操作に許可される最大メモリを超えると、適切に実行されるはずのスキーマ変更操作がエラーに遭遇します。[#6322](https://github.com/StarRocks/starrocks/issues/6322)
- 主キーテーブルを使用するテーブルに対してスキーマ変更が実行された後、そのテーブルにデータをロードすると「duplicate key xxx」エラーが発生することがあります。[#5878](https://github.com/StarRocks/starrocks/issues/5878)
- Shuffle Join 操作中に低基数最適化が実行されると、パーティショニングエラーが発生することがあります。[#4890](https://github.com/StarRocks/starrocks/issues/4890)
- 大量のテーブルを含むコロケーショングループ (CG) に頻繁にデータがロードされる場合、CG が安定した状態を維持できないことがあります。この場合、JOIN ステートメントは Colocate Join 操作をサポートしません。StarRocks はデータロード中に少し長く待つように最適化されています。これにより、データがロードされるタブレットレプリカの整合性を最大限に高めることができます。

## 2.1.6

リリース日: 2022年5月10日

### バグ修正

以下のバグが修正されました:

- 複数の DELETE 操作を実行した後にクエリを実行すると、クエリに対して低基数列の最適化が行われた場合、誤ったクエリ結果が得られることがあります。[#5712](https://github.com/StarRocks/starrocks/issues/5712)
- 特定のデータ取り込みフェーズでタブレットが移行されると、タブレットが保存されている元のディスクにデータが書き込まれ続けます。その結果、データが失われ、クエリが正常に実行できなくなります。[#5160](https://github.com/StarRocks/starrocks/issues/5160)
- DECIMAL と STRING データ型の間で値を変換すると、返される値が予期しない精度になることがあります。[#5608](https://github.com/StarRocks/starrocks/issues/5608)
- DECIMAL 値を BIGINT 値で乗算すると、算術オーバーフローが発生することがあります。このバグを修正するためにいくつかの調整と最適化が行われました。[#4211](https://github.com/StarRocks/starrocks/pull/4211)

## 2.1.5

リリース日: 2022年4月27日

### バグ修正

以下のバグが修正されました:

- 小数の乗算がオーバーフローすると計算結果が正しくありません。バグが修正された後、小数の乗算がオーバーフローすると NULL が返されます。
- 統計が実際の統計と大きく異なる場合、Collocate Join の優先順位が Broadcast Join よりも低くなることがあります。その結果、クエリプランナーが Colocate Join をより適切なジョイン戦略として選択しないことがあります。[#4817](https://github.com/StarRocks/starrocks/pull/4817)
- 複雑な式のプランが誤っているため、4 つ以上のテーブルをジョインするとクエリが失敗します。
- Shuffle Join の下でシャッフル列が低基数列である場合、BEs が動作を停止することがあります。[#4890](https://github.com/StarRocks/starrocks/issues/4890)
- SPLIT 関数が NULL パラメータを使用すると、BEs が動作を停止することがあります。[#4092](https://github.com/StarRocks/starrocks/issues/4092)

## 2.1.4

リリース日: 2022年4月8日

### 新機能

- `UUID_NUMERIC` 関数がサポートされ、LARGEINT 値を返します。`UUID` 関数と比較して、`UUID_NUMERIC` 関数のパフォーマンスはほぼ 2 桁向上します。

### バグ修正

以下のバグが修正されました:

- 列を削除し、新しいパーティションを追加し、タブレットをクローンした後、古いタブレットと新しいタブレットの列のユニーク ID が同じでない場合があり、システムが共有タブレットスキーマを使用するため、BE が動作を停止することがあります。[#4514](https://github.com/StarRocks/starrocks/issues/4514)
- StarRocks 外部テーブルにデータをロードする際に、ターゲット StarRocks クラスターの設定された FE が Leader でない場合、FE が動作を停止します。[#4573](https://github.com/StarRocks/starrocks/issues/4573)
- `CAST` 関数の結果が StarRocks バージョン 1.19 と 2.1 で異なります。[#4701](https://github.com/StarRocks/starrocks/pull/4701)
- Duplicate Key table がスキーマ変更を行い、同時にマテリアライズドビューを作成すると、クエリ結果が正しくない可能性があります。[#4839](https://github.com/StarRocks/starrocks/issues/4839)

## 2.1.3

リリース日: 2022年3月19日

### バグ修正

以下のバグが修正されました:

- BE の障害によるデータ損失の可能性のある問題（Batch publish version を使用して解決）。[#3140](https://github.com/StarRocks/starrocks/issues/3140)
- 一部のクエリが不適切な実行プランのためにメモリ制限超過エラーを引き起こすことがあります。
- 異なる Compaction プロセスでレプリカ間のチェックサムが一致しないことがあります。[#3438](https://github.com/StarRocks/starrocks/issues/3438)
- JSON リオーダープロジェクションが正しく処理されない場合、クエリが失敗することがあります。[#4056](https://github.com/StarRocks/starrocks/pull/4056)

## 2.1.2

リリース日: 2022年3月14日

### バグ修正

以下のバグが修正されました:

- バージョン 1.19 から 2.1 へのローリングアップグレードで、2 つのバージョン間のチャンクサイズが一致しないため、BE ノードが動作を停止します。[#3834](https://github.com/StarRocks/starrocks/issues/3834)
- StarRocks がバージョン 2.0 から 2.1 に更新される際に、ロードタスクが失敗することがあります。[#3828](https://github.com/StarRocks/starrocks/issues/3828)
- 単一タブレットテーブルのジョインに適切な実行プランがない場合、クエリが失敗します。[#3854](https://github.com/StarRocks/starrocks/issues/3854)
- FE ノードが低基数最適化のためのグローバル辞書を構築するための情報を収集する際にデッドロック問題が発生することがあります。[#3839](https://github.com/StarRocks/starrocks/issues/3839)
- デッドロックのために BE ノードが仮死状態になると、クエリが失敗します。
- SHOW VARIABLES コマンドが失敗すると、BI ツールが StarRocks に接続できません。[#3708](https://github.com/StarRocks/starrocks/issues/3708)

## 2.1.0

リリース日: 2022年2月24日

### 新機能

- [プレビュー] StarRocks は Iceberg 外部テーブルをサポートします。
- [プレビュー] パイプラインエンジンが利用可能になりました。これはマルチコアスケジューリング用に設計された新しい実行エンジンです。クエリの並行性は、parallel_fragment_exec_instance_num パラメータを設定することなく適応的に調整できます。これにより、高い同時実行性のシナリオでのパフォーマンスも向上します。
- CTAS (CREATE TABLE AS SELECT) ステートメントがサポートされ、ETL とテーブル作成が容易になります。
- SQL フィンガープリントがサポートされます。SQL フィンガープリントは audit.log に生成され、遅いクエリの位置特定を容易にします。
- ANY_VALUE、ARRAY_REMOVE、SHA2 関数がサポートされます。

### 改善点

- Compaction が最適化されました。フラットテーブルには最大 10,000 列を含めることができます。
- 初回スキャンとページキャッシュのパフォーマンスが最適化されました。ランダム I/O が削減され、初回スキャンのパフォーマンスが向上します。この改善は、初回スキャンが SATA ディスクで発生する場合により顕著です。StarRocks のページキャッシュは元のデータを保存でき、ビットシャッフルエンコーディングや不要なデコードが不要になります。これにより、キャッシュヒット率とクエリ効率が向上します。
- 主キーテーブルでスキーマ変更がサポートされます。`Alter table` を使用してビットマップインデックスを追加、削除、変更できます。
- [プレビュー] 文字列のサイズは最大 1 MB まで可能です。
- JSON ロードパフォーマンスが最適化されました。1 つのファイルで 100 MB を超える JSON データをロードできます。
- ビットマップインデックスのパフォーマンスが最適化されました。
- StarRocks Hive 外部テーブルのパフォーマンスが最適化されました。CSV 形式のデータを読み取ることができます。
- `create table` ステートメントで DEFAULT CURRENT_TIMESTAMP がサポートされます。
- StarRocks は複数の区切り文字を持つ CSV ファイルのロードをサポートします。

### バグ修正

以下のバグが修正されました:

- JSON データのロードに使用されるコマンドで jsonpaths が指定されている場合、Auto __op マッピングが効果を発揮しません。
- Broker Load を使用してデータをロードする際に、ソースデータが変更されると BE ノードが失敗します。
- マテリアライズドビューが作成された後、一部の SQL ステートメントがエラーを報告します。
- ルーチンロードが引用符付き jsonpaths のために機能しません。
- クエリする列数が 200 を超えると、クエリの同時実行性が急激に低下します。

### 動作の変更

コロケーショングループを無効にするための API が `DELETE /api/colocate/group_stable` から `POST /api/colocate/group_unstable` に変更されました。

### その他

flink-connector-starrocks が利用可能になり、Flink が StarRocks データをバッチで読み取ることができます。これにより、JDBC コネクタと比較してデータ読み取り効率が向上します。