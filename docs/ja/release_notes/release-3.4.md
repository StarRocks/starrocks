---
displayed_sidebar: docs
---

# StarRocks version 3.4

## 3.4.1

リリース日: 2025年3月12日

### 新機能と改善点

- Data Lake 分析で Delta Lake の Deletion Vector をサポートしました。
- セキュアビューをサポートしました。セキュアビューを作成することで、基となるテーブルの SELECT 権限を持たないユーザーが、ビューをクエリできないようにできます（そのユーザーがビュー自体の SELECT 権限を持っていたとしても）。
- Sketch HLL ([`ds_hll_count_distinct`](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/ds_hll_count_distinct/)) をサポートしました。`approx_count_distinct`と比較して、より高精度な近似重複排除が可能になります。
- 共有データクラスタで、クラスターリカバリのための自動 Snapshot 作成をサポートしました。
- 共有データクラスタの Storage Volume が Azure Data Lake Storage Gen2 をサポートしました。
- MySQL プロトコルを使用した StarRocks への接続で SSL 認証をサポートしました。これにより、クライアントと StarRocks クラスター間のデータ通信が不正アクセスから保護されます。

### バグ修正

以下の問題を修正しました：

- OLAP ビューがマテリアライズドビューの処理ロジックに影響を与える問題を修正しました。[#52989](https://github.com/StarRocks/starrocks/pull/52989)
- 1 つのレプリカが見つからない場合、他のレプリカがいくつ成功してもトランザクションが失敗する問題を修正しました。（修正後は、大多数のレプリカが成功すればトランザクションが完了するようになりました。）[#55212](https://github.com/StarRocks/starrocks/pull/55212)
- Alive 状態が false のノードに Stream Load がスケジュールされるとインポートが失敗する問題を修正しました。[#55371](https://github.com/StarRocks/starrocks/pull/55371)
- クラスター Snapshot 内のファイルが誤って削除される問題を修正しました。[#56338](https://github.com/StarRocks/starrocks/pull/56338)

### 動作の変更

- グレースフルシャットダウンのデフォルト設定を「無効」から「有効」に変更しました。関連するBE/CNパラメータ `loop_count_wait_fragments_finish` のデフォルト値が `2` に変更され、システムは実行中のクエリが完了するまで最大20秒待機するようになりました。[#56002](https://github.com/StarRocks/starrocks/pull/56002)

## 3.4.0

リリース日: 2025年1月24日

### データレイク分析

- Iceberg V2のクエリパフォーマンスを最適化し、delete-filesの繰り返し読み込みを減らすことでメモリ使用量を削減しました。
- Delta Lakeテーブルのカラムマッピングをサポートし、Delta Schema Evolution後のデータに対するクエリを可能にしました。詳細は[Delta Lake catalog - Feature support](https://docs.starrocks.io/docs/data_source/catalog/deltalake_catalog/#feature-support)をご覧ください。
- Data Cacheに関連する改善:
  - Segmented LRU (SLRU) キャッシュ削除戦略を導入し、偶発的な大規模クエリによるキャッシュ汚染を大幅に防ぎ、キャッシュヒット率を向上させ、クエリパフォーマンスの変動を減少させます。大規模クエリのシミュレーションテストケースでは、SLRUベースのクエリパフォーマンスが70%以上向上することがあります。詳細は[Data Cache - Cache replacement policies](https://docs.starrocks.io/docs/data_source/data_cache/#cache-replacement-policies)をご覧ください。
  - 共有データアーキテクチャとデータレイククエリシナリオの両方で使用されるData Cacheインスタンスを統一し、設定を簡素化し、リソース利用を向上させました。詳細は[Data Cache](https://docs.starrocks.io/docs/using_starrocks/caching/block_cache/)をご覧ください。
  - Data Cacheの適応型I/O戦略最適化を提供し、キャッシュディスクの負荷とパフォーマンスに基づいて一部のクエリ要求をリモートストレージに柔軟にルーティングし、全体的なアクセススループットを向上させます。
  - データレイククエリシナリオでのData Cache内のデータの永続化をサポートします。以前にキャッシュされたデータは、BE再起動後に再利用でき、クエリパフォーマンスの変動を減少させます。
- クエリによってトリガーされる自動ANALYZEタスクを通じて、外部テーブル統計の自動収集をサポートします。これにより、メタデータファイルと比較してより正確なNDV情報を提供し、クエリプランを最適化し、クエリパフォーマンスを向上させます。詳細は[Query-triggered collection](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/#query-triggered-collection)をご覧ください。
- IcebergのTime Travelクエリ機能を提供し、TIMESTAMPまたはVERSIONを指定することで、指定されたBRANCHまたはTAGからデータを読み取ることができます。
- データレイククエリのクエリフラグメントの非同期配信をサポートします。これにより、FEがクエリを実行する前にすべてのファイルを取得する必要があるという制限を回避し、FEがクエリファイルを取得し、BEがクエリを並行して実行できるようになり、キャッシュにない多数のファイルを含むデータレイククエリの全体的な遅延を削減します。同時に、ファイルリストをキャッシュすることによるFEのメモリ負荷を軽減し、クエリの安定性を向上させます。（現在、HudiとDelta Lakeの最適化が実装されており、Icebergの最適化はまだ開発中です。）

### パフォーマンスの向上とクエリの最適化

- [実験的] 遅いクエリの自動最適化のための初期的なQuery Feedback機能を提供します。システムは遅いクエリの実行詳細を収集し、潜在的な最適化の機会を自動的に分析し、クエリに対するカスタマイズされた最適化ガイドを生成します。CBOが後続の同一クエリに対して同じ悪いプランを生成した場合、システムはこのクエリプランをガイドに基づいてローカルに最適化します。詳細は[Query Feedback](https://docs.starrocks.io/docs/using_starrocks/query_feedback/)をご覧ください。
- [実験的] Python UDFをサポートし、Java UDFと比較してより便利な関数カスタマイズを提供します。詳細は[Python UDF](https://docs.starrocks.io/docs/sql-reference/sql-functions/Python_UDF/)をご覧ください。
- [実験的] クエリ結果の大容量データのより効率的な読み取りのためのArrow Flightインターフェースをサポートします。また、FEではなくBEが返された結果を処理できるようにし、FEへの負担を大幅に軽減します。特にビッグデータ分析と処理、機械学習を含むビジネスシナリオに適しています。
- 複数カラムのOR述語のプッシュダウンを可能にし、複数カラムのOR条件（例: `a = xxx OR b = yyy`）を持つクエリが特定のカラムインデックスを利用できるようにし、データの読み取り量を削減し、クエリパフォーマンスを向上させます。
- TPC-DS 1TB IcebergデータセットでTPC-DSクエリパフォーマンスを約20%最適化しました。最適化方法には、テーブルプルーニングと主キーおよび外部キーを使用した集約カラムプルーニング、集約プッシュダウンが含まれます。

### 共有データの強化

- Query Cacheをサポートし、共有なしアーキテクチャと整合性を持たせました。
- 同期マテリアライズドビューをサポートし、共有なしアーキテクチャと整合性を持たせました。

### ストレージエンジン

- すべてのパーティション化の手法を式に基づくパーティション化に統一し、各レベルが任意の式であるマルチレベルパーティション化をサポートしました。詳細は[Expression Partitioning](https://docs.starrocks.io/docs/table_design/data_distribution/expression_partitioning/)をご覧ください。
- [プレビュー] 集計テーブルでのすべてのネイティブ集計関数をサポートします。汎用集計関数状態ストレージフレームワークを導入することで、StarRocksがサポートするすべてのネイティブ集計関数を使用して集計テーブルを定義できます。
- ベクトルインデックスをサポートし、大規模で高次元のベクトルの高速な近似最近傍検索（ANNS）を可能にします。これは、ディープラーニングや機械学習のシナリオで一般的に必要とされます。現在、StarRocksは2種類のベクトルインデックスをサポートしています: IVFPQとHNSW。

### ロード

- INSERT OVERWRITEは新しいセマンティック - Dynamic Overwriteをサポートします。このセマンティックが有効な場合、取り込まれたデータは新しいパーティションを作成するか、または新しいデータレコードに対応する既存のパーティションを上書きします。関与しないパーティションは切り捨てられたり削除されたりしません。このセマンティックは、ユーザーが特定のパーティションのデータを復元したい場合に特に便利です。詳細は[Dynamic Overwrite](https://docs.starrocks.io/docs/loading/InsertInto/#dynamic-overwrite)をご覧ください。
- INSERT from FILESを使用したデータ取り込みを最適化し、Broker Loadを優先するロード方法として置き換えました:
  - FILESはリモートストレージ内のファイルのリスト化をサポートし、ファイルの基本統計を提供します。詳細は[FILES - list_files_only](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#list_files_only)をご覧ください。
  - INSERTはカラム名による一致をサポートし、特に同一の名前を持つ多数のカラムからデータをロードする際に便利です。（デフォルトの動作はカラムの位置による一致です。）詳細は[Match column by name](https://docs.starrocks.io/docs/loading/InsertInto/#match-column-by-name)をご覧ください。
  - INSERTは他のロード方法と整合性を持たせてPROPERTIESを指定することをサポートします。ユーザーはINSERT操作のために`strict_mode`、`max_filter_ratio`、および`timeout`を指定してデータ取り込みの動作と品質を制御できます。詳細は[INSERT - PROPERTIES](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT/#properties)をご覧ください。
  - INSERT from FILESは、より正確なソースデータスキーマを推測するために、FILESのスキャン段階にターゲットテーブルスキーマチェックをプッシュダウンすることをサポートします。詳細は[Push down target table schema check](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#push-down-target-table-schema-check)をご覧ください。
  - FILESは異なるスキーマを持つファイルの統合をサポートします。ParquetとORCファイルのスキーマはカラム名に基づいて統合され、CSVファイルのスキーマはカラムの位置（順序）に基づいて統合されます。カラムが一致しない場合、ユーザーはプロパティ`fill_mismatch_column_with`を指定してカラムをNULLで埋めるかエラーを返すかを選択できます。詳細は[Union files with different schema](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#union-files-with-different-schema)をご覧ください。
  - FILESはParquetファイルからSTRUCT型データを推測することをサポートします。（以前のバージョンでは、STRUCTデータはSTRING型として推測されていました。）詳細は[Infer STRUCT type from Parquet](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#infer-struct-type-from-parquet)をご覧ください。
- 複数の同時Stream Loadリクエストを単一のトランザクションにマージし、データをバッチでコミットすることをサポートし、リアルタイムデータ取り込みのスループットを向上させます。これは、高い同時実行性、小規模バッチ（KBから数十MB）のリアルタイムロードシナリオ向けに設計されています。頻繁なロード操作によって引き起こされる過剰なデータバージョン、Compaction中のリソース消費、および過剰な小ファイルによるIOPSとI/O遅延を削減できます。

### その他

- BEとCNの優雅な終了プロセスを最適化し、優雅な終了中のBEまたはCNノードのステータスを正確に`SHUTDOWN`として表示します。
- ログの印刷を最適化し、過剰なディスクスペースの占有を回避します。
- 共有なしクラスタは、ビュー、external catalogメタデータ、および式に基づくパーティション化とリストパーティション化戦略で作成されたパーティションなど、より多くのオブジェクトのバックアップと復元をサポートします。
- [プレビュー] CheckPointをFollower FEでサポートし、CheckPoint中のLeader FEの過剰なメモリを回避し、Leader FEの安定性を向上させます。

### ダウングレードノート

- クラスタはv3.4.0からv3.3.9以降にのみダウングレードできます。
