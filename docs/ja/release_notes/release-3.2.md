---
displayed_sidebar: docs
---

# StarRocks version 3.2

## 3.2.15

リリース日: 2025年2月14日

### 新機能

- ウィンドウ関数が `max_by` と `min_by` をサポートしました。 [#54961](https://github.com/StarRocks/starrocks/pull/54961)

### 改善点

- StarClient のタイムアウトパラメータを追加しました。 [#54496](https://github.com/StarRocks/starrocks/pull/54496)
  - star_client_read_timeout_seconds
  - star_client_list_timeout_seconds
  - star_client_write_timeout_seconds
- リストパーティション化戦略を持つテーブルが DELETE ステートメントのためのパーティションプルーニングをサポートしました。 [#55400](https://github.com/StarRocks/starrocks/pull/55400)

### バグ修正

以下の問題を修正しました:

- Alive ステータスが false のノードがスケジュールされたときに Stream Load が失敗する問題を修正しました。 [#55371](https://github.com/StarRocks/starrocks/pull/55371)
- Stream Load で主キーテーブルの部分更新中にエラーが返される問題を修正しました。 [#53403](https://github.com/StarRocks/starrocks/pull/55430)
- BE ノードの再起動後に bRPC エラーが持続する問題を修正しました。 [#40229](https://github.com/StarRocks/starrocks/pull/40229)

## 3.2.14

リリース日: 2025年1月8日

### 改善点

- Paimon テーブルの統計収集をサポートしました。 [#52858](https://github.com/StarRocks/starrocks/pull/52858)
- JSON メトリクスにノード情報とヒストグラムメトリクスを含めました。 [#53735](https://github.com/StarRocks/starrocks/pull/53735)

### バグ修正

以下の問題を修正しました:

- 主キーテーブルインデックスのスコアがコミットフェーズで更新されない問題を修正しました。 [#41737](https://github.com/StarRocks/starrocks/pull/41737)
- 低カーディナリティ最適化が有効な場合に `max(count(distinct))` の実行計画が誤っている問題を修正しました。 [#53403](https://github.com/StarRocks/starrocks/pull/53403)
- リストパーティション列に NULL 値がある場合、パーティション列の Min/Max 値に対するクエリが誤ったパーティションプルーニングを引き起こす問題を修正しました。 [#53235](https://github.com/StarRocks/starrocks/pull/53235)
- データを HDFS にバックアップする際のアップロードリトライが失敗する問題を修正しました。 [#53679](https://github.com/StarRocks/starrocks/pull/53679)

## 3.2.13

リリース日: 2024年12月13日

### 改善点

- 特定のテーブルに対してベースコンパクションが禁止される時間範囲を設定することをサポートしました。 [#50120](https://github.com/StarRocks/starrocks/pull/50120)

### バグ修正

以下の問題を修正しました:

- SHOW ROUTINE LOAD を実行した後に `loadRowsRate` フィールドが `0` を返す問題を修正しました。 [#52151](https://github.com/StarRocks/starrocks/pull/52151)
- `Files()` 関数がクエリされていない列を読み込む問題を修正しました。 [#52210](https://github.com/StarRocks/starrocks/pull/52210)
- 特殊文字を含む名前のマテリアライズドビューのメトリクスを Prometheus が解析できない問題を修正しました。（現在、マテリアライズドビューのメトリクスはタグをサポートしています。） [#52782](https://github.com/StarRocks/starrocks/pull/52782)
- `array_map` 関数が BE をクラッシュさせる問題を修正しました。 [#52909](https://github.com/StarRocks/starrocks/pull/52909)
- メタデータキャッシュの問題が BE をクラッシュさせる問題を修正しました。 [#52968](https://github.com/StarRocks/starrocks/pull/52968)
- 期限切れのトランザクションのために Routine Load タスクがキャンセルされる問題を修正しました。（現在、データベースまたはテーブルが存在しない場合にのみタスクがキャンセルされます。） [#50334](https://github.com/StarRocks/starrocks/pull/50334)
- HTTP 1.0 を使用して送信された場合に Stream Load が失敗する問題を修正しました。 [#53010](https://github.com/StarRocks/starrocks/pull/53010) [#53008](https://github.com/StarRocks/starrocks/pull/53008)
- Glue と S3 の統合に関連する問題: [#48433](https://github.com/StarRocks/starrocks/pull/48433)
  - 一部のエラーメッセージが根本原因を表示しない。
  - Glue をメタデータサービスとして使用する場合、STRING 型のパーティション列を持つ Hive パーティションテーブルへの書き込みに関するエラーメッセージ。
  - ユーザーに十分な権限がない場合に Hive テーブルの削除が適切なエラーメッセージなしに失敗する。
- マテリアライズドビューの `storage_cooldown_time` プロパティが `maximum` に設定されている場合に効果がない問題を修正しました。 [#52079](https://github.com/StarRocks/starrocks/pull/52079)

## 3.2.12

リリース日: 2024年10月23日

### 改善点

- 特定の複雑なクエリシナリオで OOM を回避するために BE のメモリアロケーションと統計を最適化しました。 [#51382](https://github.com/StarRocks/starrocks/pull/51382)
- スキーマ変更シナリオでの FE のメモリ使用量を最適化しました。 [#50855](https://github.com/StarRocks/starrocks/pull/50855)
- Follower FE ノードから `information_schema.routine_load_jobs` システム定義ビューをクエリする際のジョブステータス表示を最適化しました。 [#51763](https://github.com/StarRocks/starrocks/pull/51763)
- リストパーティション化されたテーブルのバックアップとリストアをサポートしました。 [#51993](https://github.com/StarRocks/starrocks/pull/51993)

### バグ修正

以下の問題を修正しました:

- Hive への書き込みが失敗した後にエラーメッセージが失われる問題を修正しました。 [#33167](https://github.com/StarRocks/starrocks/pull/33167)
- 過剰な定数パラメータが使用されると `array_map` 関数がクラッシュする問題を修正しました。 [#51244](https://github.com/StarRocks/starrocks/pull/51244)
- 式に基づくパーティション化されたテーブルの PARTITION BY 列に特殊文字が含まれると FE CheckPoint が失敗する問題を修正しました。 [#51677](https://github.com/StarRocks/starrocks/pull/51677)
- システム定義ビュー `information_schema.fe_locks` へのアクセスがクラッシュを引き起こす問題を修正しました。 [#51742](https://github.com/StarRocks/starrocks/pull/51742)
- 生成列をクエリするとエラーが発生する問題を修正しました。 [#51755](https://github.com/StarRocks/starrocks/pull/51755)
- テーブル名に特殊文字が含まれるとテーブルの最適化が失敗する問題を修正しました。 [#51755](https://github.com/StarRocks/starrocks/pull/51755)
- 特定のシナリオでタブレットがバランスされない問題を修正しました。 [#51828](https://github.com/StarRocks/starrocks/pull/51828)

### 動作の変更

- バックアップとリストア関連のパラメータの動的変更をサポートしました。[#52111](https://github.com/StarRocks/starrocks/pull/52111)

## 3.2.11

リリース日: 2024年9月9日

### 改善点

- Files() と PIPE の認証情報のマスキングをサポートしました。 [#47629](https://github.com/StarRocks/starrocks/pull/47629)
- Parquet ファイルを Files() 経由で読み取る際の STRUCT 型の自動推論をサポートしました。 [#50481](https://github.com/StarRocks/starrocks/pull/50481)

### バグ修正

以下の問題を修正しました:

- グローバル辞書によって書き換えられなかったために等値結合クエリでエラーが返される問題を修正しました。 [#50690](https://github.com/StarRocks/starrocks/pull/50690)
- Tablet Clone 中に FE 側で無限ループが発生し、「バージョンがコンパクト化された」というエラーが発生する問題を修正しました。 [#50561](https://github.com/StarRocks/starrocks/pull/50561)
- ラベルに基づいてデータを配布した後、不健康なレプリカの修復のスケジューリングが誤っている問題を修正しました。 [#50331](https://github.com/StarRocks/starrocks/pull/50331)
- 統計収集ログで「Unknown column '%s' in '%s."」というエラーが発生する問題を修正しました。 [#50785](https://github.com/StarRocks/starrocks/pull/50785)
- Files() 経由で Parquet ファイルから TIMESTAMP などの複雑な型を読み取る際にタイムゾーンの使用が誤っている問題を修正しました。 [#50448](https://github.com/StarRocks/starrocks/pull/50448)

### 動作の変更

- StarRocks を v3.3.x から v3.2.11 にダウングレードする際、互換性のないメタデータがある場合はシステムがそれを無視します。 [#49636](https://github.com/StarRocks/starrocks/pull/49636)

## 3.2.10

リリース日: 2024年8月23日

### 改善点

- Files() は Parquet ファイル内の `logical_type` が `JSON` の `BYTE_ARRAY` データを StarRocks の JSON 型に自動変換します。 [#49385](https://github.com/StarRocks/starrocks/pull/49385)
- Access Key ID と Secret Access Key が欠落している場合の Files() のエラーメッセージを最適化しました。 [#49090](https://github.com/StarRocks/starrocks/pull/49090)
- `information_schema.columns` が `GENERATION_EXPRESSION` フィールドをサポートしました。 [#49734](https://github.com/StarRocks/starrocks/pull/49734)

### バグ修正

以下の問題を修正しました:

- `persistent_index_type` プロパティを `"CLOUD_NATIVE"` に設定した後、v3.3 共有データクラスタを v3.2 にダウングレードするとクラッシュする問題を修正しました。 [#48149](https://github.com/StarRocks/starrocks/pull/48149)
- SELECT INTO OUTFILE を使用して CSV ファイルにデータをエクスポートする際にデータの不整合が発生する問題を修正しました。 [#48052](https://github.com/StarRocks/starrocks/pull/48052)
- 同時クエリ実行中にクエリが失敗する問題を修正しました。 [#48180](https://github.com/StarRocks/starrocks/pull/48180)
- プランフェーズでのタイムアウトによりクエリがハングする問題を修正しました。 [#48405](https://github.com/StarRocks/starrocks/pull/48405)
- 以前のバージョンで主キーテーブルのインデックス圧縮を無効にした後、v3.2.9 にアップグレードすると `page_off` 情報へのアクセスが配列の範囲外クラッシュを引き起こす問題を修正しました。 [#48230](https://github.com/StarRocks/starrocks/pull/48230)
- ADD/DROP COLUMN 操作の同時実行により BE がクラッシュする問題を修正しました。 [#49355](https://github.com/StarRocks/starrocks/pull/49355)
- ORC 形式ファイル内の負の `TINYINT` 値に対するクエリが aarch64 アーキテクチャで `None` を返す問題を修正しました。 [#49517](https://github.com/StarRocks/starrocks/pull/49517)
- ディスク書き込み操作が失敗した場合、主キー永続インデックスの `l0` スナップショットの失敗がデータ損失を引き起こす可能性がある問題を修正しました。 [#48045](https://github.com/StarRocks/starrocks/pull/48045)
- 主キーテーブルの部分更新が大規模データ更新シナリオで失敗する問題を修正しました。 [#49054](https://github.com/StarRocks/starrocks/pull/49054)
- v3.3.0 共有データクラスタを v3.2.9 にダウングレードする際に Fast Schema Evolution によって BE がクラッシュする問題を修正しました。 [#42737](https://github.com/StarRocks/starrocks/pull/42737)
- `partition_linve_nubmer` が効果を発揮しない問題を修正しました。 [#49213](https://github.com/StarRocks/starrocks/pull/49213)
- 主キーテーブルのインデックス永続性とコンパクションの競合がクローン失敗を引き起こす問題を修正しました。 [#49341](https://github.com/StarRocks/starrocks/pull/49341)
- ALTER TABLE を使用した `partition_line_number` の変更が効果を発揮しない問題を修正しました。 [#49437](https://github.com/StarRocks/starrocks/pull/49437)
- CTE の異なるグループセットの書き換えが無効なプランを生成する問題を修正しました。 [#48765](https://github.com/StarRocks/starrocks/pull/48765)
- RPC の失敗がスレッドプールを汚染する問題を修正しました。 [#49619](https://github.com/StarRocks/starrocks/pull/49619)
- PIPE 経由で AWS S3 からファイルをロードする際の認証失敗の問題を修正しました。 [#49837](https://github.com/StarRocks/starrocks/pull/49837)

### 動作の変更

- FE 起動スクリプトに `meta` ディレクトリのチェックを追加しました。ディレクトリが存在しない場合、自動的に作成されます。  [#48940](https://github.com/StarRocks/starrocks/pull/48940)
- データロードのためのメモリ制限パラメータ `load_process_max_memory_hard_limit_ratio` を追加しました。メモリ使用量が制限を超えると、後続のロードタスクは失敗します。 [#48495](https://github.com/StarRocks/starrocks/pull/48495)

## 3.2.9

リリース日: 2024年7月11日

### 新機能

- Paimon テーブルが DELETE Vector をサポートしました。 [#45866](https://github.com/StarRocks/starrocks/issues/45866)
- Apache Ranger を通じてカラムレベルのアクセス制御をサポートしました。 [#47702](https://github.com/StarRocks/starrocks/pull/47702)
- Stream Load がロード中に JSON 文字列を STRUCT/MAP/ARRAY 型に自動変換できるようになりました。 [#45406](https://github.com/StarRocks/starrocks/pull/45406)
- JDBC Catalog が Oracle と SQL Server をサポートしました。 [#35691](https://github.com/StarRocks/starrocks/issues/35691)

### 改善点

- `user_admin` ロールのユーザーが root ユーザーのパスワードをリセットすることを制限することで、特権管理を改善しました。 [#47801](https://github.com/StarRocks/starrocks/pull/47801)
- Stream Load が行と列の区切り文字として `\t` と `\n` を使用することをサポートしました。ユーザーはこれらを16進数の ASCII コードに変換する必要がなくなりました。 [#47302](https://github.com/StarRocks/starrocks/pull/47302)
- データロード中のメモリ使用量を最適化しました。 [#47047](https://github.com/StarRocks/starrocks/pull/47047)
- Files() 関数の監査ログで認証情報のマスキングをサポートしました。 [#46893](https://github.com/StarRocks/starrocks/pull/46893)
- Hive テーブルが `skip.header.line.count` プロパティをサポートしました。 [#47001](https://github.com/StarRocks/starrocks/pull/47001)
- JDBC Catalog がより多くのデータ型をサポートしました。 [#47618](https://github.com/StarRocks/starrocks/pull/47618)

### 動作の変更

- `JAVA_OPTS` パラメータの値継承順序を変更しました。JDK_9 または JDK_11 以外のバージョンを使用する場合、ユーザーは `JAVA_OPTS` を直接設定する必要があります。 [#47495](https://github.com/StarRocks/starrocks/pull/47495)
- ユーザーがバケット数を指定せずに非パーティション化テーブルを作成する場合、システムがテーブルに設定する最小バケット数は `16` です（以前は `2*BE または CN カウント` に基づいていました）。小さなテーブルを作成する際に小さいバケット数を設定したい場合、ユーザーは明示的に設定する必要があります。 [#47005](https://github.com/StarRocks/starrocks/pull/47005)
- ユーザーがバケット数を指定せずにパーティション化テーブルを作成する場合、パーティション数が5を超えると、バケット数の設定ルールは `max(2*BE または CN カウント, 最大の履歴パーティションデータ量に基づいて計算されたバケット数)` に変更されます。以前のルールは最大の履歴パーティションデータ量に基づいてバケット数を計算するものでした。 [#47949](https://github.com/StarRocks/starrocks/pull/47949)

### バグ修正

以下の問題を修正しました:

- v3.2.x から v3.3.0 への共有データクラスタのアップグレード後に ALTER TABLE ADD COLUMN によって BE がクラッシュする問題を修正しました。 [#47826](https://github.com/StarRocks/starrocks/pull/47826)
- SUBMIT TASK を通じて開始されたタスクが QueryDetail インターフェースで無期限に Running ステータスを示す問題を修正しました。 [#47619](https://github.com/StarRocks/starrocks/pull/47619)
- FE Leader ノードへのクエリ転送がヌルポインタ例外を引き起こす問題を修正しました。 [#47559](https://github.com/StarRocks/starrocks/pull/47559)
- WHERE 条件を持つ SHOW MATERIALIZED VIEWS がヌルポインタ例外を引き起こす問題を修正しました。 [#47811](https://github.com/StarRocks/starrocks/pull/47811)
- 共有データクラスタの主キーテーブルでの垂直コンパクションが失敗する問題を修正しました。 [#47192](https://github.com/StarRocks/starrocks/pull/47192)
- Hive または Iceberg テーブルへのデータシンク時の I/O エラーの不適切な処理を修正しました。 [#46979](https://github.com/StarRocks/starrocks/pull/46979)
- テーブルプロパティに値の空白が追加されると効果がない問題を修正しました。 [#47119](https://github.com/StarRocks/starrocks/pull/47119)
- 主キーテーブルでの同時移行とインデックスコンパクション操作によって BE がクラッシュする問題を修正しました。 [#46675](https://github.com/StarRocks/starrocks/pull/46675)

## 3.2.8

リリース日: 2024年6月7日

### 新機能

- **[BE にラベルを追加するサポート](https://docs.starrocks.io/docs/3.2/administration/management/resource_management/be_label/)**: BE が配置されているラックやデータセンターなどの情報に基づいて BE にラベルを追加することをサポートしました。これにより、ラックやデータセンター間でのデータの均等な分散が保証され、特定のラックの停電やデータセンターの障害時に災害復旧が容易になります。 [#38833](https://github.com/StarRocks/starrocks/pull/38833)

### バグ修正

以下の問題を修正しました:

- str2date を使用した式に基づくパーティション化手法を使用するテーブルからデータ行を削除する際にエラーが返される問題を修正しました。 [#45939](https://github.com/StarRocks/starrocks/pull/45939)
- StarRocks クロスクラスタデータ移行ツールがソースクラスタからスキーマ情報を取得できない場合、宛先クラスタの BE がクラッシュする問題を修正しました。 [#46068](https://github.com/StarRocks/starrocks/pull/46068)
- 非決定的関数を持つクエリに対して `Multiple entries with same key` エラーが返される問題を修正しました。 [#46602](https://github.com/StarRocks/starrocks/pull/46602)

## 3.2.7

リリース日: 2024年5月24日

### 新機能

- Stream Load が伝送中のデータ圧縮をサポートし、ネットワーク帯域幅のオーバーヘッドを削減します。ユーザーは `compression` と `Content-Encoding` パラメータを使用して異なる圧縮アルゴリズムを指定できます。サポートされている圧縮アルゴリズムには GZIP、BZIP2、LZ4_FRAME、ZSTD があります。 [#43732](https://github.com/StarRocks/starrocks/pull/43732)
- 共有データクラスタでのガーベジコレクション (GC) メカニズムを最適化しました。オブジェクトストレージに保存されたテーブルまたはパーティションの手動コンパクションをサポートします。 [#39532](https://github.com/StarRocks/starrocks/issues/39532)
- Flink コネクタが StarRocks から ARRAY、MAP、STRUCT などの複雑なデータ型を読み取ることをサポートしました。 [#42932](https://github.com/StarRocks/starrocks/pull/42932) [#347](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/347)
- クエリ中にデータキャッシュを非同期でポピュレートすることをサポートし、キャッシュのポピュレーションがクエリパフォーマンスに与える影響を軽減します。 [#40489](https://github.com/StarRocks/starrocks/pull/40489)
- ANALYZE TABLE が外部テーブルのヒストグラムを収集することをサポートし、データスキューを効果的に解決します。詳細については、[CBO 統計](https://docs.starrocks.io/docs/3.2/using_starrocks/Cost_based_optimizer/#collect-statistics-of-hiveiceberghudi-tables) を参照してください。 [#42693](https://github.com/StarRocks/starrocks/pull/42693)
- [UNNEST](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/array-functions/unnest/) を使用した Lateral Join が LEFT JOIN をサポートしました。 [#43973](https://github.com/StarRocks/starrocks/pull/43973)
- クエリプールがメモリ使用量のしきい値を設定することをサポートし、BE 静的パラメータ `query_pool_spill_mem_limit_threshold` を介してスピリングをトリガーします。しきい値に達すると、クエリの中間結果がディスクにスピルされ、メモリ使用量を削減し、OOM を回避します。
- Hive ビューに基づく非同期マテリアライズドビューの作成をサポートしました。

### 改善点

- 指定された HDFS パスの下にデータがない場合の Broker Load タスクのエラーメッセージを最適化しました。 [#43839](https://github.com/StarRocks/starrocks/pull/43839)
- AWS S3 から Access Key と Secret Key を指定せずにデータを読み取る際の Files 関数のエラーメッセージを最適化しました。 [#42450](https://github.com/StarRocks/starrocks/pull/42450)
- データを任意のパーティションにロードしない Broker Load タスクのエラーメッセージを最適化しました。 [#44292](https://github.com/StarRocks/starrocks/pull/44292)
- SELECT ステートメントのカラム数が宛先テーブルのカラム数と一致しない場合の INSERT INTO SELECT タスクのエラーメッセージを最適化しました。 [#44331](https://github.com/StarRocks/starrocks/pull/44331)

### バグ修正

以下の問題を修正しました:

- BITMAP 型データの同時読み取りまたは書き込みが BE をクラッシュさせる可能性がある問題を修正しました。 [#44167](https://github.com/StarRocks/starrocks/pull/44167)
- 主キーインデックスが BE をクラッシュさせる可能性がある問題を修正しました。 [#43793](https://github.com/StarRocks/starrocks/pull/43793) [#43569](https://github.com/StarRocks/starrocks/pull/43569) [#44034](https://github.com/StarRocks/starrocks/pull/44034)
- 高いクエリ同時実行シナリオで、str_to_map 関数が BE をクラッシュさせる可能性がある問題を修正しました。 [#43901](https://github.com/StarRocks/starrocks/pull/43901)
- Apache Ranger のマスキングポリシーが使用されている場合、クエリでテーブルエイリアスが指定されるとエラーが返される問題を修正しました。 [#44445](https://github.com/StarRocks/starrocks/pull/44445)
- 共有データクラスタで、現在のノードが例外に遭遇した場合、クエリ実行がバックアップノードにルーティングされない問題を修正しました。この問題に対する対応として、対応するエラーメッセージが最適化されました。 [#43489](https://github.com/StarRocks/starrocks/pull/43489)
- コンテナ環境でメモリ情報が正しくない問題を修正しました。 [#43225](https://github.com/StarRocks/starrocks/issues/43225)
- INSERT タスクがキャンセルされると例外がスローされる問題を修正しました。 [#44239](https://github.com/StarRocks/starrocks/pull/44239)
- 式に基づく動的パーティションが自動的に作成されない問題を修正しました。 [#44163](https://github.com/StarRocks/starrocks/pull/44163)
- パーティションの作成が FE デッドロックを引き起こす可能性がある問題を修正しました。 [#44974](https://github.com/StarRocks/starrocks/pull/44974)

## 3.2.6

リリース日: 2024年4月18日

### バグ修正

以下の問題を修正しました:

- 外部テーブルの特権が互換性の問題のために見つからない問題を修正しました。 [#44030](https://github.com/StarRocks/starrocks/pull/44030)

## 3.2.5 (Yanked)

リリース日: 2024年4月12日

:::tip

このバージョンは、Hive や Iceberg などの外部カタログの外部テーブルをクエリする際の特権問題のためにオフラインにされました。

- **問題**: ユーザーが外部カタログの外部テーブルからデータをクエリする際、ユーザーがこのテーブルに対する SELECT 特権を持っている場合でもアクセスが拒否されます。SHOW GRANTS もユーザーがこの特権を持っていることを示します。

- **影響範囲**: この問題は外部カタログの外部テーブルに対するクエリにのみ影響します。他のクエリには影響しません。

- **一時的な回避策**: このテーブルに対する SELECT 特権が再度ユーザーに付与されるとクエリが成功します。ただし、`SHOW GRANTS` は重複する特権エントリを返します。v3.2.6 にアップグレードした後、ユーザーは `REVOKE` を実行して特権エントリの1つを削除できます。

:::

### 新機能

- [dict_mapping](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/dict-functions/dict_mapping/) カラムプロパティをサポートしました。これにより、グローバル辞書の構築中のロードプロセスが大幅に容易になり、正確な COUNT DISTINCT 計算が加速されます。

### 動作の変更

- JSON データの null 値が `IS NULL` 演算子に基づいて評価される場合、それらは SQL 言語に従って NULL 値と見なされます。たとえば、`SELECT parse_json('{"a": null}') -> 'a' IS NULL` に対して `true` が返されます（この動作変更前は `false` が返されていました）。 [#42765](https://github.com/StarRocks/starrocks/pull/42765)

### 改善点

- FILES テーブル関数の自動スキーマ検出におけるカラム型の統合ルールを最適化しました。異なるファイルに同じ名前のカラムが異なる型で存在する場合、FILES はそれらをマージし、最終的な型としてより大きな粒度の型を選択しようとします。たとえば、同じ名前のカラムが FLOAT 型と INT 型である場合、FILES は最終的な型として DOUBLE を返します。 [#40959](https://github.com/StarRocks/starrocks/pull/40959)
- 主キーテーブルがサイズ階層型コンパクションをサポートし、I/O 増幅を削減します。 [#41130](https://github.com/StarRocks/starrocks/pull/41130)
- Broker Load を使用して ORC ファイルから TIMESTAMP 型データをロードする際、StarRocks はタイムスタンプを自社の DATETIME データ型に一致させる際にマイクロ秒を保持することをサポートします。 [#42179](https://github.com/StarRocks/starrocks/pull/42179)
- Routine Load のエラーメッセージを最適化しました。 [#41306](https://github.com/StarRocks/starrocks/pull/41306)
- FILES テーブル関数を使用して無効なデータ型を変換する際のエラーメッセージを最適化しました。 [#42717](https://github.com/StarRocks/starrocks/pull/42717)

### バグ修正

以下の問題を修正しました:

- システム定義ビューが削除された後に FEs が起動しない問題を修正しました。システム定義ビューの削除は禁止されました。 [#43552](https://github.com/StarRocks/starrocks/pull/43552)
- 主キーテーブルに重複したソートキー列が存在する場合に BEs がクラッシュする問題を修正しました。重複したソートキー列は禁止されました。 [#43206](https://github.com/StarRocks/starrocks/pull/43206)
- to_json() 関数の入力値が NULL の場合にエラーが返される問題を修正しました。 [#42171](https://github.com/StarRocks/starrocks/pull/42171)
- 共有データモードで、主キーテーブルに作成された永続インデックスを処理するためのガーベジコレクションとスレッドエビクションメカニズムが CN ノードで効果を発揮しない問題を修正しました。その結果、古いデータが削除されません。 [#41955](https://github.com/StarRocks/starrocks/pull/41955)
- 共有データモードで、主キーテーブルの `enable_persistent_index` プロパティを変更するとエラーが返される問題を修正しました。 [#42890](https://github.com/StarRocks/starrocks/pull/42890)
- 共有データモードで、カラムモードでの部分更新で主キーテーブルを更新する際に変更されるべきでないカラムに NULL 値が与えられる問題を修正しました。 [#42355](https://github.com/StarRocks/starrocks/pull/42355)
- 非同期マテリアライズドビューが論理ビューに基づいて作成された場合、クエリが書き換えられない問題を修正しました。 [#42173](https://github.com/StarRocks/starrocks/pull/42173)
- クロスクラスタデータ移行ツールを使用して主キーテーブルを共有データクラスタに移行する際に CNs がクラッシュする問題を修正しました。 [#42260](https://github.com/StarRocks/starrocks/pull/42260)
- 外部カタログベースの非同期マテリアライズドビューのパーティション範囲が連続していない問題を修正しました。 [#41957](https://github.com/StarRocks/starrocks/pull/41957)

## 3.2.4 (Yanked)

リリース日: 2024年3月12日

:::tip

このバージョンは、Hive や Iceberg などの外部カタログの外部テーブルをクエリする際の特権問題のためにオフラインにされました。

- **問題**: ユーザーが外部カタログの外部テーブルからデータをクエリする際、ユーザーがこのテーブルに対する SELECT 特権を持っている場合でもアクセスが拒否されます。SHOW GRANTS もユーザーがこの特権を持っていることを示します。

- **影響範囲**: この問題は外部カタログの外部テーブルに対するクエリにのみ影響します。他のクエリには影響しません。

- **一時的な回避策**: このテーブルに対する SELECT 特権が再度ユーザーに付与されるとクエリが成功します。ただし、`SHOW GRANTS` は重複する特権エントリを返します。v3.2.6 にアップグレードした後、ユーザーは `REVOKE` を実行して特権エントリの1つを削除できます。

:::

### 新機能

- 共有データクラスタのクラウドネイティブ主キーテーブルがサイズ階層型コンパクションをサポートし、書き込み I/O 増幅を削減します。 [#41034](https://github.com/StarRocks/starrocks/pull/41034)
- 日付関数 `milliseconds_diff` を追加しました。 [#38171](https://github.com/StarRocks/starrocks/pull/38171)
- セッション変数 `catalog` を追加しました。この変数はセッションが属するカタログを指定します。 [#41329](https://github.com/StarRocks/starrocks/pull/41329)
- [ヒントでのユーザー定義変数の設定](https://docs.starrocks.io/docs/3.2/administration/Query_planning/#user-defined-variable-hint)をサポートしました。 [#40746](https://github.com/StarRocks/starrocks/pull/40746)
- Hive カタログでの CREATE TABLE LIKE をサポートしました。 [#37685](https://github.com/StarRocks/starrocks/pull/37685)
- パーティションの詳細なメタデータを記録するビュー `information_schema.partitions_meta` を追加しました。 [#39265](https://github.com/StarRocks/starrocks/pull/39265)
- StarRocks のメモリ使用量を記録するビュー `sys.fe_memory_usage` を追加しました。 [#40464](https://github.com/StarRocks/starrocks/pull/40464)

### 動作の変更

- `cbo_decimal_cast_string_strict` は CBO が DECIMAL 型から STRING 型へのデータ変換をどのように行うかを制御します。デフォルト値 `true` は v2.5.x 以降のバージョンで組み込まれたロジックが優先され、システムが厳密な変換を実施することを示します（つまり、システムは生成された文字列を切り捨て、スケール長に基づいて 0 を埋めます）。以前のバージョンでは DECIMAL 型は厳密に埋められておらず、DECIMAL 型と STRING 型を比較する際に異なる結果を引き起こす可能性があります。 [#40619](https://github.com/StarRocks/starrocks/pull/40619)
- Iceberg Catalog パラメータ `enable_iceberg_metadata_cache` のデフォルト値が `false` に変更されました。v3.2.1 から v3.2.3 までは、このパラメータは使用されるメタストアサービスに関係なくデフォルトで `true` に設定されていました。v3.2.4 以降では、Iceberg クラスタが AWS Glue をメタストアとして使用する場合、このパラメータはデフォルトで `true` になります。ただし、Iceberg クラスタが他のメタストアサービス（Hive メタストアなど）を使用する場合、このパラメータはデフォルトで `false` になります。 [#41826](https://github.com/StarRocks/starrocks/pull/41826)
- マテリアライズドビューをリフレッシュできるユーザーが `root` ユーザーからマテリアライズドビューを作成したユーザーに変更されました。この変更は既存のマテリアライズドビューには影響しません。 [#40670](https://github.com/StarRocks/starrocks/pull/40670)
- デフォルトでは、定数型と文字列型の列を比較する際、StarRocks はそれらを文字列として比較します。ユーザーはセッション変数 `cbo_eq_base_type` を使用して比較に使用するルールを調整できます。たとえば、ユーザーが `cbo_eq_base_type` を `decimal` に設定すると、StarRocks は列を数値として比較します。 [#40619](https://github.com/StarRocks/starrocks/pull/40619)

### 改善点

- 共有データ StarRocks クラスタが S3 互換オブジェクトストレージシステムのパーティション化されたプレフィックス機能をサポートしました。この機能が有効になると、StarRocks はデータをバケットの下の複数の均一にプレフィックスされたパーティション（サブパス）に保存します。これにより、S3 互換オブジェクトストレージでのデータファイルの読み取りおよび書き込み効率が向上します。 [#41627](https://github.com/StarRocks/starrocks/pull/41627)
- StarRocks はパラメータ `s3_compatible_fs_list` を使用して AWS SDK を介してアクセスできる S3 互換オブジェクトストレージを指定し、`fallback_to_hadoop_fs_list` パラメータを使用して HDFS スキーマを介してアクセスが必要な非 S3 互換オブジェクトストレージを指定することをサポートしました（この方法にはベンダー提供の JAR パッケージの使用が必要です）。 [#41123](https://github.com/StarRocks/starrocks/pull/41123)
- Trino との互換性を最適化しました。次の Trino 関数からの構文変換をサポートします: current_catalog, current_schema, to_char, from_hex, to_date, to_timestamp, および index。 [#41217](https://github.com/StarRocks/starrocks/pull/41217) [#41319](https://github.com/StarRocks/starrocks/pull/41319) [#40803](https://github.com/StarRocks/starrocks/pull/40803)
- マテリアライズドビューのクエリ書き換えロジックを最適化しました。StarRocks は論理ビューに基づいて作成されたマテリアライズドビューでクエリを書き換えることができます。 [#42173](https://github.com/StarRocks/starrocks/pull/42173)
- STRING 型から DATETIME 型への変換効率を 35% から 40% 向上させました。 [#41464](https://github.com/StarRocks/starrocks/pull/41464)
- 集計テーブルの BITMAP 型カラムの `agg_type` を `replace_if_not_null` に設定することで、テーブルの一部のカラムのみを更新することをサポートします。 [#42034](https://github.com/StarRocks/starrocks/pull/42034)
- 小さな ORC ファイルをロードする際の Broker Load パフォーマンスを改善しました。 [#41765](https://github.com/StarRocks/starrocks/pull/41765)
- 行と列のハイブリッドストレージを持つテーブルがスキーマ変更をサポートしました。 [#40851](https://github.com/StarRocks/starrocks/pull/40851)
- 行と列のハイブリッドストレージを持つテーブルが BITMAP、HLL、JSON、ARRAY、MAP、STRUCT などの複雑な型をサポートしました。 [#41476](https://github.com/StarRocks/starrocks/pull/41476)
- 統計とマテリアライズドビューに関連するログデータを記録する新しい内部 SQL ログファイルを追加しました。 [#40453](https://github.com/StarRocks/starrocks/pull/40453)

### バグ修正

以下の問題を修正しました:

- Hive ビューの作成時にクエリされたテーブルまたはビューの名前またはエイリアスに一貫性のない大文字小文字が割り当てられると「Analyze Error」がスローされる問題を修正しました。 [#40921](https://github.com/StarRocks/starrocks/pull/40921)
- 主キーテーブルに永続インデックスが作成されると I/O 使用量が上限に達する問題を修正しました。 [#39959](https://github.com/StarRocks/starrocks/pull/39959)
- 共有データクラスタで、主キーインデックスディレクトリが5時間ごとに削除される問題を修正しました。 [#40745](https://github.com/StarRocks/starrocks/pull/40745)
- ユーザーが手動で ALTER TABLE COMPACT を実行した後、コンパクション操作のメモリ使用量統計が異常になる問題を修正しました。 [#41150](https://github.com/StarRocks/starrocks/pull/41150)
- 主キーテーブルの Publish フェーズのリトライがハングする可能性がある問題を修正しました。 [#39890](https://github.com/StarRocks/starrocks/pull/39890)

## 3.2.3

リリース日: 2024年2月8日

### 新機能

- [プレビュー] テーブルの行と列のハイブリッドストレージをサポートしました。これにより、主キーテーブルに対する高並行性、低遅延のポイントルックアップと部分データ更新のパフォーマンスが向上します。現在、この機能は ALTER TABLE を介した変更、ソートキーの変更、カラムモードでの部分更新をサポートしていません。
- 非同期マテリアライズドビューのバックアップとリストアをサポートしました。
- Broker Load が JSON 型データのロードをサポートしました。
- ビューに基づいて作成された非同期マテリアライズドビューを使用したクエリの書き換えをサポートしました。ビューに対するクエリは、そのビューに基づいて作成されたマテリアライズドビューに基づいて書き換えられます。
- CREATE OR REPLACE PIPE をサポートしました。 [#37658](https://github.com/StarRocks/starrocks/pull/37658)

### 動作の変更

- セッション変数 `enable_strict_order_by` を追加しました。この変数がデフォルト値 `TRUE` に設定されている場合、クエリパターンに対してエラーが報告されます: クエリの異なる式で重複したエイリアスが使用され、かつこのエイリアスが ORDER BY のソートフィールドでもある場合、たとえば `select distinct t1.* from tbl1 t1 order by t1.k1;`。このロジックは v2.3 およびそれ以前と同じです。この変数が `FALSE` に設定されている場合、緩やかな重複排除メカニズムが使用され、これらのクエリは有効な SQL クエリとして処理されます。 [#37910](https://github.com/StarRocks/starrocks/pull/37910)
- セッション変数 `enable_materialized_view_for_insert` を追加しました。この変数はマテリアライズドビューが INSERT INTO SELECT ステートメントでクエリを書き換えるかどうかを制御します。デフォルト値は `false` です。 [#37505](https://github.com/StarRocks/starrocks/pull/37505)
- 単一のクエリがパイプラインフレームワーク内で実行される場合、そのメモリ制限は `exec_mem_limit` ではなく変数 `query_mem_limit` によって制約されます。`query_mem_limit` の値を `0` に設定すると制限がないことを示します。 [#34120](https://github.com/StarRocks/starrocks/pull/34120)

### パラメータの変更

- FE の設定項目 `http_worker_threads_num` を追加しました。これは HTTP サーバーが HTTP リクエストを処理するためのスレッド数を指定します。デフォルト値は `0` です。このパラメータの値が負の値または `0` に設定されている場合、実際のスレッド数は CPU コア数の2倍になります。 [#37530](https://github.com/StarRocks/starrocks/pull/37530)
- BE の設定項目 `lake_pk_compaction_max_input_rowsets` を追加しました。これは共有データ StarRocks クラスタ内の主キーテーブルのコンパクションタスクで許可される最大入力行セット数を制御します。これにより、コンパクションタスクのリソース消費が最適化されます。 [#39611](https://github.com/StarRocks/starrocks/pull/39611)
- セッション変数 `connector_sink_compression_codec` を追加しました。これは Hive テーブルまたは Iceberg テーブルにデータを書き込む際、または Files() を使用してデータをエクスポートする際に使用される圧縮アルゴリズムを指定します。有効なアルゴリズムには GZIP、BROTLI、ZSTD、LZ4 があります。 [#37912](https://github.com/StarRocks/starrocks/pull/37912)
- FE の設定項目 `routine_load_unstable_threshold_second` を追加しました。 [#36222](https://github.com/StarRocks/starrocks/pull/36222)
- BE の設定項目 `pindex_major_compaction_limit_per_disk` を追加し、ディスク上のコンパクションの最大同時実行数を設定します。これにより、コンパクションによるディスク間の I/O の不均一性の問題が解決されます。この問題は特定のディスクの I/O が過度に高くなる可能性があります。デフォルト値は `1` です。 [#36681](https://github.com/StarRocks/starrocks/pull/36681)
- BE の設定項目 `enable_lazy_delta_column_compaction` を追加しました。デフォルト値は `true` で、StarRocks がデルタカラムに対して頻繁なコンパクション操作を行わないことを示します。 [#36654](https://github.com/StarRocks/starrocks/pull/36654)
- FE の設定項目 `default_mv_refresh_immediate` を追加しました。これはマテリアライズドビューが作成された後にすぐにリフレッシュされるかどうかを指定します。デフォルト値は `true` です。 [#37093](https://github.com/StarRocks/starrocks/pull/37093)
- FE の設定項目 `default_mv_refresh_partition_num` のデフォルト値を `1` に変更しました。これは、マテリアライズドビューのリフレッシュ中に複数のパーティションを更新する必要がある場合、タスクがバッチで分割され、1回のリフレッシュで1つのパーティションのみが更新されることを示します。これにより、各リフレッシュ中のリソース消費が削減されます。 [#36560](https://github.com/StarRocks/starrocks/pull/36560)
- BE/CN の設定項目 `starlet_use_star_cache` のデフォルト値を `true` に変更しました。これは、共有データクラスタでデータキャッシュがデフォルトで有効になっていることを示します。アップグレード前に BE/CN の設定項目 `starlet_cache_evict_high_water` を `X` に手動で設定していた場合、BE/CN の設定項目 `starlet_star_cache_disk_size_percent` を `(1.0 - X) * 100` に設定する必要があります。たとえば、アップグレード前に `starlet_cache_evict_high_water` を `0.3` に設定していた場合、`starlet_star_cache_disk_size_percent` を `70` に設定する必要があります。これにより、ファイルデータキャッシュとデータキャッシュの両方がディスク容量制限を超えないようにします。 [#38200](https://github.com/StarRocks/starrocks/pull/38200)

### 改善点

- Apache Iceberg テーブルの TIMESTAMP パーティションフィールドをサポートするために日付フォーマット `yyyy-MM-ddTHH:mm` と `yyyy-MM-dd HH:mm` を追加しました。 [#39986](https://github.com/StarRocks/starrocks/pull/39986)
- 監視 API にデータキャッシュ関連のメトリクスを追加しました。 [#40375](https://github.com/StarRocks/starrocks/pull/40375)
- BE ログの出力を最適化し、不要なログが多すぎないようにしました。 [#22820](https://github.com/StarRocks/starrocks/pull/22820) [#36187](https://github.com/StarRocks/starrocks/pull/36187)
- ビュー `information_schema.be_tablets` にフィールド `storage_medium` を追加しました。 [#37070](https://github.com/StarRocks/starrocks/pull/37070)
- 複数のサブクエリで `SET_VAR` をサポートしました。 [#36871](https://github.com/StarRocks/starrocks/pull/36871)
- Kafka トピックの各パーティションにおける最新メッセージの位置を記録するために、SHOW ROUTINE LOAD の返り値に新しいフィールド `LatestSourcePosition` を追加しました。これにより、データロードの遅延を確認するのに役立ちます。 [#38298](https://github.com/StarRocks/starrocks/pull/38298)
- WHERE 句内の LIKE 演算子の右側の文字列に `%` または `_` が含まれていない場合、LIKE 演算子が `=` 演算子に変換されます。 [#37515](https://github.com/StarRocks/starrocks/pull/37515)
- ゴミファイルのデフォルトの保持期間を元の3日から1日に変更しました。 [#37113](https://github.com/StarRocks/starrocks/pull/37113)
- Iceberg テーブルのパーティション変換を使用した統計収集をサポートしました。 [#39907](https://github.com/StarRocks/starrocks/pull/39907)
- Routine Load のスケジューリングポリシーを最適化し、遅いタスクが他の通常のタスクの実行を妨げないようにしました。 [#37638](https://github.com/StarRocks/starrocks/pull/37638)

### バグ修正

以下の問題を修正しました:

- ANALYZE TABLE の実行が時折スタックする問題を修正しました。 [#36836](https://github.com/StarRocks/starrocks/pull/36836)
- 特定の状況で、PageCache のメモリ消費が BE 動的パラメータ `storage_page_cache_limit` によって指定されたしきい値を超える問題を修正しました。 [#37740](https://github.com/StarRocks/starrocks/pull/37740)
- Hive カタログの Hive メタデータが Hive テーブルに新しいフィールドが追加されたときに自動的に更新されない問題を修正しました。 [#37549](https://github.com/StarRocks/starrocks/pull/37549)
- 一部のケースで、`bitmap_to_string` がデータ型のオーバーフローにより不正確な結果を返す可能性がある問題を修正しました。 [#37405](https://github.com/StarRocks/starrocks/pull/37405)
- `SELECT ... FROM ... INTO OUTFILE` を実行してデータを CSV ファイルにエクスポートする際、FROM 句に複数の定数が含まれていると「Unmatched number of columns」エラーが報告される問題を修正しました。 [#38045](https://github.com/StarRocks/starrocks/pull/38045)
- 一部のケースで、テーブル内の半構造化データをクエリすると BEs がクラッシュする問題を修正しました。 [#40208](https://github.com/StarRocks/starrocks/pull/40208)

## 3.2.2

リリース日: 2023年12月30日

### バグ修正

以下の問題を修正しました:

- StarRocks を v3.1.2 以前から v3.2 にアップグレードする際、FEs が再起動に失敗する問題を修正しました。 [#38172](https://github.com/StarRocks/starrocks/pull/38172)

## 3.2.1

リリース日: 2023年12月21日

### 新機能

#### データレイク分析

- Java Native Interface (JNI) を通じて [Hive Catalog](https://docs.starrocks.io/docs/3.2/data_source/catalog/hive_catalog/) テーブルと Avro、SequenceFile、RCFile 形式のファイル外部テーブルを読み取ることをサポートしました。

#### マテリアライズドビュー

- データベース `sys` にビュー `object_dependencies` を追加しました。これは非同期マテリアライズドビューの系統情報を含みます。 [#35060](https://github.com/StarRocks/starrocks/pull/35060)
- WHERE 句を使用した同期マテリアライズドビューの作成をサポートしました。
- Iceberg カタログに基づいて作成された非同期マテリアライズドビューのパーティションレベルの増分リフレッシュをサポートしました。
- [プレビュー] Paimon カタログのテーブルに基づいて作成された非同期マテリアライズドビューのパーティションレベルのリフレッシュをサポートしました。

#### クエリと SQL 関数

- プリペアドステートメントをサポートしました。これにより、高並行性ポイントルックアップクエリの処理パフォーマンスが向上し、SQL インジェクションを効果的に防止します。
- 次の Bitmap 関数をサポートしました: [subdivide_bitmap](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/)、[bitmap_from_binary](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/bitmap-functions/bitmap_from_binary/)、および [bitmap_to_binary](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/bitmap-functions/bitmap_to_binary/)。
- Array 関数 [array_unique_agg](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/array-functions/array_unique_agg/) をサポートしました。

#### 監視とアラート

- 最大許可行セット数を設定するための新しいメトリクス `max_tablet_rowset_num` を追加しました。このメトリクスは、コンパクションの問題を検出し、「バージョンが多すぎる」というエラーの発生を減らすのに役立ちます。 [#36539](https://github.com/StarRocks/starrocks/pull/36539)

### パラメータの変更

- 新しい BE 設定項目 `enable_stream_load_verbose_log` が追加されました。デフォルト値は `false` です。このパラメータが `true` に設定されている場合、StarRocks は Stream Load ジョブの HTTP リクエストとレスポンスを記録し、トラブルシューティングを容易にします。 [#36113](https://github.com/StarRocks/starrocks/pull/36113)

### 改善点

- JDK8 のデフォルト GC アルゴリズムを G1 にアップグレードしました。 [#37268](https://github.com/StarRocks/starrocks/pull/37268)
- セッション変数 [sql_mode](https://docs.starrocks.io/docs/3.2/reference/System_variable/#sql_mode) に新しい値オプション `GROUP_CONCAT_LEGACY` を追加し、v2.5 より前のバージョンでの [group_concat](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/string-functions/group_concat/) 関数の実装ロジックとの互換性を提供します。 [#36150](https://github.com/StarRocks/starrocks/pull/36150)
- [AWS S3 の Broker Load ジョブ](https://docs.starrocks.io/docs/3.2/loading/s3/) の認証情報 `aws.s3.access_key` と `aws.s3.access_secret` を監査ログで非表示にしました。 [#36571](https://github.com/StarRocks/starrocks/pull/36571)
- `information_schema` データベースの `be_tablets` ビューに新しいフィールド `INDEX_DISK` を追加しました。これは永続インデックスのディスク使用量（バイト単位）を記録します。 [#35615](https://github.com/StarRocks/starrocks/pull/35615)
- [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/3.2/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) ステートメントが返す結果に新しいフィールド `OtherMsg` を追加しました。これは最後に失敗したタスクに関する情報を示します。 [#35806](https://github.com/StarRocks/starrocks/pull/35806)

### バグ修正

以下の問題を修正しました:

- データ破損が発生した場合にユーザーが永続インデックスを作成すると BEs がクラッシュする問題を修正しました。[#30841](https://github.com/StarRocks/starrocks/pull/30841)
- [array_distinct](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/array-functions/array_distinct/) 関数が時折 BEs をクラッシュさせる問題を修正しました。 [#36377](https://github.com/StarRocks/starrocks/pull/36377)
- DISTINCT ウィンドウオペレータプッシュダウン機能が有効になっている場合、ウィンドウ関数によって計算されたカラムの複雑な式に対して SELECT DISTINCT 操作を実行するとエラーが報告される問題を修正しました。 [#36357](https://github.com/StarRocks/starrocks/pull/36357)
- 一部の S3 互換オブジェクトストレージが重複したファイルを返し、BEs がクラッシュする問題を修正しました。 [#36103](https://github.com/StarRocks/starrocks/pull/36103)

## 3.2.0

リリース日: 2023年12月1日

### 新機能

#### 共有データクラスタ

- [主キーテーブル](https://docs.starrocks.io/docs/3.2/table_design/table_types/primary_key_table/) のインデックスをローカルディスクに永続化することをサポートしました。
- 複数のローカルディスク間でのデータキャッシュの均等な分散をサポートしました。

#### マテリアライズドビュー

**非同期マテリアライズドビュー**

- クエリダンプファイルに非同期マテリアライズドビューの情報を含めることができます。
- 非同期マテリアライズドビューのリフレッシュタスクに対してディスクへのスピル機能がデフォルトで有効になっており、メモリ消費を削減します。

#### データレイク分析

- [Hive カタログ](https://docs.starrocks.io/docs/3.2/data_source/catalog/hive_catalog/) でのデータベースと管理テーブルの作成および削除をサポートし、INSERT または INSERT OVERWRITE を使用して Hive の管理テーブルにデータをエクスポートすることをサポートしました。
- [統一カタログ](https://docs.starrocks.io/docs/3.2/data_source/catalog/unified_catalog/) をサポートし、ユーザーが共通のメタストア（Hive メタストアや AWS Glue など）を共有する異なるテーブル形式（Hive、Iceberg、Hudi、Delta Lake）にアクセスできるようにします。
- ANALYZE TABLE を使用して Hive および Iceberg テーブルの統計を収集し、StarRocks に統計を保存することをサポートしました。これにより、クエリプランの最適化が容易になり、後続のクエリが高速化されます。
- 外部テーブルの Information Schema をサポートし、外部システム（BI ツールなど）と StarRocks 間のやり取りをより便利にします。

#### ストレージエンジン、データ取り込み、およびエクスポート

- テーブル関数 [FILES()](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/table-functions/files/) を使用したロードの次の機能を追加しました:
  - Azure または GCP からの Parquet および ORC 形式データのロード。
  - パラメータ `columns_from_path` を使用して、ファイルパスからキー/値ペアの値を抽出し、カラムの値として使用。
  - ARRAY、JSON、MAP、STRUCT などの複雑なデータ型のロード。
- StarRocks から AWS S3 または HDFS に保存された Parquet 形式のファイルにデータをアンロードすることをサポートしました。詳細な手順については、[INSERT INTO FILES を使用したデータのアンロード](https://docs.starrocks.io/docs/3.2/unloading/unload_using_insert_into_files/)を参照してください。
- [既存のテーブルで使用されるテーブル構造とデータ分散戦略の手動最適化](https://docs.starrocks.io/docs/3.2/table_design/Data_distribution#optimize-data-distribution-after-table-creation-since-32)をサポートし、クエリとロードのパフォーマンスを最適化します。テーブルに新しいバケットキー、バケット数、またはソートキーを設定できます。また、特定のパーティションに異なるバケット数を設定することもできます。
- [AWS S3](https://docs.starrocks.io/docs/3.2/loading/s3/#use-pipe) または [HDFS](https://docs.starrocks.io/docs/3.2/loading/hdfs_load/#use-pipe) を使用した PIPE メソッドによる継続的なデータロードをサポートしました。
  - PIPE はリモートストレージディレクトリの新規または変更を検出すると、StarRocks の宛先テーブルに新規または変更されたデータを自動的にロードします。データをロードする際、PIPE は大きなロードタスクを小さなシリアル化されたタスクに自動的に分割し、大規模なデータ取り込みシナリオでの安定性を向上させ、エラーリトライのコストを削減します。

#### クエリ

- [HTTP SQL API](https://docs.starrocks.io/docs/3.2/reference/HTTP_API/SQL/) をサポートし、ユーザーが HTTP 経由で StarRocks データにアクセスし、SELECT、SHOW、EXPLAIN、または KILL 操作を実行できるようにします。
- Runtime Profile とテキストベースの Profile 分析コマンド (SHOW PROFILELIST、ANALYZE PROFILE、EXPLAIN ANALYZE) をサポートし、ユーザーが MySQL クライアントを介して直接プロファイルを分析できるようにし、ボトルネックの特定と最適化の機会を発見するのに役立ちます。

#### SQL リファレンス

次の関数を追加しました:

- 文字列関数: substring_index、url_extract_parameter、url_encode、url_decode、translate
- 日付関数: dayofweek_iso、week_iso、quarters_add、quarters_sub、milliseconds_add、milliseconds_sub、date_diff、jodatime_format、str_to_jodatime、to_iso8601、to_tera_date、to_tera_timestamp
- パターンマッチング関数: regexp_extract_all
- ハッシュ関数: xx_hash3_64
- 集計関数: approx_top_k
- ウィンドウ関数: cume_dist、percent_rank、session_number
- ユーティリティ関数: get_query_profile、is_role_in_session

#### 特権とセキュリティ

StarRocks は [Apache Ranger](https://docs.starrocks.io/docs/3.2/administration/ranger_plugin/) を通じてアクセス制御をサポートし、より高いレベルのデータセキュリティを提供し、外部データソースの既存のサービスを再利用できるようにします。Apache Ranger と統合することで、StarRocks は次のアクセス制御方法を有効にします:

- StarRocks 内の内部テーブル、外部テーブル、またはその他のオブジェクトにアクセスする際、Ranger で StarRocks サービスのために設定されたアクセスポリシーに基づいてアクセス制御を実施できます。
- 外部カタログにアクセスする際、元のデータソース（Hive サービスなど）の対応する Ranger サービスを利用してアクセスを制御することもできます（現在、Hive へのデータエクスポートのアクセス制御はサポートされていません）。

詳細については、[Apache Ranger を使用した権限管理](https://docs.starrocks.io/docs/3.2/administration/ranger_plugin/)を参照してください。

### 改善点

#### データレイク分析

- ORC リーダーを最適化しました:
  - ORC カラムリーダーを最適化し、VARCHAR および CHAR データ読み取りのパフォーマンスをほぼ2倍に向上させました。
  - Zlib 圧縮形式の ORC ファイルの解凍パフォーマンスを最適化しました。
- Parquet リーダーを最適化しました:
  - 適応型 I/O マージをサポートし、フィルタリング効果に基づいて述語のあるカラムとないカラムを適応的にマージし、I/O を削減します。
  - より高速な述語書き換えのための Dict フィルタを最適化しました。STRUCT サブカラムをサポートし、オンデマンドで辞書カラムをデコードします。
  - Dict デコードのパフォーマンスを最適化しました。
  - 後期実体化のパフォーマンスを最適化しました。
  - ファイルフッターのキャッシュをサポートし、繰り返しの計算オーバーヘッドを回避します。
  - lzo 圧縮形式の Parquet ファイルの解凍をサポートします。
- CSV リーダーを最適化しました:
  - リーダーのパフォーマンスを最適化しました。
  - Snappy および lzo 圧縮形式の CSV ファイルの解凍をサポートします。
- カウント計算のパフォーマンスを最適化しました。
- Iceberg カタログの機能を最適化しました:
  - クエリを高速化するために Manifest ファイルからカラム統計を収集することをサポートします。
  - クエリを高速化するために Puffin ファイルから NDV（異なる値の数）を収集することをサポートします。
  - パーティションプルーニングをサポートします。
  - Iceberg メタデータのメモリ消費を削減し、大量のメタデータや高いクエリ同時実行シナリオでの安定性を向上させます。

#### マテリアライズドビュー

**非同期マテリアライズドビュー**

- スキーマ変更がビュー、マテリアライズドビュー、またはそのベーステーブルで発生した場合、ビューまたはマテリアライズドビューに基づいて作成された非同期マテリアライズドビューの自動リフレッシュをサポートします。
- データの一貫性:
  - 非同期マテリアライズドビューの作成に対してプロパティ `query_rewrite_consistency` を追加しました。このプロパティは、一貫性チェックに基づくクエリ書き換えルールを定義します。
  - 外部カタログに基づいて作成された非同期マテリアライズドビューのクエリ書き換えを強制するかどうかを定義するプロパティ `force_external_table_query_rewrite` を追加しました。
  - 詳細については、[CREATE MATERIALIZED VIEW](https://docs.starrocks.io/docs/3.2/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/) を参照してください。
- マテリアライズドビューのパーティションキーの一貫性チェックを追加しました。
  - ユーザーが PARTITION BY 式を含むウィンドウ関数を使用して非同期マテリアライズドビューを作成する場合、ウィンドウ関数のパーティション列はマテリアライズドビューのパーティション列と一致する必要があります。

#### ストレージエンジン、データ取り込み、およびエクスポート

- 主キーテーブルの永続インデックスを最適化し、メモリ使用量のロジックを改善し、I/O 読み取りおよび書き込みの増幅を削減しました。 [#24875](https://github.com/StarRocks/starrocks/pull/24875)  [#27577](https://github.com/StarRocks/starrocks/pull/27577)  [#28769](https://github.com/StarRocks/starrocks/pull/28769)
- 主キーテーブルのローカルディスク間でのデータ再配布をサポートしました。
- パーティション化されたテーブルがパーティションの時間範囲とクールダウン時間に基づいて自動クールダウンをサポートしました。元のクールダウンロジックと比較して、パーティションレベルでのホットおよびコールドデータ管理がより便利になりました。詳細については、[初期記憶媒体、自動記憶クールダウン時間、レプリカ数の指定](https://docs.starrocks.io/docs/3.2/sql-reference/sql-statements/data-definition/CREATE_TABLE#specify-initial-storage-medium-automatic-storage-cooldown-time-replica-number) を参照してください。
- 主キーテーブルにデータを書き込むロードジョブの Publish フェーズが非同期モードから同期モードに変更されました。そのため、ロードジョブが終了した直後にロードされたデータをクエリできます。詳細については、[enable_sync_publish](https://docs.starrocks.io/docs/3.2/administration/FE_configuration#enable_sync_publish) を参照してください。
- 高速スキーマ進化をサポートし、テーブルプロパティ [`fast_schema_evolution`](https://docs.starrocks.io/docs/3.2/sql-reference/sql-statements/data-definition/CREATE_TABLE#set-fast-schema-evolution) によって制御されます。この機能が有効になると、カラムの追加または削除の実行効率が大幅に向上します。このモードはデフォルトで無効です（デフォルト値は `false` です）。既存のテーブルに対して ALTER TABLE を使用してこのプロパティを変更することはできません。
- **ランダムバケット法**を使用して作成された**重複キー**テーブルに対して、クラスタ情報とデータのサイズに応じて作成するタブレットの数を動的に調整することをサポートしました。

#### クエリ

- StarRocks の Metabase および Superset との互換性を最適化しました。外部カタログとの統合をサポートします。

#### SQL リファレンス

- [array_agg](https://docs.starrocks.io/docs/3.2/sql-reference/sql-functions/array-functions/array_agg/) がキーワード DISTINCT をサポートしました。
- INSERT、UPDATE、および DELETE 操作が `SET_VAR` をサポートしました。 [#35283](https://github.com/StarRocks/starrocks/pull/35283)

#### その他

- DECIMAL 型のオーバーフローを処理するルールを設定するためにセッション変数 `large_decimal_underlying_type = "panic"|"double"|"decimal"` を追加しました。`panic` は即座にエラーを返すことを示し、`double` はデータを DOUBLE 型に変換することを示し、`decimal` はデータを DECIMAL(38,s) に変換することを示します。

### 開発者ツール

- 非同期マテリアライズドビューのトレースクエリプロファイルをサポートし、その透明な書き換えを分析するために使用できます。

### 動作の変更

更新予定。

### パラメータの変更

#### FE パラメータ

- 次の FE 設定項目を追加しました:
  - `catalog_metadata_cache_size`
  - `enable_backup_materialized_view`
  - `enable_colocate_mv_index`
  - `enable_fast_schema_evolution`
  - `json_file_size_limit`
  - `lake_enable_ingest_slowdown`
  - `lake_ingest_slowdown_threshold`
  - `lake_ingest_slowdown_ratio`
  - `lake_compaction_score_upper_bound`
  - `mv_auto_analyze_async`
  - `primary_key_disk_schedule_time`
  - `statistic_auto_collect_small_table_rows`
  - `stream_load_task_keep_max_num`
  - `stream_load_task_keep_max_second`
- FE 設定項目 `enable_pipeline_load` を削除しました。
- デフォルト値の変更:
  - `enable_sync_publish` のデフォルト値を `false` から `true` に変更しました。
  - `enable_persistent_index_by_default` のデフォルト値を `false` から `true` に変更しました。

#### BE パラメータ

- データキャッシュ関連の設定変更。
  - `block_cache_enable` を置き換えるために `datacache_enable` を追加しました。
  - `block_cache_mem_size` を置き換えるために `datacache_mem_size` を追加しました。
  - `block_cache_disk_size` を置き換えるために `datacache_disk_size` を追加しました。
  - `block_cache_disk_path` を置き換えるために `datacache_disk_path` を追加しました。
  - `block_cache_meta_path` を置き換えるために `datacache_meta_path` を追加しました。
  - `block_cache_block_size` を置き換えるために `datacache_block_size` を追加しました。
  - `block_cache_checksum_enable` を置き換えるために `datacache_checksum_enable` を追加しました。
  - `block_cache_direct_io_enable` を置き換えるために `datacache_direct_io_enable` を追加しました。
  - `block_cache_max_concurrent_inserts` を置き換えるために `datacache_max_concurrent_inserts` を追加しました。
  - `datacache_max_flying_memory_mb` を追加しました。
  - `block_cache_engine` を置き換えるために `datacache_engine` を追加しました。
  - `block_cache_max_parcel_memory_mb` を削除しました。
  - `block_cache_report_stats` を削除しました。
  - `block_cache_lru_insertion_point` を削除しました。

  Block Cache を Data Cache にリネームした後、StarRocks は `datacache` プレフィックスを持つ新しい BE パラメータセットを導入し、元の `block_cache` プレフィックスを持つパラメータを置き換えました。v3.2 にアップグレードした後、元のパラメータは依然として有効です。一度有効にすると、新しいパラメータが元のパラメータを上書きします。新しいパラメータと元のパラメータの混在使用はサポートされておらず、一部の設定が効果を発揮しない可能性があります。将来的に、StarRocks は `block_cache` プレフィックスを持つ元のパラメータを廃止する予定であるため、`datacache` プレフィックスを持つ新しいパラメータを使用することをお勧めします。

- 次の BE 設定項目を追加しました:
  - `spill_max_dir_bytes_ratio`
  - `streaming_agg_limited_memory_size`
  - `streaming_agg_chunk_buffer_size`
- 次の BE 設定項目を削除しました:
  - 動的パラメータ `tc_use_memory_min`
  - 動的パラメータ `tc_free_memory_rate`
  - 動的パラメータ `tc_gc_period`
  - 静的パラメータ `tc_max_total_thread_cache_byte`
- デフォルト値の変更:
  - `disable_column_pool` のデフォルト値を `false` から `true` に変更しました。
  - `thrift_port` のデフォルト値を `9060` から `0` に変更しました。
  - `enable_load_colocate_mv` のデフォルト値を `false` から `true` に変更しました。
  - `enable_pindex_minor_compaction` のデフォルト値を `false` から `true` に変更しました。

#### システム変数

- 次のセッション変数を追加しました:
  - `enable_per_bucket_optimize`
  - `enable_write_hive_external_table`
  - `hive_temp_staging_dir`
  - `spill_revocable_max_bytes`
  - `thrift_plan_protocol`
- 次のセッション変数を削除しました:
  - `enable_pipeline_query_statistic`
  - `enable_deliver_batch_fragments`
- 次のセッション変数をリネームしました:
  - `enable_scan_block_cache` を `enable_scan_datacache` にリネームしました。
  - `enable_populate_block_cache` を `enable_populate_datacache` にリネームしました。

#### 予約キーワード

予約キーワード `OPTIMIZE` と `PREPARE` を追加しました。

### バグ修正

以下の問題を修正しました:

- libcurl が呼び出されたときに BEs がクラッシュする問題を修正しました。 [#31667](https://github.com/StarRocks/starrocks/pull/31667)
- 指定されたタブレットバージョンがガーベジコレクションによって処理されるため、スキーマ変更が過度に長い時間がかかる場合に失敗する問題を修正しました。 [#31376](https://github.com/StarRocks/starrocks/pull/31376)
- ファイル外部テーブルを介して MinIO の Parquet ファイルにアクセスできない問題を修正しました。 [#29873](https://github.com/StarRocks/starrocks/pull/29873)
- `information_schema.columns` で ARRAY、MAP、STRUCT 型のカラムが正しく表示されない問題を修正しました。 [#33431](https://github.com/StarRocks/starrocks/pull/33431)
- Broker Load を介してデータをロードする際に特定のパス形式が使用されると「msg:Fail to parse columnsFromPath, expected: [rec_dt]」というエラーが報告される問題を修正しました。 [#32720](https://github.com/StarRocks/starrocks/pull/32720)
- `information_schema.columns` ビューで BINARY または VARBINARY データ型の `DATA_TYPE` および `COLUMN_TYPE` が `unknown` と表示される問題を修正しました。 [#32678](https://github.com/StarRocks/starrocks/pull/32678)
- 多くのユニオン、式、および SELECT カラムを含む複雑なクエリが FE ノード内の帯域幅または CPU 使用率の急激な増加を引き起こす可能性がある問題を修正しました。
- 非同期マテリアライズドビューのリフレッシュが時折デッドロックに遭遇する問題を修正しました。 [#35736](https://github.com/StarRocks/starrocks/pull/35736)

### アップグレードノート

- **ランダムバケット法**の最適化はデフォルトで無効になっています。これを有効にするには、テーブルを作成する際にプロパティ `bucket_size` を追加する必要があります。これにより、システムはクラスタ情報とロードされたデータのサイズに基づいてタブレットの数を動的に調整できます。この最適化が有効になった場合、クラスタを v3.1 以前にロールバックする必要がある場合は、この最適化が有効になっているテーブルを削除し、メタデータチェックポイントを手動で実行する必要があります（`ALTER SYSTEM CREATE IMAGE` を実行）。そうしないと、ロールバックが失敗します。
- v3.2.0 以降、StarRocks は非パイプラインクエリを無効にしました。そのため、クラスタを v3.2 にアップグレードする前に、パイプラインエンジンをグローバルに有効にする必要があります（FE 設定ファイル **fe.conf** に `enable_pipeline_engine=true` を追加）。これを行わないと、非パイプラインクエリでエラーが発生します。