---
displayed_sidebar: docs
sidebar_label: "Feature Support"
---

# 機能サポート: データレイク分析

バージョン v2.3 以降、StarRocks は外部カタログを介して外部データソースの管理とデータレイク内のデータ分析をサポートしています。

このドキュメントでは、外部カタログの機能サポートと関連する機能のサポートバージョンについて説明します。

## 共通機能

このセクションでは、外部カタログ機能の共通機能をリストアップしています。これには、ストレージシステム、ファイルリーダー、認証情報、権限、および Data Cache が含まれます。

### 外部ストレージシステム

| ストレージシステム         | サポートバージョン |
| :---------------------- | :---------------- |
| HDFS                    | v2.3+             |
| AWS S3                  | v2.3+             |
| Microsoft Azure Storage | v3.0+             |
| Google GCS              | v3.0+             |
| Alibaba Cloud OSS       | v3.1+             |
| Huawei Cloud OBS        | v3.1+             |
| Tencent Cloud COS       | v3.1+             |
| Volcengine TOS          | v3.1+             |
| Kingsoft Cloud KS3      | v3.1+             |
| MinIO                   | v3.1+             |
| Ceph S3                 | v3.1+             |

上記のストレージシステムに対するネイティブサポートに加えて、StarRocks は以下のタイプのオブジェクトストレージサービスもサポートしています。

- **COS Cloud HDFS、OSS-HDFS、OBS PFS などの HDFS 互換オブジェクトストレージサービス**
  - **説明**: BE の設定項目 `fallback_to_hadoop_fs_list` にオブジェクトストレージ URI プレフィックスを指定し、クラウドベンダーが提供する .jar パッケージをディレクトリ **/lib/hadoop/hdfs/** にアップロードする必要があります。`fallback_to_hadoop_fs_list` に指定したプレフィックスを使用して外部カタログを作成する必要があります。
  - **サポートバージョン**: v3.1.9+, v3.2.4+
- **上記以外の S3 互換オブジェクトストレージサービス**
  - **説明**: BE の設定項目 `s3_compatible_fs_list` にオブジェクトストレージ URI プレフィックスを指定する必要があります。`s3_compatible_fs_list` に指定したプレフィックスを使用して外部カタログを作成する必要があります。
  - **サポートバージョン**: v3.1.9+, v3.2.4+

### 圧縮形式

このセクションでは、各ファイル形式でサポートされている圧縮形式のみをリストしています。各外部カタログでサポートされているファイル形式については、対応する外部カタログのセクションを参照してください。

| ファイル形式  | 圧縮形式                                                    |
| :----------- | :----------------------------------------------------------- |
| Parquet      | NO_COMPRESSION, SNAPPY, LZ4, ZSTD, GZIP, LZO (v3.1.5+)       |
| ORC          | NO_COMPRESSION, ZLIB, SNAPPY, LZO, LZ4, ZSTD                 |
| Text         | NO_COMPRESSION, LZO (v3.1.5+)                                |
| Avro         | NO_COMPRESSION (v3.2.1+), DEFLATE (v3.2.1+), SNAPPY (v3.2.1+), BZIP2 (v3.2.1+) |
| RCFile       | NO_COMPRESSION (v3.2.1+), DEFLATE (v3.2.1+), SNAPPY (v3.2.1+), GZIP (v3.2.1+) |
| SequenceFile | NO_COMPRESSION (v3.2.1+), DEFLATE (v3.2.1+), SNAPPY (v3.2.1+), BZIP2 (v3.2.1+), GZIP (v3.2.1+) |

:::note

Avro、RCFile、および SequenceFile のファイル形式は、StarRocks 内のネイティブリーダーではなく、Java Native Interface (JNI) によって読み取られます。そのため、これらのファイル形式の読み取りパフォーマンスは、Parquet や ORC よりも劣る可能性があります。

:::

### 管理、認証情報、およびアクセス制御

| 機能                                     | 説明                                                      | サポートバージョン |
| :--------------------------------------- | :------------------------------------------------------- | :---------------- |
| Information Schema                       | 外部カタログの Information Schema をサポートします。     | v3.2+             |
| データレイクアクセス制御                  | 外部カタログに対して StarRocks のネイティブ RBAC モデルをサポートします。外部カタログ内のデータベース、テーブル、およびビュー（現在は Hive ビューと Iceberge ビューのみ）の権限を、StarRocks のデフォルトカタログと同様に管理できます。 | v3.0+             |
| Apache Ranger 上の外部サービスの再利用   | アクセス制御のために Apache Ranger 上の外部サービス（Hive Service など）の再利用をサポートします。 | v3.1.9+           |
| Kerberos 認証                            | HDFS または Hive Metastore に対する Kerberos 認証をサポートします。 | v2.3+             |

### Data Cache

| 機能                                      | 説明                                                      | サポートバージョン |
| :--------------------------------------- | :------------------------------------------------------- | :---------------- |
| Data Cache (Block Cache)                 | バージョン v2.5 以降、StarRocks は CacheLib を使用して実装された Data Cache 機能（当時は Block Cache と呼ばれていました）をサポートし、その拡張性のための最適化の可能性が限られていました。バージョン v3.0 から、StarRocks はキャッシュの実装をリファクタリングし、Data Cache に新しい機能を追加し、各バージョンでより良いパフォーマンスを実現しました。 | v2.5+             |
| ローカルディスク間のデータ再バランス     | データの偏りが 10% 未満に制御されるようにするデータ再バランス戦略をサポートします。 | v3.2+             |
| Block Cache を Data Cache に置き換える   | **パラメータの変更**<br />BE 設定:<ul><li>`block_cache_enable` を `datacache_enable` に置き換えます。</li><li>`block_cache_mem_size` を `datacache_mem_size` に置き換えます。</li><li>`block_cache_disk_size` を `datacache_disk_size` に置き換えます。</li><li>`block_cache_disk_path` を `datacache_disk_path` に置き換えます。</li><li>`block_cache_meta_path` を `datacache_meta_path` に置き換えます。</li><li>`block_cache_block_size` を `datacache_block_size` に置き換えます。</li></ul>セッション変数:<ul><li>`enable_scan_block_cache` を `enable_scan_datacache` に置き換えます。</li><li>`enable_populate_block_cache` を `enable_populate_datacache` に置き換えます。</li></ul>Data Cache が利用可能なバージョンにクラスターがアップグレードされた後でも、Block Cache パラメータは引き続き有効です。Data Cache が有効になると、新しいパラメータが古いものを上書きします。両方のパラメータグループの混在使用は許可されていません。そうしないと、一部のパラメータが有効になりません。 | v3.2+             |
| Data Cache を監視する API の新しいメトリクス | Data Cache を監視する個別の API をサポートし、キャッシュ容量やヒット数を含むメトリクスを表示できます。Data Cache のメトリクスは、インターフェース `http://${BE_HOST}:${BE_HTTP_PORT}/api/datacache/stat` を介して表示できます。 | v3.2.3+           |
| Data Cache 用のメモリトラッカー          | Data Cache 用のメモリトラッカーをサポートします。メモリ関連のメトリクスは、インターフェース `http://${BE_HOST}:${BE_HTTP_PORT}/mem_tracker` を介して表示できます。 | v3.1.8+           |
| Data Cache ウォームアップ                 | CACHE SELECT を実行することで、リモートストレージから必要なデータを事前にキャッシュに読み込むことができ、最初のクエリがデータを取得するのに時間がかかるのを防ぎます。CACHE SELECT はデータを出力したり計算を行ったりしません。データを取得するだけです。 | v3.3+             |

## Hive カタログ

### メタデータ

Hive カタログの Hive Metastore (HMS) と AWS Glue のサポートはほぼ重複していますが、HMS の自動インクリメンタル更新機能は推奨されません。ほとんどの場合、デフォルトの設定が推奨されます。

メタデータの取得パフォーマンスは、ユーザーの HMS または HDFS NameNode のパフォーマンスに大きく依存します。すべての要因を考慮し、テスト結果に基づいて判断してください。

- **[デフォルトおよび推奨] 分単位のデータ不整合を許容する最高のパフォーマンス**
  - **設定**: デフォルト設定を使用できます。デフォルトで 10 分以内に更新されたデータは表示されません。この期間内のクエリには古いデータが返されます。
  - **利点**: 最高のクエリパフォーマンス。
  - **欠点**: レイテンシーによるデータ不整合。
  - **サポートバージョン**: v2.5.5+ (v2.5 ではデフォルトで無効、v3.0+ ではデフォルトで有効)
- **手動リフレッシュなしで新しくロードされたデータ（ファイル）の即時可視性**
  - **設定**: 基礎データファイルのメタデータキャッシュを無効にするには、カタログプロパティ `enable_remote_file_cache` を `false` に設定します。
  - **利点**: ファイル変更の遅延なしの可視性。
  - **欠点**: ファイルメタデータキャッシュが無効な場合のパフォーマンス低下。各クエリはファイルリストにアクセスする必要があります。
  - **サポートバージョン**: v2.5.5+
- **手動リフレッシュなしでパーティション変更の即時可視性**
  - **設定**: Hive パーティション名のキャッシュを無効にするには、カタログプロパティ `enable_cache_list_names` を `false` に設定します。
  - **利点**: パーティション変更の遅延なしの可視性。
  - **欠点**: パーティション名キャッシュが無効な場合のパフォーマンス低下。各クエリはパーティションリストにアクセスする必要があります。
  - **サポートバージョン**: v2.5.5+

:::tip

データ変更のリアルタイム更新が必要で、HMS のパフォーマンスが最適化されていない場合、キャッシュを有効にし、自動インクリメンタル更新を無効にし、上流でデータ変更があるたびにスケジューリングシステムを介してメタデータを手動でリフレッシュ（REFRESH EXTERNAL TABLE を使用）することができます。

:::

### ストレージシステム

| 機能                         | 説明                                                      | サポートバージョン  |
| :-------------------------- | :------------------------------------------------------- | :----------------- |
| 再帰的サブディレクトリリスト | カタログプロパティ `enable_recursive_listing` を `true` に設定することで、再帰的サブディレクトリリストを有効にします。再帰的リストが有効になると、StarRocks はテーブルとそのパーティションからデータを読み取り、テーブルとそのパーティションの物理的な場所内のサブディレクトリからもデータを読み取ります。この機能は、多層ネストディレクトリの問題に対処するために設計されています。 | v2.5.9+<br />v3.0.4+ (v2.5 および v3.0 ではデフォルトで無効、v3.1+ ではデフォルトで有効) |

### ファイル形式とデータ型

#### ファイル形式

| 機能 | サポートされているファイル形式                         |
| :--- | :--------------------------------------------------- |
| 読み取り | Parquet, ORC, TEXT, Avro, RCFile, SequenceFile |
| シンク | Parquet (v3.2+), ORC (v3.3+), TEXT (v3.3+)         |

#### データ型

INTERVAL、BINARY、および UNION 型はサポートされていません。

TEXT 形式の Hive テーブルは MAP および STRUCT 型をサポートしていません。

### Hive ビュー

StarRocks はバージョン v3.1.0 以降、Hive ビューのクエリをサポートしています。

:::note

StarRocks が Hive ビューに対してクエリを実行する際、StarRocks と Trino の構文を使用してビューの定義を解析しようとします。StarRocks がビューの定義を解析できない場合、エラーが返されます。StarRocks が Hive または Spark に特有の関数で作成された Hive ビューの解析に失敗する可能性があります。

:::

### クエリ統計インターフェース

| 機能                                                       | サポートバージョン |
| :-------------------------------------------------------- | :---------------- |
| SHOW CREATE TABLE をサポートして Hive テーブルスキーマを表示 | v3.0+             |
| ANALYZE をサポートして統計を収集                           | v3.2+             |
| ヒストグラムと STRUCT サブフィールド統計の収集をサポート   | v3.3+             |

### データシンキング

| 機能                | サポートバージョン | 注                                                         |
| :----------------- | :---------------- | :-------------------------------------------------------- |
| CREATE DATABASE    | v3.2+             | Hive で作成されたデータベースの場所を指定するかどうかを選択できます。データベースの場所を指定しない場合、そのデータベースの下に作成されるテーブルの場所を指定する必要があります。そうしないと、エラーが返されます。データベースの場所を指定した場合、場所が指定されていないテーブルはデータベースの場所を継承します。データベースとテーブルの両方に場所を指定した場合、最終的にテーブルの場所が有効になります。 |
| CREATE TABLE       | v3.2+             | パーティションテーブルと非パーティションテーブルの両方に対応。 |
| CREATE TABLE AS SELECT | v3.2+         |                                                            |
| INSERT INTO/OVERWRITE | v3.2+          | パーティションテーブルと非パーティションテーブルの両方に対応。 |
| CREATE TABLE LIKE  | v3.2.4+          |                                                            |
| シンクファイルサイズ | v3.3+            | セッション変数 `connector_sink_target_max_file_size` を使用して、シンクされる各データファイルの最大サイズを定義できます。 |

## Iceberg カタログ

### メタデータ

Iceberg カタログは、HMS、Glue、および Tabular をメタストアとしてサポートしています。ほとんどの場合、デフォルトの設定が推奨されます。

セッション変数 `enable_iceberg_metadata_cache` のデフォルト値は、異なるシナリオに対応するために変更されました。

- バージョン v3.2.1 から v3.2.3 まで、このパラメータは使用されるメタストアサービスに関係なくデフォルトで `true` に設定されています。
- バージョン v3.2.4 以降、Iceberg クラスターが AWS Glue をメタストアとして使用している場合、このパラメータはデフォルトで `true` です。ただし、Iceberg クラスターが他のメタストアサービス（Hive メタストアなど）を使用している場合、このパラメータはデフォルトで `false` です。
- バージョン v3.3.0 以降、このパラメータのデフォルト値は再び `true` に設定されています。これは、StarRocks が新しい Iceberg メタデータフレームワークをサポートしているためです。Iceberg カタログと Hive カタログは、同じメタデータポーリングメカニズムと FE 設定項目 `background_refresh_metadata_interval_millis` を使用します。

| 機能                                                      | サポートバージョン |
| :-------------------------------------------------------- | :---------------- |
| 分散メタデータプラン（メタデータ量が多いシナリオに推奨） | v3.3+             |
| マニフェストキャッシュ（メタデータ量が少なく、レイテンシーに高い要求があるシナリオに推奨） | v3.3+             |

### ファイル形式

| 機能 | サポートされているファイル形式 |
| :--- | :--------------------------- |
| 読み取り | Parquet, ORC               |
| シンク | Parquet                    |

- Parquet 形式および ORC 形式の Iceberg V1 テーブルは、位置削除と等価削除をサポートしています。
- ORC 形式の Iceberg V2 テーブルは、バージョン v3.0.0 から位置削除をサポートし、Parquet 形式のものはバージョン v3.1.0 から位置削除をサポートしています。
- ORC 形式の Iceberg V2 テーブルは、バージョン v3.1.8 および v3.2.3 から等価削除をサポートし、Parquet 形式のものはバージョン v3.2.5 から等価削除をサポートしています。

### Iceberg ビュー

StarRocks はバージョン v3.3.2 以降、Iceberg ビューのクエリをサポートしています。現在、StarRocks を通じて作成された Iceberg ビューのみがサポートされています。

:::note

StarRocks が Iceberg ビューに対してクエリを実行する際、StarRocks と Trino の構文を使用してビューの定義を解析しようとします。StarRocks がビューの定義を解析できない場合、エラーが返されます。StarRocks が Iceberg または Spark に特有の関数で作成された Iceberg ビューの解析に失敗する可能性があります。

:::

### クエリ統計インターフェース

| 機能                                                       | サポートバージョン |
| :-------------------------------------------------------- | :---------------- |
| SHOW CREATE TABLE をサポートして Iceberg テーブルスキーマを表示 | v3.0+             |
| ANALYZE をサポートして統計を収集                           | v3.2+             |
| ヒストグラムと STRUCT サブフィールド統計の収集をサポート   | v3.3+             |

### データシンキング

| 機能                | サポートバージョン | 注                                                         |
| :----------------- | :---------------- | :-------------------------------------------------------- |
| CREATE DATABASE    | v3.1+             | Iceberg で作成されたデータベースの場所を指定するかどうかを選択できます。データベースの場所を指定しない場合、そのデータベースの下に作成されるテーブルの場所を指定する必要があります。そうしないと、エラーが返されます。データベースの場所を指定した場合、場所が指定されていないテーブルはデータベースの場所を継承します。データベースとテーブルの両方に場所を指定した場合、最終的にテーブルの場所が有効になります。 |
| CREATE TABLE       | v3.1+             | パーティションテーブルと非パーティションテーブルの両方に対応。 |
| CREATE TABLE AS SELECT | v3.1+         |                                                            |
| INSERT INTO/OVERWRITE | v3.1+          | パーティションテーブルと非パーティションテーブルの両方に対応。 |

### その他のサポート

| 機能                                                      | サポートバージョン |
| :-------------------------------------------------------- | :---------------- |
| TIMESTAMP 型のパーティション形式 `yyyy-MM-ddTHH:mm` および `yyyy-MM-dd HH:mm` の読み取りをサポート | v2.5.19+<br />v3.1.9+<br />v3.2.3+ |

## Hudi カタログ

- StarRocks は Hudi の Parquet 形式のデータのクエリをサポートし、Parquet ファイルの圧縮形式として SNAPPY、LZ4、ZSTD、GZIP、および NO_COMPRESSION をサポートしています。
- StarRocks は Hudi の Copy On Write (COW) テーブルと Merge On Read (MOR) テーブルを完全にサポートしています。
- StarRocks はバージョン v3.0.0 以降、SHOW CREATE TABLE をサポートして Hudi テーブルスキーマを表示します。

## Delta Lake カタログ

- StarRocks は Delta Lake の Parquet 形式のデータのクエリをサポートし、Parquet ファイルの圧縮形式として SNAPPY、LZ4、ZSTD、GZIP、および NO_COMPRESSION をサポートしています。
- StarRocks は Delta Lake の MAP 型および STRUCT 型データのクエリをサポートしていません。
- StarRocks はバージョン v3.0.0 以降、SHOW CREATE TABLE をサポートして Delta Lake テーブルスキーマを表示します。

## JDBC カタログ

| カタログタイプ | サポートバージョン |
| :----------- | :---------------- |
| MySQL        | v3.0+             |
| PostgreSQL   | v3.0+             |
| ClickHouse   | v3.3+             |
| Oracle       | v3.2.9+           |
| SQL Server   | v3.2.9+           |

### MySQL

| 機能        | サポートバージョン |
| :--------- | :---------------- |
| メタデータキャッシュ | v3.3+             |

#### データ型の対応

| MySQL             | StarRocks           | サポートバージョン |
| :---------------- | :------------------ | :---------------- |
| BOOLEAN           | BOOLEAN             | v2.3+             |
| BIT               | BOOLEAN             | v2.3+             |
| SIGNED TINYINT    | TINYINT             | v2.3+             |
| UNSIGNED TINYINT  | SMALLINT            | v3.0.6+<br />v3.1.2+ |
| SIGNED SMALLINT   | SMALLINT            | v2.3+             |
| UNSIGNED SMALLINT | INT                 | v3.0.6+<br />v3.1.2+ |
| SIGNED INTEGER    | INT                 | v2.3+             |
| UNSIGNED INTEGER  | BIGINT              | v3.0.6+<br />v3.1.2+ |
| SIGNED BIGINT     | BIGINT              | v2.3+             |
| UNSIGNED BIGINT   | LARGEINT            | v3.0.6+<br />v3.1.2+ |
| FLOAT             | FLOAT               | v2.3+             |
| REAL              | FLOAT               | v3.0.1+           |
| DOUBLE            | DOUBLE              | v2.3+             |
| DECIMAL           | DECIMAL32           | v2.3+             |
| CHAR              | VARCHAR(columnsize) | v2.3+             |
| VARCHAR           | VARCHAR             | v2.3+             |
| TEXT              | VARCHAR(columnsize) | v3.0.1+           |
| DATE              | DATE                | v2.3+             |
| TIME              | TIME                | v3.1.9+<br />v3.2.4+ |
| TIMESTAMP         | DATETIME            | v2.3+             |

### PostgreSQL

#### データ型の対応

| MySQL     | StarRocks           | サポートバージョン |
| :-------- | :------------------ | :---------------- |
| BIT       | BOOLEAN             | v2.3+             |
| SMALLINT  | SMALLINT            | v2.3+             |
| INTEGER   | INT                 | v2.3+             |
| BIGINT    | BIGINT              | v2.3+             |
| REAL      | FLOAT               | v2.3+             |
| DOUBLE    | DOUBLE              | v2.3+             |
| NUMERIC   | DECIMAL32           | v2.3+             |
| CHAR      | VARCHAR(columnsize) | v2.3+             |
| VARCHAR   | VARCHAR             | v2.3+             |
| TEXT      | VARCHAR(columnsize) | v2.3+             |
| DATE      | DATE                | v2.3+             |
| TIMESTAMP | DATETIME            | v2.3+             |

### ClickHouse 

バージョン v3.3.0 以降でサポートされています。

### Oracle

バージョン v3.2.9 以降でサポートされています。

### SQL Server

バージョン v3.2.9 以降でサポートされています。

## Elasticsearch カタログ

Elasticsearch カタログはバージョン v3.1.0 以降でサポートされています。

## Paimon カタログ

Paimon カタログはバージョン v3.1.0 以降でサポートされています。

## MaxCompute カタログ

MaxCompute カタログはバージョン v3.3.0 以降でサポートされています。

## Kudu カタログ

Kudu カタログはバージョン v3.3.0 以降でサポートされています。
