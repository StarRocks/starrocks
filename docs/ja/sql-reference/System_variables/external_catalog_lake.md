---
displayed_sidebar: docs
sidebar_label: "外部カタログとデータレイク"
sidebar_position: 6
description: "外部カタログコネクタと、Hive・Iceberg・Hudi などのデータレイク形式に関するセッション変数。"
---

# システム変数 - 外部カタログとデータレイク

変数の表示および設定方法については、[システム変数の概要](../System_variable.md) を参照してください。

### avro_use_jni_reader

* **スコープ**: Session
* **説明**: Hive などの外部 Catalog にある Avro データをスキャンする際に、JNI ベースの Avro Reader を使用するかどうかを制御します。有効にすると（`true`）、StarRocks は JNI Reader を使用します。無効にすると（`false`）、StarRocks はネイティブ Avro Reader を使用します。現在この変数は主に互換性確保のためのフォールバックとして利用されます。デフォルト値は `false` のため、既定ではネイティブ Avro Reader が使用されます。

  現在の注意点:
  - ネイティブ Avro Reader と JNI Reader は、`CHAR(n)` の挙動について現在は整合しています。この整合は [#73579](https://github.com/StarRocks/starrocks/pull/73579) で反映されており、この点では native と JNI の動作は一致しています。
  - ネイティブ Avro Reader は現在 `null`、`deflate`、`snappy` のみをサポートしており、`bzip2` などその他の codec はサポートしていません。ネイティブ Reader がサポートしていない codec を扱う必要がある場合は、JNI Reader を手動で有効にしてください。
* **デフォルト**: `false`
* **データ型**: boolean
* **導入バージョン**: v4.1.1

### connector_io_tasks_per_scan_operator

* **説明**: 外部テーブルクエリ中にスキャンオペレーターによって発行される最大同時 I/O タスク数。値は整数です。現在、StarRocks は外部テーブルをクエリする際に同時 I/O タスクの数を適応的に調整できます。この機能は、デフォルトで有効になっている変数 `enable_connector_adaptive_io_tasks` によって制御されます。
* **デフォルト**: 16
* **データ型**: Int
* **導入バージョン**: v2.5

### connector_sink_compression_codec

* **説明**: Hive テーブルまたは Iceberg テーブルにデータを書き込む際、または Files() でデータをエクスポートする際に使用される圧縮アルゴリズムを指定します。このパラメータは、以下の状況でのみ有効になります：
  * Hive テーブルに `compression_codec` プロパティが存在しない場合。
  * Iceberg テーブルに `write.parquet.compression-codec` プロパティが存在しない場合。
  * `INSERT INTO FILES` に対して `compression` プロパティが設定されていない場合。
* **有効な値**: `uncompressed`, `snappy`, `lz4`, `zstd`, および `gzip`。
* **デフォルト**: uncompressed
* **データ型**: 文字列
* **導入バージョン**: v3.2.3

### connector_sink_target_max_file_size

* **説明**: Hive テーブルまたは Iceberg テーブルにデータを書き込む際、または Files() でデータをエクスポートする際のターゲットファイルの最大サイズを指定します。この制限は厳密ではなく、ベストエフォートで適用されます。
* **単位**: バイト
* **デフォルト**: 1073741824
* **データ型**: Long
* **導入バージョン**: v3.3.0

### enable_connector_adaptive_io_tasks

* **説明**: 外部テーブルをクエリする際に同時 I/O タスクの数を適応的に調整するかどうか。デフォルト値は `true` です。この機能が有効でない場合、変数 `connector_io_tasks_per_scan_operator` を使用して同時 I/O タスクの数を手動で設定できます。
* **デフォルト**: true
* **導入バージョン**: v2.5

### enable_write_hive_external_table

* **説明**: Hive の外部テーブルにデータをシンクすることを許可するかどうか。
* **デフォルト**: false
* **導入バージョン**: v3.2

### lake_bucket_assign_mode

* **説明**: データレイク内のテーブルに対するクエリにおけるバケット割り当てモード。この変数は、クエリ実行中に Bucket-aware 実行が有効になった際に、バケットがワーカーノードにどのように割り当てられるかを制御します。有効な値:
  * `balance`: ワーカーノードにバケットを均等に割り当て、バランスの取れたワークロードとより良いパフォーマンスを実現します。
  * `elastic`: 一貫性ハッシュを使用してバケットをワーカーノードに割り当て、弾力的な環境においてより良い負荷分散を実現できます。
* **デフォルト**: balance
* **タイプ**: String
* **導入バージョン**: v4.0

### metadata_collect_query_timeout

* **説明**: Iceberg Catalog メタデータ収集クエリのタイムアウト時間。
* **単位**: 秒
* **デフォルト**: 60
* **導入バージョン**: v3.3.3

### orc_use_column_names

* **説明**: StarRocks が Hive から ORC ファイルを読み取る際に列がどのように一致するかを指定するために使用されます。デフォルト値は `false` で、ORC ファイル内の列は Hive テーブル定義内の順序位置に基づいて読み取られます。この変数が `true` に設定されている場合、列は名前に基づいて読み取られます。
* **デフォルト**: false
* **導入バージョン**: v3.1.10

### allow_lake_without_partition_filter

* **説明**: レイクテーブル（Hive、Iceberg、Delta Lake、Paimon など）に対してパーティションフィルターなしのクエリを許可するかどうか。`false` に設定すると、有効なパーティションフィルターを含まないクエリは拒否され、意図しないフルテーブルスキャンを防止します。
* **スコープ**: セッション
* **デフォルト**: `true`
* **タイプ**: Boolean
* **エイリアス**: `allow_hive_without_partition_filter`

### scan_lake_partition_num_limit

* **説明**: 単一のレイクテーブル（Hive、Iceberg、Delta Lake、Paimon など）に対してスキャンできるパーティションの最大数。`0` に設定すると制限なし。上限を超えるとクエリはエラーを返します。なお、増分的にスプリットを列挙するカタログタイプ（Iceberg、Delta Lake）では、制限チェックはスキャンレンジのディスパッチ時に行われ、クエリが即座に拒否されるのではなく、実行途中で失敗する場合があります。
* **スコープ**: セッション
* **デフォルト**: `0`（制限なし）
* **タイプ**: Int
* **エイリアス**: `scan_hive_partition_num_limit`

