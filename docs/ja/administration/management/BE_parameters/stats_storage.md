---
displayed_sidebar: docs
sidebar_label: "統計とストレージ"
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

import EditionSpecificBEItem from '../../../_assets/commonMarkdown/Edition_Specific_BE_Item.mdx'

# BE 設定 - 統計とストレージ

<BEConfigMethod />

<CNConfigMethod />

## BE の設定項目を表示する

次のコマンドを使用して BE の設定項目を表示できます。

```SQL
SELECT * FROM information_schema.be_configs [WHERE NAME LIKE "%<name_pattern>%"]
```

## BE パラメータを設定する

<PostBEConfig />

<StaticBEConfigNote />

---

このトピックでは、以下の種類のBE構成について紹介します：
- [統計レポート](#統計レポート)
- [ストレージ](#ストレージ)

## 統計レポート

### report_disk_state_interval_seconds

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ストレージボリュームの状態を報告する時間間隔。これには、ボリューム内のデータサイズが含まれます。
- 導入バージョン: -

### report_resource_usage_interval_ms

- デフォルト: 1000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: Yes
- 説明: BE エージェントが FE (master) に送信する定期的なリソース使用状況レポートの間隔（ミリ秒）。エージェントのワーカースレッドは TResourceUsage（実行中クエリ数、使用中メモリ/制限、CPU 使用 permille、resource-group の使用状況）を収集して report_task を呼び出し、この設定された間隔だけスリープします（task_worker_pool を参照）。値を小さくすると報告の即時性は向上しますが CPU、ネットワーク、master の負荷が増加します。値を大きくするとオーバーヘッドは減りますがリソース情報の最新性は低下します。報告は関連するメトリクス（report_resource_usage_requests_total、report_resource_usage_requests_failed）を更新します。クラスタ規模や FE の負荷に応じて調整してください。
- 導入バージョン: v3.2.0

### report_tablet_interval_seconds

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: すべてのタブレットの最新バージョンを報告する時間間隔。
- 導入バージョン: -

### report_task_interval_seconds

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: タスクの状態を報告する時間間隔。タスクは、テーブルの作成、テーブルの削除、データのロード、またはテーブルスキーマの変更を行うことができます。
- 導入バージョン: -

### report_workgroup_interval_seconds

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: すべてのワークグループの最新バージョンを報告する時間間隔。
- 導入バージョン: -

## ストレージ

<EditionSpecificBEItem />

### alter_tablet_worker_count

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: スキーマ変更のために使用されるスレッドの数。
- 導入バージョン: -

### automatic_partition_thread_pool_thread_num

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: ロード時の自動パーティション作成に使用する自動パーティションスレッドプールのスレッド数。プールのキューサイズはスレッド数の 10 倍に自動設定されます。
- 導入バージョン: -

### avro_ignore_union_type_tag

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Avro の Union データタイプからシリアライズされた JSON 文字列からタイプタグを取り除くかどうか。
- 導入バージョン: v3.3.7, v3.4

### base_compaction_check_interval_seconds

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ベースコンパクションのスレッドポーリングの時間間隔。
- 導入バージョン: -

### base_compaction_interval_seconds_since_last_operation

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 最後のベースコンパクションからの時間間隔。この設定項目はベースコンパクションをトリガーする条件の一つです。
- 導入バージョン: -

### base_compaction_num_threads_per_disk

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 各ストレージボリュームでのベースコンパクションに使用されるスレッド数。
- 導入バージョン: -

### base_cumulative_delta_ratio

- デフォルト: 0.3
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: 累積ファイルサイズとベースファイルサイズの比率。この比率がこの値に達することがベースコンパクションをトリガーする条件の一つです。
- 導入バージョン: -

### check_consistency_worker_count

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: タブレットの一貫性をチェックするために使用されるスレッドの数。
- 導入バージョン: -

### clear_expired_replication_snapshots_interval_seconds

- デフォルト: 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 異常なレプリケーションによって残された期限切れのスナップショットをシステムがクリアする時間間隔。
- 導入バージョン: v3.3.5

### compact_threads

- デフォルト: 4
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 同時コンパクションタスクに使用される最大スレッド数。この設定は v3.1.7 および v3.2.2 以降、動的に変更されました。
- 導入バージョン: v3.0.0

### compaction_max_memory_limit_percent

- デフォルト: 100
- タイプ: Int
- 単位: Percent
- 変更可能: No
- 説明: compaction に使用できる BE プロセスメモリの割合。このパーセントに基づき BE は `compaction_max_memory_limit` と（プロセスメモリ上限 × このパーセント / 100）の小さい方を compaction メモリ上限として計算します。この値が < 0 または > 100 の場合は 100 と見なされます。`compaction_max_memory_limit` < 0 の場合は代わりにプロセスメモリ上限が使用されます。計算は `mem_limit` から導出される BE プロセスメモリも考慮します。`compaction_memory_limit_per_worker`（ワーカーごとの上限）と組み合わせて、この設定は compaction に利用可能な総メモリを制御し、したがって compaction の並列度や OOM リスクに影響します。
- 導入バージョン: v3.2.0

### compaction_memory_limit_per_worker

- デフォルト: 2147483648
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: 各コンパクションスレッドに許可される最大メモリサイズ。
- 導入バージョン: -

### compaction_trace_threshold

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 各コンパクションの時間しきい値。コンパクションがこの時間しきい値を超えて時間がかかる場合、StarRocks は対応するトレースを出力します。
- 導入バージョン: -

### create_tablet_worker_count

- デフォルト: 3
- タイプ: Int
- 単位: Threads
- 変更可能: Yes
- 説明: FE により送信される TTaskType::CREATE（create-tablet）タスクを処理する AgentServer のスレッドプール内の最大ワーカースレッド数を設定します。BE 起動時にこの値はスレッドプールの max として使用されます（プールは min threads = 1、max queue size = unlimited で作成されます）。ランタイムで変更すると ExecEnv::agent_server()->get_thread_pool(TTaskType::CREATE)->update_max_threads(...) が呼ばれます。バルクロードやパーティション作成時など、同時の tablet 作成スループットを上げたい場合に増やしてください。減らすと同時作成操作が制限されます。値を上げると CPU、メモリ、I/O の並列性が増し競合が発生する可能性があります。スレッドプールは少なくとも 1 スレッドを保証するため、1 未満の値は実質的な効果がありません。
- 導入バージョン: 3.2.0

### cumulative_compaction_check_interval_seconds

- デフォルト: 1
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 累積コンパクションのスレッドポーリングの時間間隔。
- 導入バージョン: -

### cumulative_compaction_num_threads_per_disk

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: ディスクごとの累積コンパクションスレッド数。
- 導入バージョン: -

### data_page_size

- デフォルト: 65536
- タイプ: Int
- 単位: Bytes
- 変更可能: No
- 説明: 列データおよびインデックスページを構築する際に使用されるターゲットの非圧縮ページサイズ（バイト）。この値は ColumnWriterOptions.data_page_size と IndexedColumnWriterOptions.index_page_size にコピーされ、ページビルダ（例: BinaryPlainPageBuilder::is_page_full およびバッファ予約ロジック）によってページを完了するタイミングや確保するメモリ量の判断に参照されます。値が 0 の場合、ビルダ内のページサイズ制限は無効化されます。この値を変更するとページ数、メタデータのオーバーヘッド、メモリ予約、および I/O/圧縮のトレードオフに影響します（ページを小さくするとページ数とメタデータが増え、ページを大きくするとページ数は減り圧縮効率が向上する可能性があるがメモリのスパイクが大きくなる）。変更はランタイムで反映されないため、完全に有効にするにはプロセスの再起動と再作成された rowset が必要です。
- 導入バージョン: 3.2.4

### default_num_rows_per_column_file_block

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各行ブロックに格納できる最大行数。
- 導入バージョン: -

### delete_worker_count_high_priority

- デフォルト: 1
- タイプ: Int
- 単位: Threads
- 変更可能: No
- 説明: DeleteTaskWorkerPool 内で HIGH-priority の削除スレッドとして割り当てられるワーカースレッドの数。起動時に AgentServer は total threads = delete_worker_count_normal_priority + delete_worker_count_high_priority で削除プールを作成し、最初の delete_worker_count_high_priority スレッドは専ら TPriority::HIGH タスクをポップしようとするようにマークされます（高優先度削除タスクがない場合はポーリングしてスリープ/ループします）。この値を増やすと高優先度削除リクエストの並列性が高まり、減らすと専用容量が減って高優先度削除のレイテンシが増加する可能性があります。
- 導入バージョン: v3.2.0

### dictionary_encoding_ratio

- デフォルト: 0.7
- タイプ: Double
- 単位: -
- 変更可能: No
- 説明: StringColumnWriter が encode-speculation フェーズでチャンクに対して dictionary (DICT_ENCODING) と plain (PLAIN_ENCODING) のどちらを選択するかを判断するために使用する比率（0.0–1.0）。コードは max_card = row_count * `dictionary_encoding_ratio` を計算し、チャンクの distinct key 数をスキャンします；distinct count が max_card を超えると writer は PLAIN_ENCODING を選びます。このチェックはチャンクサイズが `dictionary_speculate_min_chunk_size` を超えた場合（かつ row_count > dictionary_min_rowcount のとき）にのみ行われます。値を大きくすると dictionary encoding が有利になり（より多くの distinct key を許容）、値を小さくすると早めに plain encoding へフォールバックします。値が 1.0 の場合は事実上 dictionary encoding を強制します（distinct count は常に row_count を超え得ないため）。
- 導入バージョン: v3.2.0

### disk_stat_monitor_interval

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ディスクの健康状態を監視する時間間隔。
- 導入バージョン: -

### download_low_speed_limit_kbps

- デフォルト: 50
- タイプ: Int
- 単位: KB/秒
- 変更可能: はい
- 説明: 各 HTTP リクエストのダウンロード速度の下限。この値より低い速度で一定時間動作すると、HTTP リクエストは中止されます。この時間は、設定項目 `download_low_speed_time` で指定されます。
- 導入バージョン: -

### download_low_speed_time

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ダウンロード速度が下限より低い状態で動作できる最大時間。この時間内に `download_low_speed_limit_kbps` の値より低い速度で動作し続けると、HTTP リクエストは中止されます。
- 導入バージョン: -

### download_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ノードでのリストアジョブのダウンロードタスクの最大スレッド数。`0` は、BE が存在するマシンの CPU コア数に値を設定することを示します。
- 導入バージョン: -

### drop_tablet_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: タブレットを削除するために使用されるスレッドの数。`0` はノード内の CPU コアの半数を示します。
- 導入バージョン: -

### enable_check_string_lengths

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: 文字列の長さをチェックして、範囲外の VARCHAR データによるコンパクションの失敗を解決するかどうか。
- 導入バージョン: -

### enable_event_based_compaction_framework

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: イベントベースのコンパクションフレームワークを有効にするかどうか。`true` はイベントベースのコンパクションフレームワークが有効であることを示し、`false` は無効であることを示します。イベントベースのコンパクションフレームワークを有効にすると、多くのタブレットがある場合や単一のタブレットに大量のデータがある場合のコンパクションのオーバーヘッドを大幅に削減できます。
- 導入バージョン: -

### enable_lazy_delta_column_compaction

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: 有効にすると、部分的なカラム更新によって生成される delta columns に対する compaction に「lazy」戦略を優先します。StarRocks は compaction の I/O を節約するために、delta-column ファイルをメインのセグメントファイルへ積極的にマージすることを避けます。実際には compaction 選択コードが部分カラム更新の rowset と複数の候補をチェックし、それらが見つかりこのフラグが true の場合、エンジンは compaction にさらに入力を追加するのを止めるか、空の rowset（レベル -1）のみをマージして delta columns を別に保持します。これにより compaction 時の即時 I/O と CPU が削減されますが、統合の遅延（セグメント数増加や一時的なストレージオーバーヘッドの可能性）というコストが発生します。正確性やクエリのセマンティクスに変更はありません。
- 導入バージョン: v3.2.3

### enable_new_load_on_memory_limit_exceeded

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ハードメモリリソース制限に達したときに新しいロードプロセスを許可するかどうか。`true` は新しいロードプロセスが許可されることを示し、`false` は拒否されることを示します。
- 導入バージョン: v3.3.2

### enable_pk_index_parallel_compaction

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでプライマリキーインデックスの並列コンパクションを有効にするかどうか。
- 導入バージョン: -

### enable_pk_index_parallel_execution

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでプライマリキーインデックス操作の並列実行を有効にするかどうか。有効化されると、システムは公開操作中にスレッドプールを使用してセグメントを並行処理し、大規模なテーブルのパフォーマンスを大幅に向上させます。
- 導入バージョン: -

### enable_pk_index_eager_build

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: データインポートおよびコンパクションの段階で、Primary Key インデックスファイルを即座に構築するかどうかを決定します。有効化されると、システムはデータ書き込み時に永続的な PK インデックスファイルを直接生成し、後続のクエリパフォーマンスを向上させます。
- 導入バージョン: -

### enable_pk_size_tiered_compaction_strategy

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: 主キーテーブルのサイズ階層型コンパクションポリシーを有効にするかどうか。`true` はサイズ階層型コンパクション戦略が有効であることを示し、`false` は無効であることを示します。この項目は、共有データクラスタでは v3.2.4 および v3.1.10 以降、共有なしクラスタでは v3.2.5 および v3.1.10 以降で有効になります。
- 導入バージョン: -

### enable_rowset_verify

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 生成された rowset の正確性を検証するかどうか。 有効にすると、コンパクションとスキーマ変更後に生成された rowset の正確性がチェックされます。
- 導入バージョン: -

### enable_size_tiered_compaction_strategy

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: サイズ階層型コンパクションポリシー (主キーテーブルを除く) を有効にするかどうか。`true` はサイズ階層型コンパクション戦略が有効であることを示し、`false` は無効であることを示します。
- 導入バージョン: -

### enable_strict_delvec_crc_check

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: enable_strict_delvec_crc_check を true に設定すると、delete vector の CRC32 を厳密にチェックし、一致しない場合はエラーを返します。
- 導入バージョン: -

### enable_transparent_data_encryption

- デフォルト: false
- タイプ: Boolean
- 単位: N/A
- 変更可能: No
- 説明: 有効にすると、StarRocks は新規に書き込まれるストレージオブジェクト（segment files、delete/update files、rowset segments、lake SSTs、persistent index files など）に対してオンディスクの暗号化アーティファクトを作成します。Writers（RowsetWriter/SegmentWriter、lake UpdateManager/LakePersistentIndex および関連するコードパス）は KeyCache から暗号化情報を要求し、書き込み可能なファイルに encryption_info を付与し、rowset / segment / sstable メタデータ（segment_encryption_metas、delete/update encryption metadata）に encryption_meta を永続化します。Frontend と Backend/CN の暗号化フラグは一致している必要があり、不一致の場合は BE がハートビート時に中止します（LOG(FATAL)）。このフラグはランタイムで変更できないため、デプロイ前に有効化し、鍵管理（KEK）および KeyCache がクラスタ全体で適切に構成・同期されていることを確認してください。
- 導入バージョン: 3.3.1, 3.4.0, 3.5.0, 4.0.0

### file_descriptor_cache_clean_interval

- デフォルト: 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 一定期間使用されていないファイルディスクリプタをクリーンアップする時間間隔。
- 導入バージョン: -

### inc_rowset_expired_sec

- デフォルト: 1800
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 受信データの有効期限。この設定項目はインクリメンタルクローンで使用されます。
- 導入バージョン: -

### load_process_max_memory_hard_limit_ratio

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ノード上のすべてのロードプロセスが占有できるメモリリソースのハードリミット (比率)。`enable_new_load_on_memory_limit_exceeded` が `false` に設定されており、すべてのロードプロセスのメモリ消費が `load_process_max_memory_limit_percent * load_process_max_memory_hard_limit_ratio` を超える場合、新しいロードプロセスは拒否されます。
- 導入バージョン: v3.3.2

### load_process_max_memory_limit_percent

- デフォルト: 30
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: BE ノード上のすべてのロードプロセスが占有できるメモリリソースのソフトリミット (パーセンテージ)。
- 導入バージョン: -

### lz4_acceleration

- デフォルト: 1
- タイプ: Int
- 単位: N/A
- 変更可能: はい
- 説明: 組み込みの LZ4 圧縮器で使用される LZ4 の "acceleration" パラメータを制御します（LZ4_compress_fast_continue に渡されます）。値を大きくすると圧縮率を犠牲にして圧縮速度を優先し、値を小さく（1）すると圧縮は良くなりますが遅くなります。有効範囲: MIN=1, MAX=65537。この設定は BlockCompression にあるすべての LZ4 ベースのコーデック（例: LZ4 および Hadoop-LZ4）に影響し、圧縮の実行方法のみを変更します — LZ4 フォーマットや復号の互換性は変わりません。出力サイズが許容される CPU バウンドや低レイテンシのワークロードでは上げてチューニングしてください（例: 4、8、...）；ストレージや I/O に敏感なワークロードでは 1 のままにしてください。スループットとサイズのトレードオフはデータ依存性が高いため、変更前に代表的なデータでテストしてください。
- 導入バージョン: 3.4.1, 3.5.0, 4.0.0

### lz4_expected_compression_ratio

- デフォルト: 2.1
- タイプ: double
- 単位: Dimensionless (compression ratio)
- 変更可能: はい
- 説明: シリアライゼーション圧縮戦略が観測された LZ4 圧縮を「良い」と判断するために使用する閾値です。compress_strategy.cpp では、この値が観測された compress_ratio を割る形で lz4_expected_compression_speed_mbps と合わせて報酬メトリクスを計算します；結合した報酬が > 1.0 であれば戦略は正のフィードバックを記録します。この値を上げると期待される圧縮率が高くなり（条件を満たしにくく）、下げると観測された圧縮が満足と見なされやすくなります。典型的なデータの圧縮しやすさに合わせて調整してください。 有効範囲: MIN=1, MAX=65537。
- 導入バージョン: 3.4.1, 3.5.0, 4.0.0

### lz4_expected_compression_speed_mbps

- デフォルト: 600
- タイプ: double
- 単位: MB/s
- 変更可能: Yes
- 説明: adaptive compression policy (CompressStrategy) で使用される、期待される LZ4 圧縮スループット（メガバイト毎秒）。フィードバックルーチンは reward_ratio = (observed_compression_ratio / lz4_expected_compression_ratio) * (observed_speed / lz4_expected_compression_speed_mbps) を計算します。reward_ratio が `>` 1.0 の場合は正のカウンタ（alpha）を増やし、それ以外は負のカウンタ（beta）を増やします；これにより将来のデータが圧縮されるかどうかが影響を受けます。ハードウェア上の典型的な LZ4 スループットを反映するようこの値を調整してください — 値を上げると「良好」と判定するのが厳しく（より高い観測速度が必要）、下げると判定が容易になります。正の有限数でなければなりません。
- 導入バージョン: v3.4.1, 3.5.0, 4.0.0

### make_snapshot_worker_count

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ノードでのスナップショット作成タスクの最大スレッド数。
- 導入バージョン: -

### manual_compaction_threads

- デフォルト: 4
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 手動コンパクションのスレッド数。
- 導入バージョン: -

### max_base_compaction_num_singleton_deltas

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各ベースコンパクションでコンパクト化できる最大セグメント数。
- 導入バージョン: -

### max_compaction_candidate_num

- デフォルト: 40960
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: コンパクションの候補タブレットの最大数。値が大きすぎると、高いメモリ使用量と高い CPU 負荷を引き起こします。
- 導入バージョン: -

### max_compaction_concurrency

- デフォルト: -1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: コンパクションの最大同時実行数 (ベースコンパクションと累積コンパクションの両方を含む)。値 `-1` は同時実行数に制限がないことを示します。`0` はコンパクションを無効にすることを示します。このパラメータは、イベントベースのコンパクションフレームワークが有効な場合に可変です。
- 導入バージョン: -

### max_cumulative_compaction_num_singleton_deltas

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 単一の累積コンパクションでマージできる最大セグメント数。コンパクション中に OOM が発生した場合、この値を減少させることができます。
- 導入バージョン: -

### max_download_speed_kbps

- デフォルト: 50000
- タイプ: Int
- 単位: KB/秒
- 変更可能: はい
- 説明: 各 HTTP リクエストの最大ダウンロード速度。この値は、BE ノード間のデータレプリカ同期のパフォーマンスに影響を与えます。
- 導入バージョン: -

### max_garbage_sweep_interval

- デフォルト: 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ストレージボリュームのガーベジコレクションの最大時間間隔。この設定は v3.0 以降、動的に変更されました。
- 導入バージョン: -

### max_percentage_of_error_disk

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 対応する BE ノードが終了する前にストレージボリュームで許容されるエラーの最大パーセンテージ。
- 導入バージョン: -

### max_row_source_mask_memory_bytes

- デフォルト: 209715200
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: 行ソースマスクバッファの最大メモリサイズ。この値を超えると、データはディスク上の一時ファイルに保存されます。この値は `compaction_memory_limit_per_worker` の値よりも低く設定する必要があります。
- 導入バージョン: -

### max_update_compaction_num_singleton_deltas

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 主キーテーブルの単一コンパクションでマージできる最大 rowset 数。
- 導入バージョン: -

### memory_limitation_per_thread_for_schema_change

- デフォルト: 2
- タイプ: Int
- 単位: GB
- 変更可能: はい
- 説明: 各スキーマ変更タスクに許可される最大メモリサイズ。
- 導入バージョン: -

### min_base_compaction_num_singleton_deltas

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ベースコンパクションをトリガーする最小セグメント数。
- 導入バージョン: -

### min_compaction_failure_interval_sec

- デフォルト: 120
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 前回のコンパクション失敗からタブレットコンパクションをスケジュールできる最小時間間隔。
- 導入バージョン: -

### min_cumulative_compaction_failure_interval_sec

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 累積コンパクションが失敗時にリトライする最小時間間隔。
- 導入バージョン: -

### min_cumulative_compaction_num_singleton_deltas

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 累積コンパクションをトリガーする最小セグメント数。
- 導入バージョン: -

### min_garbage_sweep_interval

- デフォルト: 180
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ストレージボリュームのガーベジコレクションの最小時間間隔。この設定は v3.0 以降、動的に変更されました。
- 導入バージョン: -

### parallel_clone_task_per_path

- デフォルト: 8
- タイプ: Int
- 単位: Threads
- 変更可能: Yes
- 説明: BE 上の各ストレージパスに割り当てられる並列 clone ワーカースレッドの数。BE 起動時にクローンスレッドプールの max threads は max(number_of_store_paths * parallel_clone_task_per_path, MIN_CLONE_TASK_THREADS_IN_POOL) として計算されます。例えばストレージパスが4つでデフォルト=8 の場合、クローンプールの max = 32 になります。この設定は BE が処理する CLONE タスク（tablet レプリカのコピー）の並列度を直接制御します：値を増やすと並列クローンスループットが向上しますが CPU、ディスク、ネットワークの競合も増えます；値を減らすと同時実行クローンタスクが制限され、FE がスケジュールしたクローン操作をスロットルする可能性があります。値は動的 clone スレッドプールに適用され、update-config パス経由でランタイムに変更可能です（agent_server がクローンプールの max threads を更新します）。
- 導入バージョン: v3.2.0

### path_gc_check

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: No
- 説明: 有効にすると、StorageEngine は各データディレクトリごとにパス走査とガベージコレクションを定期的に行うバックグラウンドスレッドを起動します。起動時に `start_bg_threads()` は `_path_scan_thread_callback`（`DataDir::perform_path_scan` と `perform_tmp_path_scan` を呼び出す）および `_path_gc_thread_callback`（`DataDir::perform_path_gc_by_tablet`、`DataDir::perform_path_gc_by_rowsetid`、`DataDir::perform_delta_column_files_gc`、`DataDir::perform_crm_gc` を呼び出す）を生成します。走査と GC の間隔は `path_scan_interval_second` と `path_gc_check_interval_second` で制御され、CRM ファイルのクリーンアップは `unused_crm_file_threshold_second` を使用します。自動のパス単位クリーンアップを無効にすると、孤立ファイルや一時ファイルは手動で管理する必要があります。フラグを変更するにはプロセスの再起動が必要です。
- 導入バージョン: v3.2.0

### path_gc_check_interval_second

- デフォルト: 86400
- タイプ: Int
- 単位: Seconds
- 変更可能: No
- 説明: ストレージエンジンのパスガベージコレクション用バックグラウンドスレッドが実行される間隔（秒）。スレッドが起床するたびに DataDir はタブレット単位のパス GC、rowset id 単位のパス GC、デルタカラムファイルの GC、および CRM GC を実行します（CRM GC 呼び出しは `unused_crm_file_threshold_second` を使用します）。非正の値に設定すると、コードは間隔を 1800 秒（30 分）に強制し、警告を出力します。ディスク上の一時ファイルやダウンロード済みファイルがどの頻度でスキャン・削除されるかを調整するためにこの値を調整してください。
- 導入バージョン: v3.2.0

### pending_data_expire_time_sec

- デフォルト: 1800
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ストレージエンジン内の保留中データの有効期限。
- 導入バージョン: -

### pindex_major_compaction_limit_per_disk

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ディスク上のコンパクションの最大同時実行数。これは、コンパクションによるディスク間の不均一な I/O の問題に対処します。この問題は、特定のディスクに対して過度に高い I/O を引き起こす可能性があります。
- 導入バージョン: v3.0.9

### pk_index_compaction_score_ratio

- デフォルト: 1.5
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでのプライマリキーインデックスのコンパクションスコア比率。たとえば、N 個のファイルセットがある場合、コンパクションスコアは N * pk_index_compaction_score_ratio になります。
- 導入バージョン: -

### pk_index_early_sst_compaction_threshold

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでのプライマリキーインデックス early sst コンパクションの閾値。
- 導入バージョン: -

### pk_index_map_shard_size

- デフォルト: 4096
- タイプ: Int
- 単位: Count
- 変更可能: No
- 説明: lake の UpdateManager におけるプライマリキーインデックスシャードマップで使用されるシャード数。UpdateManager はこのサイズの `PkIndexShard` ベクトルを割り当て、ビットマスク (tablet_id & (`pk_index_map_shard_size` - 1)) を使って tablet id をシャードにマップします。`pk_index_map_shard_size` を増やすと、本来同じシャードを共有しているタブレット間のロック競合を低減できますが、ミューテックスオブジェクトの数が増えメモリ使用量がやや増加します。コードはビットマスクによるインデックス化に依存しているため、値は 2 の累乗でなければなりません。サイズ決定の参考は `tablet_map_shard_size` のヒューリスティックを参照してください: total_num_of_tablets_in_BE / 512.
- 導入バージョン: v3.2.0

### pk_index_memtable_flush_threadpool_max_threads

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでのプライマリキーインデックス Memtable フラッシュ用のスレッドプールの最大スレッド数。0 は CPU コア数の半分に自動設定されることを意味します。
- 導入バージョン: -

### pk_index_memtable_max_count

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでのプライマリキーインデックスの最大 Memtable 数。
- 導入バージョン: -

### pk_index_memtable_max_wait_flush_timeout_ms

- デフォルト: 30000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: ストレージ・コンピューティング分離クラスタにおける、プライマリキーインデックス MemTable のフラッシュ完了を待機する最大タイムアウト時間。すべての MemTable を同期的にフラッシュする必要がある場合（例：SST インジェスト操作の前）、システムは最大でこのタイムアウト時間まで待機します。デフォルトは 30 秒です。
- 導入バージョン: -

### pk_index_parallel_compaction_task_split_threshold_bytes

- デフォルト: 33554432
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: プライマリキーインデックスコンパクションタスクの分割閾値。タスクに関連するファイルの合計サイズがこの閾値より小さい場合、タスクは分割されません。デフォルトは 32MB です。
- 導入バージョン: -

### pk_index_parallel_compaction_threadpool_max_threads

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでのクラウドネイティブプライマリキーインデックス並列コンパクション用のスレッドプールの最大スレッド数。0 は CPU コア数の半分に自動設定されることを意味します。
- 導入バージョン: -

### pk_index_parallel_execution_min_rows

- デフォルト: 16384
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでプライマリキーインデックス操作の並列実行を有効にするための最小行数閾値。
- 導入バージョン: -

### pk_index_parallel_execution_threadpool_max_threads

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでのプライマリキーインデックス並列実行用のスレッドプールの最大スレッド数。0 は CPU コア数の半分に自動設定されることを意味します。
- 導入バージョン: -

### pk_index_parallel_load_dels_mem_ratio

- デフォルト: 50
- タイプ: Int
- 単位: パーセント (0-100)
- 変更可能: はい
- 説明: 共有データモードで、update メモリトラッカーの使用量が上限のこの割合を超えている場合、`LakePersistentIndex::load_dels` における並列二段階の delete ファイルプリフェッチをスキップし、一度に 1 つのデコード済み del ファイルカラムのみを保持するシングルパスループにフォールバックします。このメモリ圧迫下では、コールドスタートのレイテンシ改善を諦め、ピークメモリを抑制します。値を大きくすると、より高いメモリ圧迫下でも最適化を許可します。`100` に設定するとメモリゲートが無効化され、`enable_pk_index_parallel_execution=true` かつ複数の del ファイルがある場合は常に並列実行されます。
- 導入バージョン: -

### lake_partial_update_thread_pool_max_threads

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでの部分更新セグメントレベル並列実行用のスレッドプールの最大スレッド数。このスレッドプールは行モード（並列 load_segment + rewrite_segment）と列モード（並列 DCG 生成）の両方の部分更新で使用されます。0 は CPU コア数の半分に自動設定されることを意味します。実行時のオン/オフは `enable_pk_index_parallel_execution` で制御されます。
- 導入バージョン: v4.1

### lake_partial_update_thread_pool_queue_size

- デフォルト: 2048
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 部分更新スレッドプールのタスクキューサイズ。
- 導入バージョン: v4.1

### pk_index_size_tiered_level_multiplier

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: プライマリキーインデックス Size-Tiered コンパクション戦略のレベル倍数パラメータ。
- 導入バージョン: -

### pk_index_size_tiered_max_level

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: プライマリキーインデックス Size-Tiered コンパクション戦略のレベル数パラメータ。
- 導入バージョン: -

### pk_index_size_tiered_min_level_size

- デフォルト: 131072
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: プライマリキーインデックス Size-Tiered コンパクション戦略の最小レベルサイズパラメータ。
- 導入バージョン: -

### pk_index_sstable_sample_interval_bytes

- デフォルト: 16777216
- タイプ: Int
- 単位: Bytes
- 変更可能: はい
- 説明: ストレージ・コンピューティング分離クラスタにおける、プライマリキーインデックス SSTable ファイルのサンプリング間隔サイズ。SSTable ファイルのサイズがこの閾値を超える場合、システムはこの間隔で SSTable からキーをサンプリングし、コンパクションタスクの境界分割を最適化します。この閾値より小さい SSTable については、開始キーのみが境界キーとして使用されます。デフォルトは 16 MB です。
- 導入バージョン: -

### pk_index_target_file_size

- デフォルト: 67108864
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでのプライマリキーインデックスのターゲットファイルサイズ。デフォルトは 64MB です。
- 導入バージョン: -

### pk_index_eager_build_threshold_bytes

- デフォルト: 104857600
- タイプ: Int
- 単位: Bytes
- 変更可能: はい
- 説明: `enable_pk_index_eager_build` が true に設定されている場合、インポートまたはコンパクションで生成されるデータがこの閾値を超えたときのみ、システムは PK インデックスファイルを即座に構築します。デフォルトは 100MB です。
- 導入バージョン: -

### primary_key_limit_size

- デフォルト: 128
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: 主キーテーブルのキー列の最大サイズ。
- 導入バージョン: v2.5

### release_snapshot_worker_count

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ノードでのスナップショットリリースタスクの最大スレッド数。
- 導入バージョン: -

### repair_compaction_interval_seconds

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 修復コンパクションスレッドをポーリングする時間間隔。
- 導入バージョン: -

### replication_max_speed_limit_kbps

- デフォルト: 50000
- タイプ: Int
- 単位: KB/s
- 変更可能: はい
- 説明: 各レプリケーションスレッドの最大速度。
- 導入バージョン: v3.3.5

### replication_min_speed_limit_kbps

- デフォルト: 50
- タイプ: Int
- 単位: KB/s
- 変更可能: はい
- 説明: 各レプリケーションスレッドの最小速度。
- 導入バージョン: v3.3.5

### replication_min_speed_time_seconds

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: レプリケーションスレッドが最小速度を下回ることが許可される時間。実際の速度が `replication_min_speed_limit_kbps` を下回る時間がこの値を超えると、レプリケーションは失敗します。
- 導入バージョン: v3.3.5

### replication_threads

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: レプリケーションに使用される最大スレッド数。`0` は、スレッド数を BE CPU コア数の 4 倍に設定することを示します。
- 導入バージョン: v3.3.5

### size_tiered_level_multiple

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: サイズ階層型コンパクションポリシーにおける、2 つの連続するレベル間のデータサイズの倍率。
- 導入バージョン: -

### size_tiered_level_multiple_dupkey

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: サイズ階層型コンパクションポリシーにおいて、重複キーテーブルの 2 つの隣接するレベル間のデータ量の差の倍率。
- 導入バージョン: -

### size_tiered_level_num

- デフォルト: 7
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: サイズ階層型コンパクションポリシーのレベル数。各レベルには最大で 1 つの rowset が保持されます。したがって、安定した状態では、この設定項目で指定されたレベル数と同じ数の rowset が最大で存在します。
- 導入バージョン: -

### size_tiered_min_level_size

- デフォルト: 131072
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: サイズ階層型コンパクションポリシーの最小レベルのデータサイズ。この値より小さい rowset はすぐにデータコンパクションをトリガーします。
- 導入バージョン: -

### snapshot_expire_time_sec

- デフォルト: 172800
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: スナップショットファイルの有効期限。
- 導入バージョン: -

### stale_memtable_flush_time_sec

- デフォルト: 0
- タイプ: long
- 単位: Seconds
- 変更可能: Yes
- 説明: sender ジョブのメモリ使用量が高いとき、`stale_memtable_flush_time_sec` 秒より長く更新されていない memtable はメモリ圧力を下げるためにフラッシュされます。この動作はメモリ制限に近づいている場合（`limit_exceeded_by_ratio(70)` 以上）のみ考慮されます。`LocalTabletsChannel` ではさらに高いメモリ使用時（`limit_exceeded_by_ratio(95)`）に、`write_buffer_size / 4` より大きいサイズの memtable をフラッシュする追加パスが存在します。値が `0` の場合、年齢に基づく stale-memtable のフラッシュは無効になります（immutable-partition の memtable はアイドル時や高メモリ時に即座にフラッシュされます）。
- 導入バージョン: v3.2.0

### storage_flood_stage_left_capacity_bytes

- デフォルト: 107374182400
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: すべての BE ディレクトリにおける残りのストレージスペースのハードリミット。BE ストレージディレクトリの残りのストレージスペースがこの値より少なく、ストレージ使用率 (パーセンテージ) が `storage_flood_stage_usage_percent` を超える場合、ロードおよびリストアジョブは拒否されます。この項目を FE 設定項目 `storage_usage_hard_limit_reserve_bytes` と一緒に設定する必要があります。
- 導入バージョン: -

### storage_flood_stage_usage_percent

- デフォルト: 95
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: すべての BE ディレクトリにおけるストレージ使用率のハードリミット。BE ストレージディレクトリのストレージ使用率 (パーセンテージ) がこの値を超え、残りのストレージスペースが `storage_flood_stage_left_capacity_bytes` より少ない場合、ロードおよびリストアジョブは拒否されます。この項目を FE 設定項目 `storage_usage_hard_limit_percent` と一緒に設定する必要があります。
- 導入バージョン: -

### storage_medium_migrate_count

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 記憶媒体の移行 (SATA から SSD への移行) に使用されるスレッドの数。
- 導入バージョン: -

### storage_root_path

- デフォルト: `${STARROCKS_HOME}/storage`
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: ストレージボリュームのディレクトリと媒体。例: `/data1,medium:hdd;/data2,medium:ssd`。
  - 複数のボリュームはセミコロン (`;`) で区切られます。
  - ストレージ媒体が SSD の場合、ディレクトリの末尾に `,medium:ssd` を追加します。
  - ストレージ媒体が HDD の場合、ディレクトリの末尾に `,medium:hdd` を追加します。
- 導入バージョン: -

### sync_tablet_meta

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: タブレットメタデータの同期を有効にするかどうかを制御するブール値。`true` は同期を有効にすることを示し、`false` は無効にすることを示します。
- 導入バージョン: -

### tablet_map_shard_size

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: タブレットマップシャードサイズ。値は 2 の累乗でなければなりません。
- 導入バージョン: -

### tablet_max_pending_versions

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 主キー タブレットで許容される最大保留バージョン数。保留バージョンは、コミットされているがまだ適用されていないバージョンを指します。
- 導入バージョン: -

### tablet_max_versions

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: タブレットで許可される最大バージョン数。この値を超えると、新しい書き込みリクエストは失敗します。
- 導入バージョン: -

### tablet_meta_checkpoint_min_interval_secs

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: TabletMeta チェックポイントのスレッドポーリングの時間間隔。
- 導入バージョン: -

### tablet_meta_checkpoint_min_new_rowsets_num

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 最後の TabletMeta チェックポイント以降に作成される最小 rowset 数。
- 導入バージョン: -

### tablet_rowset_stale_sweep_time_sec

- デフォルト: 1800
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: タブレット内の古い rowset をスイープする時間間隔。
- 導入バージョン: -

### tablet_stat_cache_update_interval_second

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: タブレット統計キャッシュが更新される時間間隔。
- 導入バージョン: -

### lake_enable_accurate_pk_row_count

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Lake（共有データ）主キーテーブルの tablet 行数統計で正確な行数を使うかどうか。`true` の場合、各 rowset の delete vector をオブジェクトストレージから取得して削除行を差し引くため精度は上がりますが、`get_tablet_stats` RPC のオーバーヘッドが増える可能性があります。`false` の場合は rowset メタデータの近似 `num_dels` を使ってリモート I/O を回避しますが、未 compaction の削除行をわずかに過大計上する可能性があります。
- 導入バージョン: -

### lake_tablet_stat_slow_log_ms

- デフォルト: 300000
- タイプ: Int64
- 単位: Milliseconds
- 変更可能: はい
- 説明: Tablet 統計収集タスクの遅延ログしきい値（ミリ秒）。単一タスクの実行時間がこの値を超えると、`tablet_id`、バージョン、rowset 数、正確モード、経過時間などの診断情報を含む警告ログを出力します。
- 導入バージョン: -

### lake_metadata_fetch_thread_count

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ストレージとコンピュートの分離テーブル（shared-data table）tablet メタデータ取得操作（`get_tablet_stats`、`get_tablet_metadatas` など）のスレッド数。
- 導入バージョン: v3.5.16, v4.0.9

### transaction_apply_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: Threads
- 変更可能: Yes
- 説明: UpdateManager の "update_apply" スレッドプール（トランザクション、特に primary-key テーブルの rowset を適用するプール）で使用されるワーカースレッドの最大数を制御します。値が `>0` の場合は固定の最大スレッド数を設定し、0（デフォルト）の場合はプールサイズが CPU コア数と同じになります。設定値は起動時（UpdateManager::init）に適用され、update-config HTTP アクションを通じてランタイムで変更でき、プールの最大スレッド数が更新されます。適用の同時実行性（スループット）を上げるか、CPU/メモリの競合を抑えるためにチューニングしてください。最小スレッド数とアイドルタイムアウトはそれぞれ transaction_apply_thread_pool_num_min と transaction_apply_worker_idle_time_ms によって管理されます。
- 導入バージョン: v3.2.0

### trash_file_expire_time_sec

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ゴミファイルをクリーンアップする時間間隔。デフォルト値は v2.5.17、v3.0.9、v3.1.6 以降、259,200 から 86,400 に変更されました。
- 導入バージョン: -

### unused_rowset_monitor_interval

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 期限切れの rowset をクリーンアップする時間間隔。
- 導入バージョン: -

### update_cache_expire_sec

- デフォルト: 360
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Update Cache の有効期限。
- 導入バージョン: -

### update_compaction_check_interval_seconds

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 主キーテーブルのコンパクションをチェックする時間間隔。
- 導入バージョン: -

### update_compaction_delvec_file_io_amp_ratio

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 主キーテーブルの Delvec ファイルを含む rowset のコンパクションの優先順位を制御するために使用されます。値が大きいほど優先順位が高くなります。
- 導入バージョン: -

### update_compaction_num_threads_per_disk

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 主キーテーブルのディスクごとのコンパクションスレッド数。
- 導入バージョン: -

### update_compaction_per_tablet_min_interval_seconds

- デフォルト: 120
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 主キーテーブル内の各タブレットに対してコンパクションがトリガーされる最小時間間隔。
- 導入バージョン: -

### update_compaction_ratio_threshold

- デフォルト: 0.5
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタ内の主キーテーブルに対してコンパクションがマージできるデータの最大割合。単一のタブレットが過度に大きくなる場合、この値を縮小することをお勧めします。
- 導入バージョン: v3.1.5

### update_compaction_result_bytes

- デフォルト: 1073741824
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: 主キーテーブルの単一コンパクションの最大結果サイズ。
- 導入バージョン: -

### update_compaction_size_threshold

- デフォルト: 268435456
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 主キーテーブルのコンパクションスコアはファイルサイズに基づいて計算され、他のテーブルタイプとは異なります。このパラメータは、主キーテーブルのコンパクションスコアを他のテーブルタイプのコンパクションスコアに似せるために使用でき、ユーザーが理解しやすくなります。
- 導入バージョン: -

### upload_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ノードでのバックアップジョブのアップロードタスクの最大スレッド数。`0` は、BE が存在するマシンの CPU コア数に値を設定することを示します。
- 導入バージョン: -

### vertical_compaction_max_columns_per_group

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 垂直コンパクションのグループごとの最大列数。
- 導入バージョン: -
