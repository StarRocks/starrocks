---
displayed_sidebar: docs
---

# 共有データクラスタ向けクロスクラスタデータ移行ツール

StarRocks クロスクラスタデータ移行ツールは、2 つの共有データ（クラウドネイティブ）StarRocks クラスタ間のデータ移行をサポートします。移行中、ターゲットクラスタのコンピュートノード（CN）はソースクラスタのオブジェクトストレージからデータファイルを直接ターゲットクラスタのオブジェクトストレージにコピーします。BE 間のネットワーク転送は不要です。

:::note

- このガイドは、**共有データ**ソースクラスタから**共有データ**ターゲットクラスタへの移行にのみ適用されます。共有なしクラスタからの移行については、[クロスクラスタデータ移行ツール](./data_migration_tool.md)を参照してください。
- ターゲットクラスタは v4.1 以降である必要があります。
- ターゲットクラスタは共有なしクラスタにすることはできません。

:::

## 準備

### ソースクラスタの準備

移行中、ソースクラスタの自動バキューム機能が、ターゲット CN がまだ読み取る必要がある過去のデータバージョンを削除する可能性があります。これを防ぐため、移行開始前にバキュームの猶予期間を延長する必要があります。

1. `lake_autovacuum_grace_period_minutes` を非常に大きな値に設定します。

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="10000000");
   ```

   :::warning
   この設定により、ソースクラスタは移行中に古いオブジェクトストレージファイルを回収できなくなり、ストレージ増幅が発生します。移行ウィンドウをできる限り短くしてください。移行完了後は、この値をデフォルト値（`30`）にリセットすることを忘れないでください。
   :::

2. 移行完了後、値をリセットします。

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="30");
   ```

### ターゲットクラスタの準備

以下の準備は、移行開始前にターゲットクラスタで行う必要があります。

#### ポートを開く

ファイアウォールを有効にしている場合は、以下の FE ポートを開いてください。

| **コンポーネント** | **ポート**   | **デフォルト** |
| ------------------ | ------------ | -------------- |
| FE                 | query_port   | 9030 |
| FE                 | http_port    | 8030 |
| FE                 | rpc_port     | 9020 |

:::note
データはオブジェクトストレージシステム間で直接転送されるため、ソースクラスタの CN または BE ポートを開く必要は**ありません**。
:::

#### コンパクションを無効にする

移行中は、受信するレプリケーションデータとの競合を防ぐため、ターゲットクラスタのコンパクションを無効にする必要があります。

1. コンパクションを動的に無効にします。

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="0");
   ```

2. クラスタ再起動後にコンパクションが再び有効にならないよう、FE 設定ファイル **fe.conf** にも以下の設定を追加します。

   ```Properties
   lake_compaction_max_tasks = 0
   ```

移行完了後は、**fe.conf** から該当設定を削除し、以下のコマンドを実行してコンパクションを再び有効にします。

```SQL
ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="-1");
```

#### レプリケーション用レガシー互換性を有効にする

StarRocks はバージョン間で動作が異なる場合があり、クロスクラスタデータ移行中に問題が生じる可能性があります。移行前にターゲットクラスタのレガシー互換性を有効にし、移行完了後に無効にする必要があります。

1. レプリケーション用レガシー互換性がすでに有効かどうかを確認します。

   ```SQL
   ADMIN SHOW FRONTEND CONFIG LIKE 'enable_legacy_compatibility_for_replication';
   ```

   `true` が返された場合、すでに有効になっています。

2. レプリケーション用レガシー互換性を動的に有効にします。

   ```SQL
   ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="true");
   ```

3. クラスタ再起動時にこの設定が失われないよう、**fe.conf** にも以下を追加します。

   ```Properties
   enable_legacy_compatibility_for_replication = true
   ```

移行完了後は、**fe.conf** から該当設定を削除し、以下のコマンドを実行します。

```SQL
ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="false");
```

#### ターゲットクラスタにソースストレージボリュームを作成する

移行ツールは各ソーステーブルが使用するストレージボリュームを特定し、命名規則 `src_<ソースボリューム名>` を用いてターゲットクラスタ上の対応するストレージボリュームを検索します。移行開始前にこれらのボリュームを事前に作成する必要があります。

1. **ソースクラスタ**で全ストレージボリュームを一覧表示します。

   ```SQL
   SHOW STORAGE VOLUMES;
   ```

2. 移行予定のテーブルが使用する各ストレージボリュームについて、その設定を確認します。

   ```SQL
   DESCRIBE STORAGE VOLUME <volume_name>;
   ```

   出力例：

   ```
   +---------------------+------+-----------+-------------------------------+--------------------------+
   | Name                | Type | IsDefault | Location                      | Params                   |
   +---------------------+------+-----------+-------------------------------+--------------------------+
   | builtin_storage_vol | S3   | true      | s3://my-bucket                | {"aws.s3.region":"...",...} |
   +---------------------+------+-----------+-------------------------------+--------------------------+
   ```

3. **ターゲットクラスタ**で、同じオブジェクトストレージの認証情報を使用し、名前に `src_` プレフィックスを付けたミラーストレージボリュームを作成します。

   ```SQL
   CREATE STORAGE VOLUME src_<ソースボリューム名>
   TYPE = S3
   LOCATIONS = ("<ソースと同じストレージパス>")
   PROPERTIES
   (
       "enabled" = "true",
       "aws.s3.region" = "<リージョン>",
       "aws.s3.endpoint" = "<エンドポイント>",
       "aws.s3.use_aws_sdk_default_behavior" = "false",
       "aws.s3.use_instance_profile" = "false",
       "aws.s3.access_key" = "<アクセスキー>",
       "aws.s3.secret_key" = "<シークレットキー>",
       "aws.s3.enable_partitioned_prefix" = "false"
   );
   ```

   :::note
   - ソースクラスタの設定に関わらず、`aws.s3.enable_partitioned_prefix` は `false` に設定してください。移行ツールはソースパーティションの完全パスを直接使用してファイルを読み取るため、ミラーボリュームにパーティションプレフィックスを適用してはなりません。
   - 移行対象のテーブルが使用する**固有のストレージボリュームごと**にこの手順を繰り返してください。例えばソースが `builtin_storage_volume` を使用している場合は、ターゲットクラスタに `src_builtin_storage_volume` を作成します。
   - ソースストレージボリューム用の認証情報（アクセスキー / シークレットキー）は一時的なものを使用することを推奨します。移行完了後に失効させることができます。
   :::

#### データ移行パラメータの設定（任意）

以下のパラメータを使用してデータ移行の操作を設定できます。ほとんどの場合、デフォルト値で問題ありません。

:::note
以下の設定値を増やすと移行が高速化されますが、ソースクラスタへの負荷も増加します。
:::

**FE パラメータ**（動的設定、再起動不要）：

| **パラメータ**                        | **デフォルト** | **単位** | **説明**                                                     |
| ------------------------------------- | -------------- | -------- | ------------------------------------------------------------ |
| replication_max_parallel_table_count  | 100            | -        | 同時に許可されるテーブル同期タスクの最大数。 |
| replication_max_parallel_replica_count| 10240          | -        | 同時同期を許可するタブレットレプリカの最大数。 |
| replication_max_parallel_data_size_mb | 1048576        | MB       | 同時同期を許可する最大データ量。 |
| replication_transaction_timeout_sec   | 86400          | 秒       | 同期タスクのタイムアウト時間。 |

**BE/CN パラメータ**（動的設定、再起動不要）：

| **パラメータ**      | **デフォルト** | **単位** | **説明**                                                     |
| ------------------- | -------------- | -------- | ------------------------------------------------------------ |
| replication_threads | 0              | -        | 同期タスクを実行するスレッド数。`0` はマシンの CPU コア数の 4 倍に設定します。 |

## ステップ 1: ツールのインストール

移行ツールはターゲットクラスタが存在するサーバーにインストールすることを推奨します。

1. バイナリパッケージをダウンロードします。

   ```Bash
   wget https://releases.starrocks.io/starrocks/starrocks-cluster-sync.tar.gz
   ```

2. パッケージを解凍します。

   ```Bash
   tar -xvzf starrocks-cluster-sync.tar.gz
   ```

## ステップ 2: ツールの設定

### 移行関連の設定

解凍したフォルダに移動し、**conf/sync.properties** を編集します。

```Bash
cd starrocks-cluster-sync
vi conf/sync.properties
```

ファイルの内容は以下の通りです。

```Properties
# If true, all tables will be synchronized only once, and the program will exit automatically after completion.
one_time_run_mode=false

source_fe_host=
source_fe_query_port=9030
source_cluster_user=root
source_cluster_password=
source_cluster_password_secret_key=
# source_cluster_token は共有データソースクラスタでは不要です。空のままにしてください。
source_cluster_token=

target_fe_host=
target_fe_query_port=9030
target_cluster_user=root
target_cluster_password=
target_cluster_password_secret_key=

jdbc_connect_timeout_ms=30000
jdbc_socket_timeout_ms=60000

# Comma-separated list of database names or table names like <db_name> or <db_name.table_name>
# example: db1,db2.tbl2,db3
# Effective order: 1. include 2. exclude
include_data_list=
exclude_data_list=

# If there are no special requirements, please maintain the default values for the following configurations.
target_cluster_storage_volume=
target_cluster_replication_num=-1
target_cluster_max_disk_used_percent=80
# To maintain consistency with the source cluster, use null.
target_cluster_enable_persistent_index=
# Whether to use builtin_storage_volume on the target cluster.
# When set to true, tables created on the target cluster will use builtin_storage_volume uniformly,
# instead of using the source cluster's storage_volume configuration.
# This is useful when the source cluster has multiple custom storage volumes but
# you want to consolidate all tables under one storage volume on the target cluster.
target_cluster_use_builtin_storage_volume_only=false

max_replication_data_size_per_job_in_gb=1024

meta_job_interval_seconds=180
meta_job_threads=4
ddl_job_interval_seconds=5
ddl_job_batch_size=10

# table config
ddl_job_allow_drop_target_only=false
ddl_job_allow_drop_schema_change_table=true
ddl_job_allow_drop_inconsistent_partition=true
ddl_job_allow_drop_inconsistent_time_partition = true
ddl_job_allow_drop_partition_target_only=true
# index config
enable_bitmap_index_sync=false
ddl_job_allow_drop_inconsistent_bitmap_index=true
ddl_job_allow_drop_bitmap_index_target_only=true
# MV config
enable_materialized_view_sync=false
ddl_job_allow_drop_inconsistent_materialized_view=true
ddl_job_allow_drop_materialized_view_target_only=false
# View config
enable_view_sync=false
ddl_job_allow_drop_inconsistent_view=true
ddl_job_allow_drop_view_target_only=false

replication_job_interval_seconds=10
replication_job_batch_size=10
report_interval_seconds=300

enable_table_property_sync=false
```

パラメータの説明：

| **パラメータ**                                | **説明**                                                     |
| --------------------------------------------- | ------------------------------------------------------------ |
| one_time_run_mode                             | 1 回限りの同期モードを有効にするかどうか。有効にすると、移行ツールは全量同期のみを実行して終了します。 |
| source_fe_host                                | ソースクラスタ FE の IP アドレスまたは FQDN。 |
| source_fe_query_port                          | ソースクラスタ FE のクエリポート（`query_port`）。 |
| source_cluster_user                           | ソースクラスタへのログインに使用するユーザー名。このユーザーは SYSTEM レベルの OPERATE 権限が必要です。 |
| source_cluster_password                       | ソースクラスタのユーザーパスワード。 |
| source_cluster_password_secret_key            | `source_cluster_password` を暗号化するための秘密鍵。デフォルトは空文字（暗号化なし）。暗号化する場合は `SELECT TO_BASE64(AES_ENCRYPT('<password>','<secret_key>'))` を実行してください。 |
| source_cluster_token                          | ソースクラスタのトークン。**共有データソースクラスタでは不要です。空のままにしてください。** クラスタトークンは共有なしクラスタの BE 間スナップショット転送の認証にのみ使用されます。共有データクラスタでは、ファイルはオブジェクトストレージから直接読み取られ、BE は関与しません。 |
| target_fe_host                                | ターゲットクラスタ FE の IP アドレスまたは FQDN。 |
| target_fe_query_port                          | ターゲットクラスタ FE のクエリポート（`query_port`）。 |
| target_cluster_user                           | ターゲットクラスタへのログインに使用するユーザー名。このユーザーは SYSTEM レベルの OPERATE 権限が必要です。 |
| target_cluster_password                       | ターゲットクラスタのユーザーパスワード。 |
| target_cluster_password_secret_key            | `target_cluster_password` を暗号化するための秘密鍵。`source_cluster_password_secret_key` と同じ暗号化方式です。 |
| jdbc_connect_timeout_ms                       | FE クエリの JDBC 接続タイムアウト（ミリ秒）。デフォルト：`30000`。 |
| jdbc_socket_timeout_ms                        | FE クエリの JDBC ソケットタイムアウト（ミリ秒）。デフォルト：`60000`。 |
| include_data_list                             | 移行するデータベースとテーブル（カンマ区切り）。例：`db1, db2.tbl2, db3`。`exclude_data_list` より優先されます。全て移行する場合は不要です。 |
| exclude_data_list                             | 移行しないデータベースとテーブル（カンマ区切り）。`include_data_list` が優先されます。全て移行する場合は不要です。 |
| target_cluster_storage_volume                 | ターゲットクラスタで新規作成されるテーブルに使用するストレージボリューム。空の場合、ターゲットクラスタのデフォルトストレージボリュームが使用されます。`src_<name>` ボリュームとは別のものです。 |
| target_cluster_replication_num               | ターゲットクラスタでテーブルを作成する際のレプリカ数。`-1` はソースクラスタと同じ数を使用します。 |
| target_cluster_max_disk_used_percent          | （共有なしターゲットクラスタにのみ適用）BE ノードのディスク使用率の上限。いずれかの BE がこの値を超えると移行が停止します。デフォルト：`80`（80%）。 |
| target_cluster_enable_persistent_index        | ターゲットクラスタで永続インデックスを有効にするかどうか。空（デフォルト）の場合はソースクラスタと一致します。 |
| target_cluster_use_builtin_storage_volume_only | `true` の場合、ターゲットクラスタの全テーブルが `builtin_storage_volume` を使用し、ソースクラスタのストレージボリューム設定を無視します。デフォルト：`false`。 |
| meta_job_interval_seconds                     | ソース・ターゲットクラスタからメタデータを取得する間隔（秒）。 |
| meta_job_threads                              | メタデータ取得に使用するスレッド数。 |
| ddl_job_interval_seconds                      | ターゲットクラスタで DDL 文を実行する間隔（秒）。 |
| ddl_job_batch_size                            | ターゲットクラスタで DDL 文を実行するバッチサイズ。 |
| ddl_job_allow_drop_target_only                | ソースクラスタに存在せずターゲットクラスタにのみ存在するデータベースやテーブルを削除するかどうか。デフォルト：`false`（削除しない）。 |
| ddl_job_allow_drop_schema_change_table        | ソース・ターゲット間でスキーマが一致しないテーブルを削除するかどうか。デフォルト：`true`（削除する）。ツールが移行中に自動で再同期します。 |
| ddl_job_allow_drop_inconsistent_partition     | データ分布が一致しないパーティションを削除するかどうか。デフォルト：`true`（削除する）。ツールが移行中に自動で再同期します。 |
| ddl_job_allow_drop_partition_target_only      | ソースクラスタで削除されたパーティションをターゲットからも削除するかどうか。デフォルト：`true`（削除する）。 |
| replication_job_interval_seconds              | データ同期タスクをトリガーする間隔（秒）。 |
| replication_job_batch_size                    | データ同期タスクをトリガーするバッチサイズ。 |
| max_replication_data_size_per_job_in_gb       | 同期ジョブあたりのデータ量の閾値（GB）。この値を超えると複数の同期タスクに分割されます。デフォルト：`1024`。 |
| report_interval_seconds                       | 進捗情報を出力する間隔（秒）。デフォルト：`300`。 |
| enable_bitmap_index_sync                      | Bitmap インデックスを同期するかどうか。デフォルト：`false`。 |
| ddl_job_allow_drop_inconsistent_bitmap_index  | ソース・ターゲット間で一致しない Bitmap インデックスを削除するかどうか。デフォルト：`true`（削除する）。 |
| ddl_job_allow_drop_bitmap_index_target_only   | ソースクラスタで削除された Bitmap インデックスをターゲットからも削除するかどうか。デフォルト：`true`（削除する）。 |
| enable_materialized_view_sync                 | マテリアライズドビューを同期するかどうか。デフォルト：`false`。 |
| ddl_job_allow_drop_inconsistent_materialized_view | ソース・ターゲット間で一致しないマテリアライズドビューを削除するかどうか。デフォルト：`true`（削除する）。 |
| ddl_job_allow_drop_materialized_view_target_only | ソースクラスタで削除されたマテリアライズドビューをターゲットからも削除するかどうか。デフォルト：`false`（削除しない）。 |
| enable_view_sync                              | 論理ビューを同期するかどうか。デフォルト：`false`。 |
| ddl_job_allow_drop_inconsistent_view          | ソース・ターゲット間で一致しない論理ビューを削除するかどうか。デフォルト：`true`（削除する）。 |
| ddl_job_allow_drop_view_target_only           | ソースクラスタで削除された論理ビューをターゲットからも削除するかどうか。デフォルト：`false`（削除しない）。 |
| enable_table_property_sync                    | テーブルプロパティを同期するかどうか。デフォルト：`false`。 |

:::note
**プライマリキーテーブルの永続インデックス**：2 つの共有データクラスタ間で移行する場合、ツールはプライマリキーテーブルの CREATE TABLE 文内の `persistent_index_type = LOCAL` を自動的に `CLOUD_NATIVE` に変換します。手動での操作は不要です。
:::

### ストレージボリュームのマッピング

移行ツールがターゲットクラスタでテーブルを作成する際、テーブルのストレージボリュームは以下の優先順位で決定されます。

1. `target_cluster_use_builtin_storage_volume_only = true` の場合：全テーブルが `builtin_storage_volume` を使用します。
2. `target_cluster_storage_volume = <名前>` が設定されている場合：全テーブルが指定されたストレージボリュームを使用します。
3. いずれも設定されていない場合（デフォルト）：ソーステーブルのストレージボリューム名をそのまま使用します。異なるソースストレージボリューム上のテーブルは、ターゲットクラスタ上の対応するストレージボリュームに作成されます（それらのストレージボリュームがターゲットに存在する場合）。

つまり、**ターゲットクラスタでは `src_` プレフィックスを持たない複数のストレージボリュームを使用できます**。例えば、ソースクラスタのテーブルが `ssd_volume` と `oss_volume` に分散している場合、ターゲットクラスタに両方のボリュームを事前に作成しておけば、移行後に各テーブルが対応するボリュームに配置されます。

`src_<名前>` ストレージボリュームは別の目的に使用されます。これらは**移行中のみ**使用され、ターゲット CN にソースクラスタのオブジェクトストレージへの読み取りアクセスを提供します。移行完了後は `src_<名前>` ボリュームは不要になり、削除できます。

### ネットワーク関連の設定（任意）

データ移行中、移行ツールはソースクラスタとターゲットクラスタ両方の**全て**の FE ノードにアクセスする必要があります。

:::note
共有なしから共有データへの移行とは異なり、データはオブジェクトストレージシステム間で直接転送されるため、ターゲットクラスタからソースクラスタの CN ノードへのネットワークアクセスを設定する必要は**ありません**。
:::

各クラスタで以下のコマンドを実行して FE ノードのネットワークアドレスを取得できます。

```SQL
-- FE ノード
SHOW FRONTENDS;
```

FE ノードが Kubernetes クラスタ内部アドレスなどのプライベートアドレスを使用していてクラスタ外からアクセスできない場合は、**conf/hosts.properties** にアドレスマッピングを追加する必要があります。

```Bash
cd starrocks-cluster-sync
vi conf/hosts.properties
```

フォーマット：

```Properties
# <SOURCE/TARGET>_<host>=<mappedHost>[;<srcPort>:<dstPort>[,<srcPort>:<dstPort>...]]
```

:::note
`<host>` は `SHOW FRONTENDS` の返り値の `IP` カラムに表示されるアドレスと完全に一致する必要があります。
:::

例：ターゲットクラスタの内部 Kubernetes FQDN をアクセス可能な IP にマッピングする場合：

```Properties
TARGET_frontend-0.frontend.mynamespace.svc.cluster.local=10.1.2.1;9030:19030
```

## ステップ 3: 移行ツールの起動

設定完了後、移行ツールを起動してデータ移行を開始します。

```Bash
./bin/start.sh
```

:::note

- 移行ツールはターゲットクラスタのデータがソースクラスタより遅れているかどうかを定期的にチェックし、遅れがある場合は同期タスクを開始します。
- ソースクラスタに新しいデータが継続的にロードされている場合、ターゲットクラスタが完全に追いつくまで同期が続きます。
- 移行中はターゲットクラスタのテーブルをクエリできますが、ターゲットクラスタに新しいデータをロードしないでください。データの不整合が生じる可能性があります。
- 移行ツールは自動的に停止しません。移行が完了したことを手動で確認してからツールを停止してください。

:::

## 移行進捗の確認

### 移行ツールのログを確認する

ログファイル **log/sync.INFO.log** で移行の進捗を確認します。

**タスク進捗の確認**（`Sync job progress` を検索）：

![img](../_assets/data_migration_tool-1.png)

主要な指標：

| **指標**    | **説明** |
| ----------- | -------- |
| `total`     | この移行実行中の全ジョブ数。 |
| `ddlPending`| 保留中の DDL ジョブ数。 |
| `jobPending`| 保留中のデータ同期ジョブ数。 |
| `sent`      | ソースクラスタから送信されたがまだ開始されていないデータ同期ジョブ数。この値が増え続ける場合はサポートに連絡してください。 |
| `running`   | 現在実行中のデータ同期ジョブ数。 |
| `finished`  | 完了したデータ同期ジョブ数。 |
| `failed`    | 失敗したデータ同期ジョブ数。失敗したジョブは自動的に再試行されます。大量の失敗が継続する場合は調査が必要です。 |
| `unknown`   | 不明な状態のジョブ数。常に `0` である必要があります。 |

`Sync job progress` が 100% の場合、現在のチェック間隔内の全データ同期が完了したことを意味します。ソースクラスタに新しいデータが継続的にロードされている場合、次の間隔で進捗が下がることがありますが、これは正常です。

**テーブル移行進捗の確認**（`Sync table progress` を検索）：

![img](../_assets/data_migration_tool-2.png)

| **指標**            | **説明** |
| ------------------- | -------- |
| `finishedTableRatio`| 少なくとも 1 回成功した同期があるテーブルの割合。 |
| `expiredTableRatio` | 期限切れデータを持つテーブルの割合。 |
| `total table`       | この移行実行でのテーブルの総数。 |
| `finished table`    | 少なくとも 1 回成功した同期があるテーブル数。 |
| `unfinished table`  | まだ成功した同期がないテーブル数。 |
| `expired table`     | 期限切れデータを持つテーブル数。 |

### 移行トランザクションの状態を確認する

移行ツールはテーブルごとに 1 つのトランザクションを開きます。以下で状態を確認します。

```SQL
SHOW PROC "/transactions/<db_name>/running";
```

### パーティションデータバージョンを確認する

ソースとターゲット間でパーティションのバージョンを比較して移行状態を確認します。

```SQL
SHOW PARTITIONS FROM <table_name>;
```

### データ量を確認する

```SQL
SHOW DATA;
```

### テーブルの行数を確認する

```SQL
SELECT
  TABLE_NAME,
  TABLE_ROWS
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
```

## 移行後の作業

`Sync job progress` が安定して 100% を維持し、ビジネス的に切り替え可能な状態になったら、以下の手順でカットオーバーを完了させます。

1. ソースクラスタへの書き込みを停止します。
2. 書き込みを停止した後、`Sync job progress` が 100% に達し維持されることを確認します。
3. 移行ツールを停止します。
4. アプリケーションのデータソースをターゲットクラスタのアドレスに切り替えます。
5. ソースクラスタの自動バキューム設定をリセットします。

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="30");
   ```

6. ターゲットクラスタのコンパクションを再び有効にします。**fe.conf** から `lake_compaction_max_tasks = 0` を削除し、以下を実行します。

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="-1");
   ```

7. ターゲットクラスタのレプリケーション用レガシー互換性を無効にします。**fe.conf** から `enable_legacy_compatibility_for_replication = true` を削除し、以下を実行します。

   ```SQL
   ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="false");
   ```

## 制限

同期をサポートするオブジェクトタイプは以下の通りです（リストにないものは同期非対応）：

- データベース
- 内部テーブルとそのデータ
- マテリアライズドビューのスキーマとビルド文（マテリアライズドビューのデータは同期されません。ベーステーブルが移行されていない場合、バックグラウンドリフレッシュタスクがエラーを報告します）
- 論理ビュー

共有データから共有データへの移行における追加制限：

- ターゲットクラスタは共有データクラスタ（v4.1 以降）である必要があります。共有なしターゲットへの移行はサポートされません。
- ソースクラスタのテーブルが使用する各ストレージボリュームに対して、対応する `src_<ボリューム名>` ストレージボリュームがターゲットクラスタに事前に作成されている必要があります。
