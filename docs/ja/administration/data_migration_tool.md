---
displayed_sidebar: docs
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# クロスクラスターデータ移行ツール

StarRocksクロスクラスターデータ移行ツールは、StarRocksコミュニティによって提供されています。このツールを使用すると、ソースクラスターからターゲットクラスターへデータを簡単に移行できます。

| 移行パス | サポート情報 |
| ------------------------------------- | ------------------------------ |
| Shared-nothingからShared-nothingへ | v3.1.8およびv3.2.3以降 |
| Shared-nothingからShared-dataへ | v3.1.8およびv3.2.3以降 |
| Shared-dataからShared-dataへ | v4.1以降 |
| Shared-dataからShared-nothingへ | サポートされていません |

## 準備

<Tabs groupId="migrationPath">
  <TabItem value="sourceNothing" label="Migrate from Shared-nothing" default>
    ### ソースクラスター上

    ソースクラスター上で準備を行う必要はありません。

    ### ターゲットクラスター上

    データ移行のために、ターゲットクラスター上で以下の準備を行う必要があります。

    #### ポートを開く

    ファイアウォールを有効にしている場合、以下のポートを開く必要があります。

    | **コンポーネント** | **ポート**     | **デフォルト** |
| ----------- | -------------- | ----------- |
| FE          | query_port     | 9030        |
| FE          | http_port      | 8030        |
| FE          | rpc_port       | 9020        |
| BE/CN       | be_http_port   | 8040        |
| BE/CN       | be_port        | 9060        |
  </TabItem>

  <TabItem value="sourceData" label="Migrate between Shared-data">
    ### ソースクラスター上

    移行中、ソースクラスターのAuto-Vacuumメカニズムが、ターゲットCNがまだ読み取る必要のある履歴データバージョンを削除する可能性があります。この状況を防ぐため、FE設定項目`lake_autovacuum_grace_period_minutes`を動的に非常に大きな値に設定して、Auto-Vacuumの猶予期間を延長する必要があります。

    ```SQL
    ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="10000000");
    ```

    :::important

    この設定は、移行中にソースクラスターが古いオブジェクトストレージファイルを再利用するのを防ぎ、ストレージの増幅を引き起こします。移行期間は可能な限り短く保ち、移行後にこの項目をデフォルト値`30`にリセットすることをお勧めします。

    ```SQL
    ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="30");
    ```

    :::

    ### ターゲットクラスター上

    データ移行のために、ターゲットクラスター上で以下の準備を行う必要があります。

    #### ポートを開く

    ファイアウォールを有効にしている場合、以下のポートを開く必要があります。

    | **コンポーネント** | **ポート**     | **デフォルト** |
| ----------- | -------------- | ----------- |
| FE          | query_port     | 9030        |
| FE          | http_port      | 8030        |
| FE          | rpc_port       | 9020        |

    #### コンパクションを無効にする

    移行中に受信レプリケーションデータとの競合を防ぐため、ターゲットクラスター上でコンパクションを無効にする必要があります。

    1. コンパクションを動的に無効にする:

       ```SQL
       ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="0");
       ```

    2. クラスターの再起動後にコンパクションが再度有効になるのを防ぐため、FE設定ファイルに以下の設定も追加してください。**fe.conf**:

       ```Properties
       lake_compaction_max_tasks = 0
       ```

    :::important

    移行が完了したら、上記の構成を削除してCompactionを再度有効にします。**fe.conf**、そして以下を実行してCompactionを動的に有効にします。

    ```SQL
    ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="-1");
    ```

    :::

    #### ターゲットクラスターにソースストレージボリュームを作成する

    移行ツールは、各ソーステーブルが使用するストレージボリュームを識別し、`src_<source_volume_name>`という命名規則を使用してターゲットクラスター上の対応するストレージボリュームを検索します。移行を開始する前に、これらのストレージボリュームを事前に作成する必要があります。

    :::important
これらのストレージボリュームは、**移行中のみ**ターゲットCNにソースクラスターのオブジェクトストレージへの読み取りアクセスを許可するために使用されます。移行が完了した後、これらは不要になり、削除できます。
:::

    1. **ソースクラスター**、すべてのストレージボリュームをリストします。

       ```SQL
       SHOW STORAGE VOLUMES;
       ```

    2. 移行する予定のテーブルが使用する各ストレージボリュームについて、その構成を取得するために記述します。

       ```SQL
       DESCRIBE STORAGE VOLUME <volume_name>;
       ```

       出力例:

       ```
       +---------------------+------+-----------+-------------------------------+-----------------------------+
       | Name                | Type | IsDefault | Location                      | Params                      |
       +---------------------+------+-----------+-------------------------------+-----------------------------+
       | builtin_storage_vol | S3   | true      | s3://my-bucket                | {"aws.s3.region":"...",...} |
       +---------------------+------+-----------+-------------------------------+-----------------------------+
       ```

    3. **ターゲットクラスター**、同じオブジェクトストレージ認証情報を使用してミラーリングされたストレージボリュームを作成しますが、名前には`src_`をプレフィックスとして付けます。

       ```SQL
       CREATE STORAGE VOLUME src_<source_volume_name>
       TYPE = S3
       LOCATIONS = ("<same_location_as_source>")
       PROPERTIES
       (
           "enabled" = "true",
           "aws.s3.region" = "<region>",
           "aws.s3.endpoint" = "<endpoint>",
           "aws.s3.use_aws_sdk_default_behavior" = "false",
           "aws.s3.use_instance_profile" = "false",
           "aws.s3.access_key" = "<access_key>",
           "aws.s3.secret_key" = "<secret_key>",
           "aws.s3.enable_partitioned_prefix" = "false"
       );
       ```

       :::note

       - ソースクラスターの設定に関係なく、`aws.s3.enable_partitioned_prefix`を`false`に設定します。移行ツールはソースパーティションのフルパスを使用してファイルを直接読み取るため、パーティション化されたプレフィックスをミラーリングされたボリュームに適用してはなりません。
       - この手順を**各**移行するテーブルが使用する一意のストレージボリュームごとに繰り返します。たとえば、ソースが`builtin_storage_volume`を使用する場合、ターゲットクラスターに`src_builtin_storage_volume`を作成します。
       - ソースストレージボリュームには一時的な認証情報（アクセスキー/シークレットキー）を使用することをお勧めします。これらは移行完了後に取り消すことができます。
:::
  </TabItem>
</Tabs>

#### レプリケーションのレガシー互換性を有効にする

StarRocksは、旧バージョンと新バージョンの間で動作が異なる場合があり、クラスター間のデータ移行中に問題を引き起こす可能性があります。そのため、データ移行前にターゲットクラスターのレガシー互換性を有効にし、データ移行完了後に無効にする必要があります。

1. レプリケーションのレガシー互換性が有効になっているかどうかは、次のステートメントを使用して確認できます。

   ```SQL
   ADMIN SHOW FRONTEND CONFIG LIKE 'enable_legacy_compatibility_for_replication';
   ```

   `true`が返された場合、レプリケーションのレガシー互換性が有効になっていることを示します。

2. レプリケーションのレガシー互換性を動的に有効にする:

   ```SQL
   ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="true");
   ```

3. クラスターの再起動時にデータ移行プロセス中にレプリケーションのレガシー互換性が自動的に無効になるのを防ぐため、FE構成ファイルに次の構成項目を追加する必要もあります。**fe.conf**:

   ```Properties
   enable_legacy_compatibility_for_replication = true
   ```

:::important

データ移行が完了したら、構成ファイルから`enable_legacy_compatibility_for_replication = true`の構成を削除し、次のステートメントを使用してレプリケーションのレガシー互換性を動的に無効にする必要があります。

```SQL
ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="false");
```

:::

#### データ移行の構成（オプション）

次のFEおよびBEパラメーターを使用してデータ移行操作を構成できます。ほとんどの場合、デフォルトの構成でニーズを満たすことができます。デフォルトの構成を使用したい場合は、この手順をスキップできます。

:::note

以下の構成項目の値を増やすと移行を高速化できますが、ソースクラスターへの負荷も増加することに注意してください。

:::

#### FEパラメーター

以下のFEパラメータは動的設定項目です。参照してください。[FE動的パラメータの設定](../administration/management/FE_configuration.md#configure-fe-dynamic-parameters)で変更方法を確認してください。

| **パラメータ**                         | **デフォルト** | **単位** | **説明**                                              |
| ------------------------------------- | ----------- | -------- | ------------------------------------------------------------ |
| replication_max_parallel_table_count  | 100         | -        | 許可される同時データ同期タスクの最大数。StarRocksはテーブルごとに1つの同期タスクを作成します。 |
| replication_max_parallel_replica_count| 10240       | -        | 同時同期に許可されるタブレットレプリカの最大数。 |
| replication_max_parallel_data_size_mb | 1048576     | MB       | 同時同期に許可されるデータの最大サイズ。 |
| replication_transaction_timeout_sec   | 86400       | 秒       | 同期タスクのタイムアウト期間。              |

#### BEパラメータ

以下のBEパラメータは動的設定項目です。参照してください。[BE動的パラメータの設定](../administration/management/BE_configuration.md)で変更方法を確認してください。

| **パラメータ**       | **デフォルト** | **単位** | **説明**                                              |
| ------------------- | ----------- | -------- | ------------------------------------------------------------ |
| replication_threads | 0           | -        | 同期タスクを実行するためのスレッド数。`0` は、BEが配置されているマシンのCPUコア数の4倍にスレッド数を設定することを示します。 |

## ステップ1: ツールをインストールする

移行ツールは、ターゲットクラスターが配置されているサーバーにインストールすることをお勧めします。

1. ターミナルを起動し、ツールのバイナリパッケージをダウンロードします。

   ```Bash
   wget https://releases.starrocks.io/starrocks/starrocks-cluster-sync.tar.gz
   ```

2. パッケージを解凍します。

   ```Bash
   tar -xvzf starrocks-cluster-sync.tar.gz
   ```

## ステップ2: ツールを設定する

### 移行関連の設定

展開されたフォルダーに移動し、設定ファイル を変更します。**conf/sync.properties**。

```Bash
cd starrocks-cluster-sync
vi conf/sync.properties
```

ファイルの内容は以下のとおりです。

```Properties
# If true, all tables will be synchronized only once, and the program will exit automatically after completion.
one_time_run_mode=false

source_fe_host=
source_fe_query_port=9030
source_cluster_user=root
source_cluster_password=
source_cluster_password_secret_key=

# You can leave this empty or omit it if you want to migrate data between shared-data source clusters.
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
# This configuration item is for migration between shared-data clusters only.
target_cluster_use_builtin_storage_volume_only=false
target_cluster_replication_num=-1
target_cluster_max_disk_used_percent=80
# To maintain consistency with the source cluster, use null.
target_cluster_enable_persistent_index=

max_replication_data_size_per_job_in_gb=1024

meta_job_interval_seconds=180
meta_job_threads=4
ddl_job_interval_seconds=10
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

パラメータの説明は以下のとおりです。

| **パラメーター**                             | **説明**                                              |
| ----------------------------------------- | ------------------------------------------------------------ |
| one_time_run_mode                         | ワンタイム同期モードを有効にするかどうか。ワンタイム同期モードが有効な場合、移行ツールは増分同期ではなく、完全同期のみを実行します。 |
| source_fe_host                            | ソースクラスターのFEのIPアドレスまたはFQDN（完全修飾ドメイン名）。 |
| source_fe_query_port                      | ソースクラスターのFEのクエリポート (`query_port`)。    |
| source_cluster_user                       | ソースクラスターにログインするために使用されるユーザー名。このユーザーには、SYSTEMレベルでOPERATE権限が付与されている必要があります。 |
| source_cluster_password                   | ソースクラスターにログインするために使用されるユーザーパスワード。      |
| source_cluster_password_secret_key        | ソースクラスターのログインユーザーのパスワードを暗号化するために使用されるシークレットキー。デフォルト値は空の文字列であり、ログインパスワードが暗号化されていないことを意味します。`source_cluster_password` を暗号化したい場合は、SQLステートメント `SELECT TO_BASE64(AES_ENCRYPT('<source_cluster_password>','<source_cluster_password_ secret_key>'))` を使用して暗号化された `source_cluster_password` 文字列を取得できます。 |
| source_cluster_token                      | ソースクラスターのトークン。クラスターのトークンを取得する方法については、以下を参照してください。 [クラスタートークンの取得](#obtain-cluster-token)。 <br />**注記**<br />共有データクラスター間での移行では、ファイルがオブジェクトストレージから直接読み取られるため、クラスタートークンは不要です。共有データソースクラスター間でデータを移行したい場合は、これを空のままにするか、省略できます。 |
| target_fe_host | ターゲットクラスターのFEのIPアドレスまたはFQDN（完全修飾ドメイン名）。 |
| target_fe_query_port | ターゲットクラスターのFEのクエリポート（`query_port`）。 |
| target_cluster_user | ターゲットクラスターへのログインに使用するユーザー名。このユーザーには、SYSTEMレベルでOPERATE権限が付与されている必要があります。 |
| target_cluster_password | ターゲットクラスターへのログインに使用するユーザーパスワード。 |
| target_cluster_password_secret_key | ターゲットクラスターのログインユーザーのパスワードを暗号化するために使用されるシークレットキー。デフォルト値は空文字列で、ログインパスワードが暗号化されていないことを意味します。`target_cluster_password`を暗号化したい場合は、SQLステートメント`SELECT TO_BASE64(AES_ENCRYPT('<target_cluster_password>','<target_cluster_password_ secret_key>'))`を使用して暗号化された`target_cluster_password`文字列を取得できます。 |
| jdbc_connect_timeout_ms | FEクエリのJDBC接続タイムアウト（ミリ秒）。デフォルト: `30000`。 |
| jdbc_socket_timeout_ms | FEクエリのJDBCソケットタイムアウト（ミリ秒）。デフォルト: `60000`。 |
| include_data_list | 移行する必要があるデータベースとテーブル。複数のオブジェクトはカンマ（`,`）で区切ります。例: `db1, db2.tbl2, db3`。この項目は`exclude_data_list`よりも優先されます。クラスター内のすべてのデータベースとテーブルを移行したい場合は、この項目を設定する必要はありません。 |
| exclude_data_list | 移行する必要がないデータベースとテーブル。複数のオブジェクトはカンマ（`,`）で区切ります。例: `db1, db2.tbl2, db3`。`include_data_list`はこの項目よりも優先されます。クラスター内のすべてのデータベースとテーブルを移行したい場合は、この項目を設定する必要はありません。 |
| target_cluster_storage_volume | ターゲットクラスターが共有データクラスターである場合に、ターゲットクラスターでテーブルを保存するために使用されるストレージボリューム。デフォルトのストレージボリュームを使用したい場合は、この項目を指定する必要はありません。 |
| target_cluster_use_builtin_storage_volume_only | ターゲットクラスターでの移行に`builtin_storage_volume`を使用するかどうか。共有データクラスター間の移行にのみ必要です。この項目が`true`に設定されている場合、ターゲットクラスターで作成されるテーブルは、ソースクラスターの`storage_volume`設定を使用する代わりに、`builtin_storage_volume`を統一的に使用します。これは、ソースクラスターに複数のカスタムストレージボリュームがあるが、ターゲットクラスターですべてのテーブルを1つのストレージボリュームに統合したい場合に役立ちます。 |
| target_cluster_replication_num | ターゲットクラスターでテーブルを作成する際に指定されるレプリカの数。ソースクラスターと同じレプリカ数を使用したい場合は、この項目を指定する必要はありません。 |
| target_cluster_max_disk_used_percent | ターゲットクラスターが共有なしの場合の、ターゲットクラスターのBEノードのディスク使用率しきい値。ターゲットクラスターのいずれかのBEのディスク使用率がこのしきい値を超えると、移行は終了します。デフォルト値は`80`で、80%を意味します。 |
| meta_job_interval_seconds | 移行ツールがソースおよびターゲットクラスターからメタデータを取得する間隔（秒）。この項目にはデフォルト値を使用できます。 |
| meta_job_threads | 移行ツールがソースおよびターゲットクラスターからメタデータを取得するために使用するスレッド数。この項目にはデフォルト値を使用できます。 |
| ddl_job_interval_seconds | 移行ツールがターゲットクラスターでDDLステートメントを実行する間隔（秒）。この項目にはデフォルト値を使用できます。 |
| ddl_job_batch_size | ターゲットクラスターでDDLステートメントを実行するバッチサイズ。この項目にはデフォルト値を使用できます。 |
| ddl_job_allow_drop_target_only | 移行ツールが、ターゲットクラスターにのみ存在し、ソースクラスターには存在しないデータベースまたはテーブルを削除することを許可するかどうか。デフォルトは`false`で、削除されないことを意味します。この項目にはデフォルト値を使用できます。 |
| ddl_job_allow_drop_schema_change_table | 移行ツールが、ソースクラスターとターゲットクラスター間でスキーマが不整合なテーブルを削除することを許可するかどうか。デフォルトは`true`で、削除されることを意味します。この項目にはデフォルト値を使用できます。移行ツールは、移行中に削除されたテーブルを自動的に同期します。 |
| ddl_job_allow_drop_inconsistent_partition | 移行ツールが、ソースクラスターとターゲットクラスター間でデータ分散が不整合なパーティションを削除することを許可するかどうか。デフォルトは`true`で、削除されることを意味します。この項目にはデフォルト値を使用できます。移行ツールは、移行中に削除されたパーティションを自動的に同期します。 |
| ddl_job_allow_drop_partition_target_only | 移行ツールが、ソースクラスターで削除されたパーティションを削除して、ソースクラスターとターゲットクラスター間でパーティションの一貫性を保つことを許可するかどうか。デフォルトは`true`で、削除されることを意味します。この項目にはデフォルト値を使用できます。 |
| replication_job_interval_seconds | 移行ツールがデータ同期タスクをトリガーする間隔（秒）。この項目にはデフォルト値を使用できます。 |
| replication_job_batch_size | 移行ツールがデータ同期タスクをトリガーするバッチサイズ。この項目にはデフォルト値を使用できます。 |
| max_replication_data_size_per_job_in_gb | 移行ツールがデータ同期タスクをトリガーするデータサイズしきい値。単位: GB。移行するパーティションのサイズがこの値を超えると、複数のデータ同期タスクがトリガーされます。デフォルト値は`1024`です。この項目にはデフォルト値を使用できます。 |
| report_interval_seconds | 移行ツールが進行状況情報を出力する時間間隔。単位: 秒。デフォルト値: `300`。この項目にはデフォルト値を使用できます。 |
| target_cluster_enable_persistent_index | ターゲットクラスターで永続インデックスを有効にするかどうか。この項目が指定されていない場合、ターゲットクラスターはソースクラスターと一貫しています。 <br />**注記**<br />2つの共有データクラスター間でデータを移行する場合、ツールはPrimary KeyテーブルのCREATE TABLEステートメントで`persistent_index_type = LOCAL`を`CLOUD_NATIVE`に自動的に変換します。手動での操作は不要です。 |
| ddl_job_allow_drop_inconsistent_time_partition | 移行ツールが、ソースクラスターとターゲットクラスター間で時間が不整合なパーティションを削除することを許可するかどうか。デフォルトは`true`で、削除されることを意味します。この項目にはデフォルト値を使用できます。移行ツールは、移行中に削除されたパーティションを自動的に同期します。 |
| enable_bitmap_index_sync | ビットマップインデックスの同期を有効にするかどうか。 |
| ddl_job_allow_drop_inconsistent_bitmap_index | 移行ツールが、ソースクラスターとターゲットクラスター間で不整合なビットマップインデックスを削除することを許可するかどうか。デフォルトは`true`で、削除されることを意味します。この項目にはデフォルト値を使用できます。移行ツールは、移行中に削除されたインデックスを自動的に同期します。 |
| ddl_job_allow_drop_bitmap_index_target_only | 移行ツールが、ソースクラスターで削除されたビットマップインデックスを削除して、ソースクラスターとターゲットクラスター間でインデックスの一貫性を保つことを許可するかどうか。デフォルトは`true`で、削除されることを意味します。この項目にはデフォルト値を使用できます。 |
| enable_materialized_view_sync | マテリアライズドビューの同期を有効にするかどうか。 |
| ddl_job_allow_drop_inconsistent_materialized_view | 移行ツールが、ソースクラスターとターゲットクラスター間で不整合なマテリアライズドビューを削除することを許可するかどうか。デフォルトは`true`で、削除されることを意味します。この項目にはデフォルト値を使用できます。移行ツールは、移行中に削除されたマテリアライズドビューを自動的に同期します。 |
| ddl_job_allow_drop_materialized_view_target_only | 移行ツールが、ソースクラスターで削除されたマテリアライズドビューを削除して、ソースクラスターとターゲットクラスター間でマテリアライズドビューの一貫性を保つことを許可するかどうか。デフォルトは`true`で、削除されることを意味します。この項目にはデフォルト値を使用できます。 |
| enable_view_sync | ビューの同期を有効にするかどうか。 |
| ddl_job_allow_drop_inconsistent_view | 移行ツールが、ソースクラスターとターゲットクラスター間で不整合なビューを削除することを許可するかどうか。デフォルトは`true`で、削除されることを意味します。この項目にはデフォルト値を使用できます。移行ツールは、移行中に削除されたビューを自動的に同期します。 |
| ddl_job_allow_drop_view_target_only | 移行ツールが、ソースクラスターで削除されたビューを削除して、ソースクラスターとターゲットクラスター間でビューの一貫性を保つことを許可するかどうか。デフォルトは`true`で、削除されることを意味します。この項目にはデフォルト値を使用できます。 |
| enable_table_property_sync | テーブルプロパティの同期を有効にするかどうか。 |

<Tabs groupId="migrationPath">
  <TabItem value="sourceNothing" label="Migrate from Shared-nothing" default>
    ### クラスタートークンの取得

    :::note
共有データクラスター間の移行では、クラスタートークンは不要です。共有データソースクラスター間でデータを移行したい場合は、この手順をスキップできます。
:::

    クラスタートークンはFEメタデータで利用可能です。FEノードが配置されているサーバーにログインし、次のコマンドを実行してください。

    ```Bash
    cat fe/meta/image/VERSION | grep token
    ```

    出力:

    ```Properties
    token=wwwwwwww-xxxx-yyyy-zzzz-uuuuuuuuuu
    ```
  </TabItem>

  <TabItem value="sourceData" label="Migrate between Shared-data">
    ### ストレージボリュームのマッピング

    移行ツールがターゲットクラスターにテーブルを作成する際、テーブルのストレージボリュームは以下の優先順位で決定されます。

    1. `target_cluster_use_builtin_storage_volume_only`が`true`に設定されている場合、すべてのテーブルに`builtin_storage_volume`が使用されます。
    2. `target_cluster_storage_volume`が特定のストレージボリュームに設定されている場合、指定されたストレージボリュームがすべてのテーブルに使用されます。
    3. それ以外の場合、デフォルトでは、ソーステーブルのストレージボリュームプロパティは保持されます。異なるソースストレージボリュームからのテーブルは、ターゲットクラスター上にそれらのストレージボリュームが存在する場合、対応するストレージボリュームの下に作成されます。

    したがって、ソースクラスター内の各テーブルのストレージボリュームプロパティを保持したい場合は、ターゲットクラスターに同じストレージボリュームを事前に作成できます。移行後、ターゲットクラスター内の各テーブルは、ソースクラスターにあったストレージボリュームプロパティを継承します。

    `src_`で始まるストレージボリュームは異なる目的で使用されることに注意してください。**移行中のみ**ターゲットCNにソースクラスターのオブジェクトストレージへの読み取りアクセスを許可するためです。移行が完了した後、`src_<name>`ボリュームは不要になり、削除できます。
  </TabItem>
</Tabs>

### ネットワーク関連の設定（オプション）

<Tabs groupId="migrationPath">
  <TabItem value="sourceNothing" label="Migrate from Shared-nothing" default>
    データ移行中、移行ツールは**すべての**ソースおよびターゲットクラスターのFEノード、およびターゲットクラスターは**すべての**ソースクラスターのBEおよびCNノードにアクセスする必要があります。

    これらのノードのネットワークアドレスは、対応するクラスターで次のステートメントを実行することで取得できます。

    ```SQL
    -- クラスター内のFEノードのネットワークアドレスを取得します。
    SHOW FRONTENDS;
    -- クラスター内のBEノードのネットワークアドレスを取得します。
    SHOW BACKENDS;
    -- クラスター内のCNノードのネットワークアドレスを取得します。
    SHOW COMPUTE NODES;
    ```

    これらのノードが、Kubernetesクラスター内の内部ネットワークアドレスなど、クラスター外からアクセスできないプライベートアドレスを使用している場合、これらのプライベートアドレスを外部からアクセス可能なアドレスにマッピングする必要があります。

    ツールの展開済みフォルダーに移動し、構成ファイルを変更します。**conf/hosts.properties**。

    ```Bash
    cd starrocks-cluster-sync
    vi conf/hosts.properties
    ```

    ファイルのデフォルトの内容は次のとおりで、ネットワークアドレスマッピングの構成方法を説明しています。

    ```Properties
    # <SOURCE/TARGET>_<host>=<mappedHost>[;<srcPort>:<dstPort>[,<srcPort>:<dstPort>...]]
    ```

    :::note
`<host>` は、`SHOW FRONTENDS`、`SHOW BACKENDS`、または `SHOW COMPUTE NODES` によって返される `IP` 列に表示されるアドレスと一致する必要があります。
:::

    次の例では、これらの操作を実行します。

    1. ソースクラスターのプライベートネットワークアドレス `192.1.1.1` と `192.1.1.2` を `10.1.1.1` と `10.1.1.2` にマッピングします。
    2. ソースクラスターのFEポート `8030` と `9030` を `10.1.1.1` 上の `38030` と `39030` にマッピングします。
    3. ターゲットクラスターのプライベートネットワークアドレス `fe-0.starrocks.svc.cluster.local` を `10.1.2.1` にマッピングし、ポート `9030` を再マッピングします。

    ```Properties
    # <SOURCE/TARGET>_<host>=<mappedHost>[;<srcPort>:<dstPort>[,<srcPort>:<dstPort>...]]
    SOURCE_192.1.1.1=10.1.1.1;8030:38030,9030:39030
    SOURCE_192.1.1.2=10.1.1.2
    TARGET_fe-0.starrocks.svc.cluster.local=10.1.2.1;9030:19030
    ```
  </TabItem>

  <TabItem value="sourceData" label="Migrate between Shared-data">
    データ移行中、移行ツールはアクセスする必要があります。**すべての**ソースクラスターとターゲットクラスターの両方のFEノード。

    :::note
シェアードナッシングクラスターからの移行とは異なり、**ありません。**ターゲットクラスターからソースクラスターのCNノードへのネットワークアクセスを構成する必要はありません。データはオブジェクトストレージシステム間で直接転送されるためです。
:::

    対応するクラスターで次のステートメントを実行することで、FEネットワークアドレスを取得できます。

    ```SQL
    -- FEノード
    SHOW FRONTENDS;
    ```

    FEノードが、Kubernetesクラスター内の内部ネットワークアドレスなど、クラスター外からアクセスできないプライベートアドレスを使用している場合、これらのプライベートアドレスを外部からアクセスできるアドレスにマッピングする必要があります。

    ツールの展開済みフォルダーに移動し、構成ファイルを変更します。**conf/hosts.properties**。

    ```Bash
    cd starrocks-cluster-sync
    vi conf/hosts.properties
    ```

    ファイルのデフォルトの内容は次のとおりで、ネットワークアドレスマッピングの構成方法を説明しています。

    ```Properties
    # <SOURCE/TARGET>_<host>=<mappedHost>[;<srcPort>:<dstPort>[,<srcPort>:<dstPort>...]]
    ```

    :::note
`<host>` は、`SHOW FRONTENDS` によって返される `IP` 列に表示されるアドレスと一致する必要があります。
:::

    次の例では、ターゲットクラスターの内部Kubernetes FQDNを到達可能なIPにマッピングします。

    ```Properties
    TARGET_frontend-0.frontend.mynamespace.svc.cluster.local=10.1.2.1;9030:19030
    ```
  </TabItem>
</Tabs>

## ステップ3：移行ツールの開始

ツールを構成した後、移行ツールを起動してデータ移行プロセスを開始します。

```Bash
./bin/start.sh
```

:::note

- シェアードナッシングクラスターからデータを移行する場合は、ソースクラスターとターゲットクラスターのBEノードがネットワーク経由で適切に通信できることを確認してください。
- 実行中、移行ツールはターゲットクラスターのデータがソースクラスターに遅れているかどうかを定期的にチェックします。遅延がある場合、データ移行タスクを開始します。
- 新しいデータがソースクラスターに継続的にロードされる場合、ターゲットクラスターのデータがソースクラスターのデータと一貫するまでデータ同期が続行されます。
- 移行中にターゲットクラスターのテーブルをクエリすることはできますが、新しいデータをテーブルにロードしないでください。ターゲットクラスターのデータとソースクラスターのデータとの間に不整合が生じる可能性があるためです。現在、移行ツールは移行中のターゲットクラスターへのデータロードを禁止していません。
- データ移行は自動的に終了しないことに注意してください。移行の完了を手動で確認し、その後移行ツールを停止する必要があります。

:::

## 移行の進行状況を表示

### 移行ツールのログを表示

移行ツールのログで移行の進行状況を確認できます。**log/sync.INFO.log**。

例1：タスクの進行状況を表示します。

![画像](../_assets/data_migration_tool-1.png)

重要なメトリックは次のとおりです。

- `Sync job progress`: データ移行の進行状況。移行ツールは、ターゲットクラスターのデータがソースクラスターに遅れているかどうかを定期的にチェックします。したがって、100%の進行状況は、現在のチェック間隔内でデータ同期が完了したことを意味するにすぎません。新しいデータがソースクラスターに継続的にロードされる場合、次のチェック間隔で進行状況が減少する可能性があります。
- `total`: この移行操作におけるすべての種類のジョブの総数。
- `ddlPending`: 実行待ちのDDLジョブの数。
- `jobPending`: 実行待ちのデータ同期ジョブの数。
- `sent`: ソースクラスターから送信されたが、まだ開始されていないデータ同期ジョブの数。理論的には、この値は大きすぎないはずです。値が増加し続ける場合は、当社のエンジニアにご連絡ください。
- `running`: 現在実行中のデータ同期ジョブの数。
- `finished`: 完了したデータ同期ジョブの数。
- `failed`: 失敗したデータ同期ジョブの数。失敗したデータ同期ジョブは再送信されます。したがって、ほとんどの場合、このメトリックは無視できます。この値が著しく大きい場合は、当社のエンジニアにご連絡ください。
- `unknown`: 不明なステータスのジョブの数。理論的には、この値は常に`0`であるべきです。この値が`0`でない場合は、当社のエンジニアにご連絡ください。

例2：テーブル移行の進捗状況を表示する。

![画像](../_assets/data_migration_tool-2.png)

- `Sync table progress`: テーブル移行の進捗状況。つまり、この移行タスクで移行されたテーブルの、移行する必要があるすべてのテーブルに対する比率。
- `finishedTableRatio`: 少なくとも1つの同期タスク実行が成功したテーブルの比率。
- `expiredTableRatio`: データが期限切れのテーブルの比率。
- `total table`: このデータ移行の進捗状況に関わるテーブルの総数。
- `finished table`: 少なくとも1つの同期タスク実行が成功したテーブルの数。
- `unfinished table`: 同期タスクが実行されていないテーブルの数。
- `expired table`: データが期限切れのテーブルの数。

### 移行トランザクションのステータスを表示する

移行ツールは各テーブルに対してトランザクションを開きます。テーブルの移行ステータスは、対応するトランザクションのステータスを確認することで表示できます。

```SQL
SHOW PROC "/transactions/<db_name>/running";
```

`<db_name>`は、テーブルが配置されているデータベースの名前です。

### パーティションのデータバージョンを表示する

ソースクラスターとターゲットクラスターの対応するパーティションのデータバージョンを比較することで、そのパーティションの移行ステータスを表示できます。

```SQL
SHOW PARTITIONS FROM <table_name>;
```

`<table_name>`は、パーティションが属するテーブルの名前です。

### データ量を表示する

ソースクラスターとターゲットクラスターのデータ量を比較することで、移行ステータスを表示できます。

```SQL
SHOW DATA;
```

### テーブルの行数を表示する

ソースクラスターとターゲットクラスターのテーブルの行数を比較することで、各テーブルの移行ステータスを表示できます。

```SQL
SELECT 
  TABLE_NAME, 
  TABLE_ROWS 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE' 
ORDER BY TABLE_NAME;
```

## 移行後

`Sync job progress`が100%で安定し、ビジネスの切り替え準備が整ったら、次のようにカットオーバーを完了します。

1. ソースクラスターへの書き込みを停止します。

2. 書き込み停止後、`Sync job progress`が100%に達し、維持されていることを確認します。

3. 移行ツールを停止します。

4. アプリケーションをターゲットクラスターのアドレスに向けます。

5. 共有データクラスター間でデータを移行した場合は、ソースクラスターでAuto-Vacuum設定を復元します。

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="30");
   ```

6. 共有データクラスター間でデータを移行した場合は、ターゲットクラスターでCompactionを再度有効にします。`lake_compaction_max_tasks = 0`を以下から削除します。**fe.conf**を実行します。

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="-1");
   ```

7. ターゲットクラスターでレプリケーションのレガシー互換性を無効にします。`enable_legacy_compatibility_for_replication = true`を以下から削除します。**fe.conf**を実行します。

   ```SQL
   ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="false");
   ```

## 制限事項

現在、同期をサポートするオブジェクトのリストは以下の通りです（含まれていないものは同期がサポートされていないことを示します）：

- データベース
- 内部テーブルとそのデータ
- マテリアライズドビューのスキーマとその構築ステートメント（マテリアライズドビュー内のデータは同期されません。また、マテリアライズドビューのベーステーブルがターゲットクラスターに同期されていない場合、マテリアライズドビューのバックグラウンドリフレッシュタスクはエラーを報告します。）
- 論理ビュー

共有データクラスター間の移行の場合：

- ターゲットクラスターはv4.1以降で実行されている必要があります。
- 共有データクラスターから共有なしターゲットへの移行はサポートされていません。
- ソースクラスターのテーブルが使用する各ストレージボリュームには、ターゲットクラスター上に`src_<volume_name>`ストレージボリュームが事前に作成されている必要があります。
