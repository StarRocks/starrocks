---
displayed_sidebar: docs
---

# データのバックアップと復元

このトピックでは、StarRocks でデータをバックアップおよび復元する方法、またはデータを新しい StarRocks クラスターに移行する方法について説明します。

StarRocks は、データをスナップショットとしてリモートストレージシステムにバックアップし、任意の StarRocks クラスターにデータを復元することをサポートしています。

StarRocks は、以下のリモートストレージシステムをサポートしています。

- Apache™ Hadoop® (HDFS) クラスター
- AWS S3
- Google GCS

> **注意**
>
> 共有データ StarRocks クラスターは、データのバックアップと復元をサポートしていません。

## データのバックアップ

StarRocks は、データベース、テーブル、またはパーティションの粒度レベルでの完全バックアップをサポートしています。

テーブルに大量のデータを保存している場合は、パーティションごとにデータをバックアップおよび復元することをお勧めします。これにより、ジョブの失敗時に再試行のコストを削減できます。定期的に増分データをバックアップする必要がある場合は、テーブルに対して [動的パーティション化](../../table_design/data_distribution/dynamic_partitioning.md) プランを立て、毎回新しいパーティションのみをバックアップすることができます。

### リポジトリの作成

データをバックアップする前に、リモートストレージシステムにデータスナップショットを保存するためのリポジトリを作成する必要があります。StarRocks クラスターには複数のリポジトリを作成できます。詳細な手順については、[CREATE REPOSITORY](../../sql-reference/sql-statements/backup_restore/CREATE_REPOSITORY.md) を参照してください。

- HDFS にリポジトリを作成

以下の例は、HDFS クラスターに `test_repo` という名前のリポジトリを作成します。

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "hdfs://<hdfs_host>:<hdfs_port>/repo_dir/backup"
PROPERTIES(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

- AWS S3 にリポジトリを作成

  AWS S3 にアクセスするための認証方法として、IAM ユーザー認証（アクセスキーとシークレットキー）、インスタンスプロファイル、またはアサインドロールを選択できます。

  - 以下の例は、IAM ユーザー認証を使用して AWS S3 バケット `bucket_s3` に `test_repo` という名前のリポジトリを作成します。

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.access_key" = "XXXXXXXXXXXXXXXXX",
      "aws.s3.secret_key" = "yyyyyyyyyyyyyyyyyyyyyyyy",
      "aws.s3.region" = "us-east-1"
  );
  ```

  - 以下の例は、インスタンスプロファイルを使用して AWS S3 バケット `bucket_s3` に `test_repo` という名前のリポジトリを作成します。

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-east-1"
  );
  ```

  - 以下の例は、アサインドロールを使用して AWS S3 バケット `bucket_s3` に `test_repo` という名前のリポジトリを作成します。

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::xxxxxxxxxx:role/yyyyyyyy",
      "aws.s3.region" = "us-east-1"
  );
  ```

> **注意**
>
> StarRocks は、S3A プロトコルに従ってのみ AWS S3 にリポジトリを作成することをサポートしています。したがって、AWS S3 にリポジトリを作成する際には、リポジトリの場所として渡す S3 URI の `s3://` を `s3a://` に置き換える必要があります。

- Google GCS にリポジトリを作成

以下の例は、Google GCS バケット `bucket_gcs` に `test_repo` という名前のリポジトリを作成します。

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "s3a://bucket_gcs/backup"
PROPERTIES(
    "fs.s3a.access.key" = "xxxxxxxxxxxxxxxxxxxx",
    "fs.s3a.secret.key" = "yyyyyyyyyyyyyyyyyyyy",
    "fs.s3a.endpoint" = "storage.googleapis.com"
);
```

> **注意**
>
> - StarRocks は、S3A プロトコルに従ってのみ Google GCS にリポジトリを作成することをサポートしています。したがって、Google GCS にリポジトリを作成する際には、リポジトリの場所として渡す GCS URI のプレフィックスを `s3a://` に置き換える必要があります。
> - エンドポイントアドレスに `https` を指定しないでください。

リポジトリが作成された後、[SHOW REPOSITORIES](../../sql-reference/sql-statements/backup_restore/SHOW_REPOSITORIES.md) を使用してリポジトリを確認できます。データを復元した後、[DROP REPOSITORY](../../sql-reference/sql-statements/backup_restore/DROP_REPOSITORY.md) を使用して StarRocks からリポジトリを削除できます。ただし、リモートストレージシステムにバックアップされたデータスナップショットは、StarRocks を通じて削除することはできません。リモートストレージシステムで手動で削除する必要があります。

### データスナップショットのバックアップ

リポジトリが作成された後、データスナップショットを作成し、リモートリポジトリにバックアップする必要があります。詳細な手順については、[BACKUP](../../sql-reference/sql-statements/backup_restore/BACKUP.md) を参照してください。

以下の例は、データベース `sr_hub` のテーブル `sr_member` のデータスナップショット `sr_member_backup` を作成し、リポジトリ `test_repo` にバックアップします。

```SQL
BACKUP SNAPSHOT sr_hub.sr_member_backup
TO test_repo
ON (sr_member);
```

:::tip
StarRocks は、以下の粒度レベルでの BACKUP および RESTORE 操作をサポートしています：

- パーティションレベル: `ON (<table_name> PARTITION (<partition_name>, ...))` の形式で ON 句を指定する必要があります。
- テーブルレベル: `ON (<table_name>)` の形式で ON 句を指定する必要があります。
- データベースレベル: ON 句を指定する必要はありません。これにより、データベース全体がバックアップまたは復元されます。

:::

BACKUP は非同期操作です。[SHOW BACKUP](../../sql-reference/sql-statements/backup_restore/SHOW_BACKUP.md) を使用して BACKUP ジョブのステータスを確認するか、[CANCEL BACKUP](../../sql-reference/sql-statements/backup_restore/CANCEL_BACKUP.md) を使用して BACKUP ジョブをキャンセルできます。

## データの復元または移行

リモートストレージシステムにバックアップされたデータスナップショットを現在の StarRocks クラスターまたは他の StarRocks クラスターに復元して、データを復元または移行できます。

### （オプション）新しいクラスターにリポジトリを作成

データを別の StarRocks クラスターに移行するには、新しいクラスターに同じ **リポジトリ名** と **場所** を持つリポジトリを作成する必要があります。そうしないと、以前にバックアップされたデータスナップショットを表示できません。詳細については、[リポジトリの作成](#create-a-repository) を参照してください。

### スナップショットの確認

データを復元する前に、指定されたリポジトリ内のスナップショットを [SHOW SNAPSHOT](../../sql-reference/sql-statements/backup_restore/SHOW_SNAPSHOT.md) を使用して確認できます。

以下の例は、`test_repo` 内のスナップショット情報を確認します。

```Plain
mysql> SHOW SNAPSHOT ON test_repo;
+------------------+-------------------------+--------+
| Snapshot         | Timestamp               | Status |
+------------------+-------------------------+--------+
| sr_member_backup | 2023-02-07-14-45-53-143 | OK     |
+------------------+-------------------------+--------+
1 row in set (1.16 sec)
```

### スナップショットを介したデータの復元

[RESTORE](../../sql-reference/sql-statements/backup_restore/RESTORE.md) ステートメントを使用して、リモートストレージシステム内のデータスナップショットを現在の StarRocks クラスターまたは他の StarRocks クラスターに復元できます。

以下の例は、`test_repo` 内のテーブル `sr_member` に `sr_member_backup` データスナップショットを復元します。これは、1 つのデータレプリカのみを復元します。

```SQL
RESTORE SNAPSHOT sr_hub.sr_member_backup
FROM test_repo
ON (sr_member)
PROPERTIES (
    "backup_timestamp"="2023-02-07-14-45-53-143",
    "replication_num" = "1"
);
```

:::tip
StarRocks は、以下の粒度レベルでの BACKUP および RESTORE 操作をサポートしています：

- パーティションレベル: `ON (<table_name> PARTITION (<partition_name>, ...))` の形式で ON 句を指定する必要があります。
- テーブルレベル: `ON (<table_name>)` の形式で ON 句を指定する必要があります。
- データベースレベル: ON 句を指定する必要はありません。これにより、データベース全体がバックアップまたは復元されます。

:::

RESTORE は非同期操作です。[SHOW RESTORE](../../sql-reference/sql-statements/backup_restore/SHOW_RESTORE.md) を使用して RESTORE ジョブのステータスを確認するか、[CANCEL RESTORE](../../sql-reference/sql-statements/backup_restore/CANCEL_RESTORE.md) を使用して RESTORE ジョブをキャンセルできます。

## BACKUP または RESTORE ジョブの設定

BE 設定ファイル **be.conf** の以下の設定項目を変更することで、BACKUP または RESTORE ジョブのパフォーマンスを最適化できます。

| 設定項目      | 説明                                                                             |
| ----------------------- | -------------------------------------------------------------------------------- |
| make_snapshot_worker_count     | BE ノード上の BACKUP ジョブのスナップショット作成タスクの最大スレッド数。デフォルト: `5`。この設定項目の値を増やすことで、スナップショット作成タスクの並行性を高めることができます。 |
| release_snapshot_worker_count     | BE ノード上の失敗した BACKUP ジョブのスナップショット解放タスクの最大スレッド数。デフォルト: `5`。この設定項目の値を増やすことで、スナップショット解放タスクの並行性を高めることができます。 |
| upload_worker_count     | BE ノード上の BACKUP ジョブのアップロードタスクの最大スレッド数。デフォルト: `0`。`0` は、BE が存在するマシンの CPU コア数に設定されます。この設定項目の値を増やすことで、アップロードタスクの並行性を高めることができます。 |
| download_worker_count   | BE ノード上の RESTORE ジョブのダウンロードタスクの最大スレッド数。デフォルト: `0`。`0` は、BE が存在するマシンの CPU コア数に設定されます。この設定項目の値を増やすことで、ダウンロードタスクの並行性を高めることができます。 |

## マテリアライズドビューの BACKUP と RESTORE

テーブルの BACKUP または RESTORE ジョブ中に、StarRocks は自動的にその [同期マテリアライズドビュー](../../using_starrocks/Materialized_view-single_table.md) をバックアップまたは復元します。

v3.2.3 以降、StarRocks は、データベースをバックアップおよび復元する際に [非同期マテリアライズドビュー](../../using_starrocks/async_mv/Materialized_view.md) のバックアップと復元をサポートしています。

データベースの BACKUP および RESTORE 中に、StarRocks は次のように動作します：

- **BACKUP**

1. データベースを巡回して、すべてのテーブルと非同期マテリアライズドビューの情報を収集します。
2. マテリアライズドビューのベーステーブルがマテリアライズドビューの前に配置されるように、BACKUP および RESTORE キュー内のテーブルの順序を調整します：
   - ベーステーブルが現在のデータベースに存在する場合、StarRocks はテーブルをキューに追加します。
   - ベーステーブルが現在のデータベースに存在しない場合、StarRocks は警告ログを出力し、プロセスをブロックせずに BACKUP 操作を続行します。
3. キューの順序に従って BACKUP タスクを実行します。

- **RESTORE**

1. BACKUP および RESTORE キューの順序に従ってテーブルとマテリアライズドビューを復元します。
2. マテリアライズドビューとそのベーステーブル間の依存関係を再構築し、リフレッシュタスクスケジュールを再提出します。

RESTORE プロセス中に発生したエラーはプロセスをブロックしません。

RESTORE 後、[SHOW MATERIALIZED VIEWS](../../sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW.md) を使用してマテリアライズドビューのステータスを確認できます。

- マテリアライズドビューがアクティブな場合、直接使用できます。
- マテリアライズドビューが非アクティブな場合、そのベーステーブルが復元されていない可能性があります。すべてのベーステーブルが復元された後、[ALTER MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md) を使用してマテリアライズドビューを再アクティブ化できます。

## 使用上の注意

- グローバル、データベース、テーブル、パーティションレベルでのバックアップおよび復元操作を実行するには、異なる権限が必要です。詳細情報については、[シナリオに基づいたロールのカスタマイズ](../user_privs/User_privilege.md#customize-roles-based-on-scenarios) を参照してください。
- 各データベースでは、一度に実行中の BACKUP または RESTORE ジョブは 1 つだけ許可されます。それ以外の場合、StarRocks はエラーを返します。
- BACKUP および RESTORE ジョブは、StarRocks クラスターの多くのリソースを占有するため、StarRocks クラスターが過負荷でないときにデータをバックアップおよび復元することができます。
- StarRocks は、データバックアップのためのデータ圧縮アルゴリズムの指定をサポートしていません。
- データはスナップショットとしてバックアップされるため、スナップショット生成時にロードされたデータはスナップショットに含まれません。したがって、スナップショットが生成された後、RESTORE ジョブが完了する前に古いクラスターにデータをロードした場合、データが復元されるクラスターにもデータをロードする必要があります。データ移行が完了した後、データとサービスの正確性を確認した後にアプリケーションを新しいクラスターに移行することをお勧めします。
- RESTORE ジョブが完了する前に、復元されるテーブルを操作することはできません。
- 主キーテーブルは v2.5 より前の StarRocks クラスターには復元できません。
- 新しいクラスターで復元する前に、復元されるテーブルを作成する必要はありません。RESTORE ジョブが自動的に作成します。
- 復元されるテーブルと同じ名前の既存のテーブルがある場合、StarRocks はまず既存のテーブルのスキーマが復元されるテーブルのスキーマと一致するかどうかを確認します。スキーマが一致する場合、StarRocks はスナップショット内のデータで既存のテーブルを上書きします。スキーマが一致しない場合、RESTORE ジョブは失敗します。`AS` キーワードを使用して復元されるテーブルの名前を変更するか、データを復元する前に既存のテーブルを削除することができます。
- RESTORE ジョブが既存のデータベース、テーブル、またはパーティションを上書きする場合、ジョブが COMMIT フェーズに入った後に上書きされたデータを復元することはできません。この時点で RESTORE ジョブが失敗またはキャンセルされた場合、データが破損してアクセスできなくなる可能性があります。この場合、再度 RESTORE 操作を実行し、ジョブが完了するのを待つしかありません。したがって、現在のデータがもはや使用されていないことを確認している場合を除き、上書きによるデータの復元を行わないことをお勧めします。上書き操作は、スナップショットと既存のデータベース、テーブル、またはパーティション間のメタデータの一貫性を最初に確認します。一貫性が検出されない場合、RESTORE 操作は実行できません。
- 現在、StarRocks は論理ビューのバックアップおよび復元をサポートしていません。
- 現在、StarRocks はユーザーアカウント、権限、およびリソースグループに関連する設定データのバックアップおよび復元をサポートしていません。
- 現在、StarRocks はテーブル間の Colocate Join 関係のバックアップおよび復元をサポートしていません。