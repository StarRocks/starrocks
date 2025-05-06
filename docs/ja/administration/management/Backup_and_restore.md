---
displayed_sidebar: docs
---

# データのバックアップとリストア

このトピックでは、StarRocksでデータをバックアップおよびリストアする方法、またはデータを新しいStarRocksクラスターに移行する方法について説明します。

StarRocksは、データをスナップショットとしてリモートストレージシステムにバックアップし、任意のStarRocksクラスターにデータをリストアすることをサポートしています。

StarRocksは以下のリモートストレージシステムをサポートしています：

- Apache™ Hadoop® (HDFS) クラスター
- AWS S3
- Google GCS

> **注意**
>
> 共有データ StarRocks クラスターはデータのバックアップとリストアをサポートしていません。

## データのバックアップ

StarRocksは、データベース、テーブル、またはパーティションの粒度レベルでの完全バックアップをサポートしています。

大量のデータをテーブルに保存している場合、パーティションごとにデータをバックアップおよびリストアすることをお勧めします。これにより、ジョブの失敗時に再試行のコストを削減できます。定期的に増分データをバックアップする必要がある場合は、テーブルに対して [動的パーティション化](../../table_design/data_distribution/dynamic_partitioning.md) プランを策定し、毎回新しいパーティションのみをバックアップすることができます。

### リポジトリの作成

データをバックアップする前に、リモートストレージシステムにデータスナップショットを保存するためのリポジトリを作成する必要があります。StarRocksクラスター内に複数のリポジトリを作成できます。詳細な手順については、 [CREATE REPOSITORY](../../sql-reference/sql-statements/backup_restore/CREATE_REPOSITORY.md) を参照してください。

- HDFSにリポジトリを作成

以下の例では、HDFSクラスターに `test_repo` という名前のリポジトリを作成します。

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "hdfs://<hdfs_host>:<hdfs_port>/repo_dir/backup"
PROPERTIES(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

- AWS S3にリポジトリを作成

  AWS S3にアクセスするための認証方法として、IAMユーザーベースのクレデンシャル（アクセスキーとシークレットキー）、インスタンスプロファイル、またはアサインされたロールを選択できます。

  - 以下の例では、IAMユーザーベースのクレデンシャルを使用して、AWS S3バケット `bucket_s3` に `test_repo` という名前のリポジトリを作成します。

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

  - 以下の例では、インスタンスプロファイルを使用して、AWS S3バケット `bucket_s3` に `test_repo` という名前のリポジトリを作成します。

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-east-1"
  );
  ```

  - 以下の例では、アサインされたロールを使用して、AWS S3バケット `bucket_s3` に `test_repo` という名前のリポジトリを作成します。

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
> StarRocksは、S3Aプロトコルに従ってのみAWS S3にリポジトリを作成することをサポートしています。したがって、AWS S3にリポジトリを作成する際には、リポジトリの場所として渡すS3 URIの `s3://` を `s3a://` に置き換える必要があります。

- Google GCSにリポジトリを作成

以下の例では、Google GCSバケット `bucket_gcs` に `test_repo` という名前のリポジトリを作成します。

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
> - StarRocksは、S3Aプロトコルに従ってのみGoogle GCSにリポジトリを作成することをサポートしています。したがって、Google GCSにリポジトリを作成する際には、リポジトリの場所として渡すGCS URIのプレフィックスを `s3a://` に置き換える必要があります。
> - エンドポイントアドレスに `https` を指定しないでください。

リポジトリが作成された後、 [SHOW REPOSITORIES](../../sql-reference/sql-statements/backup_restore/SHOW_REPOSITORIES.md) を使用してリポジトリを確認できます。データをリストアした後、 [DROP REPOSITORY](../../sql-reference/sql-statements/backup_restore/DROP_REPOSITORY.md) を使用してStarRocksでリポジトリを削除できます。ただし、リモートストレージシステムにバックアップされたデータスナップショットはStarRocksを通じて削除できません。リモートストレージシステムで手動で削除する必要があります。

### データスナップショットのバックアップ

リポジトリが作成された後、データスナップショットを作成し、リモートリポジトリにバックアップする必要があります。詳細な手順については、 [BACKUP](../../sql-reference/sql-statements/backup_restore/BACKUP.md) を参照してください。

以下の例では、データベース `sr_hub` のテーブル `sr_member` に対してデータスナップショット `sr_member_backup` を作成し、リポジトリ `test_repo` にバックアップします。

```SQL
BACKUP SNAPSHOT sr_hub.sr_member_backup
TO test_repo
ON (sr_member);
```

:::tip
StarRocksは、以下の粒度レベルでのバックアップおよびリストア操作をサポートしています：

- パーティションレベル：`ON (<table_name> PARTITION (<partition_name>, ...))` の形式でON句を指定する必要があります。
- テーブルレベル：`ON (<table_name>)` の形式でON句を指定する必要があります。
- データベースレベル：ON句を指定する必要はありません。これにより、データベース全体がバックアップまたはリストアされます。

:::

BACKUPは非同期操作です。 [SHOW BACKUP](../../sql-reference/sql-statements/backup_restore/SHOW_BACKUP.md) を使用してBACKUPジョブのステータスを確認したり、 [CANCEL BACKUP](../../sql-reference/sql-statements/backup_restore/CANCEL_BACKUP.md) を使用してBACKUPジョブをキャンセルしたりできます。

## データのリストアまたは移行

リモートストレージシステムにバックアップされたデータスナップショットを現在のStarRocksクラスターまたは他のStarRocksクラスターにリストアして、データをリストアまたは移行できます。

### （オプション）新しいクラスターにリポジトリを作成

データを別のStarRocksクラスターに移行するには、新しいクラスターに同じ**リポジトリ名**と**場所**でリポジトリを作成する必要があります。そうしないと、以前にバックアップされたデータスナップショットを表示できません。詳細については、 [リポジトリの作成](#create-a-repository) を参照してください。

### スナップショットの確認

データをリストアする前に、 [SHOW SNAPSHOT](../../sql-reference/sql-statements/backup_restore/SHOW_SNAPSHOT.md) を使用して指定されたリポジトリ内のスナップショットを確認できます。

以下の例では、`test_repo` 内のスナップショット情報を確認します。

```Plain
mysql> SHOW SNAPSHOT ON test_repo;
+------------------+-------------------------+--------+
| Snapshot         | Timestamp               | Status |
+------------------+-------------------------+--------+
| sr_member_backup | 2023-02-07-14-45-53-143 | OK     |
+------------------+-------------------------+--------+
1 row in set (1.16 sec)
```

### スナップショットを介したデータのリストア

[RESTORE](../../sql-reference/sql-statements/backup_restore/RESTORE.md) ステートメントを使用して、リモートストレージシステム内のデータスナップショットを現在のStarRocksクラスターまたは他のStarRocksクラスターにリストアできます。

以下の例では、`test_repo` 内のデータスナップショット `sr_member_backup` をテーブル `sr_member` にリストアします。これは1つのデータレプリカのみをリストアします。

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
StarRocksは、以下の粒度レベルでのバックアップおよびリストア操作をサポートしています：

- パーティションレベル：`ON (<table_name> PARTITION (<partition_name>, ...))` の形式でON句を指定する必要があります。
- テーブルレベル：`ON (<table_name>)` の形式でON句を指定する必要があります。
- データベースレベル：ON句を指定する必要はありません。これにより、データベース全体がバックアップまたはリストアされます。

:::

RESTOREは非同期操作です。 [SHOW RESTORE](../../sql-reference/sql-statements/backup_restore/SHOW_RESTORE.md) を使用してRESTOREジョブのステータスを確認したり、 [CANCEL RESTORE](../../sql-reference/sql-statements/backup_restore/CANCEL_RESTORE.md) を使用してRESTOREジョブをキャンセルしたりできます。

## BACKUPまたはRESTOREジョブの設定

BE構成ファイル **be.conf** の以下の構成項目を変更することで、BACKUPまたはRESTOREジョブのパフォーマンスを最適化できます：

| 構成項目      | 説明                                                                             |
| ----------------------- | -------------------------------------------------------------------------------- |
| make_snapshot_worker_count     | BEノード上のBACKUPジョブのスナップショット作成タスクの最大スレッド数。デフォルト：`5`。この構成項目の値を増やすことで、スナップショット作成タスクの並行性を高めることができます。 |
| release_snapshot_worker_count     | BEノード上の失敗したBACKUPジョブのスナップショット解放タスクの最大スレッド数。デフォルト：`5`。この構成項目の値を増やすことで、スナップショット解放タスクの並行性を高めることができます。 |
| upload_worker_count     | BEノード上のBACKUPジョブのアップロードタスクの最大スレッド数。デフォルト：`0`。`0`は、BEが存在するマシンのCPUコア数に設定されます。この構成項目の値を増やすことで、アップロードタスクの並行性を高めることができます。 |
| download_worker_count   | BEノード上のRESTOREジョブのダウンロードタスクの最大スレッド数。デフォルト：`0`。`0`は、BEが存在するマシンのCPUコア数に設定されます。この構成項目の値を増やすことで、ダウンロードタスクの並行性を高めることができます。 |

## マテリアライズドビューのバックアップとリストア

テーブルのバックアップまたはリストアジョブ中に、StarRocksは自動的にその [同期マテリアライズドビュー](../../using_starrocks/Materialized_view-single_table.md) をバックアップまたはリストアします。

v3.2.3から、StarRocksは、データベースをバックアップおよびリストアする際に [非同期マテリアライズドビュー](../../using_starrocks/async_mv/Materialized_view.md) のバックアップとリストアをサポートしています。

データベースのバックアップおよびリストア中に、StarRocksは以下のように動作します：

- **バックアップ**

1. データベースをトラバースして、すべてのテーブルと非同期マテリアライズドビューの情報を収集します。
2. マテリアライズドビューのベーステーブルがマテリアライズドビューの前に配置されるように、バックアップおよびリストアキュー内のテーブルの順序を調整します：
   - ベーステーブルが現在のデータベースに存在する場合、StarRocksはテーブルをキューに追加します。
   - ベーステーブルが現在のデータベースに存在しない場合、StarRocksは警告ログを出力し、プロセスをブロックせずにバックアップ操作を続行します。
3. キューの順序でバックアップタスクを実行します。

- **リストア**

1. バックアップおよびリストアキューの順序でテーブルとマテリアライズドビューをリストアします。
2. マテリアライズドビューとそのベーステーブルの依存関係を再構築し、リフレッシュタスクスケジュールを再提出します。

リストアプロセス全体で発生したエラーはプロセスをブロックしません。

リストア後、 [SHOW MATERIALIZED VIEWS](../../sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW.md) を使用してマテリアライズドビューのステータスを確認できます。

- マテリアライズドビューがアクティブである場合、直接使用できます。
- マテリアライズドビューが非アクティブである場合、ベーステーブルがリストアされていない可能性があります。すべてのベーステーブルがリストアされた後、 [ALTER MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md) を使用してマテリアライズドビューを再アクティブ化できます。

## 使用上の注意

- グローバル、データベース、テーブル、パーティションレベルでのバックアップおよびリストア操作を行うには、異なる権限が必要です。詳細については、 [シナリオに基づいたロールのカスタマイズ](../user_privs/User_privilege.md#customize-roles-based-on-scenarios) を参照してください。
- 各データベースでは、同時に1つのバックアップまたはリストアジョブのみが実行可能です。それ以外の場合、StarRocksはエラーを返します。
- バックアップおよびリストアジョブはStarRocksクラスターの多くのリソースを占有するため、StarRocksクラスターが高負荷でないときにデータをバックアップおよびリストアすることができます。
- StarRocksはデータバックアップのためのデータ圧縮アルゴリズムの指定をサポートしていません。
- データはスナップショットとしてバックアップされるため、スナップショット生成時にロードされたデータはスナップショットに含まれません。したがって、スナップショット生成後およびリストアジョブ完了前に古いクラスターにデータをロードした場合、データがリストアされたクラスターにもデータをロードする必要があります。データ移行が完了した後、データとサービスの正確性を確認した後にアプリケーションを新しいクラスターに移行することをお勧めします。
- リストアジョブが完了するまで、リストアされるテーブルを操作することはできません。
- 主キーテーブルはv2.5以前のStarRocksクラスターにリストアできません。
- リストアするテーブルを新しいクラスターに事前に作成する必要はありません。リストアジョブが自動的に作成します。
- リストアされるテーブルと同じ名前の既存のテーブルがある場合、StarRocksはまず既存のテーブルのスキーマがリストアされるテーブルのスキーマと一致するかどうかを確認します。スキーマが一致する場合、StarRocksはスナップショット内のデータで既存のテーブルを上書きします。スキーマが一致しない場合、リストアジョブは失敗します。`AS` キーワードを使用してリストアされるテーブルの名前を変更するか、データをリストアする前に既存のテーブルを削除することができます。
- リストアジョブが既存のデータベース、テーブル、またはパーティションを上書きする場合、ジョブがコミットフェーズに入った後は上書きされたデータをリストアできません。リストアジョブがこの時点で失敗またはキャンセルされた場合、データが破損してアクセスできなくなる可能性があります。この場合、リストア操作を再度実行し、ジョブが完了するのを待つことしかできません。したがって、現在のデータがもはや使用されていないと確信している場合を除き、上書きによるデータのリストアはお勧めしません。上書き操作は、スナップショットと既存のデータベース、テーブル、またはパーティションの間のメタデータの一貫性を最初にチェックします。不一致が検出された場合、リストア操作は実行できません。
- 現在、StarRocksは論理ビューのバックアップおよびリストアをサポートしていません。
- 現在、StarRocksはユーザーアカウント、権限、およびリソースグループに関連する構成データのバックアップおよびリストアをサポートしていません。
- 現在、StarRocksはテーブル間のColocate Join関係のバックアップおよびリストアをサポートしていません。