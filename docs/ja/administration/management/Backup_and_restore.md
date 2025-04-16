---
displayed_sidebar: docs
---

# データのバックアップと復元

このトピックでは、StarRocks でデータをバックアップおよび復元する方法、またはデータを新しい StarRocks クラスターに移行する方法について説明します。

StarRocks は、データをスナップショットとしてリモートストレージシステムにバックアップし、任意の StarRocks クラスターにデータを復元することをサポートしています。

バージョン 3.4.0 以降、StarRocks はより多くのオブジェクトをサポートし、構文をリファクタリングして柔軟性を向上させることで、BACKUP および RESTORE の機能を強化しました。

StarRocks は、次のリモートストレージシステムをサポートしています。

- Apache™ Hadoop® (HDFS) クラスター
- AWS S3
- Google GCS
- MinIO

StarRocks は、次のオブジェクトのバックアップをサポートしています。

- 内部データベース、テーブル（すべてのタイプとパーティショニング戦略）、およびパーティション
- 外部カタログのメタデータ（バージョン 3.4.0 以降でサポート）
- 同期マテリアライズドビューおよび非同期マテリアライズドビュー
- ビュー（バージョン 3.4.0 以降でサポート）
- ユーザー定義関数（バージョン 3.4.0 以降でサポート）

> **NOTE**
>
> 共有データ StarRocks クラスターは、データの BACKUP および RESTORE をサポートしていません。

## リポジトリの作成

データをバックアップする前に、リモートストレージシステムにデータスナップショットを保存するためのリポジトリを作成する必要があります。StarRocks クラスター内に複数のリポジトリを作成できます。詳細な手順については、[CREATE REPOSITORY](../../sql-reference/sql-statements/backup_restore/CREATE_REPOSITORY.md) を参照してください。

- HDFS にリポジトリを作成

次の例では、`test_repo` という名前のリポジトリを HDFS クラスターに作成します。

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

  AWS S3 にアクセスするための認証方法として、IAM ユーザーに基づくクレデンシャル（アクセスキーとシークレットキー）、インスタンスプロファイル、またはアサインされたロールを選択できます。

  - 次の例では、IAM ユーザーに基づくクレデンシャルを認証方法として使用して、AWS S3 バケット `bucket_s3` に `test_repo` という名前のリポジトリを作成します。

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

  - 次の例では、インスタンスプロファイルを認証方法として使用して、AWS S3 バケット `bucket_s3` に `test_repo` という名前のリポジトリを作成します。

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-east-1"
  );
  ```

  - 次の例では、アサインされたロールを認証方法として使用して、AWS S3 バケット `bucket_s3` に `test_repo` という名前のリポジトリを作成します。

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

> **NOTE**
>
> StarRocks は、S3A プロトコルに従ってのみ AWS S3 にリポジトリを作成することをサポートしています。したがって、AWS S3 にリポジトリを作成する場合、`ON LOCATION` でリポジトリの場所として渡す S3 URI の `s3://` を `s3a://` に置き換える必要があります。

- Google GCS にリポジトリを作成

次の例では、Google GCS バケット `bucket_gcs` に `test_repo` という名前のリポジトリを作成します。

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

> **NOTE**
>
> - StarRocks は、S3A プロトコルに従ってのみ Google GCS にリポジトリを作成することをサポートしています。したがって、Google GCS にリポジトリを作成する場合、`ON LOCATION` でリポジトリの場所として渡す GCS URI のプレフィックスを `s3a://` に置き換える必要があります。
> - エンドポイントアドレスに `https` を指定しないでください。

- MinIO にリポジトリを作成

次の例では、MinIO バケット `bucket_minio` に `test_repo` という名前のリポジトリを作成します。

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "s3://bucket_minio/backup"
PROPERTIES(
    "aws.s3.access_key" = "XXXXXXXXXXXXXXXXX",
    "aws.s3.secret_key" = "yyyyyyyyyyyyyyyyy",
    "aws.s3.endpoint" = "http://minio:9000"
);
```

リポジトリが作成された後、[SHOW REPOSITORIES](../../sql-reference/sql-statements/backup_restore/SHOW_REPOSITORIES.md) を使用してリポジトリを確認できます。データを復元した後、StarRocks で [DROP REPOSITORY](../../sql-reference/sql-statements/backup_restore/DROP_REPOSITORY.md) を使用してリポジトリを削除できます。ただし、リモートストレージシステムにバックアップされたデータスナップショットは、StarRocks を通じて削除することはできません。リモートストレージシステムで手動で削除する必要があります。

## データのバックアップ

リポジトリが作成された後、データスナップショットを作成し、リモートリポジトリにバックアップする必要があります。詳細な手順については、[BACKUP](../../sql-reference/sql-statements/backup_restore/BACKUP.md) を参照してください。BACKUP は非同期操作です。[SHOW BACKUP](../../sql-reference/sql-statements/backup_restore/SHOW_BACKUP.md) を使用して BACKUP ジョブのステータスを確認したり、[CANCEL BACKUP](../../sql-reference/sql-statements/backup_restore/CANCEL_BACKUP.md) を使用して BACKUP ジョブをキャンセルしたりできます。

StarRocks は、データベース、テーブル、またはパーティションの粒度レベルでの完全バックアップをサポートしています。

テーブルに大量のデータを保存している場合は、パーティションごとにデータをバックアップおよび復元することをお勧めします。これにより、ジョブの失敗時の再試行コストを削減できます。定期的に増分データをバックアップする必要がある場合は、テーブルに[パーティショニングプラン](../../table_design/data_distribution/Data_distribution.md#partitioning)を設定し、新しいパーティションのみを毎回バックアップできます。

### データベースのバックアップ

データベースに対して完全な BACKUP を実行すると、データベース内のすべてのテーブル、同期および非同期マテリアライズドビュー、ビュー、および UDF がバックアップされます。

次の例では、スナップショット `sr_hub_backup` にデータベース `sr_hub` をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
-- バージョン 3.4.0 以降でサポート。
BACKUP DATABASE sr_hub SNAPSHOT sr_hub_backup
TO test_repo;

-- 以前のバージョンの構文と互換性があります。
BACKUP SNAPSHOT sr_hub.sr_hub_backup
TO test_repo;
```

### テーブルのバックアップ

StarRocks は、すべてのタイプとパーティショニング戦略のテーブルのバックアップと復元をサポートしています。テーブルに対して完全な BACKUP を実行すると、そのテーブルとその上に構築された同期マテリアライズドビューがバックアップされます。

次の例では、スナップショット `sr_member_backup` にデータベース `sr_hub` のテーブル `sr_member` をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
-- バージョン 3.4.0 以降でサポート。
BACKUP DATABASE sr_hub SNAPSHOT sr_member_backup
TO test_repo
ON (TABLE sr_member);

-- 以前のバージョンの構文と互換性があります。
BACKUP SNAPSHOT sr_hub.sr_member_backup
TO test_repo
ON (sr_member);
```

次の例では、スナップショット `sr_core_backup` にデータベース `sr_hub` のテーブル `sr_member` と `sr_pmc` をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_core_backup
TO test_repo
ON (TABLE sr_member, TABLE sr_pmc);
```

次の例では、スナップショット `sr_all_backup` にデータベース `sr_hub` のすべてのテーブルをバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_all_backup
TO test_repo
ON (ALL TABLES);
```

### パーティションのバックアップ

次の例では、スナップショット `sr_par_backup` にデータベース `sr_hub` のテーブル `sr_member` のパーティション `p1` をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
-- バージョン 3.4.0 以降でサポート。
BACKUP DATABASE sr_hub SNAPSHOT sr_par_backup
TO test_repo
ON (TABLE sr_member PARTITION (p1));

-- 以前のバージョンの構文と互換性があります。
BACKUP SNAPSHOT sr_hub.sr_par_backup
TO test_repo
ON (sr_member PARTITION (p1));
```

複数のパーティション名をカンマ（`,`）で区切って指定することで、パーティションを一括でバックアップできます。

### マテリアライズドビューのバックアップ

同期マテリアライズドビューは、ベーステーブルの BACKUP 操作と共にバックアップされるため、手動でバックアップする必要はありません。

非同期マテリアライズドビューは、それが属するデータベースの BACKUP 操作と共にバックアップできます。また、手動でバックアップすることもできます。

次の例では、スナップショット `sr_mv1_backup` にデータベース `sr_hub` のマテリアライズドビュー `sr_mv1` をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_mv1_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1);
```

次の例では、スナップショット `sr_mv2_backup` にデータベース `sr_hub` のマテリアライズドビュー `sr_mv1` と `sr_mv2` をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_mv2_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2);
```

次の例では、スナップショット `sr_mv3_backup` にデータベース `sr_hub` のすべてのマテリアライズドビューをバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_mv3_backup
TO test_repo
ON (ALL MATERIALIZED VIEWS);
```

### ビューのバックアップ

次の例では、スナップショット `sr_view1_backup` にデータベース `sr_hub` のビュー `sr_view1` をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_view1_backup
TO test_repo
ON (VIEW sr_view1);
```

次の例では、スナップショット `sr_view2_backup` にデータベース `sr_hub` のビュー `sr_view1` と `sr_view2` をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_view2_backup
TO test_repo
ON (VIEW sr_view1, VIEW sr_view2);
```

次の例では、スナップショット `sr_view3_backup` にデータベース `sr_hub` のすべてのビューをバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_view3_backup
TO test_repo
ON (ALL VIEWS);
```

### UDF のバックアップ

次の例では、スナップショット `sr_udf1_backup` にデータベース `sr_hub` の UDF `sr_udf1` をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_udf1_backup
TO test_repo
ON (FUNCTION sr_udf1);
```

次の例では、スナップショット `sr_udf2_backup` にデータベース `sr_hub` の UDF `sr_udf1` と `sr_udf2` をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_udf2_backup
TO test_repo
ON (FUNCTION sr_udf1, FUNCTION sr_udf2);
```

次の例では、スナップショット `sr_udf3_backup` にデータベース `sr_hub` のすべての UDF をバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_udf3_backup
TO test_repo
ON (ALL FUNCTIONS);
```

### 外部カタログのメタデータのバックアップ

次の例では、スナップショット `iceberg_backup` に外部カタログ `iceberg` のメタデータをバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP EXTERNAL CATALOG (iceberg) SNAPSHOT iceberg_backup
TO test_repo;
```

次の例では、スナップショット `iceberg_hive_backup` に外部カタログ `iceberg` と `hive` のメタデータをバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP EXTERNAL CATALOGS (iceberg, hive) SNAPSHOT iceberg_hive_backup
TO test_repo;
```

次の例では、スナップショット `all_catalog_backup` にすべての外部カタログのメタデータをバックアップし、リポジトリ `test_repo` にスナップショットをアップロードします。

```SQL
BACKUP ALL EXTERNAL CATALOGS SNAPSHOT all_catalog_backup
TO test_repo;
```

外部カタログの BACKUP 操作をキャンセルするには、次のステートメントを実行します。

```SQL
CANCEL BACKUP FOR EXTERNAL CATALOG;
```

## データの復元

リモートストレージシステムにバックアップされたデータスナップショットを現在の StarRocks クラスターまたは他の StarRocks クラスターに復元して、データを復元または移行できます。

**スナップショットからオブジェクトを復元する際には、スナップショットのタイムスタンプを指定する必要があります。**

リモートストレージシステムのデータスナップショットを復元するには、[RESTORE](../../sql-reference/sql-statements/backup_restore/RESTORE.md) ステートメントを使用します。

RESTORE は非同期操作です。[SHOW RESTORE](../../sql-reference/sql-statements/backup_restore/SHOW_RESTORE.md) を使用して RESTORE ジョブのステータスを確認したり、[CANCEL RESTORE](../../sql-reference/sql-statements/backup_restore/CANCEL_RESTORE.md) を使用して RESTORE ジョブをキャンセルしたりできます。

### （オプション）新しいクラスターにリポジトリを作成

データを別の StarRocks クラスターに移行するには、ターゲットクラスターに同じ**リポジトリ名**と**場所**でリポジトリを作成する必要があります。そうしないと、以前にバックアップされたデータスナップショットを表示できません。詳細については、[リポジトリの作成](#create-a-repository)を参照してください。

### スナップショットのタイムスタンプを取得

データを復元する前に、[SHOW SNAPSHOT](../../sql-reference/sql-statements/backup_restore/SHOW_SNAPSHOT.md) を使用してリポジトリ内のスナップショットを確認し、タイムスタンプを取得できます。

次の例では、`test_repo` 内のスナップショット情報を確認します。

```Plain
mysql> SHOW SNAPSHOT ON test_repo;
+------------------+-------------------------+--------+
| Snapshot         | Timestamp               | Status |
+------------------+-------------------------+--------+
| sr_member_backup | 2023-02-07-14-45-53-143 | OK     |
+------------------+-------------------------+--------+
1 row in set (1.16 sec)
```

### データベースの復元

次の例では、スナップショット `sr_hub_backup` のデータベース `sr_hub` をターゲットクラスターのデータベース `sr_hub` に復元します。スナップショットにデータベースが存在しない場合、システムはエラーを返します。ターゲットクラスターにデータベースが存在しない場合、システムは自動的に作成します。

```SQL
-- バージョン 3.4.0 以降でサポート。
RESTORE SNAPSHOT sr_hub_backup
FROM test_repo
DATABASE sr_hub
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");

-- 以前のバージョンの構文と互換性があります。
RESTORE SNAPSHOT sr_hub.sr_hub_backup
FROM `test_repo` 
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");
```

次の例では、スナップショット `sr_hub_backup` のデータベース `sr_hub` をターゲットクラスターのデータベース `sr_hub_new` に復元します。スナップショットにデータベース `sr_hub` が存在しない場合、システムはエラーを返します。ターゲットクラスターにデータベース `sr_hub_new` が存在しない場合、システムは自動的に作成します。

```SQL
-- バージョン 3.4.0 以降でサポート。
RESTORE SNAPSHOT sr_hub_backup
FROM test_repo
DATABASE sr_hub AS sr_hub_new
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");
```

### テーブルの復元

次の例では、スナップショット `sr_member_backup` のデータベース `sr_hub` のテーブル `sr_member` をターゲットクラスターのデータベース `sr_hub` のテーブル `sr_member` に復元します。

```SQL
-- バージョン 3.4.0 以降でサポート。
RESTORE SNAPSHOT sr_member_backup
FROM test_repo 
DATABASE sr_hub 
ON (TABLE sr_member) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- 以前のバージョンの構文と互換性があります。
RESTORE SNAPSHOT sr_hub.sr_member_backup
FROM test_repo
ON (sr_member)
PROPERTIES ("backup_timestamp"="2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_member_backup` のデータベース `sr_hub` のテーブル `sr_member` をターゲットクラスターのデータベース `sr_hub_new` のテーブル `sr_member_new` に復元します。

```SQL
RESTORE SNAPSHOT sr_member_backup
FROM test_repo 
DATABASE sr_hub  AS sr_hub_new
ON (TABLE sr_member AS sr_member_new) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_core_backup` のデータベース `sr_hub` のテーブル `sr_member` と `sr_pmc` をターゲットクラスターのデータベース `sr_hub` のテーブル `sr_member` と `sr_pmc` に復元します。

```SQL
RESTORE SNAPSHOT sr_core_backup
FROM test_repo 
DATABASE sr_hub
ON (TABLE sr_member, TABLE sr_pmc) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_all_backup` のデータベース `sr_hub` のすべてのテーブルを復元します。

```SQL
RESTORE SNAPSHOT sr_all_backup
FROM test_repo
DATABASE sr_hub
ON (ALL TABLES);
```

次の例では、スナップショット `sr_all_backup` のデータベース `sr_hub` のすべてのテーブルのうちの 1 つを復元します。

```SQL
RESTORE SNAPSHOT sr_all_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

### パーティションの復元

次の例では、スナップショット `sr_par_backup` のテーブル `sr_member` のパーティション `p1` をターゲットクラスターのテーブル `sr_member` のパーティション `p1` に復元します。

```SQL
-- バージョン 3.4.0 以降でサポート。
RESTORE SNAPSHOT sr_par_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member PARTITION (p1)) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- 以前のバージョンの構文と互換性があります。
RESTORE SNAPSHOT sr_hub.sr_par_backup
FROM test_repo
ON (sr_member PARTITION (p1)) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

複数のパーティション名をカンマ（`,`）で区切って指定することで、パーティションを一括で復元できます。

### マテリアライズドビューの復元

次の例では、スナップショット `sr_mv1_backup` のデータベース `sr_hub` のマテリアライズドビュー `sr_mv1` をターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_mv1_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_mv2_backup` のデータベース `sr_hub` のマテリアライズドビュー `sr_mv1` と `sr_mv2` をターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_mv2_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_mv3_backup` のデータベース `sr_hub` のすべてのマテリアライズドビューをターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_mv3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL MATERIALIZED VIEWS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_mv3_backup` のデータベース `sr_hub` のマテリアライズドビューのうちの 1 つをターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_mv3_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

:::info

RESTORE 後、[SHOW MATERIALIZED VIEWS](../../sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW.md) を使用してマテリアライズドビューのステータスを確認できます。

- マテリアライズドビューがアクティブな場合、直接使用できます。
- マテリアライズドビューが非アクティブな場合、そのベーステーブルが復元されていない可能性があります。すべてのベーステーブルが復元された後、[ALTER MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md) を使用してマテリアライズドビューを再アクティブ化できます。

:::

### ビューの復元

次の例では、スナップショット `sr_view1_backup` のデータベース `sr_hub` のビュー `sr_view1` をターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_view1_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_view2_backup` のデータベース `sr_hub` のビュー `sr_view1` と `sr_view2` をターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_view2_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1, VIEW sr_view2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_view3_backup` のデータベース `sr_hub` のすべてのビューをターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_view3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL VIEWS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_view3_backup` のデータベース `sr_hub` のすべてのビューのうちの 1 つをターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_view3_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

### UDF の復元

次の例では、スナップショット `sr_udf1_backup` のデータベース `sr_hub` の UDF `sr_udf1` をターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_udf1_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_udf2_backup` のデータベース `sr_hub` の UDF `sr_udf1` と `sr_udf2` をターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_udf2_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1, FUNCTION sr_udf2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_udf3_backup` のデータベース `sr_hub` のすべての UDF をターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_udf3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL FUNCTIONS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `sr_udf3_backup` のデータベース `sr_hub` のすべての UDF のうちの 1 つをターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT sr_udf3_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

### 外部カタログのメタデータの復元

次の例では、スナップショット `iceberg_backup` の外部カタログ `iceberg` のメタデータをターゲットクラスターに復元し、`iceberg_new` として名前を変更します。

```SQL
RESTORE SNAPSHOT iceberg_backup
FROM test_repo
EXTERNAL CATALOG (iceberg AS iceberg_new) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `iceberg_hive_backup` の外部カタログ `iceberg` と `hive` のメタデータをターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT iceberg_hive_backup
FROM test_repo 
EXTERNAL CATALOGS (iceberg, hive)
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

次の例では、スナップショット `all_catalog_backup` のすべての外部カタログのメタデータをターゲットクラスターに復元します。

```SQL
RESTORE SNAPSHOT all_catalog_backup
FROM test_repo 
ALL EXTERNAL CATALOGS
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

外部カタログの RESTORE 操作をキャンセルするには、次のステートメントを実行します。

```SQL
CANCEL RESTORE FOR EXTERNAL CATALOG;
```

## BACKUP または RESTORE ジョブの設定

BE 設定ファイル **be.conf** の次の設定項目を変更することで、BACKUP または RESTORE ジョブのパフォーマンスを最適化できます。

| 設定項目      | 説明                                                                             |
| ----------------------- | -------------------------------------------------------------------------------- |
| make_snapshot_worker_count     | BE ノード上の BACKUP ジョブのスナップショット作成タスクの最大スレッド数。デフォルト: `5`。この設定項目の値を増やすことで、スナップショット作成タスクの並行性を向上させることができます。 |
| release_snapshot_worker_count     | BE ノード上の失敗した BACKUP ジョブのスナップショット解放タスクの最大スレッド数。デフォルト: `5`。この設定項目の値を増やすことで、スナップショット解放タスクの並行性を向上させることができます。 |
| upload_worker_count     | BE ノード上の BACKUP ジョブのアップロードタスクの最大スレッド数。デフォルト: `0`。`0` は BE が存在するマシンの CPU コア数に設定されることを示します。この設定項目の値を増やすことで、アップロードタスクの並行性を向上させることができます。 |
| download_worker_count   | BE ノード上の RESTORE ジョブのダウンロードタスクの最大スレッド数。デフォルト: `0`。`0` は BE が存在するマシンの CPU コア数に設定されることを示します。この設定項目の値を増やすことで、ダウンロードタスクの並行性を向上させることができます。 |

## 使用上の注意

- グローバル、データベース、テーブル、パーティションレベルでのバックアップおよび復元操作には、異なる権限が必要です。詳細情報については、[シナリオに基づくロールのカスタマイズ](../user_privs/User_privilege.md#customize-roles-based-on-scenarios)を参照してください。
- 各データベースでは、同時に実行中の BACKUP または RESTORE ジョブは 1 つだけ許可されます。それ以外の場合、StarRocks はエラーを返します。
- BACKUP および RESTORE ジョブは、StarRocks クラスターの多くのリソースを占有するため、StarRocks クラスターが重負荷でないときにデータをバックアップおよび復元することをお勧めします。
- StarRocks は、データバックアップのためのデータ圧縮アルゴリズムの指定をサポートしていません。
- データはスナップショットとしてバックアップされるため、スナップショット生成時にロードされたデータはスナップショットに含まれません。したがって、スナップショットが生成された後、RESTORE ジョブが完了する前に古いクラスターにデータをロードした場合、データを復元するクラスターにもデータをロードする必要があります。データ移行が完了した後、データとサービスの正確性を確認した後、新しいクラスターにアプリケーションを移行するまでの間、両方のクラスターに並行してデータをロードすることをお勧めします。
- RESTORE ジョブが完了するまで、復元するテーブルを操作することはできません。
- 主キーテーブルは、バージョン 2.5 より前の StarRocks クラスターに復元することはできません。
- 新しいクラスターで復元するテーブルを事前に作成する必要はありません。RESTORE ジョブが自動的に作成します。
- 復元するテーブルと同じ名前の既存のテーブルがある場合、StarRocks はまず既存のテーブルのスキーマが復元するテーブルのスキーマと一致するかどうかを確認します。スキーマが一致する場合、StarRocks はスナップショット内のデータで既存のテーブルを上書きします。スキーマが一致しない場合、RESTORE ジョブは失敗します。`AS` キーワードを使用して復元するテーブルの名前を変更するか、データを復元する前に既存のテーブルを削除することができます。
- RESTORE ジョブが既存のデータベース、テーブル、またはパーティションを上書きする場合、ジョブが COMMIT フェーズに入った後、上書きされたデータは復元できません。RESTORE ジョブがこの時点で失敗またはキャンセルされた場合、データが破損しアクセスできなくなる可能性があります。この場合、再度 RESTORE 操作を実行し、ジョブが完了するのを待つしかありません。したがって、現在のデータがもはや使用されていないことが確実でない限り、上書きによるデータの復元はお勧めしません。上書き操作は、スナップショットと既存のデータベース、テーブル、またはパーティションの間のメタデータの一貫性を最初にチェックします。不一致が検出された場合、RESTORE 操作は実行できません。
- 現在、StarRocks はユーザーアカウント、権限、リソースグループに関連する設定データのバックアップおよび復元をサポートしていません。
- 現在、StarRocks はテーブル間の Colocate Join 関係のバックアップおよび復元をサポートしていません。