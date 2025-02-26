---
displayed_sidebar: docs
---

# RESTORE

StarRocks は、次のオブジェクトのバックアップとリストアをサポートしています。

- 内部データベース、テーブル（すべてのタイプとパーティショニング戦略）、およびパーティション
- 外部カタログのメタデータ（v3.4.0以降でサポート）
- 同期マテリアライズドビューと非同期マテリアライズドビュー
- ビュー（v3.4.0以降でサポート）
- ユーザー定義関数（v3.4.0以降でサポート）

:::tip
バックアップとリストアの概要については、[バックアップとリストアガイド](../../../administration/management/Backup_and_restore.md)をご覧ください。
:::

RESTORE は非同期操作です。RESTORE ジョブのステータスを確認するには [SHOW RESTORE](./SHOW_RESTORE.md) を使用し、RESTORE ジョブをキャンセルするには [CANCEL RESTORE](./CANCEL_RESTORE.md) を使用します。

> **注意**
>
> - 共有データ StarRocks クラスターはデータのバックアップとリストアをサポートしていません。
> - 各データベースでは、同時に実行できるバックアップまたはリストアジョブは1つだけです。それ以外の場合、StarRocks はエラーを返します。

## 権限要件

v3.0 より前のバージョンでは、`admin_priv` 権限を持つユーザーがこの操作を実行できます。v3.0 以降のバージョンでは、特定のオブジェクトをバックアップするには、ユーザーはシステムレベルで REPOSITORY 権限を持ち、対応するテーブルまたは対応するデータベース内のすべてのテーブルに対して EXPORT 権限を持っている必要があります。例えば：

- 指定されたテーブルからデータをエクスポートするためのロール権限を付与します。

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE backup_tbl;
    GRANT EXPORT ON TABLE <table_name> TO ROLE backup_tbl;
    ```

- 指定されたデータのすべてのテーブルからデータをエクスポートするためのロール権限を付与します。

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE backup_db;
    GRANT EXPORT ON ALL TABLES IN DATABASE <database_name> TO ROLE backup_db;
    ```

- すべてのデータベース内のすべてのテーブルからデータをエクスポートするためのロール権限を付与します。

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE backup;
    GRANT EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE backup;
    ```

## 以前のバージョンとの互換性のある構文

```SQL
RESTORE SNAPSHOT <db_name>.<snapshot_name>
FROM <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
    [ AS <table_alias>] [, ...] ) ]
PROPERTIES ("key"="value", ...)
```

### パラメータ

| **パラメータ**   | **説明**                                              |
| --------------- | ------------------------------------------------------------ |
| db_name         | データがリストアされるデータベースの名前。           |
| snapshot_name   | データスナップショットの名前。                                  |
| repository_name | リポジトリ名。                                             |
| ON              | リストアするテーブルの名前。このパラメータが指定されていない場合、データベース全体がリストアされます。 |
| PARTITION       | リストアするパーティションの名前。このパラメータが指定されていない場合、テーブル全体がリストアされます。パーティション名は [SHOW PARTITIONS](../table_bucket_part_index/SHOW_PARTITIONS.md) を使用して表示できます。 |
| PROPERTIES      | RESTORE 操作のプロパティ。有効なキー:<ul><li>`backup_timestamp`: バックアップのタイムスタンプ。**必須**。バックアップのタイムスタンプは [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md) を使用して表示できます。</li><li>`replication_num`: リストアするレプリカの数を指定します。デフォルト: `3`。</li><li>`meta_version`: このパラメータは、以前のバージョンの StarRocks でバックアップされたデータをリストアするための一時的なソリューションとしてのみ使用されます。バックアップされたデータの最新バージョンにはすでに `meta version` が含まれており、指定する必要はありません。</li><li>`timeout`: タスクのタイムアウト。単位: 秒。デフォルト: `86400`。</li></ul> |

## v3.4.0 以降でサポートされる構文

```SQL
-- 外部カタログのメタデータをリストアします。
RESTORE SNAPSHOT [<db_name>.]<snapshot_name> FROM <repository_name>
{ ALL EXTERNAL CATALOGS | EXTERNAL CATALOG[S] <catalog_name>[, EXTERNAL CATALOG[S] <catalog_name> ...] [ AS <alias> ] }
[ DATABASE <db_name_in_snapshot> [AS <target_db>] ]
[ PROPERTIES ("key"="value" [, ...] ) ]

-- データベース、テーブル、パーティション、マテリアライズドビュー、ビュー、または UDF をリストアします。
RESTORE SNAPSHOT [<db_name>.]<snapshot_name> FROM <repository_name>
[ DATABASE <db_name_in_snapshot> [AS <target_db>] ]
[ ON ( restore_object [, ...] ) ]
[ PROPERTIES ("key"="value" [, ...] ) ]

restore_object ::= [
    { ALL TABLE[S]             | TABLE[S] <table_name>[, TABLE[S] <table_name> ...] [AS <alias>] } |
    { ALL MATERIALIZED VIEW[S] | MATERIALIZED VIEW[S] <mv_name>[, MATERIALIZED VIEW[S] <mv_name> ...] [AS <alias>] } |
    { ALL VIEW[S]              | VIEW[S] <view_name>[, VIEW[S] <view_name> ...] [AS <alias>] } |
    { ALL FUNCTION[S]          | FUNCTION[S] <udf_name>[, FUNCTION[S] <udf_name> ...] [AS <alias>] } |
     <table_name> PARTITION (<partition_name>[, ...]) [AS <alias>] ]
```

### パラメータ

| **パラメータ**   | **説明**                                              |
| --------------- | ------------------------------------------------------------ |
| db_name.        | オブジェクトまたはスナップショットがターゲットクラスターでリストアされるデータベースの名前。データベースが存在しない場合、システムが作成します。`AS <target_db>` または `<db_name>.` のいずれか一方のみを指定できます。 |
| snapshot_name   | データスナップショットの名前。                                   |
| repository_name | リポジトリ名。                                             |
| ALL EXTERNAL CATALOGS | すべての外部カタログのメタデータをリストアします。        |
| catalog_name    | リストアが必要な外部カタログの名前。    |
| DATABASE db_name_in_snapshot | オブジェクトまたはスナップショットがソースクラスターでバックアップされたときに属していたデータベースの名前。 |
| AS target_db    | オブジェクトまたはスナップショットがターゲットクラスターでリストアされるデータベースの名前。データベースが存在しない場合、システムが作成します。`AS <target_db>` または `<db_name>.` のいずれか一方のみを指定できます。 |
| ON              | リストアするオブジェクト。このパラメータが指定されていない場合、データベース全体がリストアされます。 |
| table_name      | リストアするテーブルの名前。                        |
| mv_name         | リストアするマテリアライズドビューの名前。            |
| view_name       | リストアするビューの名前。                 |
| udf_name        | リストアする UDF の名前。                          |
| PARTITION       | リストアするパーティションの名前。このパラメータが指定されていない場合、テーブル全体がリストアされます。 |
| AS alias        | ターゲットクラスターでリストアされるオブジェクトに新しい名前を設定します。 |
| PROPERTIES      | RESTORE 操作のプロパティ。有効なキー:<ul><li>`backup_timestamp`: バックアップのタイムスタンプ。**必須**。バックアップのタイムスタンプは [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md) を使用して表示できます。</li><li>`replication_num`: リストアするレプリカの数を指定します。デフォルト: `3`。</li><li>`meta_version`: このパラメータは、以前のバージョンの StarRocks でバックアップされたデータをリストアするための一時的なソリューションとしてのみ使用されます。バックアップされたデータの最新バージョンにはすでに `meta version` が含まれており、指定する必要はありません。</li><li>`timeout`: タスクのタイムアウト。単位: 秒。デフォルト: `86400`。</li></ul> |

## 例

### 以前のバージョンとの互換性のある構文を使用した例

例 1: `example_repo` リポジトリからスナップショット `snapshot_label1` のテーブル `backup_tbl` をデータベース `example_db` にリストアし、バックアップのタイムスタンプは `2018-05-04-16-45-08` です。1 つのレプリカをリストアします。

```SQL
RESTORE SNAPSHOT example_db.snapshot_label1
FROM example_repo
ON ( backup_tbl )
PROPERTIES
(
    "backup_timestamp"="2018-05-04-16-45-08",
    "replication_num" = "1"
);
```

例 2: `example_repo` から `snapshot_label2` のテーブル `backup_tbl` のパーティション `p1` と `p2` をデータベース `example_db` にリストアし、`backup_tbl2` を `new_tbl` にリネームします。バックアップのタイムスタンプは `2018-05-04-17-11-01` です。デフォルトで 3 つのレプリカをリストアします。

```SQL
RESTORE SNAPSHOT example_db.snapshot_label2
FROM example_repo
ON(
    backup_tbl PARTITION (p1, p2),
    backup_tbl2 AS new_tbl
)
PROPERTIES
(
    "backup_timestamp"="2018-05-04-17-11-01"
);
```

### v3.4.0 以降でサポートされる構文を使用した例

例 1: データベースをリストアします。

```SQL
-- 元の名前でデータベースをリストアします。
RESTORE SNAPSHOT sr_hub_backup
FROM test_repo
DATABASE sr_hub
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");

-- 新しい名前でデータベースをリストアします。
RESTORE SNAPSHOT sr_hub_backup
FROM test_repo
DATABASE sr_hub AS sr_hub_new
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");
```

例 2: データベース内のテーブルをリストアします。

```SQL
-- 元の名前でテーブルをリストアします。
RESTORE SNAPSHOT sr_member_backup
FROM test_repo 
DATABASE sr_hub 
ON (TABLE sr_member) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- 新しい名前でテーブルをリストアします。
RESTORE SNAPSHOT sr_member_backup
FROM test_repo 
DATABASE sr_hub  AS sr_hub_new
ON (TABLE sr_member AS sr_member_new) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- 複数のテーブルをリストアします。
RESTORE SNAPSHOT sr_core_backup
FROM test_repo 
DATABASE sr_hub
ON (TABLE sr_member, TABLE sr_pmc) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- すべてのテーブルをリストアします。
RESTORE SNAPSHOT sr_all_backup
FROM test_repo
DATABASE sr_hub
ON (ALL TABLES);

-- すべてのテーブルから 1 つをリストアします。
RESTORE SNAPSHOT sr_all_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

例 3: テーブル内のパーティションをリストアします。

```SQL
-- 1 つのパーティションをリストアします。
RESTORE SNAPSHOT sr_par_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member PARTITION (p1)) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- 複数のパーティションをリストアします。
RESTORE SNAPSHOT sr_par_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member PARTITION (p1,p2)) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

例 4: データベース内のマテリアライズドビューをリストアします。

```SQL
-- 1 つのマテリアライズドビューをリストアします。
RESTORE SNAPSHOT sr_mv1_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- 複数のマテリアライズドビューをリストアします。
RESTORE SNAPSHOT sr_mv2_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- すべてのマテリアライズドビューをリストアします。
RESTORE SNAPSHOT sr_mv3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL MATERIALIZED VIEWS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- すべてのマテリアライズドビューから 1 つをリストアします。
RESTORE SNAPSHOT sr_mv3_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

例 5: データベース内のビューをリストアします。

```SQL
-- 1 つのビューをリストアします。
RESTORE SNAPSHOT sr_view1_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- 複数のビューをリストアします。
RESTORE SNAPSHOT sr_view2_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1, VIEW sr_view2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- すべてのビューをリストアします。
RESTORE SNAPSHOT sr_view3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL VIEWS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- すべてのビューから 1 つをリストアします。
RESTORE SNAPSHOT sr_view3_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

例 6: データベース内の UDF をリストアします。

```SQL
-- 1 つの UDF をリストアします。
RESTORE SNAPSHOT sr_udf1_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- 複数の UDF をリストアします。
RESTORE SNAPSHOT sr_udf2_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1, FUNCTION sr_udf2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- すべての UDF をリストアします。
RESTORE SNAPSHOT sr_udf3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL FUNCTIONS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- すべての UDF から 1 つをリストアします。
RESTORE SNAPSHOT sr_udf3_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

## 使用上の注意

- グローバル、データベース、テーブル、およびパーティションレベルでバックアップとリストア操作を実行するには、異なる権限が必要です。
- 各データベースでは、同時に実行できるバックアップまたはリストアジョブは1つだけです。それ以外の場合、StarRocks はエラーを返します。
- バックアップとリストアジョブは、StarRocks クラスターの多くのリソースを占有するため、StarRocks クラスターが高負荷でないときにデータをバックアップおよびリストアすることができます。
- StarRocks は、データバックアップのためのデータ圧縮アルゴリズムの指定をサポートしていません。
- データはスナップショットとしてバックアップされるため、スナップショット生成時にロードされたデータはスナップショットに含まれません。したがって、スナップショットが生成された後、RESTORE ジョブが完了する前に古いクラスターにデータをロードした場合、データがリストアされるクラスターにもデータをロードする必要があります。データ移行が完了した後、データとサービスの正確性を確認した後にアプリケーションを新しいクラスターに移行することをお勧めします。
- RESTORE ジョブが完了する前に、リストアされるテーブルを操作することはできません。
- 主キーテーブルは、v2.5 より前の StarRocks クラスターにリストアすることはできません。
- 新しいクラスターでリストアする前に、リストアされるテーブルを作成する必要はありません。RESTORE ジョブが自動的に作成します。
- リストアされるテーブルと同じ名前の既存のテーブルがある場合、StarRocks はまず既存のテーブルのスキーマがリストアされるテーブルのスキーマと一致するかどうかを確認します。スキーマが一致する場合、StarRocks はスナップショット内のデータで既存のテーブルを上書きします。スキーマが一致しない場合、RESTORE ジョブは失敗します。`AS` キーワードを使用してリストアされるテーブルの名前を変更するか、データをリストアする前に既存のテーブルを削除することができます。
- RESTORE ジョブが既存のデータベース、テーブル、またはパーティションを上書きする場合、ジョブが COMMIT フェーズに入った後、上書きされたデータはリストアできません。この時点で RESTORE ジョブが失敗またはキャンセルされた場合、データが破損しアクセスできなくなる可能性があります。この場合、RESTORE 操作を再度実行し、ジョブが完了するのを待つしかありません。したがって、現在のデータがもはや使用されていないことを確認していない限り、上書きによるデータのリストアはお勧めしません。上書き操作はまずスナップショットと既存のデータベース、テーブル、またはパーティション間のメタデータの一貫性をチェックします。一貫性が検出されない場合、RESTORE 操作は実行できません。
- 現在、StarRocks はユーザーアカウント、権限、およびリソースグループに関連する設定データのバックアップとリストアをサポートしていません。
- 現在、StarRocks はテーブル間の Colocate Join 関係のバックアップとリストアをサポートしていません。