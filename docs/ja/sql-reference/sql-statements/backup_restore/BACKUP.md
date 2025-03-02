---
displayed_sidebar: docs
---

# BACKUP

StarRocks は、次のオブジェクトのバックアップと復元をサポートしています。

- 内部データベース、テーブル（すべてのタイプとパーティショニング戦略）、およびパーティション
- 外部カタログのメタデータ（v3.4.0以降でサポート）
- 同期マテリアライズドビューと非同期マテリアライズドビュー
- ビュー（v3.4.0以降でサポート）
- ユーザー定義関数（v3.4.0以降でサポート）

:::tip
バックアップと復元の概要については、[バックアップと復元ガイド](../../../administration/management/Backup_and_restore.md)を参照してください。
:::

BACKUP は非同期操作です。[SHOW BACKUP](./SHOW_BACKUP.md)を使用して BACKUP ジョブのステータスを確認したり、[CANCEL BACKUP](./CANCEL_BACKUP.md)を使用して BACKUP ジョブをキャンセルしたりできます。[SHOW SNAPSHOT](./SHOW_SNAPSHOT.md)を使用してスナップショット情報を表示できます。

> **注意**
>
> - 共有データ StarRocks クラスターはデータの BACKUP および RESTORE をサポートしていません。
> - 各データベースでは、同時に実行できる BACKUP または RESTORE ジョブは1つだけです。それ以外の場合、StarRocks はエラーを返します。
> - StarRocks はデータバックアップのためのデータ圧縮アルゴリズムの指定をサポートしていません。

## 権限要件

v3.0 より前のバージョンでは、`admin_priv` 権限を持つユーザーがこの操作を実行できます。v3.0 以降のバージョンでは、特定のオブジェクトをバックアップするには、ユーザーはシステムレベルで REPOSITORY 権限を持ち、対応するテーブルまたは対応するデータベース内のすべてのテーブルに対して EXPORT 権限を持つ必要があります。例：

- 指定されたテーブルからデータをエクスポートするためのロール権限を付与します。

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE backup_tbl;
    GRANT EXPORT ON TABLE <table_name> TO ROLE backup_tbl;
    ```

- 指定されたデータ内のすべてのテーブルからデータをエクスポートするためのロール権限を付与します。

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
BACKUP SNAPSHOT <db_name>.<snapshot_name>
TO <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
       [, ...] ) ]
[ PROPERTIES ("key"="value" [, ...] ) ]
```

### パラメータ

| **パラメータ**   | **説明**                                              |
| --------------- | ------------------------------------------------------------ |
| db_name         | バックアップするデータを格納するデータベースの名前。   |
| snapshot_name   | データスナップショットの名前を指定します。グローバルに一意です。       |
| repository_name | リポジトリ名。リポジトリは [CREATE REPOSITORY](./CREATE_REPOSITORY.md) を使用して作成できます。 |
| ON              | バックアップするテーブルの名前。このパラメータが指定されていない場合、データベース全体がバックアップされます。 |
| PARTITION       | バックアップするパーティションの名前。このパラメータが指定されていない場合、テーブル全体がバックアップされます。 |
| PROPERTIES      | データスナップショットのプロパティ。有効なキー:<ul><li>`type`: バックアップタイプ。現在、完全バックアップ `FULL` のみがサポートされています。デフォルト: `FULL`。</li><li>`timeout`: タスクのタイムアウト。単位: 秒。デフォルト: `86400`。</li></ul> |

## v3.4.0 以降でサポートされる構文

```SQL
-- 外部カタログメタデータをバックアップします。
BACKUP { ALL EXTERNAL CATALOGS | EXTERNAL CATALOG[S] (<catalog_name> [, ...]) }
[ DATABASE <db_name> ] SNAPSHOT [<db_name>.]<snapshot_name>
TO <repository_name>
[ PROPERTIES ("key"="value" [, ...] ) ]

-- データベース、テーブル、パーティション、マテリアライズドビュー、ビュー、または UDF をバックアップします。

BACKUP [ DATABASE <db_name> ] SNAPSHOT [<db_name>.]<snapshot_name>
TO <repository_name>
[ ON ( backup_object [, ...] )] 
[ PROPERTIES ("key"="value" [, ...] ) ]

backup_object ::= [
    { ALL TABLE[S]             | TABLE[S] <table_name>[, TABLE[S] <table_name> ...] } |
    { ALL MATERIALIZED VIEW[S] | MATERIALIZED VIEW[S] <mv_name>[, MATERIALIZED VIEW[S] <mv_name> ...] } |
    { ALL VIEW[S]              | VIEW[S] <view_name>[, VIEW[S] <view_name> ...] } |
    { ALL FUNCTION[S]          | FUNCTION[S] <udf_name>[, FUNCTION[S] <udf_name> ...] } |
     <table_name> PARTITION (<partition_name>[, ...]) ]
```

### パラメータ

| **パラメータ**   | **説明**                                              |
| --------------- | ------------------------------------------------------------ |
| ALL EXTERNAL CATALOGS | すべての外部カタログのメタデータをバックアップします。        |
| catalog_name    | バックアップが必要な外部カタログの名前。   |
| DATABASE db_name | オブジェクトまたはスナップショットが属するデータベースの名前。`DATABASE <db_name>` または `<db_name>.` のいずれか一方のみを指定できます。 |
| db_name.        | オブジェクトまたはスナップショットが属するデータベースの名前。`DATABASE <db_name>` または `<db_name>.` のいずれか一方のみを指定できます。 |
| snapshot_name   | データスナップショットの名前。グローバルに一意です。                  |
| repository_name | リポジトリ名。リポジトリは [CREATE REPOSITORY](./CREATE_REPOSITORY.md) を使用して作成できます。 |
| ON              | バックアップするオブジェクト。このパラメータが指定されていない場合、データベース全体がバックアップされます。 |
| table_name      | バックアップするテーブルの名前。                        |
| mv_name         | バックアップするマテリアライズドビューの名前。            |
| view_name       | バックアップするビューの名前。                 |
| udf_name        | バックアップする UDF の名前。                          |
| PARTITION       | バックアップするパーティションの名前。このパラメータが指定されていない場合、テーブル全体がバックアップされます。 |
| PROPERTIES      | データスナップショットのプロパティ。有効なキー:<ul><li>`type`: バックアップタイプ。現在、完全バックアップ `FULL` のみがサポートされています。デフォルト: `FULL`。</li><li>`timeout`: タスクのタイムアウト。単位: 秒。デフォルト: `86400`。</li></ul> |

## 例

### 以前のバージョンとの互換性のある構文を使用した例

例 1: データベース `example_db` をリポジトリ `example_repo` にバックアップします。

```SQL
BACKUP SNAPSHOT example_db.snapshot_label1
TO example_repo
PROPERTIES ("type" = "full");
```

例 2: `example_db` 内のテーブル `example_tbl` を `example_repo` にバックアップします。

```SQL
BACKUP SNAPSHOT example_db.snapshot_label2
TO example_repo
ON (example_tbl);
```

例 3: `example_db` 内の `example_tbl` のパーティション `p1` と `p2`、およびテーブル `example_tbl2` を `example_repo` にバックアップします。

```SQL
BACKUP SNAPSHOT example_db.snapshot_label3
TO example_repo
ON(
    example_tbl PARTITION (p1, p2),
    example_tbl2
);
```

### v3.4.0 以降でサポートされる構文を使用した例

例 1: データベースをバックアップします。

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_hub_backup TO test_repo;
```

例 2: データベース内のテーブルをバックアップします。

```SQL
-- 1つのテーブルをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_member_backup
TO test_repo
ON (TABLE sr_member);

-- 複数のテーブルをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_core_backup
TO test_repo
ON (TABLE sr_member, TABLE sr_pmc);

-- すべてのテーブルをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_all_backup
TO test_repo
ON (ALL TABLES);
```

例 3: テーブル内のパーティションをバックアップします。

```SQL
-- 1つのパーティションをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_par_backup
TO test_repo
ON (TABLE sr_member PARTITION (p1));

-- 複数のパーティションをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_par_backup
TO test_repo
ON (TABLE sr_member PARTITION (p1,p2,p3));
```

例 4: データベース内のマテリアライズドビューをバックアップします。

```SQL
-- 1つのマテリアライズドビューをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_mv1_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1);

-- 複数のマテリアライズドビューをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_mv2_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2);

-- すべてのマテリアライズドビューをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_mv3_backup
TO test_repo
ON (ALL MATERIALIZED VIEWS);
```

例 5: データベース内のビューをバックアップします。

```SQL
-- 1つのビューをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_view1_backup
TO test_repo
ON (VIEW sr_view1);

-- 複数のビューをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_view2_backup
TO test_repo
ON (VIEW sr_view1, VIEW sr_view2);

-- すべてのビューをバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_view3_backup
TO test_repo
ON (ALL VIEWS);
```

例 6: データベース内の UDF をバックアップします。

```SQL
-- 1つの UDF をバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_udf1_backup
TO test_repo
ON (FUNCTION sr_udf1);

-- 複数の UDF をバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_udf2_backup
TO test_repo
ON (FUNCTION sr_udf1, FUNCTION sr_udf2);

-- すべての UDF をバックアップします。
BACKUP DATABASE sr_hub SNAPSHOT sr_udf3_backup
TO test_repo
ON (ALL FUNCTIONS);
```

## 使用上の注意

- グローバル、データベース、テーブル、およびパーティションレベルでのバックアップおよび復元操作には、異なる権限が必要です。
- 各データベースでは、同時に実行できる BACKUP または RESTORE ジョブは1つだけです。それ以外の場合、StarRocks はエラーを返します。
- BACKUP および RESTORE ジョブは、StarRocks クラスターの多くのリソースを占有するため、StarRocks クラスターが高負荷でないときにデータをバックアップおよび復元することができます。
- StarRocks はデータバックアップのためのデータ圧縮アルゴリズムの指定をサポートしていません。
- データはスナップショットとしてバックアップされるため、スナップショット生成時にロードされたデータはスナップショットに含まれません。したがって、スナップショット生成後、RESTORE ジョブが完了する前に古いクラスターにデータをロードした場合、データを復元するクラスターにもデータをロードする必要があります。データ移行が完了した後、データとサービスの正確性を確認した後、新しいクラスターにアプリケーションを移行することをお勧めします。
- RESTORE ジョブが完了する前に、復元するテーブルを操作することはできません。
- 主キーテーブルは v2.5 より前の StarRocks クラスターに復元することはできません。
- 新しいクラスターで復元する前に、復元するテーブルを作成する必要はありません。RESTORE ジョブが自動的に作成します。
- 復元するテーブルと同じ名前の既存のテーブルがある場合、StarRocks はまず既存のテーブルのスキーマが復元するテーブルのスキーマと一致するかどうかを確認します。スキーマが一致する場合、StarRocks はスナップショット内のデータで既存のテーブルを上書きします。スキーマが一致しない場合、RESTORE ジョブは失敗します。`AS` キーワードを使用して復元するテーブルの名前を変更するか、データを復元する前に既存のテーブルを削除できます。
- RESTORE ジョブが既存のデータベース、テーブル、またはパーティションを上書きする場合、ジョブが COMMIT フェーズに入った後、上書きされたデータは復元できません。RESTORE ジョブがこの時点で失敗またはキャンセルされた場合、データが破損してアクセスできなくなる可能性があります。この場合、再度 RESTORE 操作を実行し、ジョブが完了するのを待つしかありません。したがって、現在のデータがもう使用されていないと確信している場合を除き、上書きによるデータの復元はお勧めしません。上書き操作は、スナップショットと既存のデータベース、テーブル、またはパーティション間のメタデータの一貫性を最初にチェックします。一貫性が検出されない場合、RESTORE 操作は実行できません。
- 現在、StarRocks はユーザーアカウント、権限、およびリソースグループに関連する構成データのバックアップと復元をサポートしていません。
- 現在、StarRocks はテーブル間の Colocate Join 関係のバックアップと復元をサポートしていません。