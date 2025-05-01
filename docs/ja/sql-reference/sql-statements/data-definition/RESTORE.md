---
displayed_sidebar: docs
---

# RESTORE

## 説明

指定されたデータベース、テーブル、またはパーティションにデータを復元します。現在、StarRocks は OLAP テーブルへのデータ復元のみをサポートしています。詳細は [データのバックアップと復元](../../../administration/Backup_and_restore.md) を参照してください。

RESTORE は非同期操作です。 [SHOW RESTORE](../data-manipulation/SHOW_RESTORE.md) を使用して RESTORE ジョブのステータスを確認したり、 [CANCEL RESTORE](../data-definition/CANCEL_RESTORE.md) を使用して RESTORE ジョブをキャンセルしたりできます。

> **注意**
>
> - ADMIN 権限を持つユーザーのみがデータを復元できます。
> - 各データベースでは、同時に実行中の BACKUP または RESTORE ジョブは 1 つのみ許可されます。それ以外の場合、StarRocks はエラーを返します。

## 構文

```SQL
RESTORE SNAPSHOT <db_name>.<snapshot_name>
FROM <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
    [ AS <table_alias>] [, ...] ) ]
PROPERTIES ("key"="value", ...)
```

## パラメータ

| **パラメータ**  | **説明**                                                     |
| --------------- | ------------------------------------------------------------ |
| db_name         | データが復元されるデータベースの名前。                       |
| snapshot_name   | データスナップショットの名前。                               |
| repository_name | リポジトリ名。                                               |
| ON              | 復元するテーブルの名前。このパラメータが指定されていない場合、データベース全体が復元されます。 |
| PARTITION       | 復元するパーティションの名前。このパラメータが指定されていない場合、テーブル全体が復元されます。パーティション名は [SHOW PARTITIONS](../data-manipulation/SHOW_PARTITIONS.md) を使用して確認できます。 |
| PROPERTIES      | RESTORE 操作のプロパティ。有効なキー:<ul><li>`backup_timestamp`: バックアップのタイムスタンプ。**必須**。バックアップのタイムスタンプは [SHOW SNAPSHOT](../data-manipulation/SHOW_SNAPSHOT.md) を使用して確認できます。</li><li>`replication_num`: 復元するレプリカの数を指定します。デフォルト: `3`。</li><li>`meta_version`: このパラメータは、以前のバージョンの StarRocks でバックアップされたデータを復元するための一時的な解決策としてのみ使用されます。バックアップされたデータの最新バージョンにはすでに `meta version` が含まれており、指定する必要はありません。</li><li>`timeout`: タスクのタイムアウト。単位: 秒。デフォルト: `86400`。</li></ul> |

## 例

例 1: `example_repo` リポジトリから `snapshot_label1` スナップショットのテーブル `backup_tbl` をデータベース `example_db` に復元し、バックアップのタイムスタンプは `2018-05-04-16-45-08` です。1 つのレプリカを復元します。

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

例 2: `example_repo` から `snapshot_label2` のテーブル `backup_tbl` のパーティション `p1` と `p2`、およびテーブル `backup_tbl2` をデータベース `example_db` に復元し、`backup_tbl2` を `new_tbl` にリネームします。バックアップのタイムスタンプは `2018-05-04-17-11-01` です。デフォルトで 3 つのレプリカを復元します。

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

## 使用上の注意

- グローバル、データベース、テーブル、およびパーティションレベルでのバックアップと復元操作には異なる権限が必要です。
- 各データベースでは、同時に実行中の BACKUP または RESTORE ジョブは 1 つのみ許可されます。それ以外の場合、StarRocks はエラーを返します。
- BACKUP および RESTORE ジョブは StarRocks クラスターの多くのリソースを占有するため、StarRocks クラスターが高負荷でないときにデータのバックアップと復元を行うことができます。
- StarRocks はデータバックアップのためのデータ圧縮アルゴリズムの指定をサポートしていません。
- データはスナップショットとしてバックアップされるため、スナップショット生成時にロードされたデータはスナップショットに含まれません。したがって、スナップショット生成後、RESTORE ジョブが完了する前に古いクラスターにデータをロードした場合、復元先のクラスターにもデータをロードする必要があります。データ移行が完了した後、データとサービスの正確性を確認した後、新しいクラスターにアプリケーションを移行するまでの間、両方のクラスターに並行してデータをロードすることをお勧めします。
- RESTORE ジョブが完了する前に、復元対象のテーブルを操作することはできません。
- 主キーテーブルは v2.5 より前の StarRocks クラスターに復元することはできません。
- 新しいクラスターで復元する前に、復元対象のテーブルを作成する必要はありません。RESTORE ジョブが自動的に作成します。
- 復元対象のテーブルと重複した名前の既存のテーブルがある場合、StarRocks はまず既存のテーブルのスキーマが復元対象のテーブルのスキーマと一致するかどうかを確認します。スキーマが一致する場合、StarRocks はスナップショット内のデータで既存のテーブルを上書きします。スキーマが一致しない場合、RESTORE ジョブは失敗します。`AS` キーワードを使用して復元対象のテーブルの名前を変更するか、データを復元する前に既存のテーブルを削除することができます。
- RESTORE ジョブが既存のデータベース、テーブル、またはパーティションを上書きする場合、ジョブが COMMIT フェーズに入った後、上書きされたデータは復元できません。この時点で RESTORE ジョブが失敗またはキャンセルされた場合、データが破損しアクセスできなくなる可能性があります。この場合、RESTORE 操作を再度実行し、ジョブが完了するのを待つしかありません。したがって、現在のデータがもはや使用されていないことを確信していない限り、上書きによるデータの復元はお勧めしません。上書き操作は、スナップショットと既存のデータベース、テーブル、またはパーティション間のメタデータの一貫性を最初に確認します。一貫性が検出されない場合、RESTORE 操作は実行できません。
- 現在、StarRocks は論理ビューのバックアップと復元をサポートしていません。
- 現在、StarRocks はユーザーアカウント、権限、およびリソースグループに関連する設定データのバックアップと復元をサポートしていません。
- 現在、StarRocks はテーブル間の Colocate Join 関係のバックアップと復元をサポートしていません。