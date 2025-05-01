---
displayed_sidebar: docs
---

# BACKUP

## Description

指定されたデータベース、テーブル、またはパーティションのデータをバックアップします。現在、StarRocks は OLAP テーブルのデータのみをバックアップすることをサポートしています。詳細については、 [data backup and restoration](../../../administration/management/Backup_and_restore.md) を参照してください。

BACKUP は非同期操作です。 [SHOW BACKUP](./SHOW_BACKUP.md) を使用して BACKUP ジョブのステータスを確認したり、 [CANCEL BACKUP](./CANCEL_BACKUP.md) を使用して BACKUP ジョブをキャンセルしたりできます。 [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md) を使用してスナップショット情報を表示できます。

> **注意**
>
> - ADMIN 権限を持つユーザーのみがデータをバックアップできます。
> - 各データベースでは、同時に 1 つの BACKUP または RESTORE ジョブのみが許可されます。それ以外の場合、StarRocks はエラーを返します。
> - StarRocks はデータバックアップのためのデータ圧縮アルゴリズムの指定をサポートしていません。

## Syntax

```SQL
BACKUP SNAPSHOT <db_name>.<snapshot_name>
TO <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
       [, ...] ) ]
[ PROPERTIES ("key"="value" [, ...] ) ]
```

## Parameters

| **Parameter**   | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| db_name         | バックアップするデータを格納するデータベースの名前。         |
| snapshot_name   | データスナップショットの名前を指定します。グローバルに一意です。 |
| repository_name | リポジトリ名。 [CREATE REPOSITORY](./CREATE_REPOSITORY.md) を使用してリポジトリを作成できます。 |
| ON              | バックアップするテーブルの名前。このパラメータが指定されていない場合、データベース全体がバックアップされます。 |
| PARTITION       | バックアップするパーティションの名前。このパラメータが指定されていない場合、テーブル全体がバックアップされます。 |
| PROPERTIES      | データスナップショットのプロパティ。有効なキー:`type`: バックアップタイプ。現在、フルバックアップ `FULL` のみがサポートされています。デフォルト: `FULL`。`timeout`: タスクのタイムアウト。単位: 秒。デフォルト: `86400`。 |

## Examples

Example 1: データベース `example_db` をリポジトリ `example_repo` にバックアップします。

```SQL
BACKUP SNAPSHOT example_db.snapshot_label1
TO example_repo
PROPERTIES ("type" = "full");
```

Example 2: `example_db` 内のテーブル `example_tbl` を `example_repo` にバックアップします。

```SQL
BACKUP SNAPSHOT example_db.snapshot_label2
TO example_repo
ON (example_tbl);
```

Example 3: `example_db` 内の `example_tbl` のパーティション `p1` と `p2`、およびテーブル `example_tbl2` を `example_repo` にバックアップします。

```SQL
BACKUP SNAPSHOT example_db.snapshot_label3
TO example_repo
ON(
    example_tbl PARTITION (p1, p2),
    example_tbl2
);
```

## Usage notes

- グローバル、データベース、テーブル、パーティションレベルでバックアップおよびリストア操作を行うには、異なる権限が必要です。
- 各データベースでは、同時に 1 つの BACKUP または RESTORE ジョブのみが許可されます。それ以外の場合、StarRocks はエラーを返します。
- BACKUP および RESTORE ジョブは StarRocks クラスターの多くのリソースを占有するため、StarRocks クラスターが高負荷でないときにデータをバックアップおよびリストアすることができます。
- StarRocks はデータバックアップのためのデータ圧縮アルゴリズムの指定をサポートしていません。
- データはスナップショットとしてバックアップされるため、スナップショット生成時にロードされたデータはスナップショットに含まれません。したがって、スナップショットが生成された後、RESTORE ジョブが完了する前に古いクラスターにデータをロードした場合、データがリストアされるクラスターにもデータをロードする必要があります。データ移行が完了した後、データとサービスの正確性を確認した上で、新しいクラスターにアプリケーションを移行するまで、両方のクラスターに並行してデータをロードすることをお勧めします。
- RESTORE ジョブが完了する前に、リストアされるテーブルを操作することはできません。
- 主キーテーブルは v2.5 より前の StarRocks クラスターにリストアすることはできません。
- リストアするテーブルを新しいクラスターにリストアする前に作成する必要はありません。RESTORE ジョブが自動的に作成します。
- リストアされるテーブルと同じ名前の既存のテーブルがある場合、StarRocks はまず既存のテーブルのスキーマがリストアされるテーブルのスキーマと一致するかどうかを確認します。スキーマが一致する場合、StarRocks はスナップショット内のデータで既存のテーブルを上書きします。スキーマが一致しない場合、RESTORE ジョブは失敗します。`AS` キーワードを使用してリストアされるテーブルの名前を変更するか、データをリストアする前に既存のテーブルを削除することができます。
- RESTORE ジョブが既存のデータベース、テーブル、またはパーティションを上書きする場合、ジョブが COMMIT フェーズに入った後、上書きされたデータはリストアできません。この時点で RESTORE ジョブが失敗またはキャンセルされた場合、データが破損しアクセスできなくなる可能性があります。この場合、RESTORE 操作を再度実行し、ジョブが完了するのを待つしかありません。したがって、現在のデータがもはや使用されていないことを確信していない限り、上書きによるデータのリストアはお勧めしません。上書き操作はまずスナップショットと既存のデータベース、テーブル、またはパーティション間のメタデータの一貫性を確認します。一貫性が検出されない場合、RESTORE 操作は実行できません。
- 現在、StarRocks はビューのバックアップおよびリストアをサポートしていません。
- 現在、StarRocks はユーザーアカウント、権限、およびリソースグループに関連する設定データのバックアップおよびリストアをサポートしていません。
- 現在、StarRocks はテーブル間の Colocate Join 関係のバックアップおよびリストアをサポートしていません。