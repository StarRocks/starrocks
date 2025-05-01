---
displayed_sidebar: docs
---

# RESTORE

指定されたデータベース、テーブル、またはパーティションにデータを復元します。現在、StarRocks は OLAP テーブルへのデータ復元のみをサポートしています。

:::tip
バックアップと復元の概要については、 [backup and restore guide](../../../administration/management/Backup_and_restore.md) を参照してください。
:::

RESTORE は非同期操作です。 [SHOW RESTORE](./SHOW_RESTORE.md) を使用して RESTORE ジョブのステータスを確認するか、 [CANCEL RESTORE](./CANCEL_RESTORE.md) を使用して RESTORE ジョブをキャンセルできます。

> **注意**
>
> - ADMIN 権限を持つユーザーのみがデータを復元できます。
> - 各データベースでは、同時に実行中の BACKUP または RESTORE ジョブは 1 つだけ許可されます。それ以外の場合、StarRocks はエラーを返します。

## 構文

```SQL
RESTORE SNAPSHOT <db_name>.<snapshot_name>
FROM <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
    [ AS <table_alias>] [, ...] ) ]
PROPERTIES ("key"="value", ...)
```

## パラメータ

| **パラメータ**   | **説明**                                                      |
| --------------- | ------------------------------------------------------------ |
| db_name         | データが復元されるデータベースの名前。                         |
| snapshot_name   | データスナップショットの名前。                                 |
| repository_name | リポジトリ名。                                                |
| ON              | 復元するテーブルの名前。このパラメータが指定されていない場合、データベース全体が復元されます。 |
| PARTITION       | 復元するパーティションの名前。このパラメータが指定されていない場合、テーブル全体が復元されます。パーティション名は [SHOW PARTITIONS](../table_bucket_part_index/SHOW_PARTITIONS.md) を使用して確認できます。 |
| PROPERTIES      | RESTORE 操作のプロパティ。 有効なキー:<ul><li>`backup_timestamp`: バックアップのタイムスタンプ。 **必須**。バックアップのタイムスタンプは [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md) を使用して確認できます。</li><li>`replication_num`: 復元するレプリカの数を指定します。デフォルト: `3`。</li><li>`meta_version`: このパラメータは、以前のバージョンの StarRocks でバックアップされたデータを復元するための一時的な解決策としてのみ使用されます。バックアップされたデータの最新バージョンにはすでに `meta version` が含まれているため、指定する必要はありません。</li><li>`timeout`: タスクのタイムアウト。単位: 秒。デフォルト: `86400`。</li></ul> |

## 例

例 1: `example_repo` リポジトリから `snapshot_label1` スナップショット内のテーブル `backup_tbl` をデータベース `example_db` に復元し、バックアップのタイムスタンプは `2018-05-04-16-45-08` です。1 つのレプリカを復元します。

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

例 2: `example_repo` からデータベース `example_db` に `snapshot_label2` 内のテーブル `backup_tbl` のパーティション `p1` と `p2` およびテーブル `backup_tbl2` を復元し、`backup_tbl2` を `new_tbl` にリネームします。バックアップのタイムスタンプは `2018-05-04-17-11-01` です。デフォルトで 3 つのレプリカを復元します。

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

- グローバル、データベース、テーブル、パーティションレベルでのバックアップおよび復元操作には異なる権限が必要です。
- 各データベースでは、同時に実行中の BACKUP または RESTORE ジョブは 1 つだけ許可されます。それ以外の場合、StarRocks はエラーを返します。
- BACKUP および RESTORE ジョブは StarRocks クラスターの多くのリソースを占有するため、StarRocks クラスターが高負荷でないときにデータをバックアップおよび復元することができます。
- StarRocks はデータバックアップのためのデータ圧縮アルゴリズムの指定をサポートしていません。
- データはスナップショットとしてバックアップされるため、スナップショット生成時にロードされたデータはスナップショットに含まれません。したがって、スナップショットが生成された後、RESTORE ジョブが完了する前に古いクラスターにデータをロードした場合、データを復元するクラスターにもデータをロードする必要があります。データ移行が完了した後、データとサービスの正確性を確認した後、新しいクラスターにアプリケーションを移行するまで、両方のクラスターに並行してデータをロードすることをお勧めします。
- RESTORE ジョブが完了する前に、復元するテーブルを操作することはできません。
- 主キーテーブルは v2.5 より前の StarRocks クラスターに復元することはできません。
- 新しいクラスターで復元するテーブルを事前に作成する必要はありません。RESTORE ジョブが自動的に作成します。
- 復元するテーブルと同じ名前の既存のテーブルがある場合、StarRocks はまず既存のテーブルのスキーマが復元するテーブルのスキーマと一致するかどうかを確認します。スキーマが一致する場合、StarRocks はスナップショット内のデータで既存のテーブルを上書きします。スキーマが一致しない場合、RESTORE ジョブは失敗します。キーワード `AS` を使用して復元するテーブルの名前を変更するか、データを復元する前に既存のテーブルを削除することができます。
- RESTORE ジョブが既存のデータベース、テーブル、またはパーティションを上書きする場合、ジョブが COMMIT フェーズに入った後、上書きされたデータは復元できません。この時点で RESTORE ジョブが失敗したりキャンセルされたりすると、データが破損してアクセスできなくなる可能性があります。この場合、RESTORE 操作を再度実行し、ジョブが完了するのを待つしかありません。したがって、現在のデータがもはや使用されていないことが確実でない限り、上書きによるデータの復元はお勧めしません。上書き操作はまずスナップショットと既存のデータベース、テーブル、またはパーティション間のメタデータの一貫性をチェックします。不一致が検出された場合、RESTORE 操作は実行できません。
- 現在、StarRocks は論理ビューのバックアップおよび復元をサポートしていません。
- 現在、StarRocks はユーザーアカウント、権限、およびリソースグループに関連する設定データのバックアップおよび復元をサポートしていません。
- 現在、StarRocks はテーブル間の Colocate Join 関係のバックアップおよび復元をサポートしていません。