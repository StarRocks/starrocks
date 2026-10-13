---
displayed_sidebar: docs
description: "データスナップショットとそのデータファイルをリポジトリから削除します。"
---

# DROP SNAPSHOT

データスナップショットをリポジトリから削除し、あわせてリモートストレージシステム上のデータファイルも削除します。StarRocks 内のマッピングのみを削除する [DROP REPOSITORY](./DROP_REPOSITORY.md) とは異なり、この文はバックアップされたデータそのものを削除します。

削除は取り消せません。スナップショットを削除すると、[RESTORE](./RESTORE.md) で復元することはできなくなります。

この文は v4.2.0 以降でサポートされます。

:::caution

- BACKUP ジョブが書き込み中、または RESTORE ジョブが読み取り中のスナップショットは削除できません。
- スナップショットが属するリポジトリが読み取り専用の場合は削除できません。

:::

## 権限要件

System レベルの REPOSITORY 権限が必要です。

```SQL
GRANT REPOSITORY ON SYSTEM TO ROLE <role_name>;
```

## 構文

```SQL
DROP SNAPSHOT <snapshot_name> ON <repository_name> [FORCE]
```

## パラメータ

| **パラメータ**  | **説明**                                                     |
| --------------- | ------------------------------------------------------------ |
| snapshot_name   | 削除するスナップショットの名前。                             |
| repository_name | スナップショットが属するリポジトリの名前。                   |
| FORCE           | 作成元クラスタの確認を行わずに削除します。下記を参照してください。 |

## スナップショットの所有者

各スナップショットには、それを作成したクラスタの ID が記録されています。`FORCE` を指定しない場合、StarRocks はその ID が現在のクラスタと一致するときにのみ削除します。現在のクラスタの ID は、[SHOW SNAPSHOT](./SHOW_SNAPSHOT.md) の `ClusterId` 列および [SHOW FRONTENDS](../cluster-management/nodes_processes/SHOW_FRONTENDS.md) の `ClusterId` 列で確認できます。

次の場合、この文はエラーになります。

- 同じリポジトリを共有する別のクラスタが作成したスナップショットである
- v4.2.0 より前に作成されたため、クラスタ ID が記録されていない
- スナップショットのメタデータを読み取れない。バックアップの実行中、およびバックアップが中断された後の残骸がこれにあたる

これらのスナップショットを削除するには `FORCE` を使用します。本クラスタが作成していないスナップショットの削除は、他のクラスタがまだ依存している可能性のあるデータを消すことになるため、削除してよいデータであることを事前に確認してください。

## 例

例 1: リポジトリ `example_repo` からスナップショット `backup1` を削除します。

```SQL
DROP SNAPSHOT backup1 ON example_repo;
```

例 2: リポジトリ `example_repo` から、本クラスタが作成したものではないスナップショット `legacy_backup` を削除します。

```SQL
DROP SNAPSHOT legacy_backup ON example_repo FORCE;
```

## 関連ドキュメント

- [BACKUP](./BACKUP.md)
- [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md)
- [RESTORE](./RESTORE.md)
