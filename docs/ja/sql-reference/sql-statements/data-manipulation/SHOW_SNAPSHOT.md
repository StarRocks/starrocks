---
displayed_sidebar: docs
---

# SHOW SNAPSHOT

## 説明

指定されたリポジトリ内のデータスナップショットを表示します。詳細については、 [data backup and restoration](../../../administration/Backup_and_restore.md) を参照してください。

## 構文

```SQL
SHOW SNAPSHOT ON <repo_name>
[WHERE SNAPSHOT = <snapshot_name> [AND TIMESTAMP = <backup_timestamp>]]
```

## パラメータ

| **パラメータ**    | **説明**                                      |
| ---------------- | ---------------------------------------------------- |
| repo_name        | スナップショットが属するリポジトリの名前。 |
| snapshot_name    | スナップショットの名前。                                |
| backup_timestamp | スナップショットのバックアップタイムスタンプ。                    |

## 戻り値

| **戻り値** | **説明**                                              |
| ---------- | ------------------------------------------------------------ |
| Snapshot   | スナップショットの名前。                                        |
| Timestamp  | スナップショットのバックアップタイムスタンプ。                            |
| Status     | スナップショットが正常な場合は `OK` を表示します。正常でない場合はエラーメッセージを表示します。 |
| Database   | スナップショットが属するデータベースの名前。           |
| Details    | スナップショットの JSON 形式のディレクトリと構造。      |

## 例

例 1: リポジトリ `example_repo` 内のスナップショットを表示します。

```SQL
SHOW SNAPSHOT ON example_repo;
```

例 2: リポジトリ `example_repo` 内の `backup1` という名前のスナップショットを表示します。

```SQL
SHOW SNAPSHOT ON example_repo
WHERE SNAPSHOT = "backup1";
```

例 3: リポジトリ `example_repo` 内の `backup1` という名前でバックアップタイムスタンプ `2018-05-05-15-34-26` のスナップショットを表示します。

```SQL
SHOW SNAPSHOT ON example_repo 
WHERE SNAPSHOT = "backup1" AND TIMESTAMP = "2018-05-05-15-34-26";
```