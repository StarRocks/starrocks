---
displayed_sidebar: docs
---

# SHOW SNAPSHOT

指定されたリポジトリ内のデータスナップショットを表示します。詳細については、 [data backup and restoration](../../../administration/management/Backup_and_restore.md) を参照してください。

## Syntax

```SQL
SHOW SNAPSHOT ON <repo_name>
[WHERE SNAPSHOT = <snapshot_name> [AND TIMESTAMP = <backup_timestamp>]]
```

## Parameters

| **Parameter**    | **Description**                                      |
| ---------------- | ---------------------------------------------------- |
| repo_name        | スナップショットが属するリポジトリの名前。            |
| snapshot_nam     | スナップショットの名前。                              |
| backup_timestamp | スナップショットのバックアップタイムスタンプ。        |

## Return

| **Return** | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| Snapshot   | スナップショットの名前。                                      |
| Timestamp  | スナップショットのバックアップタイムスタンプ。                |
| Status     | スナップショットが正常な場合は `OK` を表示します。正常でない場合はエラーメッセージを表示します。 |
| Database   | スナップショットが属するデータベースの名前。                  |
| Details    | スナップショットのディレクトリと構造を JSON 形式で表示します。|

## Example

例 1: リポジトリ `example_repo` 内のスナップショットを表示します。

```SQL
SHOW SNAPSHOT ON example_repo;
```

例 2: リポジトリ `example_repo` 内のスナップショット `backup1` を表示します。

```SQL
SHOW SNAPSHOT ON example_repo
WHERE SNAPSHOT = "backup1";
```

例 3: リポジトリ `example_repo` 内のバックアップタイムスタンプ `2018-05-05-15-34-26` を持つスナップショット `backup1` を表示します。

```SQL
SHOW SNAPSHOT ON example_repo 
WHERE SNAPSHOT = "backup1" AND TIMESTAMP = "2018-05-05-15-34-26";
```